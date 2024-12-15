import datetime
import time
import os
import json
import pandas as pd
import numpy as np
import re
import yaml
import time
import joblib
import pickle
from datetime import datetime
from PreProcess import PreProcess
from config import _features_modelling, _features_aggregation


def lc_scoring(params):
    scoring_date = params["scoring_date"]
    input_file = params["input_file"]
    output_path = params["output_path"]
    _model_local = params["model_local"]

    features_modelling = _features_modelling
    features_aggregation = _features_aggregation

    ### Reading File
    df = pd.read_csv(input_file)
    df_main = df.copy()
    df = df[features_aggregation]
    df["detail_page_view_setting_ld"] = 0
    df["BUR_view_ld"] = 0
    df["BUR_view_preset"] = 0
    df["catalog_page_view_diamond_engagement"] = (
        df.catalog_page_view_diamond_byo
        + df.catalog_page_view_diamond_ld
        + df.catalog_page_view_diamond_preset
    )
    df["catalog_page_view_setting_engagement"] = (
        df.catalog_page_view_setting_byo
        + df.catalog_page_view_setting_ld
        + df.catalog_page_view_setting_preset
    )
    df["detail_page_view_diamond_engagement"] = (
        df.detail_page_view_diamond_byo
        + df.detail_page_view_diamond_ld
        + df.detail_page_view_diamond_preset
    )
    df["detail_page_view_setting_engagement"] = (
        df.detail_page_view_setting_byo
        + df.detail_page_view_setting_ld
        + df.detail_page_view_setting_preset
    )
    df["BUR_view_engagement"] = df.BUR_view_byo
    df["add_to_basket_engagement"] = np.where(
        (df["add_to_basket_byo"] + df["add_to_basket_ld"] + df["add_to_basket_preset"])
        > 0,
        1,
        0,
    )

    ### Aggregation Process
    # calling date_conversion function
    df = PreProcess(df).date_conversion("date_key")
    date_to_score = datetime.strptime(str(scoring_date), "%Y%m%d").strftime("%Y-%m-%d")

    # Aggregation dates
    PD = np.timedelta64(90, "D")
    OD = np.timedelta64(1, "D")

    ######## Final Aggregation Process #########

    # Calling Aggregation function
    try:
        modelling_df = PreProcess(df).modelling_AD_creation(date_to_score, OD, PD)
    except:
        print("Data not found:101")

    # creating first_order_date for existing_cust
    Ad_existing_cust_1 = (
        df_main[["guid_key", "first_order_date"]]
        .groupby("guid_key")["first_order_date"]
        .min()
        .reset_index()
    )
    Ad_existing_cust_1 = Ad_existing_cust_1.dropna()
    Ad_existing_cust_1["first_order_date"] = Ad_existing_cust_1[
        "first_order_date"
    ].astype(int)
    Ad_existing_cust_1["first_order_date"] = pd.to_datetime(
        Ad_existing_cust_1["first_order_date"], format="%Y%m%d", errors="ignore"
    )
    modelling_df = modelling_df.merge(Ad_existing_cust_1, how="left", on="guid_key")
    modelling_df["date_key"] = modelling_df["date_key"].astype("datetime64")
    modelling_df["existing_cust_engagement"] = np.where(
        modelling_df.date_key > modelling_df.first_order_date, 1, 0
    )

    # # creating demographic data and ring sizer columns
    df1 = df_main[
        ["guid_key", "date_key", "gender", "primary_currency_code", "marital_status"]
    ]
    df1["gender"] = df1["gender"].replace({0: "U", 1: "F", 2: "M"})
    df1["primary_currency_code"] = np.where(
        df1["primary_currency_code"] == "USD", df1["primary_currency_code"], "others"
    )
    df1["marital_status"] = df1["marital_status"].replace(
        {
            "Engaged": "eng/In_a_relationship",
            "In A Relationship": "eng/In_a_relationship",
        }
    )
    df1["date_key"] = pd.to_datetime(df1["date_key"], format="%Y%m%d", errors="ignore")
    df1 = df1[
        ["guid_key", "marital_status", "primary_currency_code", "gender"]
    ].drop_duplicates()
    df1.reset_index(inplace=True)

    # # Merging new variables with existing AD
    modelling_df = modelling_df.merge(df1, how="left", on=["guid_key"])

    # Creating Dates and months
    dates = list(modelling_df["date_key"].unique())
    modelling_df["year"] = [i.year for i in modelling_df["date_key"]]
    modelling_df["month"] = [i.month for i in modelling_df["date_key"]]

    # Converting day difference to integer
    date_columns = [
        "Days_since_last_search_journey",
        "Days_since_last_search_engagement",
        "Days_since_last_search_OJ",
        "Days_since_last_addtobasket_journey",
        "Days_since_last_addtobasket_engagement",
        "Days_since_last_addtobasket_OJ",
    ]

    for i in range(len(date_columns)):
        modelling_df[date_columns[i]] = modelling_df[date_columns[i]].apply(
            lambda x: str(x).split("days")[0].replace(" ", "")
        )
        modelling_df[date_columns[i]] = (
            modelling_df[date_columns[i]].replace([np.nan, "NaT"], 0).astype("float64")
        )

    # Creating dummies
    AD = pd.get_dummies(
        data=modelling_df, columns=["marital_status", "primary_currency_code", "gender"]
    )

    AD_main = AD.copy()
    AD_score = AD_main[features_modelling]
    AD_score = AD_score.fillna(0)

    # Load downlaoded PKL file
    clf = joblib.load(_model_local)

    y_pred = clf.predict_proba(AD_score)
    # Final file
    prob = [i[1] for i in y_pred]
    AD_main["Predicted_probabilities"] = np.array(prob)
    Results = AD_main[["GUID_PJ", "guid_key", "Predicted_probabilities"]]
    Results.columns = ["BNID_PJ", "BNID", "Predicted_probabilities"]

    Results_bnid_level = (
        Results[["BNID", "Predicted_probabilities"]].groupby("BNID").max().reset_index()
    )
    Results_bnid_level["Propensity_category"] = np.where(
        Results_bnid_level.Predicted_probabilities > 0.43,
        "High",
        np.where(
            (Results_bnid_level.Predicted_probabilities >= 0.22)
            & (Results_bnid_level.Predicted_probabilities <= 0.43),
            "Medium",
            "Low",
        ),
    )
    Results_bnid_level["Run_date"] = scoring_date

    Results_bnid_journey_level = (
        Results[["BNID_PJ", "Predicted_probabilities"]]
        .groupby("BNID_PJ")
        .max()
        .reset_index()
    )
    Results_bnid_journey_level[
        ["BNID", "Journey"]
    ] = Results_bnid_journey_level.BNID_PJ.str.split("_", expand=True)
    Results_bnid_journey_level = Results_bnid_journey_level[
        ["BNID", "Journey", "Predicted_probabilities"]
    ]
    Results_bnid_journey_level["Propensity_category"] = np.where(
        Results_bnid_journey_level.Predicted_probabilities > 0.43,
        "High",
        np.where(
            (Results_bnid_journey_level.Predicted_probabilities >= 0.22)
            & (Results_bnid_journey_level.Predicted_probabilities <= 0.43),
            "Medium",
            "Low",
        ),
    )
    Results_bnid_journey_level["Run_date"] = scoring_date

    # Saving output files
    Results_bnid_level.to_csv(
        output_path + "long_conversion_bnid_level_" + scoring_date + ".csv"
    )
    Results_bnid_journey_level.to_csv(
        output_path + "long_conversion_bnid_journey_level_" + scoring_date + ".csv"
    )


if __name__ == "__main__":
    params = json.load(open("lc_params.json"))
    sc = lc_scoring(params)
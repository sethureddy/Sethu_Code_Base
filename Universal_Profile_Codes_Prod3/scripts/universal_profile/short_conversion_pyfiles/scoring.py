import datetime
import time
import os
import json
import pandas as pd
import numpy as np
import re
import time
import gcsfs
import joblib
#import tensorflow as tf
import pickle
from datetime import datetime
from PreProcess import PreProcess
from config import _features_modelling, _features_aggregation
#from google.cloud import storage


def sc_scoring(params):

    scoring_date = params["scoring_date"]
    output_path = params["output_path"]
    model_path = params["model_path"]
    input_file = params["input_file"]
    _bucket_name = params["bucket_name"]
    _model_bucket = params["model_bucket"]
    _model_local = params["model_local"]

    features_modelling = _features_modelling
    features_aggregation = _features_aggregation

    ### Reading File
    df = pd.read_csv(input_file)
    df_main = df.copy()
    df = df[features_aggregation]

    ### Aggregation Process
    # calling date_conversion function
    df = PreProcess(df).date_conversion("date_key")
    date_to_score = datetime.strptime(str(scoring_date), "%Y%m%d").strftime("%Y-%m-%d")

    # Aggregation dates
    PD = np.timedelta64(60, "D")
    OD = np.timedelta64(1, "D")

    ######## Final Aggregation Process #########

    # Calling Aggregation function
    modelling_df = PreProcess(df).modelling_AD_creation(date_to_score, OD, PD)

    # creating demographic data
    df1 = df_main[
        [
            "guid_key",
            "date_key",
            "derived_gender",
            "primary_currency_code",
            "MARITAL_STATUS",
        ]
    ]
    df1["primary_currency_code"] = np.where(
        df1["primary_currency_code"] == "USD", df1["primary_currency_code"], "others"
    )
    df1["MARITAL_STATUS"] = df1["MARITAL_STATUS"].replace(
        {
            "Engaged": "eng/In_a_relationship",
            "In A Relationship": "eng/In_a_relationship",
        }
    )
    df1["date_key"] = pd.to_datetime(df1["date_key"], format="%Y%m%d", errors="ignore")
    df1 = df1[
        [
            "guid_key",
            "date_key",
            "MARITAL_STATUS",
            "primary_currency_code",
            "derived_gender",
        ]
    ].drop_duplicates()

    # creating seasonality variables
    df2 = df_main[
        ["date_key", "WEEKEND_FLAG", "SEASON_NAME", "HOLIDAY_FLAG"]
    ].drop_duplicates()
    season_flag = {
        "New_Year_Week": 1,
        "Lunar_New_Year_Week": 1,
        "Valentines_Week": 1,
        "Mothers_Day_Week_UK": 1,
        "Mothers_Day_Week_US": 1,
        "Memorial_Day_Week": 1,
        "Fathers_Day_Week": 1,
        "Labor_Day_Week": 1,
        "Labor_Day": 1,
        "Diwali_Week": 1,
        "Thanksgiving_Week": 1,
        "Cyber_Week": 1,
        "Christmas_Week": 1,
    }
    df2 = df2.replace({"SEASON_NAME": season_flag})
    df2 = df2.fillna(0)
    df2["date_key"] = pd.to_datetime(df2["date_key"], format="%Y%m%d", errors="ignore")
    df2["date_key"] = df2.date_key.astype("datetime64")
    modelling_df["date_key"] = modelling_df.date_key.astype("datetime64")

    # Merging new variables with existing AD
    modelling_df = modelling_df.merge(df1, how="left", on=["guid_key", "date_key"])
    modelling_df = modelling_df.merge(df2, how="left", on=["date_key"])

    # Creating Dates and months
    dates = list(modelling_df["date_key"].unique())
    modelling_df["year"] = [i.year for i in modelling_df["date_key"]]
    modelling_df["month"] = [i.month for i in modelling_df["date_key"]]

    # Converting day difference to integer
    date_columns = [
        "Days_since_last_search_journey",
        "Days_since_last_search_OJ",
        "Days_since_last_addtobasket_journey",
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
        data=modelling_df,
        columns=["MARITAL_STATUS", "primary_currency_code", "derived_gender"],
    )

    AD_main = AD.copy()
    AD_score = AD_main[features_modelling]
    AD_score = AD_score.fillna(0)

    # Scoring
    #storage_client = storage.Client()

    #bucket = storage_client.get_bucket(_bucket_name)
    # select bucket file
    #blob = bucket.blob(_model_bucket)
    # download that file and name it 'local.joblib'
    #blob.download_to_filename(_model_local)
    # load that file from local file
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
        Results_bnid_level.Predicted_probabilities > 0.45,
        "High",
        np.where(
            (Results_bnid_level.Predicted_probabilities >= 0.20)
            & (Results_bnid_level.Predicted_probabilities <= 0.45),
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
        Results_bnid_journey_level.Predicted_probabilities > 0.45,
        "High",
        np.where(
            (Results_bnid_journey_level.Predicted_probabilities >= 0.20)
            & (Results_bnid_journey_level.Predicted_probabilities <= 0.45),
            "Medium",
            "Low",
        ),
    )
    Results_bnid_journey_level["Run_date"] = scoring_date

    # Saving output files
    Results_bnid_level.to_csv(
        output_path + "short_conversion_bnid_level_" + scoring_date + ".csv"
    )
    Results_bnid_journey_level.to_csv(
        output_path + "short_conversion_bnid_journey_level_" + scoring_date + ".csv"
    )

if __name__ == "__main__":
    params = json.load(open("sc_params.json"))
    sc = sc_scoring(params)
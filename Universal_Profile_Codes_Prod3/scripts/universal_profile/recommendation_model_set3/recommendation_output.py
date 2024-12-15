# importing libraries
import pandas as pd
import numpy as np
import os
import json
from datetime import date


def recommendation_model_output(params):
    diamond_input_file_ = params["diamond_file"]
    setting_input_file_ = params["setting_file"]
    jewellery_input_file_ = params["jewellery_file"]


    output_path = params["output_path"]
    today = params["scoring_date"]

    diamond_input_file = diamond_input_file_ + "diamond_recommendation_user_product_feature" + today + ".csv"
    setting_input_file = setting_input_file_ + "settings_recommendation_user_product_feature" + today + ".csv"
    jewellery_input_file = jewellery_input_file_ + "jewellery_recommendation_user_product_feature" + today + ".csv"

    # reading files
    diamond = pd.read_csv(diamond_input_file)
    setting = pd.read_csv(setting_input_file)
    jewellery_input = pd.read_csv(jewellery_input_file)

    # DIAMOND
    diamond_output = diamond[
        ["bnid", "SKU_ID_Recommended", "recommended_avg_price", "Rank", "DIAMOND_SHAPE"]
    ].drop_duplicates()

    # Creating format
    diamond_output["MODEL_SEQ"] = "3"
    diamond_output["RECO_ID"] = (
        diamond_output["bnid"]
        + "_"
        + diamond_output["MODEL_SEQ"]
        + "_"
        + diamond_output["Rank"].astype(int).astype(str)
    )
    diamond_output["BYO_OPTIONS"] = np.nan
    diamond_output["PRODUCT_CLASS_NAME"] = "Loose Diamonds"
    diamond_output["MERCH_PRODUCT_CATEGORY"] = "Loose Diamonds"
    diamond_output["RUN_DATE"] = today
    diamond_output["TOTAL_PRICE"] = diamond_output["recommended_avg_price"]
    diamond_output["SETTING_OFFER_ID"] = np.nan
    diamond_output["SETTING_PRICE"] = np.nan
    diamond_output["ITEM_OFFER_ID"] = np.nan
    diamond_output["CHAIN_OFFER_ID"] = np.nan
    diamond_output["CHAIN_PRICE"] = np.nan
    diamond_output["DIAMOND_SKU_1"] = np.nan
    diamond_output["DIAMOND_1_PRICE"] = np.nan
    diamond_output["DIAMOND_SKU_2"] = np.nan
    diamond_output["DIAMOND_2_PRICE"] = np.nan
    diamond_output["DIAMOND_SKU_3"] = np.nan
    diamond_output["DIAMOND_3_PRICE"] = np.nan
    diamond_output["DIAMOND_SKU_4"] = np.nan
    diamond_output["DIAMOND_4_PRICE"] = np.nan
    diamond_output["DIAMOND_SKU_5"] = np.nan
    diamond_output["DIAMOND_5_PRICE"] = np.nan

    # Changing column names
    diamond_output = diamond_output[
        [
            "bnid",
            "MODEL_SEQ",
            "RECO_ID",
            "Rank",
            "BYO_OPTIONS",
            "PRODUCT_CLASS_NAME",
            "MERCH_PRODUCT_CATEGORY",
            "RUN_DATE",
            "TOTAL_PRICE",
            "SETTING_OFFER_ID",
            "SETTING_PRICE",
            "SKU_ID_Recommended",
            "ITEM_OFFER_ID",
            "recommended_avg_price",
            "CHAIN_OFFER_ID",
            "CHAIN_PRICE",
            "DIAMOND_SHAPE",
            "DIAMOND_SKU_1",
            "DIAMOND_1_PRICE",
            "DIAMOND_SKU_2",
            "DIAMOND_2_PRICE",
            "DIAMOND_SKU_3",
            "DIAMOND_3_PRICE",
            "DIAMOND_SKU_4",
            "DIAMOND_4_PRICE",
            "DIAMOND_SKU_5",
            "DIAMOND_5_PRICE",
        ]
    ]
    diamond_output.columns = [
        "BNID",
        "MODEL_SEQ",
        "RECO_ID",
        "RECO_RANK",
        "BYO_OPTIONS",
        "PRODUCT_CLASS_NAME",
        "MERCH_PRODUCT_CATEGORY",
        "RUN_DATE",
        "TOTAL_PRICE",
        "SETTING_OFFER_ID",
        "SETTING_PRICE",
        "ITEM_SKU",
        "ITEM_OFFER_ID",
        "ITEM_PRICE",
        "CHAIN_OFFER_ID",
        "CHAIN_PRICE",
        "PRIMARY_DIAMOND_SHAPE",
        "DIAMOND_SKU_1",
        "DIAMOND_1_PRICE",
        "DIAMOND_SKU_2",
        "DIAMOND_2_PRICE",
        "DIAMOND_SKU_3",
        "DIAMOND_3_PRICE",
        "DIAMOND_SKU_4",
        "DIAMOND_4_PRICE",
        "DIAMOND_SKU_5",
        "DIAMOND_5_PRICE",
    ]

    # Pivoting diamond columns wrt bnids
    diam_piv = diamond_output[
        ["BNID", "RECO_RANK", "ITEM_SKU", "ITEM_PRICE", "PRIMARY_DIAMOND_SHAPE"]
    ].drop_duplicates()
    diamond_piv2 = diam_piv.pivot(
        index="BNID",
        columns="RECO_RANK",
        values=["ITEM_SKU", "ITEM_PRICE", "PRIMARY_DIAMOND_SHAPE"],
    ).reset_index()
    diamond_piv2.columns = [
        "BNID",
        "ITEM_SKU_1",
        "ITEM_SKU_2",
        "ITEM_SKU_3",
        "ITEM_SKU_4",
        "ITEM_SKU_5",
        "ITEM_PRICE_1",
        "ITEM_PRICE_2",
        "ITEM_PRICE_3",
        "ITEM_PRICE_4",
        "ITEM_PRICE_5",
        "PRIMARY_DIAMOND_SHAPE_1",
        "PRIMARY_DIAMOND_SHAPE_2",
        "PRIMARY_DIAMOND_SHAPE_3",
        "PRIMARY_DIAMOND_SHAPE_4",
        "PRIMARY_DIAMOND_SHAPE_5",
    ]
    diamond_piv2 = diamond_piv2[
        [
            "BNID",
            "ITEM_SKU_1",
            "ITEM_SKU_2",
            "ITEM_SKU_3",
            "ITEM_SKU_4",
            "ITEM_SKU_5",
            "ITEM_PRICE_1",
            "ITEM_PRICE_2",
            "ITEM_PRICE_3",
            "ITEM_PRICE_4",
            "ITEM_PRICE_5",
            "PRIMARY_DIAMOND_SHAPE_1",
        ]
    ]

    # Setting
    setting = setting[
        [
            "bnid",
            "Recommended_items",
            "recommended_avg_price",
            "Rank",
            "MERCH_PRODUCT_CATEGORY",
            "PRODUCT_CLASS_NAME",
        ]
    ]
    setting.columns = [
        "BNID",
        "Recommended_items",
        "recommended_avg_price",
        "Rank",
        "MERCH_PRODUCT_CATEGORY",
        "PRODUCT_CLASS_NAME",
    ]

    setting_output = setting.merge(diamond_piv2, how="left", on="BNID")

    # creting setting columns as per format
    setting_output["MODEL_SEQ"] = "3"
    setting_output["RECO_ID"] = (
        setting_output["BNID"]
        + "_"
        + setting_output["MODEL_SEQ"]
        + "_"
        + setting_output["Rank"].astype(int).astype(str)
    )
    setting_output["BYO_OPTIONS"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "BYO Ring",
        "BNRING_MTD",
        np.where(
            setting_output.PRODUCT_CLASS_NAME == "BYO 3 Stone", "BN3STN_MTD", "PREBUILT"
        ),
    )
    setting_output["RUN_DATE"] = today
    setting_output["ITEM_SKU"] = np.nan
    setting_output["ITEM_OFFER_ID"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "Pre",
        setting_output.Recommended_items,
        np.nan,
    )
    setting_output["ITEM_PRICE"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "Pre",
        setting_output.recommended_avg_price,
        np.nan,
    )
    setting_output["CHAIN_OFFER_ID"] = np.nan
    setting_output["CHAIN_PRICE"] = np.nan
    setting_output["DIAMOND_SKU_1"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "Pre", np.nan, setting_output.ITEM_SKU_1
    )
    setting_output["DIAMOND_1_PRICE"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "Pre", np.nan, setting_output.ITEM_PRICE_1
    )
    setting_output["DIAMOND_SKU_2"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "BYO 3 Stone",
        setting_output.ITEM_SKU_2,
        np.nan,
    )
    setting_output["DIAMOND_2_PRICE"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "BYO 3 Stone",
        setting_output.ITEM_PRICE_2,
        np.nan,
    )
    setting_output["DIAMOND_SKU_3"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "BYO 3 Stone",
        setting_output.ITEM_SKU_3,
        np.nan,
    )
    setting_output["DIAMOND_3_PRICE"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "BYO 3 Stone",
        setting_output.ITEM_PRICE_3,
        np.nan,
    )
    setting_output["DIAMOND_SKU_4"] = np.nan
    setting_output["DIAMOND_4_PRICE"] = np.nan
    setting_output["DIAMOND_SKU_5"] = np.nan
    setting_output["DIAMOND_5_PRICE"] = np.nan
    setting_output["SETTING_OFFER_ID"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "Pre",
        np.nan,
        setting_output.Recommended_items,
    )
    setting_output["SETTING_PRICE"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "Pre",
        np.nan,
        setting_output.recommended_avg_price,
    )
    setting_output["TOTAL_PRICE"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "BYO Ring",
        setting_output.recommended_avg_price + np.where(setting_output.ITEM_PRICE_1.isna(),0,setting_output.ITEM_PRICE_1),
        np.where(
            setting_output.PRODUCT_CLASS_NAME == "BYO 3 Stone",
            setting_output.recommended_avg_price
            + np.where(setting_output.ITEM_PRICE_1.isna(),0,setting_output.ITEM_PRICE_1)
            + np.where(setting_output.ITEM_PRICE_2.isna(),0,setting_output.ITEM_PRICE_2)
            + np.where(setting_output.ITEM_PRICE_3.isna(),0,setting_output.ITEM_PRICE_3),
            setting_output.ITEM_PRICE,
        ),
    )
    setting_output["PRIMARY_DIAMOND_SHAPE"] = np.where(
        setting_output.PRODUCT_CLASS_NAME == "Pre",
        np.nan,
        setting_output.PRIMARY_DIAMOND_SHAPE_1,
    )
    # Column names
    setting_output = setting_output[
        [
            "BNID",
            "MODEL_SEQ",
            "RECO_ID",
            "Rank",
            "BYO_OPTIONS",
            "PRODUCT_CLASS_NAME",
            "MERCH_PRODUCT_CATEGORY",
            "RUN_DATE",
            "TOTAL_PRICE",
            "SETTING_OFFER_ID",
            "SETTING_PRICE",
            "ITEM_SKU",
            "ITEM_OFFER_ID",
            "ITEM_PRICE",
            "CHAIN_OFFER_ID",
            "CHAIN_PRICE",
            "PRIMARY_DIAMOND_SHAPE",
            "DIAMOND_SKU_1",
            "DIAMOND_1_PRICE",
            "DIAMOND_SKU_2",
            "DIAMOND_2_PRICE",
            "DIAMOND_SKU_3",
            "DIAMOND_3_PRICE",
            "DIAMOND_SKU_4",
            "DIAMOND_4_PRICE",
            "DIAMOND_SKU_5",
            "DIAMOND_5_PRICE",
        ]
    ]

    setting_output.columns = [
        "BNID",
        "MODEL_SEQ",
        "RECO_ID",
        "RECO_RANK",
        "BYO_OPTIONS",
        "PRODUCT_CLASS_NAME",
        "MERCH_PRODUCT_CATEGORY",
        "RUN_DATE",
        "TOTAL_PRICE",
        "SETTING_OFFER_ID",
        "SETTING_PRICE",
        "ITEM_SKU",
        "ITEM_OFFER_ID",
        "ITEM_PRICE",
        "CHAIN_OFFER_ID",
        "CHAIN_PRICE",
        "PRIMARY_DIAMOND_SHAPE",
        "DIAMOND_SKU_1",
        "DIAMOND_1_PRICE",
        "DIAMOND_SKU_2",
        "DIAMOND_2_PRICE",
        "DIAMOND_SKU_3",
        "DIAMOND_3_PRICE",
        "DIAMOND_SKU_4",
        "DIAMOND_4_PRICE",
        "DIAMOND_SKU_5",
        "DIAMOND_5_PRICE",
    ]

    # Jewel
    # Renaming Jewellery columns
    jewellery = jewellery_input[
        [
            "bnid",
            "Recommended_items",
            "recommended_avg_price",
            "Rank",
            "Merch_product_category",
            "product_class_name",
        ]
    ]
    jewellery.columns = [
        "BNID",
        "Recommended_items",
        "recommended_avg_price",
        "Rank",
        "MERCH_PRODUCT_CATEGORY",
        "PRODUCT_CLASS_NAME",
    ]

    # merging diamond data with jewel data
    jewellery_output = jewellery.merge(diamond_piv2, how="left", on="BNID")

    jewellery_output["MODEL_SEQ"] = "3"
    jewellery_output["RECO_ID"] = (
        jewellery_output["BNID"]
        + "_"
        + jewellery_output["MODEL_SEQ"]
        + "_"
        + jewellery_output["Rank"].astype(int).astype(str)
    )
    jewellery_output["BYO_OPTIONS"] = np.where(
        jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Solitaire Pendant Metal",
        "BNPEND_MTD",
        np.where(
            jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Stud Metal",
            "BNEAR_SET",
            np.where(
                jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
                "BN5STN_MTD",
                "PREBUILT",
            ),
        ),
    )
    jewellery_output["RUN_DATE"] = today
    jewellery_output["ITEM_SKU"] = np.nan
    jewellery_output["ITEM_OFFER_ID"] = np.where(
        (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Solitaire Pendant Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Stud Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Diamond Band Metal"),
        jewellery_output.Recommended_items,
        np.nan,
    )
    jewellery_output["ITEM_PRICE"] = np.where(
        (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Solitaire Pendant Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Stud Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Diamond Band Metal"),
        jewellery_output.recommended_avg_price,
        np.nan,
    )
    jewellery_output["CHAIN_OFFER_ID"] = np.nan
    jewellery_output["CHAIN_PRICE"] = np.nan
    jewellery_output["DIAMOND_SKU_1"] = np.where(
        (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Solitaire Pendant Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Stud Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Diamond Band Metal"),
        np.nan,
        jewellery_output.ITEM_SKU_1,
    )
    jewellery_output["DIAMOND_1_PRICE"] = np.where(
        (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Solitaire Pendant Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Stud Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Diamond Band Metal"),
        np.nan,
        jewellery_output.ITEM_PRICE_1,
    )
    jewellery_output["DIAMOND_SKU_2"] = np.where(
        jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
        jewellery_output.ITEM_SKU_2,
        np.nan,
    )
    jewellery_output["DIAMOND_2_PRICE"] = np.where(
        jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
        jewellery_output.ITEM_PRICE_2,
        np.nan,
    )
    jewellery_output["DIAMOND_SKU_3"] = np.where(
        jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
        jewellery_output.ITEM_SKU_3,
        np.nan,
    )
    jewellery_output["DIAMOND_3_PRICE"] = np.where(
        jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
        jewellery_output.ITEM_PRICE_3,
        np.nan,
    )
    jewellery_output["DIAMOND_SKU_4"] = np.where(
        jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
        jewellery_output.ITEM_SKU_4,
        np.nan,
    )
    jewellery_output["DIAMOND_4_PRICE"] = np.where(
        jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
        jewellery_output.ITEM_PRICE_4,
        np.nan,
    )
    jewellery_output["DIAMOND_SKU_5"] = np.where(
        jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
        jewellery_output.ITEM_SKU_5,
        np.nan,
    )
    jewellery_output["DIAMOND_5_PRICE"] = np.where(
        jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
        jewellery_output.ITEM_PRICE_5,
        np.nan,
    )
    jewellery_output["SETTING_OFFER_ID"] = np.where(
        (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Stud Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Diamond Band Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Solitaire Pendant Metal"),
        np.nan,
        jewellery_output.Recommended_items,
    )
    jewellery_output["SETTING_PRICE"] = np.where(
        (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Stud Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Diamond Band Metal")
        & (jewellery_output.MERCH_PRODUCT_CATEGORY != "BYO Solitaire Pendant Metal"),
        np.nan,
        jewellery_output.recommended_avg_price,
    )
    jewellery_output["TOTAL_PRICE"] = np.where(
        (
            (jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Solitaire Pendant Metal")
            | (jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Stud Metal")
        ),
        jewellery_output.recommended_avg_price + np.where(jewellery_output.ITEM_PRICE_1.isna(),0,jewellery_output.ITEM_PRICE_1),
        np.where(
            jewellery_output.MERCH_PRODUCT_CATEGORY == "BYO Diamond Band Metal",
            jewellery_output.recommended_avg_price
            + np.where(jewellery_output.ITEM_PRICE_1.isna(),0,jewellery_output.ITEM_PRICE_1)
            + np.where(jewellery_output.ITEM_PRICE_2.isna(),0,jewellery_output.ITEM_PRICE_2)
            + np.where(jewellery_output.ITEM_PRICE_3.isna(),0,jewellery_output.ITEM_PRICE_3)
            + np.where(jewellery_output.ITEM_PRICE_4.isna(),0,jewellery_output.ITEM_PRICE_4)
            + np.where(jewellery_output.ITEM_PRICE_5.isna(),0,jewellery_output.ITEM_PRICE_5),
            jewellery_output.ITEM_PRICE,
        ),
    )

    jewellery_output["PRIMARY_DIAMOND_SHAPE"] = np.where(
        jewellery_output.BYO_OPTIONS == "PREBUILT",
        np.nan,
        jewellery_output.PRIMARY_DIAMOND_SHAPE_1,
    )

    # Renaming and structuring jewllery
    jewellery_output = jewellery_output[
        [
            "BNID",
            "MODEL_SEQ",
            "RECO_ID",
            "Rank",
            "BYO_OPTIONS",
            "PRODUCT_CLASS_NAME",
            "MERCH_PRODUCT_CATEGORY",
            "RUN_DATE",
            "TOTAL_PRICE",
            "SETTING_OFFER_ID",
            "SETTING_PRICE",
            "ITEM_SKU",
            "ITEM_OFFER_ID",
            "ITEM_PRICE",
            "CHAIN_OFFER_ID",
            "CHAIN_PRICE",
            "PRIMARY_DIAMOND_SHAPE",
            "DIAMOND_SKU_1",
            "DIAMOND_1_PRICE",
            "DIAMOND_SKU_2",
            "DIAMOND_2_PRICE",
            "DIAMOND_SKU_3",
            "DIAMOND_3_PRICE",
            "DIAMOND_SKU_4",
            "DIAMOND_4_PRICE",
            "DIAMOND_SKU_5",
            "DIAMOND_5_PRICE",
        ]
    ]

    jewellery_output.columns = [
        "BNID",
        "MODEL_SEQ",
        "RECO_ID",
        "RECO_RANK",
        "BYO_OPTIONS",
        "PRODUCT_CLASS_NAME",
        "MERCH_PRODUCT_CATEGORY",
        "RUN_DATE",
        "TOTAL_PRICE",
        "SETTING_OFFER_ID",
        "SETTING_PRICE",
        "ITEM_SKU",
        "ITEM_OFFER_ID",
        "ITEM_PRICE",
        "CHAIN_OFFER_ID",
        "CHAIN_PRICE",
        "PRIMARY_DIAMOND_SHAPE",
        "DIAMOND_SKU_1",
        "DIAMOND_1_PRICE",
        "DIAMOND_SKU_2",
        "DIAMOND_2_PRICE",
        "DIAMOND_SKU_3",
        "DIAMOND_3_PRICE",
        "DIAMOND_SKU_4",
        "DIAMOND_4_PRICE",
        "DIAMOND_SKU_5",
        "DIAMOND_5_PRICE",
    ]

    # appending diamond , jewellery and setting output
    recommendations_output = jewellery_output.append(diamond_output).append(
        setting_output
    )

    recommendations_output["ITEM_SKU"] = recommendations_output["ITEM_SKU"].str.upper()
    recommendations_output["DIAMOND_SKU_1"] = recommendations_output[
        "DIAMOND_SKU_1"
    ].str.upper()
    recommendations_output["DIAMOND_SKU_2"] = recommendations_output[
        "DIAMOND_SKU_2"
    ].str.upper()
    recommendations_output["DIAMOND_SKU_3"] = recommendations_output[
        "DIAMOND_SKU_3"
    ].str.upper()
    recommendations_output["DIAMOND_SKU_4"] = recommendations_output[
        "DIAMOND_SKU_4"
    ].str.upper()
    recommendations_output["DIAMOND_SKU_5"] = recommendations_output[
        "DIAMOND_SKU_5"
    ].str.upper()

    # Saving ouput
    recommendations_output.to_csv(output_path + "recommendation.csv")

    print("Recommendation3 output is done.")

if __name__ == "__main__":
    params = json.load(open("recommendation.json"))
    recommendation_model_output(params)
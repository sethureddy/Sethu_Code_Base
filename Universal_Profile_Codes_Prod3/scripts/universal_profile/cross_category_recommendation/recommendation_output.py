# importing libraries
import pandas as pd
import numpy as np
import os
import json
from datetime import date
from datetime import datetime

from preprocess_user_features import create_categories
from google.cloud import bigquery as bq

def recommendation_model_output(params):

    input_path = params["recommendation_file"]
    output_path = params["output_path"]
    today = params["scoring_date"][:4]+"-"+params["scoring_date"][4:6]+"-"+params["scoring_date"][6:]

    input_file = input_path + "recommendation_user_product_feature_" + params["scoring_date"] + ".csv"

    # Final Recommendations File Reading
    scored_input = pd.read_csv(input_file)
    scored_input.rename(columns = {'MERCH_PRODUCT_CATEGORY':'Merch_product_category','PRODUCT_CLASS_NAME':'product_class_name'},inplace=True)
    scored_input['Defined_Category'] = scored_input['product_class_name'].apply( lambda x: create_categories(x))

    # Category Level Data Creation
    setting = scored_input[scored_input['Defined_Category'] == 'Engagement Ring']
    setting = setting[["bnid","Recommended_items","recommended_avg_price","Rank","Merch_product_category","product_class_name","MATCHING_PRODUCTS"]]

    diamond = scored_input[scored_input['Defined_Category'] == 'Loose Diamonds']
    diamond = diamond[["bnid","Recommended_items","recommended_avg_price","Rank","Merch_product_category","product_class_name","MATCHING_PRODUCTS","DIAMOND_SHAPE"]]
    diamond.columns = ["bnid","SKU_ID_Recommended","recommended_avg_price","Rank","Merch_product_category","product_class_name","MATCHING_PRODUCTS","DIAMOND_SHAPE"]

    jewellery_input = scored_input[~scored_input['Defined_Category'].isin(['Engagement Ring','Loose Diamonds'])]
    jewellery = jewellery_input[["bnid","Recommended_items","recommended_avg_price","Rank","Merch_product_category","product_class_name","MATCHING_PRODUCTS"]]
    
        # DIAMOND
    diamond_output = diamond[
        ["bnid", "SKU_ID_Recommended", "recommended_avg_price", "Rank", "DIAMOND_SHAPE","MATCHING_PRODUCTS"]
    ].drop_duplicates()

    # Creating format
    diamond_output["RECO_ID"] = (
        diamond_output["bnid"]
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
            "MATCHING_PRODUCTS"
        ]
    ]
    diamond_output.columns = [
        "BNID",
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
        "MATCHING_PRODUCTS"
    ]

    # Pivoting diamond columns wrt bnids
    diam_piv = diamond_output[
        ["BNID", "RECO_RANK", "ITEM_SKU", "ITEM_PRICE", "PRIMARY_DIAMOND_SHAPE","MATCHING_PRODUCTS"]
    ].drop_duplicates()
    diam_piv['COL_RANK'] = diam_piv.groupby('BNID')['RECO_RANK'].rank()

    sample_df = pd.DataFrame({'BNID':[np.nan]*5, 'RECO_RANK':[np.nan]*5,'ITEM_SKU':[np.nan]*5,'ITEM_PRICE':[np.nan]*5,
                                'PRIMARY_DIAMON_SHAPE':[np.nan]*5,'MATCHING_PRODUCTS':[np.nan]*5,'COL_RANK':np.arange(1,6)})
    diam_piv = pd.concat([diam_piv,sample_df]).reset_index(drop=True)

    diamond_piv2 = diam_piv.pivot(
        index="BNID",
        columns="COL_RANK",
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
    diamond_piv2 = diamond_piv2[diamond_piv2['BNID'].notnull()]
    
    # Setting
    setting = setting[
        [
            "bnid",
            "Recommended_items",
            "recommended_avg_price",
            "Rank",
            "Merch_product_category",
            "product_class_name",
            "MATCHING_PRODUCTS"
        ]
    ]
    setting.columns = [
        "BNID",
        "Recommended_items",
        "recommended_avg_price",
        "Rank",
        "MERCH_PRODUCT_CATEGORY",
        "PRODUCT_CLASS_NAME",
        "MATCHING_PRODUCTS"
    ]

    setting_output = setting.merge(diamond_piv2, how="left", on="BNID")

    # creting setting columns as per format
    setting_output["RECO_ID"] = (
        setting_output["BNID"]
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
            "MATCHING_PRODUCTS"
        ]
    ]

    setting_output.columns = [
        "BNID",
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
        "MATCHING_PRODUCTS"
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
            "MATCHING_PRODUCTS"
        ]
    ]
    jewellery.columns = [
        "BNID",
        "Recommended_items",
        "recommended_avg_price",
        "Rank",
        "MERCH_PRODUCT_CATEGORY",
        "PRODUCT_CLASS_NAME",
        "MATCHING_PRODUCTS"
    ]

    # merging diamond data with jewel data
    jewellery_output = jewellery.merge(diamond_piv2, how="left", on="BNID")

    jewellery_output["RECO_ID"] = (
        jewellery_output["BNID"]
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
            "MATCHING_PRODUCTS"
        ]
    ]

    jewellery_output.columns = [
        "BNID",
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
        "MATCHING_PRODUCTS"
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

    recommendations_output = recommendations_output[recommendations_output['RECO_RANK'] <=10]
    
    #Converting DataTypes
    recommendations_output = recommendations_output.convert_dtypes()
    recommendations_output['ITEM_OFFER_ID'] = pd.to_numeric(recommendations_output['ITEM_OFFER_ID'], errors='coerce')
    recommendations_output['SETTING_OFFER_ID'] = pd.to_numeric(recommendations_output['SETTING_OFFER_ID'], errors='coerce')
    recommendations_output = recommendations_output.convert_dtypes()
    
    recommendations_output['RUN_DATE'] = [datetime.strptime(i, '%Y-%m-%d') for i in recommendations_output['RUN_DATE']]
    recommendations_output = recommendations_output.astype({'RECO_RANK':'int64','DIAMOND_SKU_2':'str','DIAMOND_SKU_3':'str','DIAMOND_SKU_4':'str','DIAMOND_SKU_5':'str',
    'CHAIN_PRICE':'float','DIAMOND_1_PRICE':'float','DIAMOND_2_PRICE':'float','DIAMOND_3_PRICE':'float','DIAMOND_4_PRICE':'float','DIAMOND_5_PRICE':'float'})
    
    recommendations_output['ITEM_OFFER_ID'] = pd.to_numeric(recommendations_output['ITEM_OFFER_ID'], errors='coerce')
    recommendations_output['SETTING_OFFER_ID'] = pd.to_numeric(recommendations_output['SETTING_OFFER_ID'], errors='coerce')
    recommendations_output['DIAMOND_SKU_2'] = recommendations_output['DIAMOND_SKU_2'].replace({'<NA>':None})
    recommendations_output['DIAMOND_SKU_3'] = recommendations_output['DIAMOND_SKU_3'].replace({'<NA>':None})
    recommendations_output['DIAMOND_SKU_4'] = recommendations_output['DIAMOND_SKU_4'].replace({'<NA>':None})
    recommendations_output['DIAMOND_SKU_5'] = recommendations_output['DIAMOND_SKU_5'].replace({'<NA>':None})
    
    recommendations_output = recommendations_output[['BNID', 'RECO_ID', 'RECO_RANK', 'BYO_OPTIONS',
       'PRODUCT_CLASS_NAME', 'MERCH_PRODUCT_CATEGORY', 'RUN_DATE',
       'TOTAL_PRICE', 'SETTING_OFFER_ID', 'SETTING_PRICE', 'ITEM_SKU',
       'ITEM_OFFER_ID', 'ITEM_PRICE', 'CHAIN_OFFER_ID', 'CHAIN_PRICE',
       'PRIMARY_DIAMOND_SHAPE', 'DIAMOND_SKU_1', 'DIAMOND_1_PRICE',
       'DIAMOND_SKU_2', 'DIAMOND_2_PRICE', 'DIAMOND_SKU_3', 'DIAMOND_3_PRICE',
       'DIAMOND_SKU_4', 'DIAMOND_4_PRICE', 'DIAMOND_SKU_5', 'DIAMOND_5_PRICE',
       'MATCHING_PRODUCTS']]
    recommendations_output.sort_values(by = ['BNID','RECO_RANK'],ascending=True,inplace=True)
    
    recommendations_output.reset_index(drop=True,inplace=True)
    
    client = bq.Client()
    project = "bnile-cdw-prod"
    bq_client = bq.Client(project=project)
    dataset = bq_client.dataset("o_customer")
    table_name = "bn_crosscategory_recommendation"
    table_id = bq_client.get_table(dataset.table(table_name))

    table = client.get_table(table_id)

    ## Utility to load data from csv to Bigquery table
    job_config = bq.LoadJobConfig()
    job_config.schema = table.schema ## schema can be pre-defined as variable and assign to job config. Please use final schema for recommendation output csv
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.source_format = bq.SourceFormat.CSV
    job_config.allow_quoted_newlines = True
    
    recommendations_output.to_csv(output_path + "bn_crosscategory_recommendation_" + params["scoring_date"] + ".csv")
    load_job = bq_client.load_table_from_dataframe(recommendations_output,dataset.table(table_name) ,job_config=job_config)


if __name__ == "__main__":
    params = json.load(open("cross_category_recommendation.json"))
    recommendation_model_output(params)

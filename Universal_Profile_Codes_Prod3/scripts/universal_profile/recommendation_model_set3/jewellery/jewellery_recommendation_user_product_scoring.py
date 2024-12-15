import pandas as pd
import numpy as np
import scipy.sparse
import scipy
import json

from lightfm import LightFM
from scipy import sparse
from google.cloud import bigquery as bgq
from preprocess_user_features import preprocess_user_features


def jewellery_recommendation_user_product_scoring(params):
    """
    recommendation of products on basis of user-item 
    interaction and product features
    """
    input_file = params["input_file"]
    output_path = params["output_path"]
    scoring_date = params["scoring_date"]

    # Reading raw data from bigquery
    df = pd.read_csv(input_file)

    # converting into offer_id to integer
    df["offer_id"] = df["offer_id"].astype("int64")
    df_main = df

    # Conversion to date
    df_main["date_key"] = pd.to_datetime(
        df_main["date_key"], format="%Y%m%d", errors="ignore"
    )

    # Shorlisting for 3 months
    df_shortlisted = df_main.copy()
    df_toscore = df_main[(df_main["date_key"] == df_main.date_key.max())]

    ##price mapping
    df_price = df_main[["offer_id", "PRODUCT_LIST_PRICE"]].drop_duplicates()

    # Creating dataframe based on bnid and items
    df_bnid_offer_count = (
        df_main.groupby(["bnid", "offer_id"]).agg({"view_cnt": "sum"}).reset_index()
    )

    df_category = (
        df_main[["offer_id", "offer_id"]].drop_duplicates().reset_index(drop=True)
    )
    df_category.columns = ["offer_id_IDENTIFIER", "offer_id"]
    df_category["offer_id"] = df_category["offer_id"].astype(str)

    # Creating the dataframe into 0's and 1's, this is the interaction matrix that needs to be fed into the light fm model mandatory
    interaction_matrix = preprocess_user_features(
        df_bnid_offer_count, df_category
    ).create_interaction_matrix("bnid", "offer_id", "view_cnt")

    # creating user_dictonary
    user_dict = preprocess_user_features(
        df_bnid_offer_count, df_category
    ).create_user_dict(interaction_matrix)

    # creating item dictonary
    item_dict = preprocess_user_features(
        df_bnid_offer_count, df_category
    ).create_item_dict("offer_id_IDENTIFIER", "offer_id")

    ###creating product features
    df_product_class_name = pd.get_dummies(
        df_main[["offer_id", "product_class_name"]], columns=["product_class_name"]
    )
    df_pcn = df_product_class_name.groupby("offer_id").sum()
    ls = [
        "offer_id",
        "CHAIN_LENGTH_INCHES",
        "CHAIN_WIDTH_MM",
        "CLASP_TYPE",
        "CHAIN_TYPE",
        "GRAM_WEIGHT",
        "MOUNTING_STYLE",
        "PRIMARY_METAL_COLOR",
        "PRIMARY_METAL_NAME",
        "PRIMARY_SHAPE_NAME",
        "PRIMARY_SURFACE_MARKINGS",
        "PRIMARY_NACRE_THICKNESS",
        "PRIMARY_SETTING_TYPE",
        "PRIMARY_STONE_TYPE",
    ]
    df16 = df_main[["offer_id", "GRAM_WEIGHT", "CHAIN_WIDTH_MM", "CHAIN_LENGTH_INCHES"]]
    ls1 = [
        "CLASP_TYPE",
        "CHAIN_TYPE",
        "MOUNTING_STYLE",
        "PRIMARY_METAL_COLOR",
        "PRIMARY_METAL_NAME",
        "PRIMARY_SHAPE_NAME",
        "PRIMARY_SURFACE_MARKINGS",
        "PRIMARY_NACRE_THICKNESS",
        "PRIMARY_SETTING_TYPE",
        "PRIMARY_STONE_TYPE",
    ]

    df11 = df_main[ls]
    ################ creating dummies for categorical features other than product class
    df2_F = pd.get_dummies(df11, columns=ls1)
    df3_F = df2_F.groupby(by="offer_id").sum()

    ############### creating changing distinct values to columns for continous variables
    df_x = df16[["offer_id", "CHAIN_WIDTH_MM"]]
    df_x["Count"] = np.where(df_x["CHAIN_WIDTH_MM"].isna(), 0, 1)
    df_chainwidth = df_x.copy()
    df_chainwidth_matrix = pd.pivot_table(
        data=df_chainwidth,
        index="offer_id",
        columns="CHAIN_WIDTH_MM",
        values="Count",
        aggfunc="max",
    )
    df_chainwidth_matrix = df_chainwidth_matrix.fillna(0)
    df_chainwidth_matrix = df_chainwidth_matrix.add_prefix("CHAIN_WIDTH_MM_")

    ##############
    df_x = df16[["offer_id", "GRAM_WEIGHT"]]
    df_x["Count"] = np.where(df_x["GRAM_WEIGHT"].isna(), 0, 1)
    df_gramweight = df_x.copy()
    df_gramweight_matrix = pd.pivot_table(
        data=df_gramweight,
        index="offer_id",
        columns="GRAM_WEIGHT",
        values="Count",
        aggfunc="max",
    )
    df_x = df16[["offer_id", "CHAIN_LENGTH_INCHES"]]
    df_x["Count"] = np.where(df_x["CHAIN_LENGTH_INCHES"].isna(), 0, 1)
    df_chainlength = df_x.copy()
    df_chainlength_matrix = pd.pivot_table(
        data=df_chainlength,
        index="offer_id",
        columns="CHAIN_LENGTH_INCHES",
        values="Count",
        aggfunc="max",
    )
    df_chainlength_matrix = df_chainlength_matrix.fillna(0)
    df_chainlength_matrix = df_chainlength_matrix.add_prefix("CHAIN_LENGTH_INCHES_")
    df_x = df16[["offer_id", "GRAM_WEIGHT"]]
    df_x["Count"] = np.where(df_x["GRAM_WEIGHT"].isna(), 0, 1)
    df_gramlength = df_x
    df_gramweight_matrix = pd.pivot_table(
        data=df_gramlength,
        index="offer_id",
        columns="GRAM_WEIGHT",
        values="Count",
        aggfunc="max",
    )
    df_gramweight_matrix = df_gramweight_matrix.fillna(0)
    df_gramweight_matrix = df_gramweight_matrix.add_prefix("GRAM_WEIGHT_")

    ###Merging the dataframes for product matrix
    merge1_gramnchain = pd.merge(
        df_gramweight_matrix, df_chainlength_matrix, on="offer_id", how="outer"
    )
    merge2_con = pd.merge(
        merge1_gramnchain, df_chainwidth_matrix, on="offer_id", how="outer"
    )
    merge2_con = merge2_con.reset_index()
    merge3 = pd.merge(merge2_con, df3_F, on="offer_id", how="outer")
    df3_total = merge3.copy()
    feature_name = np.array(df3_total.reset_index()["offer_id"])
    item_ids = np.array(df2_F.columns)
    df3_total.index = df3_total.offer_id
    df3_total.drop("offer_id", axis=1, inplace=True)
    df4 = pd.merge(df3_total, df_pcn, on="offer_id", how="outer")

    # Converting the data into 0's and 1's
    threshold = 0
    df5 = df4.applymap(lambda x: 1 if x > threshold else 0)
    feature_matrix = scipy.sparse.csr_matrix(df5.values)

    ###creating user matrix
    df_user_feat = df_main[["bnid", "derived_gender", "add_to_basket", "item_purchase"]]
    ls_user_feat = ["derived_gender", "add_to_basket", "item_purchase"]
    df1_dummy = pd.get_dummies(df_user_feat.drop(["bnid"], axis=1))
    df2_concat = pd.concat([df_user_feat[["bnid"]], df1_dummy], axis=1)
    df3_final = df2_concat.groupby(by=["bnid"]).sum().reset_index()
    df3_final.index = df3_final.bnid
    df3_final.drop("bnid", axis=1, inplace=True)

    # Converting the data into 0's and 1's
    threshold = 0
    df4 = df3_final.applymap(lambda x: 1 if x > threshold else 0)
    user_matrix = scipy.sparse.csr_matrix(df4.values)

    # creating Model
    recommender = preprocess_user_features(df_bnid_offer_count, df_category).runMF(
        interaction_matrix, feature_matrix, user_matrix
    )

    bnid_toscore = df_toscore[
        df_toscore["bnid"].isin(df_bnid_offer_count["bnid"].unique())
    ]["bnid"].unique()
    final_df = pd.DataFrame()
    category_column = "offer_id"
    for i in bnid_toscore:
        known_products, recommended_products_df = preprocess_user_features(
            df_bnid_offer_count, df_category
        ).sample_recommendation_user(
            recommender,
            interaction_matrix,
            feature_matrix,
            user_matrix,
            i,
            user_dict,
            item_dict,
            category_column,
        )
        int_df = recommended_products_df.copy()
        int_df["BNID"] = i
        int_df.reset_index(drop=True, inplace=True)
        final_df = final_df.append(int_df)
        final_df.reset_index(drop=True, inplace=True)

    final_df = final_df[["BNID", "offer_id", "Score"]]
    final_df.columns = ["bnid", "Recommended_items", "Score"]

    final_df = final_df.reset_index(drop=True)[["bnid", "Recommended_items", "Score"]]
    # final_df.columns=['bnid','Recommended_Categories',"Score"]

    final_df = final_df.reset_index(drop=True)[["bnid", "Recommended_items", "Score"]]
    user_sku_mapping = df_shortlisted[["bnid", "offer_id"]].drop_duplicates()

    ##Mapping price of viewed items
    user_sku_price_mapping = pd.merge(
        user_sku_mapping, df_price, how="left", on=["offer_id"]
    )

    # Mapping price of recommended items
    final_df1 = pd.merge(
        final_df, df_price, left_on="Recommended_items", right_on="offer_id"
    )
    final_df1 = final_df1[["bnid", "Recommended_items", "Score", "PRODUCT_LIST_PRICE"]]
    final_df1.columns = [
        "bnid",
        "Recommended_items",
        "Score",
        "recommended_avg_price",
    ]  # renaming columns

    ## Merging recommended items and viewed items under a single dataframe
    final_df2 = pd.merge(final_df1, user_sku_price_mapping, on="bnid")
    final_df2.columns = [
        "bnid",
        "Recommended_items",
        "Score",
        "recommended_avg_price",
        "offer_id",
        "known_avg_price",
    ]

    ##Type casting for prices in dataframe
    final_df2["recommended_avg_price"] = final_df2["recommended_avg_price"].astype(
        "float"
    )
    final_df2["known_avg_price"] = final_df2["known_avg_price"].astype("float")

    # bnids with skus not falling under jewellery_data
    final_bnids_df = (
        final_df2[["bnid", "Recommended_items"]]
        .drop_duplicates()
        .groupby("bnid")
        .count()
        .reset_index()
    )
    final_bnids = final_bnids_df[final_bnids_df.Recommended_items >= 5]["bnid"].unique()
    final_df2 = final_df2[final_df2.bnid.isin(final_bnids)]

    # finding the min and maximum of the price range searched by each customer
    user_price_min_max = (
        final_df2[["bnid", "known_avg_price"]]
        .groupby(["bnid"])
        .agg(["min", "max"])
        .reset_index()
    )
    user_price_min_max["known_avg_price_min"] = (
        user_price_min_max["known_avg_price"]["min"]
    ) * 0.75
    user_price_min_max["known_avg_price_max"] = (
        user_price_min_max["known_avg_price"]["max"]
    ) * 1.25

    # merging min_max price columns to final dataframe
    final_df3 = pd.merge(final_df2, user_price_min_max, on="bnid")

    # Creating  dataframe with records where recommendations satisfies price range for each bnid
    final_df4 = final_df3[
        (final_df3["recommended_avg_price"] > final_df3[("known_avg_price_min", "")])
        & (final_df3["recommended_avg_price"] < final_df3[("known_avg_price_max", "")])
    ]
    final_df5 = final_df4[
        ["bnid", "Recommended_items", "recommended_avg_price", "Score"]
    ].drop_duplicates()

    # # Ranking Diamonds on bases of price for each bnid
    final_df5["for_rank"] = (
        final_df5["Score"] * 10000000 + final_df5["recommended_avg_price"]
    )
    final_df5["Rank"] = final_df5.groupby("bnid")["for_rank"].rank(
        "first", ascending=False
    )
    final_df6 = final_df5[
        ["bnid", "Recommended_items", "recommended_avg_price", "Rank"]
    ].drop_duplicates()

    # Filtering top5 items based on rankings
    bnid_rank_df = final_df6[["bnid", "Rank"]].groupby(["bnid"]).max().reset_index()
    bnid_5 = bnid_rank_df[bnid_rank_df["Rank"] >= 5]["bnid"]

    # filtering bnids where recommendations are less then 5 skus
    final_df7 = final_df6[final_df6.bnid.isin(bnid_5)]
    ultimate_df1 = final_df7[final_df7.Rank <= 5]

    # filtering customers that got recommendations from 1 to 4
    recom_less5_df = final_df6[["bnid", "Rank"]].groupby(["bnid"]).max().reset_index()
    recom_less5_df = recom_less5_df[recom_less5_df["Rank"] < 5]
    recom_less5_bnid = recom_less5_df[recom_less5_df["Rank"] < 5]["bnid"].unique()

    # filtering customers that got zero recommendations
    bnid_priced = final_df4.bnid.unique()
    non_priced_bnid = final_df3[~final_df3.bnid.isin(bnid_priced)]["bnid"].unique()
    recom_zero_df = pd.DataFrame()

    recom_zero_df["bnid"] = non_priced_bnid
    recom_zero_df["Rank"] = np.repeat(0, len(non_priced_bnid), axis=None)
    recom_df = recom_less5_df.append(recom_zero_df)
    recom_df.columns = ["bnid", "products_recommended"]

    final_df3_1 = pd.merge(final_df3, recom_df, on="bnid", how="inner")

    final_df5_1 = final_df3_1[
        [
            "bnid",
            "Recommended_items",
            "recommended_avg_price",
            "Score",
            "products_recommended",
        ]
    ].drop_duplicates()

    final_df5_1["for_rank"] = (
        final_df5_1["Score"] * 10000000 + final_df5_1["recommended_avg_price"]
    )
    final_df5_1["Rank"] = final_df5_1.groupby("bnid")["for_rank"].rank(
        "first", ascending=False
    )
    final_df6_1 = final_df5_1[
        [
            "bnid",
            "Recommended_items",
            "recommended_avg_price",
            "Rank",
            "products_recommended",
        ]
    ].drop_duplicates()

    final_df6_1 = final_df6_1[
        final_df6_1.Rank <= (5 - final_df6_1.products_recommended)
    ]
    final_df6_1["Rank"] = final_df6_1["Rank"] + final_df6_1["products_recommended"]
    final_df6_1 = final_df6_1[
        ["bnid", "Recommended_items", "recommended_avg_price", "Rank"]
    ].drop_duplicates()

    final_df7_1 = final_df6[final_df6.bnid.isin(recom_less5_bnid)]

    ultimate_df2 = final_df7_1.append(final_df6_1)

    ultimate_df = ultimate_df1.append(ultimate_df2)

    ##finding the Merch_product_category and product_class_name for each recommendation
    df7 = df_main[
        ["offer_id", "Merch_product_category", "product_class_name"]
    ].drop_duplicates()
    final_df8 = pd.merge(
        ultimate_df, df7, left_on="Recommended_items", right_on="offer_id", how="left"
    )
    final_df8.drop("offer_id", axis=1, inplace=True)

    # saving results
    final_df8.to_csv(
        output_path
        + "jewellery_recommendation_user_product_feature"
        + scoring_date
        + ".csv"
    )
    
    print("Jewellery is done")

if __name__ == "__main__":
    params = json.load(open("jewellery.json"))
    jewellery_recommendation_user_product_scoring(params)
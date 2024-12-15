import pandas as pd
import numpy as np
import json
import scipy.sparse
import scipy
import yaml
from lightfm import LightFM
from scipy import sparse
from itertools import product
from google.cloud import bigquery as bgq
from preprocess import preprocess


def jewellery_recommendation_scoring(params):
    input_file = params["input_file"]
    output_path = params["output_path"]
    scoring_date = params["scoring_date"]

    # Reading raw data from bigquery
    df = pd.read_csv(input_file)
    #df = pd.concat(map(pd.read_csv, ["gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000000", "gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000001", "gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000002", "gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000003", "gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000004", "gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000005", "gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000006", "gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000007", "gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000008", "gs://us-central1-prod-7440cabe-bucket/data/recommendation/input/jewellery_recommendation.csv000000000009"]))

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

    interaction_matrix = preprocess(
        df_bnid_offer_count, df_category
    ).create_interaction_matrix("bnid", "offer_id", "view_cnt")

    # creating user_dictonary
    user_dict = preprocess(df_bnid_offer_count, df_category).create_user_dict(
        interaction_matrix
    )

    # creating item dictonary
    item_dict = preprocess(df_bnid_offer_count, df_category).create_item_dict(
        "offer_id_IDENTIFIER", "offer_id"
    )

    # creating Model
    recommender = preprocess(df_bnid_offer_count, df_category).runMF(interaction_matrix)

    bnid_toscore = df_toscore[
        df_toscore["bnid"].isin(df_bnid_offer_count["bnid"].unique())
    ]["bnid"].unique()
    counter = 0
    import time

    final_df = pd.DataFrame()
    # int_df=pd.DataFrame()
    category_column = "offer_id"
    for i in bnid_toscore:
        known_products, recommended_products_df = preprocess(
            df_bnid_offer_count, df_category
        ).sample_recommendation_user(
            recommender, interaction_matrix, i, user_dict, item_dict, category_column
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

    final_df8.to_csv(output_path)
    
    print("jewelry is done")

if __name__ == "__main__":
    params = json.load(open("jewellery.json"))
    jewellery_recommendation_scoring(params)
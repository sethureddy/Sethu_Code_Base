import pandas as pd
import numpy as np
import json

from lightfm import LightFM
from scipy import sparse
from itertools import product
from google.cloud import bigquery as bgq
from preprocess_product_features import preprocess_product_features

DIAMOND_PRICE = "bnile-cdw-prod.dssprod_o_diamond.diamond_daily_data"


def diamond_recommendation_product_scoring(paramas):
    """
    recommmendation of diamonds on the basis
    of user-item interaction and item features
    """
    input_file = params["input_file"]
    output_path = params["output_path"]
    scoring_date = paramas["scoring_date"]

    # Reading raw data from bigquery
    df = pd.read_csv(input_file)

    # Pulling Diamond prices from Diamond Daily Data
    client = bgq.Client()
    sql = """SELECT distinct lower(SKU) as SKU_ID, cast(USD_PRICE as int64) as avg_price FROM `{DIAMOND_PRICE}` where CAPTURE_DATE_KEY = {df_date} and usd_price is not null """.format(
        df_date=df.date_key.max(), DIAMOND_PRICE=DIAMOND_PRICE
    )
    df_price = client.query(sql).to_dataframe(progress_bar_type="tqdm")
    df_price["avg_price"] = df_price["avg_price"].astype(float)

    # Carat Categorisation
    df1 = df[df["DIAMOND_CARAT_WEIGHT"] <= 3]
    df2 = df[df["DIAMOND_CARAT_WEIGHT"] > 3]
    df1["DIAMOND_CARAT"] = (df1["DIAMOND_CARAT_WEIGHT"] / 0.1).astype("int64")
    df2["DIAMOND_CARAT"] = ((df2["DIAMOND_CARAT_WEIGHT"] / 0.5) + 25).astype("int64")
    df = df1.append(df2)

    # Creating Categories
    cols = [
        "DIAMOND_CUT",
        "DIAMOND_COLOR",
        "DIAMOND_CLARITY",
        "DIAMOND_SHAPE",
        "DIAMOND_CARAT",
    ]
    uniques = [df[i].unique().tolist() for i in cols]
    cartesian = pd.DataFrame(product(*uniques), columns=cols)
    cartesian.reset_index(inplace=True)
    cartesian.columns = [
        "DIAMOND_CATEGORY",
        "DIAMOND_CUT",
        "DIAMOND_COLOR",
        "DIAMOND_CLARITY",
        "DIAMOND_SHAPE",
        "DIAMOND_CARAT",
    ]

    # merging combinations with old dataframe
    df_main = df.merge(
        cartesian,
        how="left",
        on=[
            "DIAMOND_CUT",
            "DIAMOND_COLOR",
            "DIAMOND_CLARITY",
            "DIAMOND_SHAPE",
            "DIAMOND_CARAT",
        ],
    )

    # Conversion to date
    df_main["date_key"] = pd.to_datetime(
        df_main["date_key"], format="%Y%m%d", errors="ignore"
    )

    # Shorlisting for 6 months
    df_shortlisted = df_main.copy()
    df_toscore = df_main[(df_main["date_key"] == df_main.date_key.max())]
    price_category_mapping = pd.merge(
        df_shortlisted[["SKU_ID", "DIAMOND_CATEGORY"]].drop_duplicates(),
        df_price,
        how="left",
        on="SKU_ID",
    )
    df_bnid_cat_count = (
        df_shortlisted.groupby(["bnid", "DIAMOND_CATEGORY"])
        .agg({"view_cnt": "sum"})
        .reset_index()
    )
    df_category = (
        df_main[["DIAMOND_CATEGORY", "DIAMOND_CATEGORY"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    df_category.columns = ["DIAMOND_CATEGORY_IDENTIFIER", "DIAMOND_CATEGORY"]
    df_category["DIAMOND_CATEGORY"] = df_category["DIAMOND_CATEGORY"].astype(str)

    # Creating the dataframe into 0's and 1's, this is the interaction matrix that needs to be fed into the light fm model mandatory
    interaction_matrix = preprocess_product_features(
        df_bnid_cat_count, df_category
    ).create_interaction_matrix("bnid", "DIAMOND_CATEGORY", "view_cnt")

    # creating user_dictonary
    user_dict = preprocess_product_features(
        df_bnid_cat_count, df_category
    ).create_user_dict(interaction_matrix)

    # creating item dictonary
    item_dict = preprocess_product_features(
        df_bnid_cat_count, df_category
    ).create_item_dict("DIAMOND_CATEGORY_IDENTIFIER", "DIAMOND_CATEGORY")

    # creating Feature Matrix by dummies
    diamond_item_list = [
        "DIAMOND_CATEGORY",
        "DIAMOND_CUT",
        "DIAMOND_COLOR",
        "DIAMOND_CLARITY",
        "DIAMOND_CARAT",
        "DIAMOND_SHAPE",
    ]
    diamond_features = [
        "DIAMOND_CUT",
        "DIAMOND_COLOR",
        "DIAMOND_CLARITY",
        "DIAMOND_CARAT",
        "DIAMOND_SHAPE",
    ]
    df_diamond_item_list = df_main[diamond_item_list]
    df_item_dummies = pd.get_dummies(df_diamond_item_list, columns=diamond_features)
    df_group_diamond_cat = df_item_dummies.groupby(by="DIAMOND_CATEGORY").sum()
    item_ids = np.array(df_item_dummies.columns)

    # Converting the data into 0's and 1's
    threshold = 0
    df_item_sparse = df_group_diamond_cat.applymap(lambda x: 1 if x > threshold else 0)
    feature_matrix = sparse.csr_matrix(df_item_sparse.values)

    # creating Model
    recommender = preprocess_product_features(df_bnid_cat_count, df_category).runMF(
        interaction_matrix, feature_matrix
    )
    bnid_toscore = df_toscore[
        df_toscore["bnid"].isin(df_bnid_cat_count["bnid"].unique())
    ]["bnid"].unique()
    final_df = pd.DataFrame()
    category_column = "DIAMOND_CATEGORY"
    for i in bnid_toscore:
        known_products, recommended_products_df = preprocess_product_features(
            df_bnid_cat_count, df_category
        ).sample_recommendation_user(
            recommender,
            interaction_matrix,
            feature_matrix,
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
    final_df = final_df[["BNID", "DIAMOND_CATEGORY", "Score"]]
    final_df.columns = ["bnid", "Recommended_Categories", "Score"]
    final_df = final_df.reset_index(drop=True)[
        ["bnid", "Recommended_Categories", "Score"]
    ]

    # mapping price with recommended categories
    user_cat_sku_mapping = df_shortlisted[
        ["bnid", "DIAMOND_CATEGORY", "SKU_ID"]
    ].drop_duplicates()
    cat_sku_mapping = df_shortlisted[["DIAMOND_CATEGORY", "SKU_ID"]].drop_duplicates()
    user_cat_sku_price_mapping = pd.merge(
        user_cat_sku_mapping, df_price, how="inner", on=["SKU_ID"]
    )
    cat_sku_price_mapping = pd.merge(
        cat_sku_mapping, df_price, how="inner", on=["SKU_ID"]
    )

    # mapping final recommendations with mapped prices
    final_df1 = pd.merge(
        final_df, user_cat_sku_price_mapping[["bnid", "SKU_ID", "avg_price"]], on="bnid"
    )
    final_df1.columns = [
        "bnid",
        "Recommended_Categories",
        "Score",
        "SKU_ID_KNOWN",
        "known_avg_price",
    ]
    final_df2 = pd.merge(
        final_df1,
        cat_sku_price_mapping,
        left_on=["Recommended_Categories"],
        right_on=["DIAMOND_CATEGORY"],
    )
    final_df2.columns = [
        "bnid",
        "Recommended_Categories",
        "Score",
        "SKU_ID_KNOWN",
        "known_avg_price",
        "DIAMOND_CATEGORY",
        "SKU_ID_Recommended",
        "recommended_avg_price",
    ]

    # bnids with skus not falling under diamond_daily_data
    final_bnids_df = (
        final_df2[["bnid", "SKU_ID_Recommended"]]
        .drop_duplicates()
        .groupby("bnid")
        .count()
        .reset_index()
    )
    final_bnids = final_bnids_df[final_bnids_df.SKU_ID_Recommended >= 5][
        "bnid"
    ].unique()
    final_df2 = final_df2[final_df2.bnid.isin(final_bnids)]

    # Price mapping with skus
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

    # merging dataframe with price mapping
    final_df3 = pd.merge(final_df2, user_price_min_max, on="bnid")

    # Subsetting price
    final_df4 = final_df3[
        (final_df3["recommended_avg_price"] > final_df3[("known_avg_price_min", "")])
        & (final_df3["recommended_avg_price"] < final_df3[("known_avg_price_max", "")])
    ]
    final_df5 = final_df4[
        ["bnid", "SKU_ID_Recommended", "recommended_avg_price", "Score"]
    ].drop_duplicates()

    # Ranking Diamonds on bases of price for each bnid
    final_df5["for_rank"] = (
        final_df5["Score"] * 10000000 + final_df5["recommended_avg_price"]
    )
    final_df5["Rank"] = final_df5.groupby("bnid")["for_rank"].rank(
        "first", ascending=False
    )
    final_df6 = final_df5[
        ["bnid", "SKU_ID_Recommended", "recommended_avg_price", "Rank", "Score"]
    ].drop_duplicates()

    # Filtering top5 Diamonds based on rankings
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

    # Ranking Diamonds on bases of price for each bnid
    recom_df.columns = ["bnid", "products_recommended"]
    final_df3_1 = pd.merge(final_df3, recom_df, on="bnid", how="inner")
    final_df5_1 = final_df3_1[
        [
            "bnid",
            "SKU_ID_Recommended",
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
            "SKU_ID_Recommended",
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
        ["bnid", "SKU_ID_Recommended", "recommended_avg_price", "Rank"]
    ].drop_duplicates()
    final_df7_1 = final_df6[final_df6.bnid.isin(recom_less5_bnid)]

    # appending recommendations
    ultimate_df2 = final_df7_1.append(final_df6_1)
    ultimate_df = ultimate_df1.append(ultimate_df2)
    merch_categories = df_shortlisted[
        ["SKU_ID", "DIAMOND_SHAPE"]
    ].drop_duplicates()
    ultimate_df = ultimate_df.merge(
        merch_categories, left_on="SKU_ID_Recommended", right_on="SKU_ID", how="inner"
    )
    ultimate_df = ultimate_df[
        [
            "bnid",
            "SKU_ID_Recommended",
            "recommended_avg_price",
            "Rank",
            "DIAMOND_SHAPE",
        ]
    ]

    # saving recommendation results
    ultimate_df.to_csv(output_path)
    
    print("Diamond is done")

if __name__ == "__main__":
    params = json.load(open("rc2_diamond.json"))
    diamond_recommendation_product_scoring(params)

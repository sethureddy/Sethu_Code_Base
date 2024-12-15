import pandas as pd
import numpy as np
import scipy.sparse
import scipy
import yaml
import json


from lightfm import LightFM
from scipy import sparse
from preprocess_item_similarity import preprocess_item_similarity


def item_item_similarity_jewellery(params):
    """
     Creating the item-item recommendation 
     using cosine-similarity
    """
    input_file = params["input_file"]
    output_path = params["output_path"]

    # Reading raw data from bigquery
    df = pd.read_csv(input_file)

    # Aggregating on bnid and offer level for interaction matrix
    df_bnid_offer_view = (
        df.groupby(["bnid", "offer_id"]).agg({"view_cnt": "sum"}).reset_index()
    )

    # Creating the dataframe for offer id
    df_category = df[["offer_id", "offer_id"]].drop_duplicates().reset_index(drop=True)
    df_category.columns = ["offer_id_IDENTIFIER", "offer_id"]
    df_category["offer_id"] = df_category["offer_id"].astype(str)

    # Creating the dataframe into 0's and 1's, this is the interaction matrix that needs to be fed into the light fm model mandatory
    interaction_matrix = preprocess_item_similarity(
        df_bnid_offer_view, df_category
    ).create_interaction_matrix("bnid", "offer_id", "view_cnt")

    # creating user_dictonary
    user_dict = preprocess_item_similarity(
        df_bnid_offer_view, df_category
    ).create_user_dict(interaction_matrix)

    # creating item dictonary
    item_dict = preprocess_item_similarity(
        df_bnid_offer_view, df_category
    ).create_item_dict("offer_id_IDENTIFIER", "offer_id")

    # creating the Model object
    recommender = preprocess_item_similarity(df_bnid_offer_view, df_category).runMF(
        interaction_matrix
    )

    # creating item_emdedding usine cosne similarity
    item_item_dist = preprocess_item_similarity(
        df_bnid_offer_view, df_category
    ).create_item_emdedding_distance_matrix(recommender, interaction_matrix)

    # creating recommendation for all offer ids
    recommended_offer_list = []

    for offer_id in df["offer_id"].unique():

        recommended_offers = preprocess_item_similarity(
            df_bnid_offer_view, df_category
        ).item_item_recommendation(item_item_dist, offer_id, item_dict, n_items=10)

        items = [offer_id.astype(int)] + recommended_offers

        dict_item_list = {}
        dict_item_list.update(
            {"Recommended Items": items}
        )  # updating the Recommended dictionary for each offer_id
        recommended_offer_list.append(items)

    # Creating the DataFrame and renaming the columns
    df_offerid_recommended = pd.DataFrame(recommended_offer_list)
    df_offerid_recommended.columns = [
        "Seed_OID",
        "OID_rank1",
        "OID_rank2",
        "OID_rank3",
        "OID_rank4",
        "OID_rank5",
        "OID_rank6",
        "OID_rank7",
        "OID_rank8",
        "OID_rank9",
        "OID_rank10",
    ]
    df_offerid_recommended["OID_type"] = "jewelery"

    # Saving Item-item Similarities
    df_offerid_recommended.to_csv(output_path + "jewellery_output.csv", index=False)

if __name__ == "__main__":
    params = json.load(open("jewellery.json"))
    item_item_similarity_jewellery(params)

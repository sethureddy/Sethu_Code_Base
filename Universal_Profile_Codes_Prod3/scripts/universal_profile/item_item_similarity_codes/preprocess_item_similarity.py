import pandas as pd
import numpy as np
import scipy.sparse
import scipy

from lightfm import LightFM
from scipy import sparse
from sklearn.metrics.pairwise import cosine_similarity


class preprocess_item_similarity:

    norm = False
    threshold = None
    n_components = 30
    loss = "warp"
    k = 15
    epoch = 30
    n_jobs = 4

    def __init__(self, df_to_interaction, df_to_item_dict):
        self.df_to_interaction = df_to_interaction
        self.df_to_item_dict = df_to_item_dict

    def create_interaction_matrix(self, user_col, item_col, views):
        """
        Function to create an interaction matrix dataframe from transactional type interactions
        Required Input -
            - df = Pandas DataFrame containing user-item interactions
            - user_col = column name containing user's identifier
            - item_col = column name containing item's identifier
            - views col = column name containing user views on interaction with a given item
            - norm (optional) = True if a normalization of ratings is needed
            - threshold (required if norm = True) = value above which the rating is favorable
        Expected output - 
            - Pandas dataframe with user-item interactions ready to be fed in a recommendation algorithm
        """
        interactions = (
            self.df_to_interaction.groupby([user_col, item_col])[views]
            .sum()
            .unstack()
            .reset_index()
            .fillna(0)
            .set_index(user_col)
        )
        if preprocess_item_similarity.norm:
            interactions = interactions.applymap(
                lambda x: 1 if x > preprocess_item_similarity.threshold else 0
            )
        return interactions

    def create_user_dict(self, interactions):
        """
        Function to create a user dictionary based on their index and number in interaction dataset
        Required Input - 
            interactions - dataset create by create_interaction_matrix
        Expected Output -
            user_dict - Dictionary type output containing interaction_index as key and user_id as value
        """
        user_id = list(interactions.index)
        user_dict = {}
        counter = 0
        for i in user_id:
            user_dict[i] = counter
            counter += 1
        return user_dict

    def create_item_dict(self, id_col, name_col):
        """
        Function to create an item dictionary based on their item_id and item name
        Required Input - 
            - df_category = Pandas dataframe with Item information
            - id_col = Column name containing unique identifier for an item
            - name_col = Column name containing name of the item
        Expected Output -
            item_dict = Dictionary type output containing item_id as key and item_name as value
        """
        item_dict = {}
        for i in range(self.df_to_item_dict.shape[0]):
            item_dict[(self.df_to_item_dict.loc[i, id_col])] = self.df_to_item_dict.loc[
                i, name_col
            ]
        return item_dict

    def runMF(self, interaction_matrix):
        """
        Function to run matrix-factorization algorithm
        Required Input -
            - interaction_matrix = dataset create by create_interaction_matrix
            - n_components = number of embeddings you want to create to define Item and user
            - loss = loss function other options are logistic, brp
            - epoch = number of epochs to run 
            - n_jobs = number of cores used for execution 
        Expected Output  -
            Model - Trained model
        """
        x = sparse.csr_matrix(interaction_matrix.values)
        model = LightFM(
            no_components=preprocess_item_similarity.n_components,
            loss=preprocess_item_similarity.loss,
            k=preprocess_item_similarity.k,
        )
        model.fit(
            x,
            epochs=preprocess_item_similarity.epoch,
            num_threads=preprocess_item_similarity.n_jobs,
        )
        return model

    def create_item_emdedding_distance_matrix(self, model, interactions):
        """
        Function to create item-item distance embedding matrix
        Required Input -
            - model = Trained matrix factorization model
            - interactions = dataset used for training the model
        Expected Output -
            - item_emdedding_distance_matrix = Pandas dataframe containing cosine distance matrix b/w items
        """
        df_item_norm_sparse = sparse.csr_matrix(model.item_embeddings)
        similarities = cosine_similarity(df_item_norm_sparse)
        item_emdedding_distance_matrix = pd.DataFrame(similarities)
        item_emdedding_distance_matrix.columns = interactions.columns
        item_emdedding_distance_matrix.index = interactions.columns
        return item_emdedding_distance_matrix

    def item_item_recommendation(
        self, item_emdedding_distance_matrix, item_id, item_dict, n_items=10, show=True
    ):
        """
        Function to create item-item recommendation
        Required Input - 
            - item_emdedding_distance_matrix = Pandas dataframe containing cosine distance matrix b/w items
            - item_id  = item ID for which we need to generate recommended items
            - item_dict = Dictionary type input containing item_id as key and item_name as value
            - n_items = Number of items needed as an output
        Expected Output -
            - recommended_items = List of recommended items
        """
        recommended_items = list(
            pd.Series(
                item_emdedding_distance_matrix.loc[item_id, :]
                .sort_values(ascending=False)
                .head(n_items + 1)
                .index[1 : n_items + 1]
            )
        )
        if show == True:
            counter = 1
            for i in recommended_items:
                counter += 1
        return recommended_items

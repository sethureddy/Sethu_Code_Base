import pandas as pd
import numpy as np
import scipy.sparse 
import scipy
import yaml

from lightfm import LightFM
from scipy import sparse
from itertools import product


class preprocess_product_features:
    
    norm = False
    threshold = None
    n_components=10
    loss='warp'
    k=5
    epoch=30
    n_jobs = 4
    threshold_recom = 0
    nrec_items = 5
    show = True
    
    def __init__(self,df_to_interaction,df_to_item_dict):
        self.df_to_interaction = df_to_interaction
        self.df_to_item_dict = df_to_item_dict
        
    def create_interaction_matrix(self,user_col,item_col,views):
        '''
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
        '''
        interactions = self.df_to_interaction.groupby([user_col,item_col])[views].sum().unstack().reset_index().fillna(0).set_index(user_col)
        if preprocess_product_features.norm:
            interactions = interactions.applymap(lambda x: 1 if x > preprocess_product_features.threshold else 0)
        return interactions

    def create_user_dict(self,interactions):
        '''
        Function to create a user dictionary based on their index and number in interaction dataset
        Required Input - 
            interactions - dataset create by create_interaction_matrix
        Expected Output -
            user_dict - Dictionary type output containing interaction_index as key and user_id as value
        '''
        user_id = list(interactions.index)
        user_dict = {}
        counter = 0 
        for i in user_id:
            user_dict[i] = counter
            counter += 1
        return user_dict
    
    def create_item_dict(self,id_col,name_col):
        '''
        Function to create an item dictionary based on their item_id and item name
        Required Input - 
            - df_category = Pandas dataframe with Item information
            - id_col = Column name containing unique identifier for an item
            - name_col = Column name containing name of the item
        Expected Output -
            item_dict = Dictionary type output containing item_id as key and item_name as value
        '''
        item_dict ={}
        for i in range(self.df_to_item_dict.shape[0]):
            item_dict[(self.df_to_item_dict.loc[i,id_col])] = self.df_to_item_dict.loc[i,name_col]
        return item_dict
    
    
    def runMF(self,interaction_matrix,feature_matrix):
        '''
        Function to run matrix-factorization algorithm
        Required Input -
            - interaction_matrix = dataset create by create_interaction_matrix
            - n_components = number of embeddings you want to create to define Item and user
            - loss = loss function other options are logistic, brp
            - epoch = number of epochs to run 
            - n_jobs = number of cores used for execution 
        Expected Output  -
            Model - Trained model
        '''
        x = sparse.csr_matrix(interaction_matrix.values)
        model = LightFM(no_components= preprocess_product_features.n_components, loss=preprocess_product_features.loss,k=preprocess_product_features.k)
        model.fit(x,item_features = feature_matrix, epochs=preprocess_product_features.epoch,num_threads = preprocess_product_features.n_jobs)
        return model
    
    def sample_recommendation_user(self,model, interactions, feature_matrix, user_id, user_dict,item_dict,category_column):
        '''
        Function to produce user recommendations
        Required Input - 
            - model = Trained matrix factorization model
            - interactions = dataset used for training the model
            - user_id = user ID for which we need to generate recommendation
            - user_dict = Dictionary type input containing interaction_index as key and user_id as value
            - item_dict = Dictionary type input containing item_id as key and item_name as value
            - threshold = value above which the rating is favorable in new interaction matrix
            - nrec_items = Number of output recommendation needed
        Expected Output - 
            - Prints list of items the given user has already bought
            - Prints list of N recommended items  which user hopefully will be interested in
        '''
        n_users, n_items = interactions.shape
        user_x = user_dict[user_id]
        scores = pd.Series(model.predict(user_x,np.arange(n_items),item_features=feature_matrix))
        scores.index = interactions.columns
        scores_df = pd.DataFrame(scores,columns=["Score"]).reset_index(drop=False).sort_values(by='Score',ascending=False)
        scores = list(pd.Series(scores.sort_values(ascending=False).index))
        known_categories = list(pd.Series(interactions.loc[user_id,:][interactions.loc[user_id,:] > preprocess_product_features.threshold_recom].index).sort_values(ascending=False))
        scores = [x for x in scores if x not in known_categories]
        return_score_list = scores[0:preprocess_product_features.nrec_items]
        return_score_df = scores_df[scores_df[category_column].isin(return_score_list)]
        known_categories = list(pd.Series(known_categories).apply(lambda x: item_dict[x]))
        scores = list(pd.Series(return_score_list).apply(lambda x: item_dict[x]))
        return known_categories,return_score_df
import pandas as pd
import numpy as np
import scipy.sparse 
import scipy

from lightfm import LightFM
from scipy import sparse
from itertools import product

class preprocess_user_features:
    
    norm = False
    threshold = None
    n_components=10
    loss='warp'
    k=5
    epoch=30
    n_jobs = 4
    threshold_recom = 0
    nrec_items = 10
    show = True
    
    def __init__(self,df_to_interaction,df_to_item_dict):
        self.df_to_interaction = df_to_interaction
        self.df_to_item_dict = df_to_item_dict
        
    def create_interaction_matrix(self,user_col,item_col,views):
        """
		Function to create an interaction matrix dataframe from transactional type interactions
		arguments :
			df(DataFrame) : Pandas DataFrame containing user-item interactions
			user_col(str) : column name containing user's identifier
			item_col(str) : column name containing item's identifier
			views col(str) : column name containing user views on interaction with a given item
			norm (optional) : True if a normalization of ratings is needed
			threshold (required if norm = True) : value above which the rating is favorable
		returns :
			Pandas dataframe with user-item interactions ready to be fed in a recommendation algorithm
        """
        interactions = self.df_to_interaction.groupby([user_col,item_col])[views].sum().unstack().reset_index().fillna(0).set_index(user_col)
        if preprocess_user_features.norm:
            interactions = interactions.applymap(lambda x: 1 if x > preprocess_user_features.threshold else 0)
        return interactions

    def create_user_dict(self,interactions):
        """
		Function to create a user dictionary based on their index and number in interaction dataset
		arguments :
			interactions(DataFrame) : dataset create by create_interaction_matrix
		returns :
			user_dict(Dictionary) : Dictionary type output containing interaction_index as key and user_id as value
        """
        user_id = list(interactions.index)
        user_dict = {}
        counter = 0 
        for i in user_id:
            user_dict[i] = counter
            counter += 1
        return user_dict
    
    def create_item_dict(self,id_col,name_col):
        """
		Function to create an item dictionary based on their item_id and item name
		arguments :
			df_category(DataFrame) : Pandas dataframe with Item information
			id_col(str) : Column name containing unique identifier for an item
			name_col(str) : Column name containing name of the item
		returns : 
			item_dict(Dictionary) : Dictionary type output containing item_id as key and item_name as value
        """
        item_dict ={}
        for i in range(self.df_to_item_dict.shape[0]):
            item_dict[(self.df_to_item_dict.loc[i,id_col])] = self.df_to_item_dict.loc[i,name_col]
        return item_dict
    
    
    def runMF(self,interaction_matrix,feature_matrix,user_matrix):
        """
		Function to run matrix-factorization algorithm
		arguments :
			interaction_matrix(DataFrame) : dataset create by create_interaction_matrix
			n_components(int) : number of embeddings you want to create to define Item and user
			loss(float) : loss function other options are logistic, brp
			epoch(int) : number of epochs to run 
			n_jobs(int) : number of cores used for execution 
			feature_matrix(sparse matrix) : contains the product information
			user_matrix(sparse matrix) : contains the user information
		returns :
			Model(object) : Trained model
        """
        x = sparse.csr_matrix(interaction_matrix.values)
        model = LightFM(no_components= preprocess_user_features.n_components, loss=preprocess_user_features.loss,k=preprocess_user_features.k)
        model.fit(x,item_features=feature_matrix,user_features = user_matrix,epochs=preprocess_user_features.epoch,num_threads = preprocess_user_features.n_jobs)
        return model
    
	
def create_categories(class_name):
    """
	Function to Create Categories based on Product Class Name
	arguments :
		class_name(str) : Product Class Name
	returns :
		Returns Defined Category to Create price buckets and for recommendations
    """
    if class_name in ('Bracelets'):
        return 'Bracelets'
    elif class_name in ('Stud Earrings', 'Earrings','Fashion Earrings'):
        return 'Earrings'
    elif class_name in ('BYO Ring','BYO 3 Stone', 'Complete 3 Stone', 'Solitaires','Semi-Mount','Complete Solitaire', 'Pre'):
        return 'Engagement Ring'
    elif class_name in ('Loose Diamonds'):
        return 'Loose Diamonds'
    elif class_name in ('Pendants','Necklaces','SolitairePendant','Fashion Pendants'):
        return 'Necklaces'
    elif class_name in ('BYO 5 Stone','Diamond Band/Ring','Plain Bands','Fash Ring','Rings'):
        return 'NonEngagement Ring'
    else:
        return 'Other'

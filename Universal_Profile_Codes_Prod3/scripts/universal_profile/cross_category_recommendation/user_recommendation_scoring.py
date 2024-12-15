import pandas as pd
import numpy as np
from tqdm import tqdm
import json
import scipy.sparse
import scipy
import yaml

from lightfm import LightFM
from sklearn.metrics.pairwise import cosine_similarity

from scipy import sparse
from itertools import product
from google.cloud import bigquery as bgq
from preprocess_user_features import preprocess_user_features
from preprocess_user_features import create_categories

def create_jewellery_matrices(df):
	"""
	Creates Jewellery Interaction matrix with user and product feature tables
	arguments :
		df(DataFrame) : Jewellery Input DataFrame
	returns :
		interaction_matrix(DataFrame) : BNID and Offer ID view count dataframe
		feature_sparse_df(DataFrame) : Offer ID and product feature information
		user_sparse_df(DataFrame) : Users and their PII information
		df_bnid_offer_count(DataFrame) : Users, offerid and the corresponding view count 
		df_category(DataFrame): Offer ids
		df_main(DataFrame): Offer ids and corresponding price
		user_dict(Dict) : BNID dictonary
		item_dict(Dict) : Offer id dictonary
	"""

	# converting into offer_id to integer
	df["offer_id"] = df["offer_id"].astype("int64")
	df_main = df.copy()

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
		df_main[["offer_id", "offer_id"]].drop_duplicates()
	)
	df_category.columns = ["offer_id_IDENTIFIER", "offer_id"]
	df_category["offer_id"] = df_category["offer_id"].astype(str)


	top_offerids = df_bnid_offer_count.groupby('offer_id')['view_cnt'].sum().nlargest(5000).reset_index()  # Make the count of offer ids as a parameter
	df_bnid_offer_count = df_bnid_offer_count[df_bnid_offer_count['offer_id'].isin(top_offerids['offer_id'].unique())]
	df_category = df_category[df_category['offer_id_IDENTIFIER'].isin(top_offerids['offer_id'].unique())].reset_index(drop=True)

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
	df16 = df_main[["offer_id", "GRAM_WEIGHT"]]
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
	df_gramweight_matrix = df_gramweight_matrix.fillna(0)
	df_gramweight_matrix = df_gramweight_matrix.add_prefix("GRAM_WEIGHT_")

	###Merging the dataframes for product matrix

	merge_df = pd.merge(df_gramweight_matrix, df3_F, on="offer_id", how="outer")
	df3_total = merge_df.copy()
	feature_name = np.array(df3_total.reset_index()["offer_id"])
	item_ids = np.array(df2_F.columns)
	feature_df = pd.merge(df3_total, df_pcn, on="offer_id", how="outer")

	# Converting the data into 0's and 1's
	threshold = 0
	feature_sparse_df = feature_df.applymap(lambda x: 1 if x > threshold else 0)
	feature_sparse_df.reset_index(inplace=True)

	###creating user matrix
	df_user_feat = df_main[["bnid", "derived_gender", "add_to_basket", "item_purchase","marital_status"]]
	df_user_feat = df_user_feat.rename(columns={'derived_gender': 'gender'})
	ls_user_feat = ["gender", "add_to_basket", "item_purchase","marital_status"]
	df1_dummy = pd.get_dummies(df_user_feat, columns=ls_user_feat)
	df3_final = df1_dummy.groupby(by=["bnid"]).sum().reset_index()
	df3_final.index = df3_final.bnid
	df3_final.drop("bnid", axis=1, inplace=True)

	# Converting the data into 0's and 1's
	threshold = 0
	user_sparse_df = df3_final.applymap(lambda x: 1 if x > threshold else 0)
	interaction_matrix.reset_index(inplace=True)

	return interaction_matrix, feature_sparse_df, user_sparse_df, df_bnid_offer_count, df_category, df_main, user_dict, item_dict


DIAMOND_PRICE = "bnile-cdw-prod.dssprod_o_diamond.diamond_daily_data"


def create_diamond_matrices(df):
	"""
	Creates Diamond Interaction matrix with user and product feature tables
	arguments :
		df(DataFrame) : Diamond Input DataFrame
	returns :
		interaction_matrix(DataFrame) : BNID and Offer ID view count dataframe
		feature_sparse_df(DataFrame) : Offer ID and product feature information
		user_sparse_df(DataFrame) : Users and their PII information
		df_bnid_offer_count(DataFrame) : Users, offerid and the corresponding view count 
		df_category(DataFrame): Offer ids
		df_main(DataFrame): Offer ids and corresponding price
		user_dict(Dict) : BNID dictonary
		item_dict(Dict) : Offer id dictonary
	"""

	# Pulling Diamond prices from Diamond Daily Data
	client = bgq.Client()
	sql = """SELECT distinct lower(SKU) as SKU_ID, cast(USD_PRICE as int64) as avg_price FROM `{DIAMOND_PRICE}` where CAPTURE_DATE_KEY = {df_date} and usd_price is not null""".format(
	 df_date=df.date_key.max(), DIAMOND_PRICE=DIAMOND_PRICE
	)
	df_price = client.query(sql).to_dataframe(progress_bar_type="tqdm")
	df_price["avg_price"] = df_price["avg_price"].astype(float)

	# Carat Categorisation
	df1 = df[df["DIAMOND_CARAT_WEIGHT"] <= 3]
	df2 = df[df["DIAMOND_CARAT_WEIGHT"] > 3]
	df1["DIAMOND_CARAT"] = (df1["DIAMOND_CARAT_WEIGHT"]/0.1).astype("int64")
	df2["DIAMOND_CARAT"] = ((df2["DIAMOND_CARAT_WEIGHT"]/0.5) + 25).astype("int64")
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

	df_main['DIAMOND_CATEGORY'] = df_main['SKU_ID'].apply(lambda x: int(x[2:]))
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

	top_offerids = df_bnid_cat_count.groupby('DIAMOND_CATEGORY')['view_cnt'].sum().nlargest(5000).reset_index() # Make count of offer ids dynamic
	df_bnid_cat_count = df_bnid_cat_count[df_bnid_cat_count['DIAMOND_CATEGORY'].isin(top_offerids['DIAMOND_CATEGORY'].unique())]
	df_category = df_category[df_category['DIAMOND_CATEGORY_IDENTIFIER'].isin(top_offerids['DIAMOND_CATEGORY'].unique())]
	df_category.reset_index(drop=True,inplace=True)

	# Creating the dataframe into 0's and 1's, this is the interaction matrix that needs to be fed into the light fm model mandatory
	interaction_matrix = preprocess_user_features(
		df_bnid_cat_count, df_category
	).create_interaction_matrix("bnid", "DIAMOND_CATEGORY", "view_cnt")


	# creating user_dictonary
	user_dict = preprocess_user_features(
		df_bnid_cat_count, df_category
	).create_user_dict(interaction_matrix)

	# creating item dictonary
	item_dict = preprocess_user_features(
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
	feature_sparse_df = df_group_diamond_cat.applymap(lambda x: 1 if x > threshold else 0)
	feature_sparse_df.reset_index(inplace=True)

	# creating User-Features (User Matrix)
	df_user_feat = df_main[["bnid", "gender", "add_to_basket", "item_purchase","marital_status"]]
	ls_user_feat = ["gender", "add_to_basket", "item_purchase","marital_status"]
	df_user_dummies = pd.get_dummies(df_user_feat, columns=ls_user_feat)
	df_group_bnid_user = df_user_dummies.groupby(by=["bnid"]).sum().reset_index()
	df_group_bnid_user.index = df_group_bnid_user.bnid
	df_group_bnid_user.drop("bnid", axis=1, inplace=True)

	# Converting the data into 0's and 1's
	threshold = 0
	user_sparse_df = df_group_bnid_user.applymap(lambda x: 1 if x > threshold else 0)

	df_bnid_cat_count.rename(columns = {"DIAMOND_CATEGORY":"offer_id"},inplace=True)
	interaction_matrix.reset_index(inplace=True)
	return interaction_matrix, feature_sparse_df, user_sparse_df, df_bnid_cat_count, df_category, df_main, user_dict, item_dict

def create_setting_matrices(df):
	"""
	Creates Setting Interaction matrix with user and product feature tables
	arguments :
		df(dict) : Setting Input DataFrame
	returns :
		interaction_matrix(DataFrame) : BNID and Offer ID view count dataframe
		feature_sparse_df(DataFrame) : Offer ID and product feature information
		user_sparse_df(DataFrame) : Users and their PII information
		df_bnid_offer_count(DataFrame) : Users, offerid and the corresponding view count 
		df_category(DataFrame): Offer ids
		df_main(DataFrame): Offer ids and corresponding price
		user_dict(Dict) : BNID dictonary
		item_dict(Dict) : Offer id dictonary
	"""

	df['offer_id'] = df['offer_id'].astype(int)

	# Conversion to date
	df_main = df.copy()
	df_main["date_key"] = pd.to_datetime(
		df_main["date_key"], format="%Y%m%d", errors="ignore"
	)

	df_shortlisted = df_main.copy()
	df_toscore = df_main[(df_main["date_key"] == df_main.date_key.max())]

	# Selecting the price data
	df_price = df[["offer_id", "PRODUCT_LIST_PRICE"]].drop_duplicates()

	df_bnid_cat_count = (
		df_shortlisted.groupby(["bnid", "offer_id"])
		.agg({"view_cnt": "sum"})
		.reset_index()
	)

	df_category = (
		df_main[["offer_id", "offer_id"]].drop_duplicates().reset_index(drop=True)
	)
	df_category.columns = ["offer_id_IDENTIFIER", "offer_id"]
	df_category["offer_id"] = df_category["offer_id"].astype(str)

	# Creating the dataframe into 0's and 1's, this is the interaction matrix that needs to be fed into the light fm model mandatory
	interaction_matrix = preprocess_user_features(
		df_bnid_cat_count, df_category
	).create_interaction_matrix("bnid", "offer_id", "view_cnt")


	# creating user_dictonary
	user_dict = preprocess_user_features(
		df_bnid_cat_count, df_category
	).create_user_dict(interaction_matrix)

	# creating item dictonary
	item_dict = preprocess_user_features(
		df_bnid_cat_count, df_category
	).create_item_dict("offer_id_IDENTIFIER", "offer_id")

	# creating Product-Features (Feature Matrix)
	df = df_main.copy()
	offer_item_list = [
		"offer_id",
		"PRIMARY_METAL_COLOR",
		"PRIMARY_METAL_NAME",
		"PRIMARY_SHAPE_NAME",
		"PRIMARY_SETTING_TYPE",
		"PRIMARY_STONE_TYPE",
	]
	item_features = [
		"PRIMARY_METAL_COLOR",
		"PRIMARY_METAL_NAME",
		"PRIMARY_SHAPE_NAME",
		"PRIMARY_SETTING_TYPE",
		"PRIMARY_STONE_TYPE",
	]

	df_offer_item_list = df[offer_item_list]
	df_item_dummies = pd.get_dummies(df_offer_item_list, columns=item_features)
	df_group_offer_item = df_item_dummies.groupby(by="offer_id").sum()

	# Converting the data into 0's and 1's for feature matrix
	threshold = 0
	feature_sparse_df = df_group_offer_item.applymap(lambda x: 1 if x > threshold else 0)
	feature_sparse_df.reset_index(inplace=True)

	# creating User-Features (User Matrix)
	df_user_feat = df_main[["bnid", "gender", "add_to_basket", "item_purchase","marital_status"]]

	ls_user_feat = ["gender", "add_to_basket", "item_purchase","marital_status"]
	df_user_dummies = pd.get_dummies(df_user_feat, columns=ls_user_feat)
	df_group_bnid_user = df_user_dummies.groupby(by=["bnid"]).sum().reset_index()
	df_group_bnid_user.index = df_group_bnid_user.bnid
	df_group_bnid_user.drop("bnid", axis=1, inplace=True)

	# Converting the data into 0's and 1's
	threshold = 0
	user_sparse_df = df_group_bnid_user.applymap(lambda x: 1 if x > threshold else 0)
	interaction_matrix.reset_index(inplace=True)

	return interaction_matrix, feature_sparse_df, user_sparse_df, df_bnid_cat_count, df_category, df_main, user_dict, item_dict

def create_price_percentiles(cat_price_df,Price_Col):
	"""
	Creates Category level Percentile Ranges
	arguments :
		cat_price_df(DataFrame) : Price dataframe at category level
		Price_Col(str) : Price Column
	returns :
		percent_dict(Dict) : Dictionary with Percentiles and corresponding ranges
	"""

	percent_dict = {}
	for category in cat_price_df['Defined_Category'].unique():

		if category == "Bracelets":
			percentiles = cat_price_df[cat_price_df['Defined_Category'] == category][Price_Col
									  ].describe(percentiles = [i/100 for i in np.arange(0,100,5)])[['0%','30%','45%','60%','80%']]
		elif category == "Engagement Ring":
			percentiles = cat_price_df[cat_price_df['Defined_Category'].isin(["Engagement Ring", "Loose Diamonds"])][Price_Col
									  ].describe(percentiles = [i/100 for i in np.arange(0,100,10)])[['0%','20%','40%','60%','80%']]
		else:
			percentiles = cat_price_df[cat_price_df['Defined_Category'] == category][Price_Col
									  ].describe(percentiles = [i/100 for i in np.arange(0,100,10)])[['0%','20%','40%','60%','80%']]
		percent_list = []
		for i in range(len(percentiles)):
			try:
				percent_list.append([np.round(percentiles[i],0),np.round(percentiles[i+1])])
			except:
				percent_list.append([np.round(percentiles[i]),])
			percent_list[0][0] = 0
		percent_dict[category] = percent_list
	return percent_dict

def create_price_buckets(percentile_dict,price,defined_cat):
	"""
	Creates Category level Price Buckets
	arguments :
		percentile_dict(Dict) : Dictionary with Price percentile ranges
		price(float) : Price of product
		defined_cat(str) : Category
	returns :
		Returns Price Bucket based on Price and Category of the Product
	"""

	for i in range(0,len(percentile_dict[defined_cat])):
		try:
			if percentile_dict[defined_cat][i][0] <= price < percentile_dict[defined_cat][i][1]:
				return i+1
		except:
			return 5

def create_item_emdedding_distance_matrix(model,item_features,feature_df):
	"""
	Function to create item-item distance embedding matrix
	arguments :
		model : Trained matrix factorization model
		item_features : Feature sparse matrix
		feature_df : Feature sparse dataframe
	returns :
		item_emdedding_distance_matrix : Pandas dataframe containing cosine distance matrix b/w items
	"""
	
	df_item_norm_sparse = item_features.dot(model.item_embeddings)
	similarities = cosine_similarity(df_item_norm_sparse)
	item_emdedding_distance_matrix = pd.DataFrame(similarities)
	item_emdedding_distance_matrix.columns = feature_df.index
	item_emdedding_distance_matrix.index = feature_df.index
	return item_emdedding_distance_matrix

def subset_bnids(df):
	"""
	Returns Active BNIDs who visited Bluenile site yesterday
	arguments :
		df(DataFrame) : Input category Dataframe
	returns :
		lastday_bnids(List) : BNIDs active as of yesterday
	"""

	lastday_bnids =  df[df['date_key'] == df['date_key'].max()]['bnid'].unique().tolist()
	return lastday_bnids

def get_lastbrowsed_category(df,product_col,diamond=False):
	"""
	Returns the last browsed category of the active users
	arguments :
		df(DataFrame) : Input category Dataframe
		product_col(str) : Product Class Name
	returns :
		sub_df(DataFrame) : Dataframe with Last Browsed Category
	"""

	if diamond:
		df = df[df['date_key'] == df['date_key'].max()]
		sub_df = df[['bnid','date_key','Defined_Category']]
	else:
		df['Defined_Category'] = df.apply(lambda x: create_categories(x[product_col]),axis=1)
		sub_df = df[df['date_key'] == df['date_key'].max()]
		sub_df = sub_df[['bnid','date_key','Defined_Category']]
	return sub_df
	
def get_purchase_info(bnid,purchase_df,price_df,inactive=True):
	"""
	Returns purchase information of the user 
	arguments :
		bnid(str) : Blue nile ID
		purchase_df(DataFrame) : Purchase information of all the users in last 2 years
		price_df(DataFrame) : Price Information of all the offer ids
	returns :
		similar_product(float) : Last Purchased Product
		purchase_category(str) : Last Purchased Category of the User
		price_bucket(int) : Last purchase price bucket based on the category purchased
	"""
	
	if inactive:
		purchase_df = purchase_df[purchase_df['order_date'].isin([dt1.strftime('%Y-%m-%d'),dt2.strftime('%Y-%m-%d')])]
	ids = purchase_df[purchase_df['bnid'] == bnid][:1][['OFFER_ID','SETTING_OFFER_ID','SKU']].dropna(axis=1).values[0]
	prod_id = [id for id in ids if id!=0][0]
	if prod_id == 0.0 and len(ids)==2 :
		offer_id = int(prod_id[2:])
	elif type(prod_id) == str:
		offer_id = int(prod_id[2:])
	else:
		offer_id = int(prod_id)

	sub_df = price_df[price_df['offer_id']==offer_id]

	if sub_df.shape[0] > 0:
		similar_product = sub_df['offer_id'].values[0]

	if sub_df.empty:
		try:
			offer_list = price_df['offer_id'].unique().tolist()
			similar_product = offer_list[np.argmin([np.abs(i-offer_id) for i in offer_list])]
		except:
			similar_product = similar_product[0]

	lastproduct_price = purchase_df[purchase_df['bnid'] == bnid]['usd_after_disc_sale_amount'].values[0]
	purchase_category = price_df[price_df['offer_id'] == similar_product]['Defined_Category'].values[0]
	price_bucket = price_df.iloc[price_df[price_df['Defined_Category']==purchase_category]['PRODUCT_LIST_PRICE'].sub(lastproduct_price).abs().idxmin()]['Price_Bucket']

	return similar_product, purchase_category, price_bucket

def get_inactive_data(item_item_dist,similar_product,purchase_category,price_bucket):
	"""
	Returns Product Similarity DataFrame of Inactive Users for recommendation
	arguments :
		item_item_dist(DataFrame) : Item Similarity dataframe
		similar_product(float) : Last Purchased Product
		purchase_category(str) : Last Purchased Category of the User
		price_bucket(int) : Last purchase price bucket based on the category purchased
	returns :
		similarity_df(DataFrame) : Product Similarity dataframe
	"""

	similarity_df = item_item_dist[similar_product].sort_values(ascending =False).reset_index()
	similarity_df = pd.merge(similarity_df,price_df, on='offer_id',how='left')
	return similarity_df

def get_active_data(bnid, model, interaction_matrix, feature_matrix, user_matrix, similar_product, purchase_category, price_bucket):
	"""
	Returns Product Similarity DataFrame of Active Users for recommendation
	arguments :
		bnid(str) : Blue nile ID
		model(object) : Light FM recommendation model
		interaction_matrix(DataFrame) : Interaction Matrix
		feature_matrix(sparse matrix) : Product Feature information
		user_matrix(sparse matrix) : User PII information
		similar_product(float) : Last Purchased Product
		purchase_category(str) : Last Purchased Category of the User
		price_bucket(int) : Last purchase price bucket based on the category purchased
	returns :
		similarity_df(DataFrame) : Product Similarity dataframe
	"""
	n_users, n_items = interaction_matrix.shape
	user_x = user_dict[bnid]
	scores = pd.Series(recommender.predict(user_x,np.arange(n_items),item_features=feature_matrix,user_features=user_matrix))
	scores.index = interaction_matrix.columns
	scores_df = pd.DataFrame(scores,columns=["Score"]).reset_index(drop=False).sort_values(by='Score',ascending=False)
	if 'index' in scores_df.columns:
		scores_df.rename(columns= {'index':'offer_id'},inplace=True)
	similarity_df = pd.merge(scores_df,price_df, on='offer_id',how='left')

	return similarity_df
   
def get_recommendation(similar_product,similarity_data,category,bucket,match_productdf,price_df,last_browsed,active=False):
	"""
	Returns Product Similarity DataFrame of Active Users for recommendation
	arguments :
		similar_product(float) : Last Purchased Product
		similarity_data(DataFrame) : Product Similarity Dataframe
		category(str) : Browsed Ctaegory
		bucket(int) : Price Bucket
		match_productdf(DataFrame) : Offer Combination Data
		price_df(DataFrame) : Product Price Information
		last_browsed(str) : Last Browsed Category
	returns :
		recommend_df(DataFrame) : Final Product Recommendations
	"""
   

	sub_df = similarity_data[similarity_data['Price_Bucket']==bucket]


	if category == 'Bracelets':
		recommend_df = pd.concat([sub_df[sub_df['Defined_Category']=='NonEngagement Ring'][:3],
								 sub_df[sub_df['Defined_Category']=='Necklaces'][:3],
								 sub_df[sub_df['Defined_Category']=='Earrings'][:3],
								 sub_df[sub_df['Defined_Category']=='Other'][:1]]
								 )
	elif category == 'Earrings':
		recommend_df = pd.concat([sub_df[sub_df['Defined_Category']=='NonEngagement Ring'][:3],
								 sub_df[sub_df['Defined_Category']=='Necklaces'][:3],
								 sub_df[sub_df['Defined_Category']=='Bracelets'][:3],
								 sub_df[sub_df['Defined_Category']=='Other'][:1]]
								 )
	elif category == 'Engagement Ring':
		recommend_df = pd.concat([sub_df[sub_df['Defined_Category']=='NonEngagement Ring'][:4],
								 sub_df[sub_df['Defined_Category']=='Necklaces'][:2],
								 sub_df[sub_df['Defined_Category']=='Earrings'][:2],
								 sub_df[sub_df['Defined_Category']=='Bracelets'][:2]]
								 )
	elif category == 'Loose Diamonds':
		recommend_df = pd.concat([sub_df[sub_df['Defined_Category']=='NonEngagement Ring'][:4],
								 sub_df[sub_df['Defined_Category']=='Necklaces'][:2],
								 sub_df[sub_df['Defined_Category']=='Earrings'][:2],
								 sub_df[sub_df['Defined_Category']=='Bracelets'][:2]]
								 )
	elif category == 'Necklaces':
		recommend_df = pd.concat([sub_df[sub_df['Defined_Category']=='NonEngagement Ring'][:3],
								 sub_df[sub_df['Defined_Category']=='Earrings'][:3],
								 sub_df[sub_df['Defined_Category']=='Bracelets'][:3],
								 sub_df[sub_df['Defined_Category']=='Other'][:1]]
								 )

	elif category == 'NonEngagement Ring':
		recommend_df = pd.concat([sub_df[sub_df['Defined_Category']=='Necklaces'][:3],
								 sub_df[sub_df['Defined_Category']=='Earrings'][:3],
								 sub_df[sub_df['Defined_Category']=='Bracelets'][:3],
								 sub_df[sub_df['Defined_Category']=='Other'][:1]]
								 )
	else:
		recommend_df = pd.concat([sub_df[sub_df['Defined_Category']=='NonEngagement Ring'][:3],
								 sub_df[sub_df['Defined_Category']=='Necklaces'][:3],
								 sub_df[sub_df['Defined_Category']=='Earrings'][:2],
								 sub_df[sub_df['Defined_Category']=='Bracelets'][:2]]
								 )
	recommend_df.reset_index(drop=True,inplace=True)

	matching_df = match_productdf[match_productdf['PRIMARY_OFFER_ID'] == similar_product]
	matching_products = offer_combination[(offer_combination['PRIMARY_OFFER_ID']==similar_product
										  )&(offer_combination['LINKED_OFFER_ID'].isin(price_df['offer_id']))]['LINKED_OFFER_ID'].values

	match_df = price_df[(price_df['offer_id'].isin(matching_products))&(price_df['Defined_Category'] != category)]
	match_df = match_df[~match_df['Defined_Category'].isin(["Loose Diamonds","Engagement Ring"])]
	match_df['MATCHING_PRODUCTS'] = 1

	for cat in match_df['Defined_Category'].unique():
		d_len = match_df[match_df['Defined_Category'] == cat].shape[0]
		recommend_df.drop(recommend_df[recommend_df['Defined_Category'] == cat].tail(d_len).index, inplace=True)

	recommend_df.rename(columns = {similar_product:'Score'},inplace=True)
	recommend_df = pd.concat([recommend_df,match_df])
	recommend_df['Score'].fillna(1,inplace=True)
	recommend_df['MATCHING_PRODUCTS'].fillna(0, inplace=True)

	recommend_df = recommend_df.sort_values(by='Score',ascending=False)[:10]

	if active:
		recommend_df = get_browsed_products(last_browsed,similarity_data,recommend_df,bucket)

	byo_df = recommend_df[recommend_df['PRODUCT_CLASS_NAME'].isin(['BYO Ring','BYO 3 Stone','BYO 5 Stone'])]
	ld_df = recommend_df[recommend_df['Defined_Category'] == "Loose Diamonds"]

	if byo_df.shape[0] > 0:
		byo_5sr = byo_df[byo_df['PRODUCT_CLASS_NAME'] == 'BYO 5 Stone']
		byo_3sr = byo_df[byo_df['PRODUCT_CLASS_NAME'] == 'BYO 3 Stone']
		byo_r = byo_df[byo_df['PRODUCT_CLASS_NAME'] == 'BYO Ring']
		if byo_5sr.shape[0] > 0:
			ld_df1 = similarity_data[(similarity_data['Defined_Category'] == "Loose Diamonds")&(similarity_data['Price_Bucket'] == 1)][:(5-ld_df.shape[0])]
			addn_ld = pd.concat([ld_df,ld_df1])
		elif (byo_5sr.shape[0] == 0) & (byo_3sr.shape[0] > 0):
			ld_df1 = similarity_data[(similarity_data['Defined_Category'] == "Loose Diamonds")&(similarity_data['Price_Bucket'] == 1)][:(3-ld_df.shape[0])]
			addn_ld = pd.concat([ld_df,ld_df1])
		elif (byo_5sr.shape[0] == 0) & (byo_3sr.shape[0] == 0) & (byo_r.shape[0] > 0):
			if ld_df.shape[0] >= 1:
				ld_df1 = pd.DataFrame()
			else:
				ld_df1 = similarity_data[(similarity_data['Defined_Category'] == "Loose Diamonds")&(similarity_data['Price_Bucket'] == 1)][:(1-ld_df.shape[0])]
			addn_ld = pd.concat([ld_df,ld_df1]).reset_index(drop=True)
	else:
		addn_ld = pd.DataFrame()

	recommend_df = pd.concat([recommend_df,addn_ld]).reset_index(drop=True)
	recommend_df['MATCHING_PRODUCTS'].fillna(0, inplace=True)

	return recommend_df

def get_browsed_products(last_browsed,similarity_data,recommend_df,bucket):
	"""
	Returns Product Similarity DataFrame of Inactive Users for recommendation
	arguments :
		last_browsed(str) : Last Browsed Category
		sub_df(DataFrame) : Subset of Similarity DataFrame
		recommend_df(DataFrame) : Final Recommendations dataframe
	returns :
		final_df(DataFrame) : Final Dataframe with extra recommendations added for users browsing in Engagement and Loose Diamonds Category
	"""
	if last_browsed == "Engagement Ring":
		sub_df = similarity_data[similarity_data['Price_Bucket'] == bucket]
	if last_browsed == "Loose Diamonds":
		sub_df = similarity_data[similarity_data['Price_Bucket'] == 1]

	if (last_browsed == "Loose Diamonds") or (last_browsed == "Engagement Ring"):
		addn_df = sub_df[sub_df['Defined_Category'] == last_browsed][:2]
	else:
		addn_df = pd.DataFrame()

	final_df = pd.concat([addn_df,recommend_df]).reset_index(drop=True)[:10]
	return final_df

if __name__ == "__main__":
	params = json.load(open("crosscategory.json"))

jewel_df = pd.read_csv(params["jewel_input_file"])
setting_df = pd.read_csv(params["setting_input_file"])
diamond_df = pd.read_csv(params["diamond_input_file"])
last_purchase = pd.read_csv(params['last_purchase'])

# Removing common ids across categories
common_ids = list(set(setting_df['offer_id']) & set(jewel_df['offer_id']))

def remove_common_ids(df,common_ids):
	df = df[~df['offer_id'].isin(common_ids)]
	return df

setting_df = remove_common_ids(setting_df,common_ids)

# Category Level Matrices and required table Creation

jewel_interaction_matrix, jewel_feature_df, jewel_user_df, jewel_bnid_cat_count, jewel_category, jewel_main_df, jewel_user_dict, jewel_item_dict = create_jewellery_matrices(jewel_df)
setting_interaction_matrix, setting_feature_df, setting_user_df, setting_bnid_cat_count, setting_category, setting_main_df, setting_user_dict, setting_item_dict = create_setting_matrices(setting_df)
diamond_interaction_matrix, diamond_feature_df, diamond_user_df, diamond_bnid_cat_count, diamond_category, diamond_main_df, diamond_user_dict, diamond_item_dict = create_diamond_matrices(diamond_df)

# Creating Interaction Matrix

sub_mat = pd.merge(jewel_interaction_matrix,setting_interaction_matrix,on='bnid',how='outer')
interaction_matrix = pd.merge(sub_mat,diamond_interaction_matrix,on='bnid',how='outer')
interaction_matrix.set_index('bnid',inplace=True)
interaction_matrix.fillna(0,inplace=True)

# Creating Feature Matrix

diamond_feature_df = diamond_feature_df[diamond_feature_df['DIAMOND_CATEGORY'].isin(diamond_interaction_matrix.columns)]
diamond_feature_df.rename(columns= {'DIAMOND_CATEGORY':'offer_id'},inplace=True)
sub_mat = pd.merge(jewel_feature_df,setting_feature_df,on='offer_id',how='outer')
feature_sparse_df = pd.merge(sub_mat,diamond_feature_df,on='offer_id',how='outer')
feature_sparse_df.fillna(0,inplace=True)

# Creating User Matrix

user_cols = [i for i in diamond_user_df.columns if i in setting_user_df.columns if i in jewel_user_df.columns]
jewel_user_df = jewel_user_df[user_cols]
user_sparse_df = pd.concat([jewel_user_df, setting_user_df,diamond_user_df],ignore_index=False)
user_sparse_df.reset_index(inplace=True)
user_sparse_df = user_sparse_df.groupby('bnid').sum()
threshold = 0
user_sparse_df = user_sparse_df.applymap(lambda x: 1 if x > threshold else 0)
user_sparse_df.reset_index(inplace=True)
bnids = set(np.append(np.append(jewel_interaction_matrix['bnid'].values,setting_interaction_matrix['bnid'].values),diamond_interaction_matrix['bnid'].values))
user_sparse_df = user_sparse_df[user_sparse_df['bnid'].isin(bnids)]

df_bnid_cat_count = pd.concat([diamond_bnid_cat_count,jewel_bnid_cat_count,setting_bnid_cat_count],ignore_index=True)
df_category = pd.concat([diamond_category,jewel_category,setting_category],ignore_index=True)
feature_sparse_df.set_index('offer_id',inplace=True)
user_sparse_df.set_index('bnid',inplace=True)
user_sparse_df.fillna(0,inplace=True)

# Updating Matrices
common_offers = list(set(interaction_matrix.columns.tolist()).intersection(feature_sparse_df.index.tolist()))
common_bnids = list(set(interaction_matrix.index.tolist()).intersection(user_sparse_df.index.tolist()))

interaction_matrix = interaction_matrix[common_offers]
feature_sparse_df = feature_sparse_df[feature_sparse_df.index.isin(common_offers)]
user_sparse_df = user_sparse_df[user_sparse_df.index.isin(common_bnids)]

feature_matrix = scipy.sparse.csr_matrix(feature_sparse_df.values)
user_matrix = scipy.sparse.csr_matrix(user_sparse_df.values)

# Building Recommendation Engine

recommender = preprocess_user_features(df_bnid_cat_count, df_category).runMF(interaction_matrix, feature_matrix, user_matrix)

# Creating Price Buckets
client = bgq.Client()
sql = """SELECT distinct lower(SKU) as SKU_ID, cast(USD_PRICE as int64) as avg_price FROM `{DIAMOND_PRICE}` where CAPTURE_DATE_KEY = {df_date} and usd_price is not null """.format(
		df_date=jewel_df.date_key.max(), DIAMOND_PRICE=DIAMOND_PRICE
	)
diamond_df_price = client.query(sql).to_dataframe(progress_bar_type="tqdm")
diamond_df_price["avg_price"] = diamond_df_price["avg_price"].astype(float)

jewel_price = jewel_main_df[['offer_id','product_class_name','Merch_product_category','PRODUCT_LIST_PRICE']].drop_duplicates()
jewel_price.rename(columns = {'product_class_name':'PRODUCT_CLASS_NAME','Merch_product_category':'MERCH_PRODUCT_CATEGORY'},inplace=True)
jewel_price['Defined_Category'] = jewel_price.apply(lambda x: create_categories(x['PRODUCT_CLASS_NAME']),axis=1)
jewel_percentile = create_price_percentiles(jewel_price,'PRODUCT_LIST_PRICE')


diamond_main_df['DIAMOND_CATEGORY'] = diamond_main_df['SKU_ID'].apply(lambda x: int(x[2:]))
diamond_price = diamond_main_df[diamond_main_df['DIAMOND_CATEGORY'].isin(diamond_feature_df['offer_id'].unique())]
diamond_price = pd.merge(diamond_price[["SKU_ID", "DIAMOND_CATEGORY"]].drop_duplicates(),diamond_df_price,how="left",on="SKU_ID")
diamond_price['PRODUCT_CLASS_NAME'] = "Loose Diamonds"
diamond_price['MERCH_PRODUCT_CATEGORY'] = "Engagement"
diamond_price['Defined_Category'] = "Loose Diamonds"
diamond_percentile = create_price_percentiles(diamond_price,'avg_price')


setting_price = setting_main_df[['offer_id','PRODUCT_CLASS_NAME','MERCH_PRODUCT_CATEGORY','PRODUCT_LIST_PRICE']].drop_duplicates()
setting_price['Defined_Category'] = setting_price.apply(lambda x: create_categories(x['PRODUCT_CLASS_NAME']),axis=1)
combined_df = pd.concat([setting_price,diamond_price],ignore_index=True)
setting_percentile = create_price_percentiles(combined_df,'PRODUCT_LIST_PRICE')
del setting_percentile["Loose Diamonds"]

# Creating Percentile Dict Based on the Product Prices

percentile_dict = {**jewel_percentile , **setting_percentile , **diamond_percentile}

# Creating Price Buckets 

jewel_price['Price_Bucket'] = jewel_price.apply(lambda x: create_price_buckets(percentile_dict,x['PRODUCT_LIST_PRICE'],x['Defined_Category']), axis=1)
setting_price['Price_Bucket'] = setting_price.apply(lambda x: create_price_buckets(percentile_dict,x['PRODUCT_LIST_PRICE'],x['Defined_Category']), axis=1)
diamond_price['Price_Bucket'] = diamond_price.apply(lambda x: create_price_buckets(percentile_dict,x['avg_price'],x['Defined_Category']), axis=1)

price_df = pd.concat([jewel_price,setting_price,diamond_price])
price_df.reset_index(drop=True, inplace=True)
price_df['offer_id'] =price_df['offer_id'].fillna(price_df['DIAMOND_CATEGORY'])
price_df['PRODUCT_LIST_PRICE'] = price_df['PRODUCT_LIST_PRICE'].fillna(price_df['avg_price'])
price_df = price_df[['offer_id', 'PRODUCT_CLASS_NAME', 'MERCH_PRODUCT_CATEGORY',
	   'PRODUCT_LIST_PRICE', 'Defined_Category', 'Price_Bucket', 'SKU_ID']]

diamond_price = diamond_price[['DIAMOND_CATEGORY','PRODUCT_CLASS_NAME','MERCH_PRODUCT_CATEGORY',
							   'avg_price', 'Defined_Category', 'Price_Bucket']]
diamond_price.rename(columns = {'DIAMOND_CATEGORY':'offer_id','avg_price':'PRODUCT_LIST_PRICE'},inplace=True)

# Active BNIDs to Score
lastday_bnids = list(set(np.append(np.append(subset_bnids(jewel_main_df),subset_bnids(setting_main_df)),subset_bnids(diamond_main_df))))

# Extracting Last Browsed Category for each User

jewel_df = get_lastbrowsed_category(jewel_main_df,'product_class_name')
setting_df = get_lastbrowsed_category(setting_main_df,'PRODUCT_CLASS_NAME')
diamond_main_df['Defined_Category'] = "Loose Diamonds"
diamond_df = get_lastbrowsed_category(diamond_main_df,'',True)

# Subsetting Active and Inactive BNIDs based on their last purchase and Creating Active and Inactive Tables for Scoring

from dateutil import parser
from dateutil.relativedelta import relativedelta

dt1 = (parser.parse(params['scoring_date']) - relativedelta(years=1)).date()
dt2 = (parser.parse(params['scoring_date']) - relativedelta(years=2)).date()

active_category = pd.concat([jewel_df,setting_df,diamond_df]).groupby(['bnid','Defined_Category']).count().sort_values('date_key',ascending=False).reset_index().groupby('bnid').first().reset_index()[['bnid','Defined_Category']]
active_category.rename(columns = {'Defined_Category':'Lastbrowsed_Category'},inplace=True)
active_df = last_purchase[last_purchase['bnid'].isin(lastday_bnids)]
active_df = active_df[active_df['bnid'].isin(interaction_matrix.index.unique())]
active_df['active'] = 1
active_df = pd.merge(active_df,active_category,on='bnid',how = 'inner')
inactive_df = last_purchase[(last_purchase['order_date'].isin([dt1.strftime('%Y-%m-%d'),dt2.strftime('%Y-%m-%d')]))&(last_purchase['bnid'].notnull())]
inactive_df['active'] = 0
inactive_df['Lastbrowsed_Category'] = np.nan

scoring_df = pd.concat([active_df,inactive_df])

client = bgq.Client()
sql = """SELECT * FROM `bnile-cdw-prod.nileprod_o_warehouse.offer_combination`""" 
offer_combination= client.query(sql).to_dataframe(progress_bar_type='tqdm')

# Creating Combines user and item dictionaries
item_dict = {**jewel_item_dict , **setting_item_dict , **diamond_item_dict}
user_dict = {**jewel_user_dict , **setting_user_dict , **diamond_user_dict}

# Item Item Similarity Matrix Creation
item_item_dist = create_item_emdedding_distance_matrix(model = recommender,item_features = feature_matrix,feature_df = feature_sparse_df)

# Scoring Inactive BNIDs

recommend_list = []
for bnid in tqdm(inactive_df['bnid'].unique()):
	last_browsed = np.nan
	similar_product, purchase_category, price_bucket = get_purchase_info(bnid,last_purchase,price_df)
	try:
		similarity_df = get_inactive_data(item_item_dist,similar_product, purchase_category, price_bucket)
	except:
		continue
	recommendations = get_recommendation(similar_product, similarity_df,purchase_category,price_bucket,offer_combination,price_df,last_browsed)
	recommendations['bnid'] = bnid

	if recommendations.shape[0] >10:
		temp_df = recommendations.iloc[:10].sort_values(by='Score',ascending = False)
		temp_df1 = recommendations.iloc[10:]
		recommendations = pd.concat([temp_df,temp_df1]).reset_index(drop=True)
		recommendations['Rank'] = np.arange(1,recommendations.shape[0]+1)
	else:
		recommendations['Rank'] = recommendations['Score'].rank(method = 'first',ascending=False)
	recommend_list.append(recommendations)

inactive_recommendations = pd.concat(recommend_list,ignore_index=True)

# Scoring Active BNIDs
print("Start: Active loop")
recommend_list = []
for bnid in (active_df['bnid'].unique()):
	last_browsed = active_df[active_df['bnid'] == bnid]['Lastbrowsed_Category'].values[0]
	similar_product, purchase_category, price_bucket = get_purchase_info(bnid,last_purchase,price_df,False)
	try:
		similarity_df = get_active_data(bnid, recommender, interaction_matrix, feature_matrix, user_matrix, similar_product, purchase_category, price_bucket)
	except:
		continue
	recommendations = get_recommendation(similar_product, similarity_df,purchase_category,price_bucket,offer_combination,price_df,last_browsed,True)
	recommendations['bnid'] = bnid
	if recommendations.shape[0] >10:
		temp_df = recommendations.iloc[:10].sort_values(by='Score',ascending = False)
		temp_df1 = recommendations.iloc[10:]
		recommendations = pd.concat([temp_df,temp_df1]).reset_index(drop=True)
		recommendations['Rank'] = np.arange(1,recommendations.shape[0]+1)
	else:
		recommendations['Rank'] = recommendations['Score'].rank(method = 'first',ascending=False)
	recommend_list.append(recommendations)

active_recommendations = pd.concat(recommend_list,ignore_index=True)
print("End: Active loop")

# Creating final recommendation Data
recommendation_data = pd.concat([inactive_recommendations, active_recommendations])
recommendation_data['Recommended_items'] = recommendation_data['SKU_ID'].fillna(recommendation_data['offer_id'])
recommendation_data.rename(columns = {'PRODUCT_LIST_PRICE':'recommended_avg_price'},inplace=True)
diamond_sku_shape = diamond_main_df[['SKU_ID','DIAMOND_SHAPE']].drop_duplicates()
recommendation_data = pd.merge(recommendation_data,diamond_sku_shape, on = 'SKU_ID',how = 'left')
scored_recommendations = recommendation_data[["bnid","Recommended_items","recommended_avg_price","Rank","MERCH_PRODUCT_CATEGORY","PRODUCT_CLASS_NAME","DIAMOND_SHAPE","MATCHING_PRODUCTS"]]

output_path = params['output_path']
scoring_date = params['scoring_date']
scored_recommendations.to_csv(
		output_path
		+ "recommendation_user_product_feature_"
		+ scoring_date
		+ ".csv",
		index=False
	)
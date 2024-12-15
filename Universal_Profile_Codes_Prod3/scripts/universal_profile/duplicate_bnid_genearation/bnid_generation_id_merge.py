#Install all the required python libraries and import them as shown below		
from pandas.io import gbq
import pandas as pd
import numpy as np
import re
from google.cloud import bigquery as bgq
#this library is will help to get the values for the variables that are stored in Airflow server
from airflow.models import Variable


client = bgq.Client()


# Import output of ID Resolution Pre-process into data from
## table :  up_stg_dedup.bnid_dedup_map
read_dedup_map="""SELECT distinct primary_bnid,duplicate_bnid FROM `bnile-cdw-prod.dag_migration_test.bnid_dedup_map_probablistic`"""
data= client.query(read_dedup_map).to_dataframe()

# Sort and remove duplicates
data['primary_bnid'], data['duplicate_bnid'] = data.min(axis=1), data.max(axis=1)
data = data[['primary_bnid','duplicate_bnid']].drop_duplicates().reset_index(drop=True)


#
d_bnid_unique_df = data[~data['primary_bnid'].isin(data.duplicate_bnid.unique())].reset_index(drop=True)
d_duplicate = d_bnid_unique_df[d_bnid_unique_df.duplicate_bnid.duplicated()].drop_duplicates()
joined = data.merge(d_duplicate,on = 'duplicate_bnid',how="outer",indicator=True)
joined[['primary_bnid_x','duplicate_bnid']] = joined[['duplicate_bnid','primary_bnid_x']].where(joined['_merge'] == 'both', joined[['primary_bnid_x','duplicate_bnid']].values)
data = joined.rename(columns={'primary_bnid_x':'primary_bnid'})[['primary_bnid','duplicate_bnid']].reset_index(drop=True)

# Loop and Merge

df = pd.DataFrame()
i = 1
count = 0
unique_p_bnid = len(data[~data['primary_bnid'].isin(data.duplicate_bnid.unique())])
data1 = data[~data['primary_bnid'].isin(data.duplicate_bnid.unique())].reset_index(drop=True)
df = data1
while(count < unique_p_bnid):
    a=data.merge(data1,right_on='duplicate_bnid',left_on = 'primary_bnid',how='right')[['primary_bnid_y','duplicate_bnid_x']].drop_duplicates()
    a.rename(columns={'primary_bnid_y':'primary_bnid','duplicate_bnid_x':'duplicate_bnid'},inplace = True)
    count+=a.isna().sum().sum()
    df = df.append(a,ignore_index = True)
    data1 = a
    i+=1
df = df.dropna()



# Write back to BQ
df.to_gbq(destination_table='dag_migration_test.bnid_dedup_merge',project_id='bnile-cdw-prod',if_exists='replace',chunksize=1000000)
import warnings
warnings.filterwarnings("ignore")
import time
import pandas as pd
import numpy as np

import json

from calendar import month_name,day_name
from datetime import date

from tqdm import tqdm
from multiprocessing import Pool
from google.cloud import bigquery as bgq

import joblib
import xgboost as xgb

from google.cloud import storage

storage_client = storage.Client()
import pandas as pd
import glob


def create_open_ad(df):
    
    #print('Function started - step1')
    bnid = df['bnid'].iloc[0]
    month_df = pd.DataFrame({'bnid':[bnid]*7,'year':[current_year]*7,'month_name':[current_month]*7,
                           'day_name':days.values.tolist(),'marital_status':[df['marital_status'].iloc[0]]*7
                           ,'derived_gender':[df['derived_gender'].iloc[0]]*7,
                           'ship_to_city':[df['ship_to_city'].iloc[0]]*7})
    df = pd.concat([df,month_df],axis=0)
    df.reset_index(drop=True,inplace=True)
    
    cf = clickfact[clickfact['bnid'] ==bnid]
    updated_list = []
    final_list = []
    
    loop_df = df[df['month_name']==current_month]
    count_i = 0
    # print('Before Loop - step2')
    for i in loop_df.index:
        # print('Loop After - step3')
        d1 = df.iloc[i].to_frame().T
        year = d1['year'].iloc[0]
        month = d1['month_name'].iloc[0]
        day = d1['day_name'].iloc[0]
        
        idx = months[months==month].index[0]

        if month == 'January':
            # print('open jan - step4')
            df_1mnth = df[(df['year']==year-1)&(df['month_name']==months[12])&(df['day_name']==day)]
            oc_1mnth = df[(df['year']==year-1)&(df['month_name']==months[12])]
            df_3mnth = df[(df['year']==year-1)&(df['month_name'].isin([months[i] for i in [10,11,12]]))&(df['day_name']==day)]
            oc_3mnth = df[(df['year']==year-1)&(df['month_name'].isin([months[i] for i in [10,11,12]]))]
            
            df_1mnth = df_1mnth[oc_req_columns].sum().to_frame().T
            oc_1mnth = oc_1mnth[oc_req_columns].sum().to_frame().T
            df_3mnth = df_3mnth[oc_req_columns].sum().to_frame().T
            oc_3mnth = oc_3mnth[oc_req_columns].sum().to_frame().T
            
            df_1mnth.columns = cols_1mnth
            oc_1mnth.columns = oc_cols_1mnth
            df_3mnth.columns = cols_3mnth
            oc_3mnth.columns = oc_cols_3mnth
            
            sub_df = pd.concat([df_1mnth,oc_1mnth,df_3mnth,oc_3mnth],axis=1)
            
            if sub_df.empty:
                sub_df.loc[len(sub_df)] = 0
            temp = pd.concat([d1.reset_index(drop=True),sub_df],axis=1)
            temp = temp[final_cols]
            updated_list.append(temp)
        elif month == 'February':
            # print('open feb - step5')
            df_1mnth = df[(df['year']==year) & (df['month_name']==months[1])&(df['day_name']==day)]
            oc_1mnth = df[(df['year']==year) & (df['month_name']==months[1])]
            df_3mnth = df[(df['year']==year-1)&(df['month_name'].isin([months[i] for i in [11,12]]))&(df['day_name']==day)]
            df_3mnth = pd.concat([df_1mnth,df_3mnth],axis=0)
            oc_3mnth = df[(df['year']==year-1)&(df['month_name'].isin([months[i] for i in [11,12]]))]
            oc_3mnth = pd.concat([oc_1mnth,oc_3mnth],axis=0)
            
            df_1mnth = df_1mnth[oc_req_columns].sum().to_frame().T
            oc_1mnth = oc_1mnth[oc_req_columns].sum().to_frame().T
            df_3mnth = df_3mnth[oc_req_columns].sum().to_frame().T
            oc_3mnth = oc_3mnth[oc_req_columns].sum().to_frame().T
            
            df_1mnth.columns = cols_1mnth
            oc_1mnth.columns = oc_cols_1mnth
            df_3mnth.columns = cols_3mnth
            oc_3mnth.columns = oc_cols_3mnth
            
            sub_df = pd.concat([df_1mnth,oc_1mnth,df_3mnth,oc_3mnth],axis=1)
            
            if sub_df.empty:
                sub_df.loc[len(sub_df)] = 0
            temp = pd.concat([d1.reset_index(drop=True),sub_df],axis=1)
            temp = temp[final_cols]
            updated_list.append(temp)

        elif month == 'March':
            # print('open mar - step6')
            df1 = df[(df['year']==year) & (df['month_name']==months[2])&(df['day_name']==day)]
            oc_df1 = df[(df['year']==year) & (df['month_name']==months[2])]
            df11 = df[(df['year']==year)&(df['month_name'].isin([months[i] for i in [1,2]]))&(df['day_name']==day)]
            oc_df11 = df[(df['year']==year)&(df['month_name'].isin([months[i] for i in [1,2]]))]
            df12 = df[(df['year']==year-1)&(df['month_name']==months[12])&(df['day_name']==day)]
            oc_df12 = df[(df['year']==year-1)&(df['month_name']==months[12])]
            
            df2 = pd.concat([df11,df12],axis=0)
            oc_df2 = pd.concat([oc_df11,oc_df12],axis=0)
            
            df_1mnth = df1[oc_req_columns].sum().to_frame().T
            oc_1mnth = oc_df1[oc_req_columns].sum().to_frame().T
            df_3mnth = df2[oc_req_columns].sum().to_frame().T
            oc_3mnth = oc_df2[oc_req_columns].sum().to_frame().T
            
            df_1mnth.columns = cols_1mnth
            oc_1mnth.columns = oc_cols_1mnth
            df_3mnth.columns = cols_3mnth
            oc_3mnth.columns = oc_cols_3mnth
            
            sub_df = pd.concat([df_1mnth,oc_1mnth,df_3mnth,oc_3mnth],axis=1)
            
            if sub_df.empty:
                sub_df.loc[len(sub_df)] = 0
            temp = pd.concat([d1.reset_index(drop=True),sub_df],axis=1)
            temp = temp[final_cols]
            updated_list.append(temp)
        else:
            # print('open rest - step7')
            df_1mnth = df[(df['month_name'] == months[idx-1])&(df['day_name']==day)]
            oc_1mnth = df[(df['month_name'] == months[idx-1])]
            df_3mnth = df[(df['month_name'].isin([months[i] for i in range(idx-3,idx)]))&(df['day_name']==day)]
            oc_3mnth = df[(df['month_name'].isin([months[i] for i in range(idx-3,idx)]))]
            
            df_1mnth = df_1mnth[oc_req_columns].sum().to_frame().T
            oc_1mnth = oc_1mnth[oc_req_columns].sum().to_frame().T
            df_3mnth = df_3mnth[oc_req_columns].sum().to_frame().T
            oc_3mnth = oc_3mnth[oc_req_columns].sum().to_frame().T
            
            df_1mnth.columns = cols_1mnth
            oc_1mnth.columns = oc_cols_1mnth
            df_3mnth.columns = cols_3mnth
            oc_3mnth.columns = oc_cols_3mnth
            
            sub_df = pd.concat([df_1mnth,oc_1mnth,df_3mnth,oc_3mnth],axis=1)

            if sub_df.empty:
                sub_df.loc[len(sub_df)] = 0
            temp = pd.concat([d1.reset_index(drop=True),sub_df],axis=1)
            temp = temp[final_cols]
            updated_list.append(temp)

        """Click Fact"""
        if not cf.empty:
            # print('click started - step8')
            if month == 'January':
                # print('click jan - step9')
                df_1mnth = cf[(cf['year']==year-1)&(cf['month_name']==months[12])&(cf['day_name']==day)]
                cf_1mnth = cf[(cf['year']==year-1)&(cf['month_name']==months[12])]
                df_3mnth = cf[(cf['year']==year-1)&(cf['month_name'].isin([months[i] for i in [10,11,12]]))&(cf['day_name']==day)]
                cf_3mnth = cf[(cf['year']==year-1)&(cf['month_name'].isin([months[i] for i in [10,11,12]]))]
                
                df_1mnth = df_1mnth[cf_req_columns].sum().to_frame().T
                cf_1mnth = cf_1mnth[cf_req_columns].sum().to_frame().T
                df_3mnth = df_3mnth[cf_req_columns].sum().to_frame().T
                cf_3mnth = cf_3mnth[cf_req_columns].sum().to_frame().T
                
                df_1mnth.columns = cols_1mnth_c
                cf_1mnth.columns = cf_cols_1mnth_c
                df_3mnth.columns = cols_3mnth_c
                cf_3mnth.columns = cf_cols_3mnth_c
                
                sub_df = pd.concat([df_1mnth,cf_1mnth,df_3mnth,cf_3mnth],axis=1)
                
                if sub_df.empty:
                    sub_df.loc[len(sub_df)] = 0
                AD = pd.concat([updated_list[count_i],sub_df],axis=1)
                final_list.append(AD)

            elif month == 'February':
                # print('click feb - step10')
                df_1mnth = cf[(cf['year']==year) & (cf['month_name']==months[1])&(cf['day_name']==day)]
                cf_1mnth = cf[(cf['year']==year) & (cf['month_name']==months[1])]
                df_3mnth = cf[(cf['year']==year-1)&(cf['month_name'].isin([months[i] for i in [11,12]]))&(cf['day_name']==day)]
                df_3mnth = pd.concat([df_1mnth,df_3mnth],axis=0)
                cf_3mnth = cf[(cf['year']==year-1)&(cf['month_name'].isin([months[i] for i in [11,12]]))]
                cf_3mnth = pd.concat([cf_1mnth,cf_3mnth],axis=0)
                
                df_1mnth = df_1mnth[cf_req_columns].sum().to_frame().T
                cf_1mnth = cf_1mnth[cf_req_columns].sum().to_frame().T
                df_3mnth = df_3mnth[cf_req_columns].sum().to_frame().T
                cf_3mnth = cf_3mnth[cf_req_columns].sum().to_frame().T
                
                df_1mnth.columns = cols_1mnth_c
                cf_1mnth.columns = cf_cols_1mnth_c
                df_3mnth.columns = cols_3mnth_c
                cf_3mnth.columns = cf_cols_3mnth_c
                
                sub_df = pd.concat([df_1mnth,cf_1mnth,df_3mnth,cf_3mnth],axis=1)
                
                if sub_df.empty:
                    sub_df.loc[len(sub_df)] = 0
                AD = pd.concat([updated_list[count_i],sub_df],axis=1)
                final_list.append(AD)
                
            elif month == 'March':
                # print('click mar - step11')
                df1 = cf[(cf['year']==year) & (cf['month_name']==months[2])&(cf['day_name']==day)]
                cf_df1 = cf[(cf['year']==year) & (cf['month_name']==months[2])]
                df11 = cf[(cf['year']==year)&(cf['month_name'].isin([months[i] for i in [1,2]]))&(cf['day_name']==day)]
                cf_df11 = cf[(cf['year']==year)&(cf['month_name'].isin([months[i] for i in [1,2]]))]
                df12 = cf[(cf['year']==year-1)&(cf['month_name']==months[12])&(cf['day_name']==day)]
                cf_df12 = cf[(cf['year']==year-1)&(cf['month_name']==months[12])]
                
                df2 = pd.concat([df11,df12],axis=0)
                cf_df2 = pd.concat([cf_df11,cf_df12],axis=0)
                
                df_1mnth = df1[cf_req_columns].sum().to_frame().T
                cf_1mnth = cf_df1[cf_req_columns].sum().to_frame().T
                df_3mnth = df2[cf_req_columns].sum().to_frame().T
                cf_3mnth = cf_df2[cf_req_columns].sum().to_frame().T
                
                df_1mnth.columns = cols_1mnth_c
                cf_1mnth.columns = cf_cols_1mnth_c
                df_3mnth.columns = cols_3mnth_c
                cf_3mnth.columns = cf_cols_3mnth_c
                
                sub_df = pd.concat([df_1mnth,cf_1mnth,df_3mnth,cf_3mnth],axis=1)
                
                if sub_df.empty:
                    sub_df.loc[len(sub_df)] = 0
                    
                AD = pd.concat([updated_list[count_i],sub_df],axis=1)
                final_list.append(AD)

            else:
                # print('click rest - step12')
                df_1mnth = cf[(cf['month_name'] == months[idx-1])&(cf['day_name']==day)]
                cf_1mnth = cf[(cf['month_name'] == months[idx-1])]
                df_3mnth = cf[(cf['month_name'].isin([months[i] for i in range(idx-3,idx)]))&(cf['day_name']==day)]
                cf_3mnth = cf[(cf['month_name'].isin([months[i] for i in range(idx-3,idx)]))]
                
                df_1mnth = df_1mnth[cf_req_columns].sum().to_frame().T
                cf_1mnth = cf_1mnth[cf_req_columns].sum().to_frame().T
                df_3mnth = df_3mnth[cf_req_columns].sum().to_frame().T
                cf_3mnth = cf_3mnth[cf_req_columns].sum().to_frame().T
                
                df_1mnth.columns = cols_1mnth_c
                cf_1mnth.columns = cf_cols_1mnth_c
                df_3mnth.columns = cols_3mnth_c
                cf_3mnth.columns = cf_cols_3mnth_c
                
                sub_df = pd.concat([df_1mnth,cf_1mnth,df_3mnth,cf_3mnth],axis=1)
                
                if sub_df.empty:
                    sub_df.loc[len(sub_df)] = 0
                AD = pd.concat([updated_list[count_i],sub_df],axis=1)
                final_list.append(AD)
                
        count_i=count_i+1
    
    oc_final = pd.concat(updated_list,ignore_index=True)
    if cf.empty:
        cf_final = pd.DataFrame(np.zeros(shape=(len(loop_df),28)),columns=cols_1mnth_c+cf_cols_1mnth_c+cols_3mnth_c+cf_cols_3mnth_c)
        AD = pd.concat([oc_final,cf_final],axis=1)
        final_list.append(AD)
    
    ad = pd.concat(final_list,ignore_index=True)
    # print('bnid level AD created - step13')
    
    return ad.to_dict(orient='records')
    

def create_clicks_ad(df):
    
    bnid = df['bnid'].iloc[0]
    month_df = pd.DataFrame({'bnid':[bnid]*7,'year':[current_year]*7,'month_name':[current_month]*7,
                           'day_name':days.values.tolist(),'marital_status':[df['marital_status'].iloc[0]]*7
                           ,'derived_gender':[df['derived_gender'].iloc[0]]*7,
                           'ship_to_city':[df['ship_to_city'].iloc[0]]*7})
    df = pd.concat([df,month_df],axis=0)
    df.reset_index(drop=True,inplace=True)
    
    loop_df = df[df['month_name']==current_month]
    
    final_list = []
    
    for i in loop_df.index:
        d1 = df.iloc[i].to_frame().T
        
        merge_df = d1[['bnid','year','month_name','day_name','marital_status','derived_gender','ship_to_city']]
        merge_df.reset_index(drop=True,inplace=True)
        
        year = d1['year'].iloc[0]
        month = d1['month_name'].iloc[0]
        day = d1['day_name'].iloc[0]
        
        idx = months[months==month].index[0]
        
        if month == 'January':
            df_1mnth = df[(df['year']==year-1)&(df['month_name']==months[12])&(df['day_name']==day)]
            cf_1mnth = df[(df['year']==year-1)&(df['month_name']==months[12])]
            df_3mnth = df[(df['year']==year-1)&(df['month_name'].isin([months[i] for i in [10,11,12]]))&(df['day_name']==day)]
            cf_3mnth = df[(df['year']==year-1)&(df['month_name'].isin([months[i] for i in [10,11,12]]))]

            df_1mnth = df_1mnth[cf_req_columns].sum().to_frame().T
            cf_1mnth = cf_1mnth[cf_req_columns].sum().to_frame().T
            df_3mnth = df_3mnth[cf_req_columns].sum().to_frame().T
            cf_3mnth = cf_3mnth[cf_req_columns].sum().to_frame().T

            df_1mnth.columns = cols_1mnth_c
            cf_1mnth.columns = cf_cols_1mnth_c
            df_3mnth.columns = cols_3mnth_c
            cf_3mnth.columns = cf_cols_3mnth_c

            sub_df = pd.concat([df_1mnth,cf_1mnth,df_3mnth,cf_3mnth],axis=1)

            if sub_df.empty:
                sub_df.loc[len(sub_df)] = 0
            AD = pd.concat([merge_df,sub_df],axis=1)
            final_list.append(AD)

        elif month == 'February':
            df_1mnth = df[(df['year']==year) & (df['month_name']==months[1])&(df['day_name']==day)]
            cf_1mnth = df[(df['year']==year) & (df['month_name']==months[1])]
            df_3mnth = df[(df['year']==year-1)&(df['month_name'].isin([months[i] for i in [11,12]]))&(df['day_name']==day)]
            df_3mnth = pd.concat([df_1mnth,df_3mnth],axis=0)
            cf_3mnth = df[(df['year']==year-1)&(df['month_name'].isin([months[i] for i in [11,12]]))]
            cf_3mnth = pd.concat([cf_1mnth,cf_3mnth],axis=0)

            df_1mnth = df_1mnth[cf_req_columns].sum().to_frame().T
            cf_1mnth = cf_1mnth[cf_req_columns].sum().to_frame().T
            df_3mnth = df_3mnth[cf_req_columns].sum().to_frame().T
            cf_3mnth = cf_3mnth[cf_req_columns].sum().to_frame().T

            df_1mnth.columns = cols_1mnth_c
            cf_1mnth.columns = cf_cols_1mnth_c
            df_3mnth.columns = cols_3mnth_c
            cf_3mnth.columns = cf_cols_3mnth_c

            sub_df = pd.concat([df_1mnth,cf_1mnth,df_3mnth,cf_3mnth],axis=1)

            if sub_df.empty:
                sub_df.loc[len(sub_df)] = 0
            AD = pd.concat([merge_df,sub_df],axis=1)
            final_list.append(AD)

        elif month == 'March':
            df1 = df[(df['year']==year) & (df['month_name']==months[2])&(df['day_name']==day)]
            cf_df1 = df[(df['year']==year) & (df['month_name']==months[2])]
            df11 = df[(df['year']==year)&(df['month_name'].isin([months[i] for i in [1,2]]))&(df['day_name']==day)]
            cf_df11 = df[(df['year']==year)&(df['month_name'].isin([months[i] for i in [1,2]]))]
            df12 = df[(df['year']==year-1)&(df['month_name']==months[12])&(df['day_name']==day)]
            cf_df12 = df[(df['year']==year-1)&(df['month_name']==months[12])]

            df2 = pd.concat([df11,df12],axis=0)
            cf_df2 = pd.concat([cf_df11,cf_df12],axis=0)

            df_1mnth = df1[cf_req_columns].sum().to_frame().T
            cf_1mnth = cf_df1[cf_req_columns].sum().to_frame().T
            df_3mnth = df2[cf_req_columns].sum().to_frame().T
            cf_3mnth = cf_df2[cf_req_columns].sum().to_frame().T

            df_1mnth.columns = cols_1mnth_c
            cf_1mnth.columns = cf_cols_1mnth_c
            df_3mnth.columns = cols_3mnth_c
            cf_3mnth.columns = cf_cols_3mnth_c

            sub_df = pd.concat([df_1mnth,cf_1mnth,df_3mnth,cf_3mnth],axis=1)

            if sub_df.empty:
                sub_df.loc[len(sub_df)] = 0

            AD = pd.concat([merge_df,sub_df],axis=1)
            final_list.append(AD)

        else:

            df_1mnth = df[(df['month_name'] == months[idx-1])&(df['day_name']==day)]
            cf_1mnth = df[(df['month_name'] == months[idx-1])]
            df_3mnth = df[(df['month_name'].isin([months[i] for i in range(idx-3,idx)]))&(df['day_name']==day)]
            cf_3mnth = df[(df['month_name'].isin([months[i] for i in range(idx-3,idx)]))]

            df_1mnth = df_1mnth[cf_req_columns].sum().to_frame().T
            cf_1mnth = cf_1mnth[cf_req_columns].sum().to_frame().T
            df_3mnth = df_3mnth[cf_req_columns].sum().to_frame().T
            cf_3mnth = cf_3mnth[cf_req_columns].sum().to_frame().T

            df_1mnth.columns = cols_1mnth_c
            cf_1mnth.columns = cf_cols_1mnth_c
            df_3mnth.columns = cols_3mnth_c
            cf_3mnth.columns = cf_cols_3mnth_c

            sub_df = pd.concat([df_1mnth,cf_1mnth,df_3mnth,cf_3mnth],axis=1)

            if sub_df.empty:
                sub_df.loc[len(sub_df)] = 0
            AD = pd.concat([merge_df,sub_df],axis=1)
            final_list.append(AD)
    ad = pd.concat(final_list,ignore_index=True)
    
    return ad.to_dict(orient='records')
    
    
def last_n_months(n):
    mnths = []
    current_month_idx = date.today().month - 1
    for i in range(1, n+1):
        previous_month_idx = (current_month_idx - i) % 12
        m = int(previous_month_idx + 1)
        mnths.append(month_name[m])
    return mnths

def predict(trained_model,X_test):
    test_dmatrix = xgb.DMatrix(X_test)
    y_pred = trained_model.predict(test_dmatrix)
    return y_pred

def create_lastopen(bnid,slot):
    df = pd.DataFrame({'bnid':[bnid]*7,'year':[current_year]*7,'month_name':[current_month]*7,
                           'day_name':days.values.tolist(),'time_slot':[reverse_dict[slot]]*7,'pred_time':[slot]*7})
    return df.to_dict(orient='records')
    
def create_subscription_slots(bnid,city):
    df = pd.DataFrame({'bnid':[bnid]*7,'year':[current_year]*7,'month_name':[current_month]*7,
                           'day_name':days.values.tolist(),'city':[city]*7})
    return df.to_dict(orient='records')


if __name__ == "__main__":
    params = json.load(open("email_sendtime.json"))


# print('Code Started - step1a')
print('Code Started at 440 line')
current_month = date.today().strftime("%B")
current_year = date.today().year

month_dict = {'JAN':'January','FEB':'February','MAR':'March','APR':'April','MAY':'May','JUN':'June','JUL':'July',
             'AUG':'August','SEP':'September','OCT':'October','NOV':'November','DEC':'December'}

months = pd.Series(list(month_name))
days = pd.Series(list(day_name))

last_3_mnths = last_n_months(3)

# openfact = pd.read_csv(params["openfact"])
todays_date = time.strftime("%Y%m%d")
bucket_name='us-central1-prod-3-4076075b-bucket'
blobs = storage_client.list_blobs(bucket_name ,prefix='data/email_sendtime_propensity/input/'+todays_date+'/openfact')
paths=[]
for blob in blobs:  
    paths.append(blob.name)
    
fn= r"gs://us-central1-prod-3-4076075b-bucket/"

openfact=pd.DataFrame()
for filename in paths:
    df = pd.read_csv(fn+filename, index_col=None, header=0)
    openfact = openfact.append(df,ignore_index=True)

print("Loading of Openfact data is completed")

openfact = openfact[openfact['month_name'].isin(last_3_mnths)]

clickfact = pd.read_csv(params["clickfact"])
try:
    clickfact['month_name'] = clickfact['month_name'].apply(lambda x: month_dict[x])
except:
    pass
clickfact = clickfact[clickfact['month_name'].isin(last_3_mnths)]

# print('Data Loaded - step1b')

print('We are at 480 line')
## Preprocessing

months_sort = dict(zip(list(month_name), range(len(list(month_name)))))
days_sort = dict(zip(list(day_name), range(len(list(day_name)))))

openfact['month_rank'] = openfact['month_name'].map(months_sort)
openfact['day_rank'] = openfact['day_name'].map(days_sort)
openfact.sort_values(by=['bnid','year','month_rank','day_rank'],inplace=True)

clickfact['month_rank'] = clickfact['month_name'].map(months_sort)
clickfact['day_rank'] = clickfact['day_name'].map(days_sort)
clickfact.sort_values(by=['bnid','year','month_rank','day_rank'],inplace=True)

oc_req_columns = ['oc_8_10', 'oc_10_12', 'oc_12_14', 'oc_14_16','oc_16_18', 'oc_18_20', 'oc_20_8']
cf_req_columns = ['Browse_count_class_1', 'Browse_count_class_2', 'Browse_count_class_3','Browse_count_class_4', 'Browse_count_class_5', 'Browse_count_class_6','Browse_count_class_7']
cols_1mnth_c = ['1mnth_'+col for col in clickfact.columns if col.startswith('Browse')]
cf_cols_1mnth_c = ['all_1mnth_'+col for col in clickfact.columns if col.startswith('Browse')]
cols_3mnth_c = ['3mnth_'+col for col in clickfact.columns if col.startswith('Browse')]
cf_cols_3mnth_c = ['all_3mnth_'+col for col in clickfact.columns if col.startswith('Browse')]

cols_1mnth = ['1mnth_'+col for col in openfact.columns if col.startswith('oc')]
oc_cols_1mnth = ['all_1mnth_'+col for col in openfact.columns if col.startswith('oc')]
cols_3mnth = ['3mnth_'+col for col in openfact.columns if col.startswith('oc')]
oc_cols_3mnth = ['all_3mnth_'+col for col in openfact.columns if col.startswith('oc')]

final_cols = ['bnid', 'year', 'month_name', 'day_name', 'marital_status','derived_gender','ship_to_city']+cols_1mnth+oc_cols_1mnth +cols_3mnth+oc_cols_3mnth

## Openfact
openfact_grps = [grp.reset_index(drop=True) for di, grp in openfact.groupby('bnid')]
print('We are at 510 line')
# print('Data sub setted for 100k bnids - step1d')
results = []
try:
    with Pool(120) as spool:
        for file in (spool.imap_unordered(create_open_ad, openfact_grps)):
            # print('bnid level ad creation completed - step1f')
            results.extend(file)
            pass
except BrokenPipeError:
    pass
print('We are at 520 line')
spool.close()
spool.join()
print('We are at 525 line')
openfact_ad =  pd.DataFrame.from_dict(results)

openfact_ad.to_csv(params['output_path'] + "openfact_ad.csv",index=False) #TODO Remove

other_bnids = list(set(clickfact.bnid.unique().tolist()) - set(openfact.bnid.unique().tolist()))

sub_clickfact = clickfact[clickfact['bnid'].isin(other_bnids)]
sub_clickfact.sort_values(by=['bnid','year','month_rank','day_rank'],inplace=True)

## Clickfact

clickfact_grps = [grp.reset_index(drop=True) for di, grp in sub_clickfact.groupby('bnid')]

##data = clickfact_grps[:100000]

print('We are at 540 line')
results = []
with Pool(100) as spool:
    for file in (spool.imap_unordered(create_clicks_ad, clickfact_grps)):
        results.extend(file)
        pass
print('We are at 546 line')      
spool.close()
spool.join()

clickfact_ad =  pd.DataFrame.from_dict(results)

print('We are at 550 line')
clickfact_ad.to_csv(params['output_path'] + "clickfact_ad.csv",index=False) #TODO Remove

AD = pd.concat([openfact_ad,clickfact_ad],axis=0)
AD.reset_index(drop=True,inplace=True)

gender_df = pd.get_dummies(AD['derived_gender'],prefix = 'gender')
gender_df = gender_df[['gender_F','gender_M']]
marital_df = pd.get_dummies(AD['marital_status'],prefix = 'marital')
AD= pd.concat([AD,gender_df,marital_df],axis=1)
AD.drop(['marital_status','derived_gender', 'ship_to_city',],axis=1,inplace=True)

AD = AD.fillna(0)
AD['weekend_flag'] = AD['day_name'].apply(lambda x: 1 if x in ['Saturday','Sunday'] else 0)
AD['weekday_flag'] = AD['day_name'].apply(lambda x: 1 if x in ['Monday','Tuesday','Wednesday','Thursday','Friday'] else 0)

features = ['all_3mnth_oc_8_10',
'3mnth_oc_8_10',
'1mnth_oc_8_10',
'3mnth_Browse_count_class_1',

'all_3mnth_oc_10_12',
'3mnth_oc_10_12',
'1mnth_oc_10_12',
'3mnth_Browse_count_class_2',

'all_3mnth_oc_12_14',
'3mnth_oc_12_14',
'1mnth_oc_12_14',
'3mnth_Browse_count_class_3',

'all_3mnth_oc_14_16',
'3mnth_oc_14_16',
'1mnth_oc_14_16',
'3mnth_Browse_count_class_4',

'all_3mnth_oc_16_18',
'3mnth_oc_16_18',
'1mnth_oc_16_18',
'3mnth_Browse_count_class_5',

'all_3mnth_oc_18_20',
'3mnth_oc_18_20',
'1mnth_oc_18_20',
'3mnth_Browse_count_class_6',

'all_3mnth_oc_20_8',
'3mnth_oc_20_8',
'1mnth_oc_20_8',
'3mnth_Browse_count_class_7',
'gender_F', 'gender_M',
'marital_Married', 'marital_Single',
'weekend_flag','weekday_flag']

# pred_data = AD[features]

#Change..
pred_data = AD.reindex(columns=features)

xgb_model = joblib.load(params['local_path'])

pred_data['predicted_timeslot'] = predict(xgb_model,pred_data)

map_dict = {0: '16-18',1: '18-20',2: '20-8',3: '14-16',4: '8-10',5: '10-12',6: '12-14'}
map_df = pd.DataFrame({'pred_slot':map_dict.keys(),'time_slot':map_dict.values()})

pred_data = pd.merge(pred_data,map_df,left_on = 'predicted_timeslot',right_on='pred_slot',how='left')

timemap_dict = {'8-10':'08:00:00','10-12':'10:00:00','12-14':'12:00:00','14-16':'14:00:00','16-18':'16:00:00','18-20':'18:00:00','20-8':'20:00:00'}
timemap_df = pd.DataFrame({'time_slot':timemap_dict.keys(),'pred_time':timemap_dict.values()})

final = AD[['bnid', 'year', 'month_name', 'day_name']]
final = pd.merge(final,pred_data['time_slot'],left_index = True,right_index=True,how='left')
model_df = pd.merge(final,timemap_df,on = 'time_slot',how='left')

model_df.to_csv(params['output_path'] + "model_df.csv",index=False) #TODO Remove

## Last Open Logic

lastopen = pd.read_csv(params["lastopen"])
lastopen = lastopen[~lastopen['last_openslot'].isnull()]

model_bnids = list(set(clickfact.bnid.unique().tolist() + openfact.bnid.unique().tolist()))

lastopen = lastopen[~lastopen['bnid'].isin(model_bnids)]
lastopen.reset_index(drop=True,inplace=True)

reverse_dict = {i:v for i,v in zip(timemap_dict.values(),timemap_dict.keys())}

ipt = [(i,j) for i,j in zip(lastopen['bnid'],lastopen['last_openslot'])]
print('We are at 640 line')

results = []
with Pool(100) as spool:
    for d in spool.starmap(create_lastopen,(ipt)):
        results.extend(d)
        pass
spool.close()
spool.join()

print('We are at 650 line')
lastopen_df =  pd.DataFrame.from_dict(results)

lastopen_df.to_csv(params['output_path'] + "lastopen_df.csv",index=False) #TODO Remove

## Subscribed Logic

email_subscribed = pd.read_csv(params["subscribed"])

common_open = list(set(email_subscribed.bnid.unique().tolist())&set(openfact.bnid.unique().tolist()))
common_click = list(set(email_subscribed.bnid.unique().tolist())&set(other_bnids))
common_last = list(set(email_subscribed.bnid.unique().tolist())&set(lastopen_df.bnid.unique().tolist()))
common_model = common_open + common_click
common_all = common_open + common_click + common_last

model_df = model_df[model_df['bnid'].isin(common_model)]
model_df.rename(columns={'day_name':'day_of_week','time_slot':'preferred_time_slot','pred_time':'preferred_send_time'},inplace=True)
model_df = model_df[['bnid','year','month_name','day_of_week','preferred_time_slot','preferred_send_time']]

lastopen_df = lastopen_df[lastopen_df['bnid'].isin(common_last)]
lastopen_df.rename(columns={'day_name':'day_of_week','time_slot':'preferred_time_slot','pred_time':'preferred_send_time'},inplace=True)
lastopen_df = lastopen_df[['bnid','year','month_name','day_of_week','preferred_time_slot','preferred_send_time']]

subscribed_bnids =  list(set(email_subscribed['bnid'].unique().tolist()) - set(common_all))

email_subscribed = email_subscribed[email_subscribed['bnid'].isin(subscribed_bnids)]
    
ipt = [(i,j) for i,j in zip(email_subscribed['bnid'],email_subscribed['ship_to_city'])]

results = []
with Pool(100) as spool:
    for d in spool.starmap(create_subscription_slots,(ipt)):
        results.extend(d)
        pass
spool.close()
spool.join()

print("We are on 665 line")

subscribed_df = pd.DataFrame.from_dict(results)

subscribed_df.to_csv(params['output_path'] + "subscribed_output.csv",index=False) #TODO remove

subscribed_df['time_slot'] = '16-18'
subscribed_df['pred_time'] = '16:00:00'


# sql1 = """SELECT * FROM bnile-data-eng-dev.email_sendtime.city_zone_mapping"""
# print("SQL query executed.")
# time_df_1 = client_1.query(sql1).to_dataframe(progress_bar_type="tqdm")

time_df = pd.read_csv("gs://us-central1-prod-3-4076075b-bucket/data/email_sendtime_propensity/city_zone_mapping.csv")

print("We are on 680 line")

subscribed_df = pd.merge(subscribed_df,time_df[['City','Corrected_Time_Slot']],left_on= 'city',right_on='City',how='left')

subscribed_df['Corrected_Time_Slot'] = subscribed_df['Corrected_Time_Slot'].apply(lambda x: '16:00:00' if pd.isnull(x) else x)

subscribed_df = subscribed_df[['bnid','year','month_name','day_name','time_slot','Corrected_Time_Slot']]
subscribed_df.rename(columns={'Corrected_Time_Slot':'pred_time'},inplace=True)

subscribed_df = subscribed_df[['bnid','year','month_name','day_name','pred_time']]
subscribed_df = pd.merge(subscribed_df,timemap_df,on= 'pred_time',how='left')
subscribed_df.rename(columns={'day_name':'day_of_week','time_slot':'preferred_time_slot','pred_time':'preferred_send_time'},inplace=True)
subscribed_df = subscribed_df[['bnid','year','month_name','day_of_week','preferred_time_slot','preferred_send_time']]

model_df.rename(columns={'day_name':'day_of_week','time_slot':'preferred_time_slot','pred_time':'preferred_send_time'},inplace=True)
lastopen_df.rename(columns={'day_name':'day_of_week','time_slot':'preferred_time_slot','pred_time':'preferred_send_time'},inplace=True)

model_df = model_df[['bnid','year','month_name','day_of_week','preferred_time_slot','preferred_send_time']]
lastopen_df = lastopen_df[['bnid','year','month_name','day_of_week','preferred_time_slot','preferred_send_time']]

subscribed_df.bnid.nunique()+model_df.bnid.nunique()+lastopen_df.bnid.nunique()

## Final Predictions

predicted_df = pd.concat([model_df,lastopen_df,subscribed_df])


predicted_df.to_csv(params['output_path'] + "predicted_emailslot.csv",index=False)

print("Its Done")
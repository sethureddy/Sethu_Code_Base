import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
from PreProcess import PreProcess
import json

def topsis_score(dataframe, p_sln, n_sln):
    """
    Description: This function gives us distance_positive, distance_negative & topsis score as output.
    Arguments for this function are dataframe, ideal best and ideal worst values.
    """
    score = [] # Topsis score
    pp = [] # distance positive
    nn = [] # distance negative

    for i in range(len(dataframe)):
        temp_p, temp_n = 0, 0
        for j in range(1, dataframe.shape[1]):
            temp_p = temp_p + (p_sln[j-1] - dataframe.iloc[i, j])**2
            temp_n = temp_n + (n_sln[j-1] - dataframe.iloc[i, j])**2   
        temp_p, temp_n = temp_p**0.5, temp_n**0.5
        score.append(temp_n/(temp_p + temp_n))
        nn.append(temp_n)
        pp.append(temp_p)
    return pp,nn,score

	
def campaign_ranking(params):
    
    input_file = params['input_file']
    output_path = params["output_path"]

    ##Reading file
    df = pd.read_csv(input_file)
    print("line 35 done")
    ################### Overall Ranking #################
    """ Considering clicks, click_ratio, revenue, margin columns for overall ranking """
    df_overall=df[['placement_key','clicks','click_ratio','revenue','margin']].drop_duplicates().fillna(0)
    df_overall['clicks'] = df_overall['clicks'].astype('float64')

    df_overall_norm = PreProcess(df_overall).Normalize(df_overall.shape[1],[1,1,2,1.5]) # weights: clicks and click_ratio = 1, Revenue=2, Margin=1.5
    p_sln, n_sln = PreProcess(df_overall_norm).Calc_Values(df_overall.shape[1],['+','+','+','+'])

    df_overall_norm1 = pd.DataFrame()
    """ Calculating distance_positive, distance_negative & topsis_score """
    df_overall_norm1['distance_positive'] = topsis_score(df_overall_norm,p_sln,n_sln)[0]
    df_overall_norm1['distance_negative'] = topsis_score(df_overall_norm,p_sln,n_sln)[1]
    df_overall_norm1['Topsis_Score_Overall'] = topsis_score(df_overall_norm,p_sln,n_sln)[2]

    print("line 50 done")
    df_overall_norm = df_overall_norm.reset_index()
    df_overall_norm_new = pd.concat([df_overall_norm,df_overall_norm1],axis=1)

    # calculating the rank according to topsis score
    df_overall_norm_new['Overall_Rank'] = (df_overall_norm_new['Topsis_Score_Overall'].rank(method='max', ascending=False))
    df_overall_norm_new = df_overall_norm_new.astype({"Overall_Rank": int})
    df_overall_norm_new=df_overall_norm_new.sort_values('Overall_Rank')

    ################# Revenue Ranking ##################
    """ Considering revenue, margin columns for revenue ranking """
    df_rev=df[["placement_key","revenue","margin"]].drop_duplicates().fillna(0)
    df_rev_norm = PreProcess(df_rev).Normalize(df_rev.shape[1],[1,1]) # we do not apply weights in Revenue Ranking, so giving all columns 1 as weight. 
    p_sln1, n_sln1 = PreProcess(df_rev_norm).Calc_Values(df_rev.shape[1],['+','+'])

    df_rev_norm1 = pd.DataFrame()
    df_rev_norm1['distance_positive'] = topsis_score(df_rev_norm,p_sln1,n_sln1)[0]
    df_rev_norm1['distance_negative'] = topsis_score(df_rev_norm,p_sln1,n_sln1)[1]
    df_rev_norm1['Topsis_Score_Revenue'] = topsis_score(df_rev_norm,p_sln1,n_sln1)[2]

    df_rev_norm = df_rev_norm.reset_index()
    df_rev_norm_new = pd.concat([df_rev_norm,df_rev_norm1],axis=1)

    # calculating the rank according to topsis score
    df_rev_norm_new['Revenue_Rank'] = (df_rev_norm_new['Topsis_Score_Revenue'].rank(method='max', ascending=False))
    df_rev_norm_new = df_rev_norm_new.astype({"Revenue_Rank": int})
    df_rev_norm_new=df_rev_norm_new.sort_values('Revenue_Rank')

    ################ Response Ranking ###################
    """ Considering clicks, click_ratio columns for response ranking """
    df_response = df[["placement_key","clicks","click_ratio"]].drop_duplicates().fillna(0)
    df_response['clicks'] = df_response['clicks'].astype('float64')

    df_response_norm = PreProcess(df_response).Normalize(df_response.shape[1],[1,1]) # we do not apply weights in Response Ranking, so giving all columns 1 as weight. 
    p_sln2, n_sln2 = PreProcess(df_response_norm).Calc_Values(df_response.shape[1],['+','+'])

    df_response_norm1 = pd.DataFrame()
    df_response_norm1['distance_positive'] = topsis_score(df_response_norm,p_sln2,n_sln2)[0]
    df_response_norm1['distance_negative'] = topsis_score(df_response_norm,p_sln2,n_sln2)[1]
    df_response_norm1['Topsis_Score_Response'] = topsis_score(df_response_norm,p_sln2,n_sln2)[2]
    print("line 90 done")
    df_response_norm = df_response_norm.reset_index()
    df_response_norm_new = pd.concat([df_response_norm,df_response_norm1],axis=1)

    # calculating the rank according to topsis score
    df_response_norm_new['Response_Rank'] = (df_response_norm_new['Topsis_Score_Response'].rank(method='max', ascending=False))
    df_response_norm_new = df_response_norm_new.astype({"Response_Rank": int})
    df_response_norm_new=df_response_norm_new.sort_values('Response_Rank')


    ###### Combining all three rankings into final table ##########

    df1 = df[['placement_key','campaign_id','placement_name','year','month','sends','clicks','click_ratio','revenue','margin']].drop_duplicates(subset='placement_key')
    df_overall_final = pd.merge(df1,df_overall_norm_new[['placement_key','Topsis_Score_Overall','Overall_Rank']],on='placement_key',how='left')
    df_response_final = pd.merge(df_overall_final,df_response_norm_new[['placement_key','Topsis_Score_Response','Response_Rank']],on='placement_key',how='left')
    df_revenue_final = pd.merge(df_response_final,df_rev_norm_new[['placement_key','Topsis_Score_Revenue','Revenue_Rank']],on='placement_key',how='left')
    df_revenue_final = df_revenue_final.sort_values('Overall_Rank')
    df_final = df_revenue_final[['placement_key','placement_name','campaign_id','year','month','sends','clicks','click_ratio','revenue','margin','Topsis_Score_Response','Topsis_Score_Revenue','Topsis_Score_Overall','Response_Rank','Revenue_Rank','Overall_Rank']]
    df_final.rename(columns={'Topsis_Score_Response':'topsis_score_response','Topsis_Score_Revenue':'topsis_score_revenue','Topsis_Score_Overall':'topsis_score_overall','Response_Rank':'response_rank','Revenue_Rank':'revenue_rank','Overall_Rank':'overall_rank'},inplace=True)

    ###### Adding refresh_date (run date) columns 
    df_final['refresh_date'] = datetime.today().strftime('%Y-%m-%d')
    df_final['refresh_date'] = pd.to_datetime(df_final['refresh_date'])
    df_final['ranking_month'] = df_final['refresh_date'].dt.strftime('%b-%Y')

    #### Saving the output
    # df_final.to_csv(output_path + 'trigger_campaign_rank_output.csv')
    print("File is saving....")
    df_final.to_csv(output_path)
    print("Done")
    
if __name__ == "__main__":
    # params = json.load(open("trigger_campaign_rank.json"))
    params = json.load(open("campaign_ranking.json"))
    res = campaign_ranking(params)
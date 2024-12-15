import pandas as pd
import numpy as np
from difflib import SequenceMatcher

class postprocess():
    
    def postprocess_address(self,match_dd):
        #Combining bill to street & bill to postal code columns
        match_dd = match_dd[match_dd['similarity']>0.4]
        match_dd['left_billto'] = match_dd['left_side'].apply(lambda x: ' '.join([x.split('$')[0],x.split('$')[1]]))
        match_dd['right_billto'] = match_dd['right_side'].apply(lambda x: ' '.join([x.split('$')[0],x.split('$')[1]]))
        match_dd['billto_score'] = match_dd.apply(lambda x: SequenceMatcher(None,x['left_billto'],x['right_billto']).ratio(),axis=1)
        match_dd['flag'] = np.where(match_dd['left_side'] == match_dd['right_side'],1,0)
        match1 = match_dd[match_dd['flag']==0]
        match1 = match1.loc[pd.DataFrame(np.sort(match1[['left_side','right_side']],1),index=match1.index).drop_duplicates().index]
        final = match1[match1['billto_score']>0.8]
        final['left_name'] = final['left_side'].apply(lambda x: x.split('$')[2])
        final['right_name'] = final['right_side'].apply(lambda x: x.split('$')[2])
        final['name_score'] = final.apply(lambda x: SequenceMatcher(None,x['left_name'],x['right_name']).ratio(),axis=1)
        final_df = final[final.name_score > 0.8]
        final_names =  list(set(final_df['left_side'].tolist()+final_df['right_side'].tolist()))
        return final_names,final_df

    def postprocess_email_base1(self,match_dd):
        match_dd = match_dd[match_dd['similarity']>0.4]
        match_dd['left_emailpostal'] = match_dd['left_side'].apply(lambda x: ' '.join([x.split('$')[0],x.split('$')[1]]))
        match_dd['right_emailpostal'] = match_dd['right_side'].apply(lambda x: ' '.join([x.split('$')[0],x.split('$')[1]]))
        match_dd['emailpostal_score'] = match_dd.apply(lambda x: SequenceMatcher(None,x['left_emailpostal'],x['right_emailpostal']).ratio(),axis=1)    
        match_dd['flag'] = np.where(match_dd['left_side'] == match_dd['right_side'],1,0)
        match1 = match_dd[match_dd['flag']==0]
        match1 = match1.loc[pd.DataFrame(np.sort(match1[['left_side','right_side']],1),index=match1.index).drop_duplicates().index]
        final = match1[match1['emailpostal_score']>0.8]
        final['left_name'] = final['left_side'].apply(lambda x: ' '.join([x.split('$')[2],x.split('$')[3]]))
        final['right_name'] = final['right_side'].apply(lambda x: ' '.join([x.split('$')[2],x.split('$')[3]]))
        final['name_score'] = final.apply(lambda x: SequenceMatcher(None,x['left_name'],x['right_name']).ratio(),axis=1)
        final_df = final[final.name_score > 0.8]
        final_names =  list(set(final_df['left_side'].tolist()+final_df['right_side'].tolist()))
        return final_names,final_df
    
    def postprocess_email_base2(self,match_dd):
        match_dd = match_dd[match_dd['similarity']>0.4]
        match_dd['left_emailpostal'] = match_dd['left_side'].apply(lambda x: ' '.join([x.split('$')[0],x.split('$')[1]]))
        match_dd['right_emailpostal'] = match_dd['right_side'].apply(lambda x: ' '.join([x.split('$')[0],x.split('$')[1]]))
        match_dd['emailpostal_score'] = match_dd.apply(lambda x: SequenceMatcher(None,x['left_emailpostal'],x['right_emailpostal']).ratio(),axis=1)
        match_dd['flag'] = np.where(match_dd['left_side'] == match_dd['right_side'],1,0)
        match1 = match_dd[match_dd['flag']==0]
        match1 = match1.loc[pd.DataFrame(np.sort(match1[['left_side','right_side']],1),index=match1.index).drop_duplicates().index]
        final = match1[match1['emailpostal_score']>0.8]
        final['left_billtostreet'] = final['left_side'].apply(lambda x: ' '.join([x.split('$')[2]]))
        final['right_billtostreet'] = final['right_side'].apply(lambda x: ' '.join([x.split('$')[2]]))
        final['billtostreet_score'] = final.apply(lambda x: SequenceMatcher(None,x['left_billtostreet'],x['right_billtostreet']).ratio(),axis=1)
        final = final[final.billtostreet_score > 0.8]
        final['left_name'] = final['left_side'].apply(lambda x: ' '.join([x.split('$')[3]]))
        final['right_name'] = final['right_side'].apply(lambda x: ' '.join([x.split('$')[3]]))
        final['name_score'] = final.apply(lambda x: SequenceMatcher(None,x['left_name'],x['right_name']).ratio(),axis=1)
        final_df = final[final.name_score > 0.8]
        
        final_names =  list(set(final_df['left_side'].tolist()+final_df['right_side'].tolist()))
        return final_names,final_df

    def postprocess_payment(self,match_dd):
        match_dd = match_dd[match_dd['similarity']>0.4]
        #Combining payment
        match_dd['left_pay'] = match_dd['left_side'].apply(lambda x: ' '.join([x.split('$')[0]]))
        match_dd['right_pay'] = match_dd['right_side'].apply(lambda x: ' '.join([x.split('$')[0]]))
        match_dd['pay_score'] = match_dd.apply(lambda x: SequenceMatcher(None,x['left_pay'],x['right_pay']).ratio(),axis=1)
        match_dd['flag'] = np.where(match_dd['left_side'] == match_dd['right_side'],1,0)
        match1 = match_dd[match_dd['flag']==0]
        match1 = match1.loc[pd.DataFrame(np.sort(match1[['left_side','right_side']],1),index=match1.index).drop_duplicates(keep='first').index]
        final = match1[match1['pay_score']>=1]
        #Combining postal codes
        final['left_postal'] = final['left_side'].apply(lambda x: ' '.join([x.split('$')[1],x.split('$')[2],x.split('$')[3]]))
        final['right_postal'] = final['right_side'].apply(lambda x: ' '.join([x.split('$')[1],x.split('$')[2],x.split('$')[3]]))
        final['postal_score'] = final.apply(lambda x: SequenceMatcher(None,x['left_postal'],x['right_postal']).ratio(),axis=1)
        final = final[final['postal_score']>0.8]
        final['left_billtostreet'] = final['left_side'].apply(lambda x: x.split('$')[3])
        final['right_billtostreet'] = final['right_side'].apply(lambda x: x.split('$')[3])
        final['billtostreet_score'] = final.apply(lambda x: SequenceMatcher(None,x['left_billtostreet'],x['right_billtostreet']).ratio(),axis=1)
        final = final[final['billtostreet_score']>0.8]
        final['left_name'] = final['left_side'].apply(lambda x: x.split('$')[4])
        final['right_name'] = final['right_side'].apply(lambda x: x.split('$')[4])
        final['name_score'] = final.apply(lambda x: SequenceMatcher(None,x['left_name'],x['right_name']).ratio(),axis=1)
        final_df = final[final.name_score > 0.7]
        final_names =  list(set(final_df['left_side'].tolist()+final_df['right_side'].tolist()))
        return final_names,final_df
    def postprocess_phone_base1(self,match_dd):
        match_dd = match_dd[match_dd['similarity']>0.4]
        #Combining phone
        match_dd['left_phone'] = match_dd['left_side'].apply(lambda x: x.split('$')[0])
        match_dd['right_phone'] = match_dd['right_side'].apply(lambda x: x.split('$')[0])
        match_dd['ph_score'] = match_dd.apply(lambda x: SequenceMatcher(None,x['left_phone'],x['right_phone']).ratio(),axis=1)
        match_dd['flag'] = np.where(match_dd['left_side'] == match_dd['right_side'],1,0)
        match1 = match_dd[match_dd['flag']==0]
        match1 = match1.loc[pd.DataFrame(np.sort(match1[['left_side','right_side']],1),index=match1.index).drop_duplicates(keep='first').index]
        final = match1[match1['ph_score']>0.7]
        final['left_name'] = final['left_side'].apply(lambda x: x.split('$')[1])
        final['right_name'] = final['right_side'].apply(lambda x: x.split('$')[1])
        final['name_score'] = final.apply(lambda x: SequenceMatcher(None,x['left_name'],x['right_name']).ratio(),axis=1)
        final_df = final[final.name_score > 0.7]
        final_names =  list(set(final_df['left_side'].tolist()+final_df['right_side'].tolist()))
        return final_names,final_df


    def postprocess_phone_base2(self,match_dd):
        match_dd = match_dd[match_dd['similarity']>0.4]
        #Combining phone
        match_dd['left_phone'] = match_dd['left_side'].apply(lambda x: x.split('$')[0])
        match_dd['right_phone'] = match_dd['right_side'].apply(lambda x: x.split('$')[0])
        match_dd['ph_score'] = match_dd.apply(lambda x: SequenceMatcher(None,x['left_phone'],x['right_phone']).ratio(),axis=1)
        match_dd['flag'] = np.where(match_dd['left_side'] == match_dd['right_side'],1,0)
        match1 = match_dd[match_dd['flag']==0]
        match1 = match1.loc[pd.DataFrame(np.sort(match1[['left_side','right_side']],1),index=match1.index).drop_duplicates(keep='first').index]
        final = match1[match1['ph_score']>0.7]
        final['left_name'] = final['left_side'].apply(lambda x: ' '.join([x.split('$')[1],x.split('$')[2]]))
        final['right_name'] = final['right_side'].apply(lambda x: ' '.join([x.split('$')[1],x.split('$')[2]]))
        final['name_score'] = final.apply(lambda x: SequenceMatcher(None,x['left_name'],x['right_name']).ratio(),axis=1)
        final_df = final[final.name_score > 0.7]
        final_names =  list(set(final_df['left_side'].tolist()+final_df['right_side'].tolist()))
        return final_names,final_df


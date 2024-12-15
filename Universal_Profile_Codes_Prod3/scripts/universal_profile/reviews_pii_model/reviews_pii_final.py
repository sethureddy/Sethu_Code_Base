import os
import calendar
import argparse
from multiprocessing import Pool
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import yaml
from tqdm import tqdm

from google.cloud import bigquery as bgq

import nltk
from nltk import ngrams
import re
from textblob import Word, TextBlob
from nltk.corpus import stopwords
from string import punctuation

nltk.download('stopwords')
nltk.download('wordnet')
stop_words = stopwords.words('english')


def create_flags(review):

    if (len(set(review.split(' ')).intersection(keyword_dict['marital_words'])) > 0) or (len([i for i in ngrams(review.split(), 2) if i[0]+" "+i[1] in keyword_dict['marital_words']]) > 0):
        marital_flag = "Married"
    else:
        marital_flag = np.nan
        
    if (len(set(review.split(' ')).intersection(keyword_dict['recently_engaged'])) > 0) or (len([i for i in ngrams(review.split(), 2) if i[0]+" "+i[1] in keyword_dict['recently_engaged']]) > 0):
        engagement_flag = "Engaged"
    else:
        engagement_flag = np.nan
        
    if (len(set(review.split(' ')).intersection(keyword_dict['birthday'])) > 0) or (len([i for i in ngrams(review.split(), 2) if i[0]+" "+i[1] in keyword_dict['birthday']]) > 0):
        birthday = 1
    else:
        birthday = 0

    if (len(set(review.split(' ')).intersection(keyword_dict['anniversary'])) > 0) or (len([i for i in ngrams(review.split(), 2) if i[0]+" "+i[1] in keyword_dict['anniversary']]) > 0):
        anniversary_flag = 1
    else:
        anniversary_flag = 0
        
    if (len(set(review.split(' ')).intersection(keyword_dict['wedding_day'])) > 0) or (len([i for i in ngrams(review.split(), 2) if i[0]+" "+i[1] in keyword_dict['wedding_day']]) > 0):
        wedding_flag = 1
    else:
        wedding_flag = 0
        
    if (len(set(review.split(' ')).intersection(keyword_dict['gender_M'])) > 0) or (len([i for i in ngrams(review.split(), 2) if i[0]+" "+i[1] in keyword_dict['gender_M']]) > 0):
        gender = "M"
    
    elif (len(set(review.split(' ')).intersection(keyword_dict['gender_F'])) > 0) or (len([i for i in ngrams(review.split(), 2) if i[0]+" "+i[1] in keyword_dict['gender_F']]) > 0):
        gender = "F"
    
    else:
        gender = np.nan
        
    return marital_flag, engagement_flag, birthday, anniversary_flag, wedding_flag, gender

def create_date(date, type1, threshold, flag = "Null"):
    
    if flag == "Null":
        final_date = correct_date(date, threshold, type1)
        
    elif flag == "birthday":
        
        temp_date = correct_date(date, threshold, type1)
        final_date = temp_date.strftime('%Y-%m-%d').replace(str(temp_date.year),"0000")
        
    return final_date

def correct_date(date,threshold,type1):

    if type1 == "order":
        end_date = date + timedelta(days=threshold)
    else:
        end_date = date - timedelta(days=threshold)
    
    return end_date

def date_process(flag,order_date,review_date,threshold,birth_flag="Null"):

    if flag > 0 and pd.isnull(order_date):
        if birth_flag == "birthday":
            return create_date(review_date,"review",threshold,"birthday")
        else:
            return create_date(review_date,"review",threshold)
        
    elif flag > 0 and pd.notnull(order_date):
        if birth_flag == "birthday":
            return create_date(order_date,"order",threshold,"birthday")
        else:
            return create_date(order_date,"order",threshold)
    else:
        return np.nan
    
def extract_person(tags,corpus):
    list_len = []
    
    if len(tags.split(',')) > 1:
        family_words = tags.split(',')
        
        for j in family_words:
            sub_elem = corpus.split(" ")
            base = sub_elem.index(list(set(sub_elem).intersection(keyword_dict["birthday"]))[0])
            list_len.append(base-sub_elem.index(j))
            
        return family_words[np.argmax(list_len)]
    else:
        return tags

def correct_spelling(idx,review):
    rdict = {}
    rdict["index"] = idx
    rdict["corrected_corpus"] = str(TextBlob(review).correct())
    return rdict

def extract_pii_info(spell_correction=False,CPU_COUNT=20,threshold=10):

    client = bgq.Client()

    sql = """Select * From (
    Select distinct orf.*,
    first_value(order_date_key) over (partition by reviewer_email_address, review_date_key order by order_date_key desc) as order_date_key
    From `{OFFER_REVIEWS_DATA}` orf
    Left join
    (
    SELECT distinct pof.guid_key, pof.order_date_key,email_address
    FROM `{PRODUCT_ORDER_FACT}`  pof
    Left join `{EMAIL_GUID}` eg
    on pof.guid_key= eg.guid_key
    ) pof
    on orf.reviewer_email_address= pof.email_address  and pof.order_date_key<=orf.review_date_key
    )""".format(OFFER_REVIEWS_DATA=OFFER_REVIEWS_DATA, PRODUCT_ORDER_FACT=PRODUCT_ORDER_FACT, EMAIL_GUID=EMAIL_GUID)
    reviews_data = client.query(sql).to_dataframe(progress_bar_type="tqdm")
    
    reviews_data['order_date_key'] = reviews_data['order_date_key'].apply(lambda x: x if pd.isnull(x) else datetime.strptime(str(int(x)), '%Y%m%d'))
    reviews_data['review_date_key'] = reviews_data['review_date_key'].apply(lambda x: x if pd.isnull(x) else datetime.strptime(str(x), '%Y%m%d'))
    
    try:
        sql = """SELECT * FROM `{OFFER_REVIEWS_PII}` where run_date = max(run_date)""".format(OFFER_REVIEWS_PII=OFFER_REVIEWS_PII)
        reviews_pii = client.query(sql).to_dataframe(progress_bar_type="tqdm")
        reviews_data = reviews_data[reviews_data['review_date_key'] > reviews_pii['run_date']]
    except:
        pass
    
    # Adding RUN Date
    reviews_data["RUN_DATE"] = datetime.today().strftime('%Y-%m-%d')

    if reviews_data.empty:
        return None

    reviews_data['review'] = reviews_data['review_content'].apply(lambda text: " ".join(x.lower() for x in str(text).split()))
    
    reviews_data['corpus'] = reviews_data['review_content'].apply(lambda text: " ".join(x.lower() for x in str(text).split()))
    reviews_data['corpus'] = reviews_data['corpus'].apply(lambda text: text.replace("my girl","girlfriend "))
    reviews_data['corpus'] = reviews_data['corpus'].apply(lambda text: text.replace("mom","mother "))
    reviews_data['corpus'] = reviews_data['corpus'].apply(lambda text: text.replace("."," "))
    reviews_data['corpus'] = reviews_data['corpus'].str.replace('[^\w\s]','')
    reviews_data['corpus'] = reviews_data['corpus'].apply(lambda text: "".join(x for x in text if not x.isdigit()))
    reviews_data['corpus'] = reviews_data['corpus'].apply(lambda text: "".join(x for x in text if x not in punctuation))
    reviews_data['corpus'] = reviews_data['corpus'].apply(lambda text: " ".join(x for x in text.split() if x not in stop_words))
    reviews_data['corpus'] = reviews_data['corpus'].apply(lambda text: " ".join([Word(word).lemmatize() for word in text.split()]))
    
    if spell_correction:
        print("Spelling Correction Started")
        ipt = [(i,j) for i,j in zip(reviews_data['corpus'].index,reviews_data['corpus'])]                         
        results = []
        with Pool(CPU_COUNT) as spool:
            for d in spool.starmap(correct_spelling,tqdm(ipt,total=len(ipt))):
                results.append(d)
                pass
        spool.close()
        spool.join()
        corrected_reviews = pd.DataFrame(results)
        reviews_data = pd.merge(reviews_data,corrected_reviews, left_on=reviews_data.index,right_on=corrected_reviews['index'],how='left')
        del reviews_data["corpus"]
        reviews_data.rename(columns = {"corrected_corpus":"corpus"},inplace=True)
    
    reviews_data['corpus'] = reviews_data['corpus'].apply(lambda text: " ".join([Word(word).lemmatize() for word in text.split()]))
    reviews_data['corpus'] = reviews_data['corpus'].apply(lambda text: " ".join(x for x in text.split() if len(x)>=3))
        
    marital_flag, engagement_flag, birthday, anniversary,wedding, gender = [],[],[],[],[],[]
    for review in tqdm(reviews_data['corpus']):
        mf, ef, bday, anv, wed, gen  = create_flags(review)
        marital_flag.append(mf)
        engagement_flag.append(ef)
        birthday.append(bday)
        anniversary.append(anv)
        wedding.append(wed)
        gender.append(gen)
        
    reviews_data['marital_flag'],reviews_data['engagement_flag'],reviews_data['birthday'],reviews_data['anniversary'],reviews_data['wedding'], reviews_data['gender'] = marital_flag, engagement_flag, birthday, anniversary, wedding, gender
    
    reviews_data["marital_status"] = reviews_data["engagement_flag"].fillna(reviews_data["marital_flag"])
    
    reviews_data["anniversary_date"] = reviews_data.apply(lambda x: date_process(x["anniversary"],x["order_date_key"],x["review_date_key"],threshold),axis=1)
    
    reviews_data["wedding_date"] = reviews_data.apply(lambda x: date_process(x["wedding"],x["order_date_key"],x["review_date_key"],threshold),axis=1)
    
    reviews_data["birth_date"] = reviews_data.apply(lambda x: date_process(x["birthday"],x["order_date_key"],x["review_date_key"],threshold,"birthday"),axis=1)
    
    reviews_data["birthday_info"] = reviews_data["corpus"].apply(lambda x: ",".join([i for i in set(x.split(' ')).intersection(keyword_dict['family_flag'])]))
    
    reviews_data["birthday_info"] = reviews_data.apply(lambda x: x["birthday_info"] if pd.notnull(x["birth_date"]) else np.nan, axis=1)
    
    try:
        reviews_data["birthday_info"].replace({"":None},inplace=True)
    except:
        pass
        
    reviews_data['birthday_info'] = reviews_data.apply(lambda x: extract_person(x['birthday_info'],x['corpus']) if pd.notnull(x["birthday_info"]) else x["birthday_info"], axis=1)
    
    reviews_data['review_date_key'] = reviews_data['review_date_key'].apply(lambda x: int(x.strftime('%Y%m%d')))
    reviews_data['anniversary_month'] = reviews_data['anniversary_date'].apply(lambda x: calendar.month_name[x.month] if pd.notnull(x) else x)
    
    reviews_data['wedding_month'] = reviews_data['wedding_date'].apply(lambda x: calendar.month_name[x.month] if pd.notnull(x) else x)
    reviews_data['wedding_year'] = reviews_data['wedding_date'].apply(lambda x: x.year if pd.notnull(x) else x)
    reviews_data['wedding_year'] = reviews_data['wedding_year'].convert_dtypes()
    
    reviews_data['marital_status'] = reviews_data.apply(lambda x: "Married" if (pd.notnull(x["wedding_date"]) or pd.notnull(x['anniversary_date'])) else x["marital_status"], axis=1)

    reviews_data['birthday_month'] = reviews_data['birth_date'].apply(lambda x: calendar.month_name[int(x.split('-')[1])] if pd.notnull(x) else x)
    reviews_data["birthday_month"] = reviews_data.apply(lambda x: np.nan if pd.notnull(x["anniversary_month"]) else x["birthday_month"],axis=1)
    reviews_data["wedding_month"] = reviews_data.apply(lambda x: np.nan if (pd.notnull(x["anniversary_month"]) or pd.notnull(x["birthday_month"])) else x['wedding_month'],axis=1)
    reviews_data["wedding_year"] = reviews_data.apply(lambda x: np.nan if (pd.notnull(x["anniversary_month"]) or pd.notnull(x["birthday_month"])) else x['wedding_year'],axis=1)
    
    
    reviews_data['birthday_info'] = reviews_data.apply(lambda x: 'individual' if (pd.notnull(x['birth_date']) and pd.isnull(x['birthday_info'])) else x['birthday_info'], axis=1)
    reviews_data['birthday_month'] = reviews_data.apply(lambda x: np.nan if (x['birthday_info'] == 'individual') and (len(set(x['review'].split(' ')).intersection(['she','her','his'])) > 0) else x['birthday_month'], axis=1)
    reviews_data['birthday_info'] = reviews_data.apply(lambda x: np.nan if pd.isnull(x['birthday_month']) else x['birthday_info'], axis=1)
    
    reviews_data['marital_status'] = reviews_data.apply(lambda x: 'Engaged' if ("fiance" in x['corpus']) or ("engaged" in x["corpus"]) else x['marital_status'],axis=1)
    reviews_data['wedding_month'] = reviews_data.apply(lambda x: np.nan if (x['marital_status'] == 'Engaged') else x['wedding_month'], axis=1)
    reviews_data['wedding_year'] = reviews_data.apply(lambda x: np.nan if (x['marital_status'] == 'Engaged') else x['wedding_year'], axis=1)
    reviews_data['anniversary_month'] = reviews_data.apply(lambda x: np.nan if (x['marital_status'] == 'Engaged') else x['anniversary_month'], axis=1)
    
    reviews_pii_data = reviews_data

    reviews_pii_data = reviews_data[['offer_key', 'offer_id', 'review_date_key', 'review_date',
       'review_title', 'review_content', 'review_score', 'published',
       'offer_url', 'product_image_url', 'reviewer_display_name',
       'reviewer_email_address', 'reviewer_country', 'reviewer_ip_address',
       'form_recommendation', 'questionaire_recommendation',
       'questionnaire_why_bluenile', 'yotpo_id', 'user_type', 'appkey',
       'published_image_url', 'unpublished_image_url', 'published_video_url',
       'unpublished_video_url','RUN_DATE', 'gender', 'marital_status','anniversary_month',
       'wedding_month', 'wedding_year','birthday_info', 'birthday_month']]

    reviews_pii_data.rename(columns = {'birthday_month':'RELATIONS_BIRTHDAY_MONTH','birthday_info':'RELATIONS_BIRTHDAY_FLAGS'},inplace=True)
    
    reviews_pii_data.to_csv(output_path + "/output/" + "reviews_pii_" + datetime.today().strftime('%Y%m%d') + ".csv",index=False)
    

parser = argparse.ArgumentParser(description="Input tables information")
parser.add_argument('params_file', metavar='FILENAME', type=str, help='Parameter file name in yaml format')
args = parser.parse_args()

try:
    params = yaml.load(open(args.params_file))
except:
    print(f'Error loading parameter file: {args.params_file}.')
    sys.exit(1)

OFFER_REVIEWS_DATA = params['OFFER_REVIEWS_DATA']
PRODUCT_ORDER_FACT = params['PRODUCT_ORDER_FACT']
EMAIL_GUID = params['EMAIL_GUID']
OFFER_REVIEWS_PII = params['OFFER_REVIEWS_PII']
output_path = params['output_path']
threshold = params['threshold']
spell_correction = params['spell_correction']
CPU_COUNT = params['CPU_COUNT']
keyword_dict = params['keyword_dict']

if __name__ == "__main__":
    final = extract_pii_info(spell_correction,CPU_COUNT,threshold)
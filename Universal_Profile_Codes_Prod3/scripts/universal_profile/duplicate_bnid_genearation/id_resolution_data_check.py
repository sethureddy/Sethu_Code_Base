import time
from airflow import models
from airflow.models import Variable
from google.cloud import bigquery as bgq
client = bgq.Client()
from datetime import timedelta, datetime
import datetime

from airflow.contrib.operators import bigquery_operator

project_name=models.Variable.get("gcp_project")
execution_date=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")

date_query = '''SELECT substring(cast(TIMESTAMP_MILLIS(last_modified_time) as string),1,10) as latest_modified_time
                FROM `o_customer.__TABLES__` where table_id = "id_resolution"'''

waiting_time_limit = 60 #one hour
waiting_gap = 2 #5 mins

def get_date():
    result=client.query(date_query).to_dataframe()
    latest_modified_time=result['latest_modified_time'][0]

    return latest_modified_time
    
def main(**kwargs):
    todays_date= time.strftime("%Y-%m-%d")
    waiting_time = 0
    while True:
        latest_modified_time = get_date()
        if latest_modified_time == todays_date:
            print("The table date is matched, So data will be available for today...")
            return 
        elif waiting_time>=waiting_time_limit:
            raise Exception("Waited for "+str(waiting_time_limit)+" mins and load has not completed yet. Requires manual check.")
        else:
            waiting_time = waiting_time + waiting_gap
            print("Wait for id_resolution data for "+str(waiting_time)+" mins.")
            time.sleep(waiting_gap*60)

if __name__ == '__main__':
    main()
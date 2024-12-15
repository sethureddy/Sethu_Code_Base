import time
from airflow import models
from airflow.models import Variable
from google.cloud import bigquery as bgq
client = bgq.Client()
from datetime import timedelta, datetime
import datetime

project_name=models.Variable.get("gcp_project")
dataset_name_dssprod=models.Variable.get("dssprod_o_warehouse_dataset")
execution_date=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")

query_data_exists = '''select count(*) as record_count from {project_name}.{dataset_name_dssprod}.click_fact where date_key ={execution_date}'''.format(project_name=project_name,dataset_name_dssprod=dataset_name_dssprod,execution_date=execution_date)

waiting_time_limit = 60 #one hour
waiting_gap = 2 #5 mins

# waiting_time_limit = 5 #one hour
# waiting_gap = 1 #5 mins

def get_count():
    result=client.query(query_data_exists).to_dataframe()
    record_count=result['record_count'][0]
    return record_count

def main():
    waiting_time = 0
    while True:
        record_count=get_count()
        if record_count>0 and record_count%120 !=0:
            print("Waited for data loading")
            print("Click fact data is available.")
            return
        elif waiting_time>=waiting_time_limit:
            raise Exception("Waited for "+str(waiting_time_limit)+" mins and load has not completed yet. Requires manual check.")
        else:
            time.sleep(waiting_gap*60)
            waiting_time = waiting_time + waiting_gap
            print("Waited for "+str(waiting_time)+"mins.")

if __name__ == '__main__':
    main()
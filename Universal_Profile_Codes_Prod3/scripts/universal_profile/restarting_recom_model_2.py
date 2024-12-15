from datetime import timedelta, datetime
import airflow
import time
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators import bash_operator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.sensors.external_task import ExternalTaskMarker
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun
import datetime
from airflow.models.dagbag import DagBag 
from airflow.models import TaskInstance
from airflow import configuration as conf
from airflow.utils.state import State
# getting the dev credentials that are stored securely in the airflow server
o_project = Variable.get("project_id")

dag_id="recommendation_model_set2"
delete_inter_table_task_id="rs2_delete_intermediate_tables"
first_task_id= "start_recommendation_set_2"
append_task_id="rs2_append_final_csv_output_to_inter_table"

waiting_time_limit = 60 #min
waiting_gap = 1 #min

def get_status():
    dag_runs = DagRun.find(dag_id=dag_id)
    exe_date = dag_runs[-1].execution_date
    ti=get_task_instance(dag_id, append_task_id, exe_date)
    status=ti.current_state()
    # print("append task status is: ", status)
    return status

def main():
    date1=datetime.date.today()
    todays_date=date1.strftime('%Y-%m-%d')
    
    waiting_time= 0
    while True:
        dag_runs = DagRun.find(dag_id=dag_id)
        exe_date = dag_runs[-1].execution_date
        execution_date = exe_date.strftime('%Y-%m-%d')
        
        if(execution_date==todays_date):
            status_of_append_task = get_status()
            print("Status of append task is: ", status_of_append_task)
            
            if(status_of_append_task =="success" and execution_date==todays_date):
                print("Task is completed so pipeline will execute successfully")
                return
            elif(status_of_append_task == None and execution_date==todays_date):
                print("Task is not yet started, so wait for 2 min")
                time.sleep(waiting_gap*120)
                waiting_time = waiting_time + 2
            elif((status_of_append_task =="running" or status_of_append_task== "scheduled" or  status_of_append_task=="queued") and execution_date==todays_date):
                print("Task is in {} state".format(status_of_append_task))
                time.sleep(waiting_gap*30)
                waiting_time = waiting_time + 0.5
            elif waiting_time>=waiting_time_limit:
                raise Exception("Waited for "+str(waiting_time_limit)+" mins and load has not completed yet. Requires manual check.")
            else:
                tasks_from_append = ["rs2_append_final_csv_output_to_inter_table","rs2_insert_from_intermediate_to_final","rs2_delete_intermediate_tables","send_mail_on_success","end_recommendation_set_2"]
                for task_id in tasks_from_append:
                    ti=get_task_instance(dag_id,task_id, exe_date)
                    ti.set_state(State.SUCCESS)
                print("failed task marked as success.")
                
                clear_delete_inter_tables_tasks = bash_operator.BashOperator(
                    task_id='clear_delete_inter_tables_tasks',
                    bash_command= f'airflow tasks clear -s {execution_date}  -t {delete_inter_table_task_id} -d -y {dag_id}'
                )
                clear_delete_inter_tables_tasks.execute(dict())
                print("clear_delete_inter_tables_tasks completed")
                
                clear_first_tasks = bash_operator.BashOperator(
                    task_id='clear_first_tasks',
                    bash_command= f'airflow tasks clear -s {execution_date}  -t {first_task_id} -d -y {dag_id}'
                )
                clear_first_tasks.execute(dict())
                print("clear_first_tasks completed")
                waiting_time = waiting_time - 30
        else:
            print("Model is not yet started so wait for 5 min.")
            time.sleep(300)
            waiting_time = waiting_time + 5

if __name__ == '__main__':
    main()
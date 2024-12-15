from airflow.api.common.experimental.get_task_instance import get_task_instance
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun
import datetime
from airflow.models.dagbag import DagBag 
from airflow.models import TaskInstance
from airflow import configuration as conf
from airflow.operators.email_operator import EmailOperator
#from twilio.rest import Client
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail



default_args = {
    'owner': 'UP',
    #'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['rohit.varma@affine.ai'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
dag_id = 'email_customer_profile_prod_dag'
task_id = 'merge_into_final_email_customer_profile'
#dagfolder=conf.get('core','DAGS_FOLDER')

def check_delay_for_task(**kwargs):
	dag_runs = DagRun.find(dag_id=dag_id)
	exe_date = dag_runs[-1].execution_date
	execution_date = exe_date.strftime('%Y-%m-%d')
	date1=datetime.date.today()
	todays_date=date1.strftime('%Y-%m-%d')
	dag_folder = conf.get('core','DAGS_FOLDER')
	dagbag = DagBag(dag_folder)
	check_dag = dagbag.dags[dag_id]
	my_task = check_dag.get_task(task_id)
	ti=get_task_instance(dag_id, task_id, exe_date)

	if(ti.current_state() == "success" and execution_date==todays_date):
		print ("Task is already successfully executed.")

	else:
		if(ti.current_state() != "success" and execution_date == todays_date):
			print ('The task: "{0}" has {1}.'.format(task_id , ti.current_state())  )
			print(execution_date)
			print(todays_date)
			print(execution_date)
			print(todays_date)
			print ('The task: "{0}" has {1}.'.format(task_id , ti.current_state())  )
			print(exe_date)
			print(dag_id)
			print(task_id)
			print(date1)
			send_mail = EmailOperator(
				task_id="start_email_send",
				# to='bluenile@affineanalytics.com,marketingetl@bluenile.com,etl@bluenile.com',
                to='rohit.varma@affine.ai',
				subject="UP ETL Alert:Delay in refresh of email_customer_profile table",
				html_content='''Hi,<br>
							  
							 <p>This email is to alert you that the refresh of email_customer_profile table is delayed beyond the threshold completion time of 6:30 AM PST.
							A possible reason for this is that the Airflow pipeline(dag_email_customer_profile) has not completed yet, or has not started yet by due to delays in upstream processes. 
							This may cause some delay in availability of the latest day’s data in the email_customer_profile.<br>
							<br>
							 We are monitoring the delay in processing and will update you if there are any further delays or failures.<br>
							 <br>
							 For any queries or concerns please write an email to  bluenile@affineanalytics.com. <br>
							 <br>
							 Thanks,<br>
							 UP Team.<p>''')
			send_mail.execute(dict())
		else:
			print('"{0}" Task is not running on {1}. Last run of "{2}" Dag is on {3}'.format(task_id, datetime.date.today(), dag_id,  execution_date))
			print(execution_date)
			print(todays_date)
			print(execution_date)
			print(todays_date)
			print ('The task: "{0}" has {1}.'.format(task_id , ti.current_state())  )
			print(exe_date)
			print(dag_id)
			print(task_id)
			print(date1)
			send_mail = EmailOperator(
				task_id="start_email_send",
				# to='bluenile@affineanalytics.com,marketingetl@bluenile.com,etl@bluenile.com',
                to='rohit.varma@affine.ai',
				subject="UP ETL Alert:Delay in refresh of email_customer_profile table",
				html_content='''Hi,<br>
							  
							 <p>This email is to alert you that the refresh of email_customer_profile table is delayed beyond the threshold completion time of 6:30 AM PST.
							A possible reason for this is that the Airflow pipeline(dag_email_customer_profile) has not completed yet, or has not started yet by due to delays in upstream processes. 
							This may cause some delay in availability of the latest day’s data in the email_customer_profile.<br>
							<br>
							 We are monitoring the delay in processing and will update you if there are any further delays or failures.<br>
							 <br>
							 For any queries or concerns please write an email to  bluenile@affineanalytics.com. <br>
							 <br>
							 Thanks,<br>
							 UP Team.<p>''')
			send_mail.execute(dict())

with DAG('delay_check_for_email_customer_profile', 
schedule_interval='30 13 * * *',
default_args=default_args) as dag:
	Checking_task = PythonOperator(task_id="delay_check_for_ecp",
						   python_callable = check_delay_for_task,
						   provide_context=True,
						   dag=dag)
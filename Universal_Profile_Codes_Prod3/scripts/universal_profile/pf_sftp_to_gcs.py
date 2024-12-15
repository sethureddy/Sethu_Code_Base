import pysftp
import warnings
from airflow.models import Variable
import time
warnings.filterwarnings("ignore")

#Fetching the credentials in the airflow server
o_Hostname=Variable.get("sftp_host")
o_Username=Variable.get("sftp_user")
o_Password=Variable.get("sftp_password")
o_port=22

#Appending the current date to the csv file
# timestr = time.strftime("%Y%m%d")
o_remoteFilePath='/ProductFeeds'

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None   
with pysftp.Connection(host=o_Hostname, port=o_port, username=o_Username, password=o_Password, cnopts=cnopts) as sftp:
   print ("Connection succesfully established ... ")
   
   remotedir=o_remoteFilePath
   # chmod(remoteFilePath,mode=777)
   localdir='/home/airflow/gcs/data/ProductFeeds'
   sftp.get_r(remotedir, localdir)
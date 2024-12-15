#!/bin/bash

# [START startup_script]
apt-get update
apt-get -y install imagemagick

# Use the metadata server to get the configuration specified during
# instance creation. Read more about metadata here:
# https://cloud.google.com/compute/docs/metadata#querying
IMAGE_URL=$(curl http://metadata/computeMetadata/v1/instance/attributes/url -H "Metadata-Flavor: Google")
TEXT=$(curl http://metadata/computeMetadata/v1/instance/attributes/text -H "Metadata-Flavor: Google")
CS_BUCKET=$(curl http://metadata/computeMetadata/v1/instance/attributes/bucket -H "Metadata-Flavor: Google")
EXECUTION_DATE=$(curl http://metadata/computeMetadata/v1/instance/attributes/execution -H "Metadata-Flavor: Google")
DIRECTORY_PATH=$(curl http://metadata/computeMetadata/v1/instance/attributes/directory -H "Metadata-Flavor: Google")
INSTANCE_NAME=$(curl http://metadata/computeMetadata/v1/instance/attributes/instance -H "Metadata-Flavor: Google")
ZONE_NAME=$(curl http://metadata/computeMetadata/v1/instance/attributes/zone -H "Metadata-Flavor: Google")
	
mkdir image-output
cd image-output
wget $IMAGE_URL
convert * -pointsize 30 -fill white -stroke black -gravity center -annotate +10+40 "$TEXT" output.png


mkdir /ds_model_run
cd /ds_model_run
sudo gsutil -m cp -r gs://$CS_BUCKET/dags/scripts/universal_profile/short_conversion_pyfiles/* .
sudo gsutil -m cp -r gs://$CS_BUCKET/$DIRECTORY_PATH/conf/* .
sudo gsutil cp gs://$CS_BUCKET/$DIRECTORY_PATH/SC_model.pkl .
echo "y" | sudo apt install python3-pip
sudo pip3 install -r requirements.txt

#diamond_recommendation
sudo python3 scoring.py

#copy vm log files
sudo gsutil cp /var/log/syslog gs://$CS_BUCKET/$DIRECTORY_PATH/logs/syslog_$EXECUTION_DATE

sudo gcloud compute instances add-metadata $INSTANCE_NAME --zone $ZONE_NAME --metadata startup_execution_status="done"
# [END startup_script]

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

# Create a Google Cloud Storage bucket.
gsutil mb gs://$CS_BUCKET

# Store the image in the Google Cloud Storage bucket and allow all users
# to read it.
gsutil cp -a public-read output.png gs://$CS_BUCKET/output.png

mkdir /ds_model_run
cd /ds_model_run
sudo gsutil -m cp -r gs://$CS_BUCKET/dags/scripts/universal_profile/recommendation_model/* .
sudo gsutil -m cp -r gs://$CS_BUCKET/$DIRECTORY_PATH/conf/* .
sudo mv diamond/diamond_recommendation_scoring.py setting/settings_recommendation_scoring.py jewellery/jewellery_recommendation_scoring.py .
echo "y" | sudo apt install python3-pip
sudo pip3 install -r requirements.txt

# TODO remove below fsspec installation once fsspec new version 0.8.0 works wihtout import issue 
# pip3 uninstall fsspec
# pip3 install fsspec==0.7.4


#updating reshape.py to int64 at num_cells 
sudo cp reshape.py /usr/local/lib/python3.5/dist-packages/pandas/core/reshape/

#diamond_recommendation
sudo python3 diamond_recommendation_scoring.py

#setting recommendation
sudo python3 settings_recommendation_scoring.py

#jewellery recommendation
sudo python3 jewellery_recommendation_scoring.py

#recommendation output run
sudo python3 recommendation_output.py

#copy vm log files
sudo gsutil cp /var/log/syslog gs://$CS_BUCKET/$DIRECTORY_PATH/logs/syslog_$EXECUTION_DATE

sudo gcloud compute instances add-metadata $INSTANCE_NAME --zone $ZONE_NAME --metadata startup_execution_status="done"
# [END startup_script]
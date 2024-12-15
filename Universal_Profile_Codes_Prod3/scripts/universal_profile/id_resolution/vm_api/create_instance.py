import argparse
import os
import time

import googleapiclient.discovery
from six.moves import input


# [START list_instances]
def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result["items"] if "items" in result else None


# [END list_instances]


# [START create_instance]
def create_instance(
    compute, project, zone, name, bucket, execution_date, machine_type, directory_path
):
    # Get the latest Debian Jessie image.
    image_response = (
        compute.images()
        .getFromFamily(project="debian-cloud", family="debian-9")
        .execute()
    )
    source_disk_image = image_response["selfLink"]

    # Configure the machine
    machine_type = "zones/%s/machineTypes/%s" % (zone, machine_type)
    startup_script = open(
        os.path.join(os.path.dirname(__file__), "startup-script.sh"), "r"
    ).read()
    image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    image_caption = "Ready for dessert?"

    config = {
        "name": name,
        "machineType": machine_type,
        # Specify the boot disk and the image to use as a source.
        "disks": [
            {
                "boot": True,
                "autoDelete": True,
                "initializeParams": {"sourceImage": source_disk_image,},
            }
        ],
        # Specify a network interface with NAT to access the public
        # internet.
        "networkInterfaces": [
            {
                "network": "global/networks/default",
                "accessConfigs": [{"type": "ONE_TO_ONE_NAT", "name": "External NAT"}],
            }
        ],
        # Allow the instance to access cloud storage and logging.
        "serviceAccounts": [
            {
                "email": "default",
                "scopes": [
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/logging.write",
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/userinfo.email",
                ],
            }
        ],
        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        "metadata": {
            "items": [
                {
                    # Startup script is automatically executed by the
                    # instance upon startup.
                    "key": "startup-script",
                    "value": startup_script,
                },
                {"key": "url", "value": image_url},
                {"key": "text", "value": image_caption},
                {"key": "bucket", "value": bucket},
                {"key": "execution", "value": execution_date},
                {"key": "directory", "value": directory_path},
                {"key": "zone", "value": zone},
                {"key": "instance", "value": name},
            ]
        },
    }

    return compute.instances().insert(project=project, zone=zone, body=config).execute()


# [END create_instance]


# [START delete_instance]
def delete_instance(compute, project, zone, name):
    return (
        compute.instances().delete(project=project, zone=zone, instance=name).execute()
    )


# [END delete_instance]


# [START wait_for_operation]
def wait_for_operation(compute, project, zone, operation, instance_name, delete):
    print("Waiting for operation to finish...")
    while True:
        result = (
            compute.zoneOperations()
            .get(project=project, zone=zone, operation=operation)
            .execute()
        )

        if result["status"] == "DONE":
            print("done.")
            if "error" in result:
                raise Exception(result["error"])

            if delete:
                return result

            flag = False
            while True:
                time.sleep(10)
                print("rotating")
                Instance_status = (
                    compute.instances()
                    .get(project=project, zone=zone, instance=instance_name)
                    .execute()
                )
                items = Instance_status["metadata"]["items"]
                for item in items:
                    if item["key"] == "startup_execution_status":
                        flag = True
                        print("flag is true now")

                if flag:
                    print("All tasks are done")
                    break

            return result

        time.sleep(1)


# [END wait_for_operation]


# [START run]
def main(
    project,
    bucket,
    zone,
    instance_name,
    delete_instance_option,
    execution_date,
    machine_type,
    directory_path,
):
    compute = googleapiclient.discovery.build("compute", "v1")
    delete = False
    if delete_instance_option == "yes":
        print("deleting instance")
        operation = delete_instance(compute, project, zone, instance_name)
        delete = True
        wait_for_operation(
            compute, project, zone, operation["name"], instance_name, delete
        )
        print("deleted instance")

    else:

        print("Creating instance.")

        operation = create_instance(
            compute,
            project,
            zone,
            instance_name,
            bucket,
            execution_date,
            machine_type,
            directory_path,
        )
        wait_for_operation(
            compute, project, zone, operation["name"], instance_name, delete
        )

        print(
            """
            Instance created.
            It will take a minute or two for the instance to complete work.
            Check this URL: http://storage.googleapis.com/{}/output.png
            Once the image is uploaded press enter to delete the instance.
            """.format(
                bucket
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("project_id", help="Your Google Cloud project ID.")
    parser.add_argument("bucket_name", help="Your Google Cloud Storage bucket name.")
    parser.add_argument("execution_date", help="execution date of the process")
    parser.add_argument("machine_type", help="Machine type")
    parser.add_argument("directory_path", help="Path of the GCS Directory")
    parser.add_argument(
        "--zone", default="us-central1-f", help="Compute Engine zone to deploy to."
    )
    parser.add_argument("--name", default="demo-instance", help="New instance name.")
    parser.add_argument(
        "--delete-instance", default="no", help="pass yes to delete the instance"
    )

    args = parser.parse_args()

    main(
        args.project_id,
        args.bucket_name,
        args.zone,
        args.name,
        args.delete_instance,
        args.execution_date,
        args.machine_type,
        args.directory_path,
    )
# [END run]

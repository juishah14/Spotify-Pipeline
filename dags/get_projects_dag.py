import re
import requests
import json

from datetime import datetime
from labelbox import Client
from labelbox_modules import get_label_data

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

LABELBOX_API_KEY = Variable.get("LABELBOX_API_KEY")

# NOTE: This file is not up to date. The e2e dag one is.

def get_project_urls(ti):

    try:
        urls = []
        client = Client(LABELBOX_API_KEY)
        projects = client.get_projects()

        for project in projects:
            # Make sure project name matches the DB <Number> (<Name>) convention
            project_name_found = re.search("(DB .+? \(.+?\))", project.name)
            project_name = None if not project_name_found else project_name_found.group(1)
            if project_name is not None:
                try:
                    print(project_name)
                    url = project.export_labels()
                    urls.append(url)
                except:
                    pass
            else:
                print("Skipping project {}".format(project.name))

        ti.xcom_push(key="all_urls", value=urls)

    except Exception as e:
        print(e)

def process_labels_from_url(ti):

    # connection = rds_database_connect(True) - COME BACK TO THIS AS WELL

    labels = []
    skipped_labels = []
    urls = ti.xcom_pull(key="all_urls")

    for url in urls:
        try:
            response = requests.get(url)
            if response.status_code == 200:  # Check if the request went through
                label_list = response.json()  # Convert list of labels into JSON
                for label in label_list:
                    
                    print(label) # FOR NOW

                    # Important, as not all clips have been labelled nor do all of them have frames
                    if (label["Label"] and "frames" in label["Label"]):
                        label_data = label["Label"]
                        uri = label_data["frames"]
                        frame_data = get_label_data(uri)
                        # Fetch the labels from LabelBox from the `frames` URL
                        if frame_data == "" or frame_data is None: # Added NoneType:
                            # Make sure API request received label data
                            print("API Request for {} returned empty. Skipping for now.".format(uri))
                            skipped_labels.append(label)
                            continue
                        frame_data_json = json.loads(frame_data)
                        # Grab the classifications (list of classifications -- need to iterate through and grab the title and its value)
                        # Replace the URL with the data pulled from the URL
                        classifications = frame_data_json["classifications"]
                        label_data["frames"] = classifications
                        label["Label"] = classifications
                        labels.append(label)
                        # update_db_entry(connection, label) - COME BACK TO THIS
                    else:
                        # Nothing to do, the label hasn't been created yet.
                        pass
            else:
                # What do? Probably try it again :)
                pass

        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("Oops! Something weird happened...", err)

    # connection.close() - COME BACK TO THIS

    skipped_count = len(skipped_labels)
    print("Skipped {} labels.".format(skipped_count))

    ti.xcom_push(key="all_labels", value=labels)

def upload_parsed_labels(ti):
    parsed_labels = ti.xcom_pull(key="all_labels")
    print(parsed_labels)
    return "We got the labels sir."


dag = DAG(
    "labelbox_projects",
    description="Connecting to Labelbox!",
    schedule_interval="@daily",
    start_date=datetime(2022, 10, 4),
    catchup=False,
)

get_labelbox_project_urls = PythonOperator(
    task_id="get_labelbox_project_urls",
    python_callable=get_project_urls,
    trigger_rule="all_success",
    dag=dag,
)

process_labelbox_urls = PythonOperator(
    task_id="process_labelbox_urls",
    python_callable=process_labels_from_url,
    trigger_rule="all_success",
    dag=dag,
)

upload_labelbox_labels = PythonOperator(
    task_id="upload_labelbox_labels",
    python_callable=upload_parsed_labels,
    trigger_rule="all_success",
    dag=dag,
)

get_labelbox_project_urls >> process_labelbox_urls >> upload_labelbox_labels
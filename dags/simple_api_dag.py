import requests
import base64
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

client_id = Variable.get("CLIENT_ID")
client_secret = Variable.get("CLIENT_SECRET")
slack_token = Variable.get('SLACK_TOKEN')

def generate_auth(ti):
    token_url = "https://accounts.spotify.com/api/token"
    token_data = {"grant_type": "client_credentials"} 

    client_creds = f"{client_id}:{client_secret}"
    client_creds_b64 = base64.b64encode(client_creds.encode())
    token_headers = {"Authorization": f"Basic {client_creds_b64.decode()}"}

    r = requests.post(token_url, data=token_data, headers=token_headers)
    if r.status_code not in range(200, 299):
        return "Could not authenticate client."
    
    access_token = r.json()['access_token']
    headers = {"Authorization": f"Bearer {access_token}"}
    ti.xcom_push(key="headers", value=headers)
    return headers

def get_albums(ti):
    id = '4aawyAB9vmqN3uQ7FjRGTy'
    headers = ti.xcom_pull(key="headers")
    endpoint = f"https://api.spotify.com/v1/albums/{id}"
    r = requests.get(endpoint, headers=headers)
    if r.status_code not in range(200, 299):
        return "{}"
    return r.json()

def slack_notification(context):
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            exec_date=context.get('execution_date'),
            )
    slack = SlackAPIPostOperator(
        task_id="slack_notif",
        dag=dag,
        token=slack_token,
        text=slack_msg,
        channel="#spotify-pipeline"
    )
    return slack.execute(context=context)


dag = DAG(
    "spotify_pipeline",
    description="Connecting to Labelbox!",
    schedule_interval="@daily",
    start_date=datetime(2022, 10, 4),
    catchup=False,
    on_failure_callback=slack_notification
)

generate_auth = PythonOperator(
    task_id="generate_auth",
    python_callable=generate_auth,
    trigger_rule="all_success",
    dag=dag,
)

get_albums = PythonOperator(
    task_id="get_albums",
    python_callable=get_albums,
    trigger_rule="all_success",
    dag=dag,
)


generate_auth >> get_albums
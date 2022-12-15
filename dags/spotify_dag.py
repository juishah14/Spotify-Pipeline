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

albums = ['07w0rG5TETcyihsEIZR3qG', '6FKP2O0oOvZlDkF0gyTjiJ', '65YAjLCn7Jp33nJpOxIPMe', '73TNMu44lT0m1h1Nn6Bfiq', '1JsySWOa2RchsBB2N4313v', '6FJxoadUE4JNVwWHghBwnb', '0Zd10MKN5j9KwUST0TdBBB', '4SZko61aMnmgvNhfhgTuD3', '4EPQtdq6vvwxuYeQTrwDVY']
playlists = ['3jRTRcUqdkiSIAm7A1snfK', '1zxvLY3HTtXMjnpmBe0iK3', '37i9dQZF1EQoqCH7BwIYb7']
artists = ['15Dh5PvHQj909E0RgAe0aN', '6vWDO969PvNqNYHIOW5v0m', '0s4kXsjYeH0S1xRyVGN4NO', '78rUTD7y6Cy67W1RVzYs7t', '1U1el3k54VvEUzo3ybLPlM', '4Gso3d4CscCijv0lmajZWs', '0EmeFodog0BfCgMzAIvKQp']

recommended_songs = []
album_tracks = []
album_artists = []
playlist_tracks = []
playlist_artists = []

tempo_values = []
loudness_values = []
danceability_values = []
energy_values = []
instrumentalness_values = []
valence_values = []

# Eventually return fail instead of {}

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
    headers = ti.xcom_pull(key="headers")
    ti.xcom_push(key="headers", value=headers)
    for album in albums:
        album_endpoint = f'https://api.spotify.com/v1/albums/{album}/tracks'
        r = requests.get(album_endpoint, headers=headers)
        if r.status_code not in range(200, 299):
            return "{}"
        
        info = r.json()['items']
        artist = info[0]['artists'][0]['id']
        if artist not in album_artists:
            album_artists.append(artist)
        for item in info:
            track_uri = item['uri']
            track = track_uri.split(':')[2]
            if track not in album_tracks:
                album_tracks.append(track)

    return "Album artists and tracks retrieved"

def get_playlists(ti):
    headers = ti.xcom_pull(key="headers")
    ti.xcom_push(key="headers", value=headers)
    for playlist in playlists:
        limit = 10
        playlist_endpoint = f"https://api.spotify.com/v1/playlists/{playlist}/tracks?limit={limit}"
        r = requests.get(playlist_endpoint, headers=headers)
        if r.status_code not in range(200, 299):
            return "{}"

        info = r.json()['items']
        for item in info:
            artist_uri = item['track']['artists'][0]['uri']
            artist = artist_uri.split(':')[2]
            if artist not in playlist_artists:
                playlist_artists.append(artist)
            track_uri = item['track']['uri']
            track = track_uri.split(':')[2]
            if track not in playlist_tracks:
                playlist_tracks.append(track)

    return "Playlist artists and tracks retrieved"
    

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
    task_id="get_album_info",
    python_callable=get_albums,
    trigger_rule="all_success",
    dag=dag,
)

get_playlists = PythonOperator(
    task_id="get_playlist_info",
    python_callable=get_playlists,
    trigger_rule="all_success",
    dag=dag,
)

generate_auth >> get_albums >> get_playlists

# ['7tYKF4w9nC0nq9CsPZTHyP', '78rUTD7y6Cy67W1RVzYs7t', '5ZS223C6JyBfXasXxrRqOk', '6vWDO969PvNqNYHIOW5v0m', '5K4W6rqBFWDnAN6FQUkS6x', '1U1el3k54VvEUzo3ybLPlM']
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

albums = ['07w0rG5TETcyihsEIZR3qG', '6FKP2O0oOvZlDkF0gyTjiJ', '65YAjLCn7Jp33nJpOxIPMe', '73TNMu44lT0m1h1Nn6Bfiq']
playlists = ['3jRTRcUqdkiSIAm7A1snfK', '1zxvLY3HTtXMjnpmBe0iK3', '37i9dQZF1EQoqCH7BwIYb7']
artists = ['15Dh5PvHQj909E0RgAe0aN', '6vWDO969PvNqNYHIOW5v0m', '0s4kXsjYeH0S1xRyVGN4NO', '78rUTD7y6Cy67W1RVzYs7t', '1U1el3k54VvEUzo3ybLPlM', '4Gso3d4CscCijv0lmajZWs', '0EmeFodog0BfCgMzAIvKQp']

top_tracks = []
top_artists = []
recommended_songs = []
tempo_values = []
loudness_values = []
danceability_values = []
energy_values = []
instrumentalness_values = []
valence_values = []

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
        if artist not in top_artists:
            top_artists.append(artist)
        for item in info:
            track_uri = item['uri']
            track = track_uri.split(':')[2]
            if track not in top_tracks:
                top_tracks.append(track)

    ti.xcom_push(key="top_tracks", value=top_tracks)
    ti.xcom_push(key="top_artists", value=top_artists)
    return "Album artists and tracks retrieved"

def get_playlists(ti):
    headers = ti.xcom_pull(key="headers")
    ti.xcom_push(key="headers", value=headers)

    top_tracks = ti.xcom_pull(key="top_tracks")
    top_artists = ti.xcom_pull(key="top_artists")

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
            if artist not in top_artists:
                top_artists.append(artist)
            track_uri = item['track']['uri']
            track = track_uri.split(':')[2]
            if track not in top_tracks:
                top_tracks.append(track)

    ti.xcom_push(key="top_tracks", value=top_tracks)
    ti.xcom_push(key="top_artists", value=top_artists)
    return "Playlist artists and tracks retrieved"

def get_audio_features(ti):
    headers = ti.xcom_pull(key="headers")
    ti.xcom_push(key="headers", value=headers)

    top_tracks = ti.xcom_pull(key="top_tracks")
    top_artists = ti.xcom_pull(key="top_artists")

    # Get audio features of tracks
    for track in top_tracks:
        audio_features_endpoint = f"https://api.spotify.com/v1/audio-features/{track}"
        r = requests.get(audio_features_endpoint, headers=headers)
        if r.status_code not in range(200, 299):
            return "{}"

        info = r.json()
        tempo = info['tempo']
        loudness = info['loudness']
        danceability = info['danceability']
        energy = info['energy']
        valence = info['valence'] # musical positiveness conveyed by a track
        instrumentalness = info['instrumentalness']
        
        tempo_values.append(tempo)
        loudness_values.append(loudness)
        danceability_values.append(danceability)
        energy_values.append(energy)
        valence_values.append(valence)
        instrumentalness_values.append(instrumentalness)

    min_tempo = min(tempo_values)
    max_tempo = max(tempo_values)
    tempo = [min_tempo, max_tempo]

    min_loudness = min(loudness_values)
    max_loudness = max(loudness_values)
    loudness = [min_loudness, max_loudness]

    min_energy = min(energy_values)
    max_energy = max(energy_values)
    energy = [min_energy, max_energy]

    min_instrumentalness = min(instrumentalness_values)
    max_instrumentalness = max(instrumentalness_values)
    instrumentalness = [min_instrumentalness, max_instrumentalness]

    min_danceability = round(sum(danceability_values) / len(danceability_values), 2)
    max_danceability = max(danceability_values)
    danceability = [min_danceability, max_danceability]

    min_valence = round(sum(valence_values) / len(valence_values), 2)
    max_valence = max(valence_values)
    valence = [min_valence, max_valence]

    ti.xcom_push(key="top_tracks", value=top_tracks)
    ti.xcom_push(key="top_artists", value=top_artists)
    
    ti.xcom_push(key="tempo", value=tempo)
    ti.xcom_push(key="loudness", value=loudness)
    ti.xcom_push(key="danceability", value=danceability)
    ti.xcom_push(key="energy", value=energy)
    ti.xcom_push(key="valence", value=valence)
    ti.xcom_push(key="instrumentalness", value=instrumentalness)

def get_recommendations(ti):
    headers = ti.xcom_pull(key="headers")
    ti.xcom_push(key="headers", value=headers)

    top_tracks = ti.xcom_pull(key="top_tracks")
    top_artists = ti.xcom_pull(key="top_artists")
    
    min_tempo, max_tempo = ti.xcom_pull(key="tempo")
    min_loudness, max_loudness = ti.xcom_pull(key="loudness")
    min_danceability, max_danceability = ti.xcom_pull(key="danceability")
    min_energy, max_energy = ti.xcom_pull(key="energy")
    min_valence, max_valence = ti.xcom_pull(key="valence")
    min_instrumentalness, max_instrumentalness = ti.xcom_pull(key="instrumentalness")

    limit = 10
    seed_track = top_tracks[1]
    seed_artist = top_artists[1]
    recommendation_endpoint = f"https://api.spotify.com/v1/recommendations?limit={limit}&market=ES&seed_artists={seed_artist}&seed_genres=pop%2Crap%2Cr-n-b&seed_tracks={seed_track}&min_danceability={min_danceability}&max_danceability={max_danceability}&min_energy={min_energy}&max_energy={max_energy}&min_instrumentalness={min_instrumentalness}&max_instrumentalness={max_instrumentalness}&min_loudness={min_loudness}&max_loudness={max_loudness}&min_tempo={min_tempo}&max_tempo={max_tempo}&min_valence={min_valence}&max_valence={max_valence}"
    r = requests.get(recommendation_endpoint, headers=headers)
    if r.status_code not in range(200, 299):
        return "{}"
    info = r.json()
    for track in info['tracks']:
        song = track['name']
        if song not in recommended_songs:
            recommended_songs.append(song)
    
    return recommended_songs

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

get_audio_features = PythonOperator(
    task_id="get_audio_features",
    python_callable=get_audio_features,
    trigger_rule="all_success",
    dag=dag,
)

get_recommendations = PythonOperator(
    task_id="get_recommendations",
    python_callable=get_recommendations,
    trigger_rule="all_success",
    dag=dag,
)

generate_auth >> get_albums >> get_playlists >> get_audio_features >> get_recommendations

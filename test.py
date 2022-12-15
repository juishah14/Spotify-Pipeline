import requests
import base64

client_id = '30f81b125e56485e8c197bc909451caf'
client_secret = 'e7d986c6bf09436ead0d1548dba0ebd7'
slack_token = 'xoxb-4517812200035-4518008498578-T9usbm6jOtgmyQVXxpH358O8'

def generate_auth():

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
    # ti.xcom_push(key="headers", value=headers)
    return headers

headers = generate_auth()

albums = ['07w0rG5TETcyihsEIZR3qG', '6FKP2O0oOvZlDkF0gyTjiJ', '65YAjLCn7Jp33nJpOxIPMe', '73TNMu44lT0m1h1Nn6Bfiq', '1JsySWOa2RchsBB2N4313v', '6FJxoadUE4JNVwWHghBwnb', '0Zd10MKN5j9KwUST0TdBBB', '4SZko61aMnmgvNhfhgTuD3', '4EPQtdq6vvwxuYeQTrwDVY']
playlists = ['3jRTRcUqdkiSIAm7A1snfK', '1zxvLY3HTtXMjnpmBe0iK3', '37i9dQZF1EQoqCH7BwIYb7']
artists = ['15Dh5PvHQj909E0RgAe0aN', '6vWDO969PvNqNYHIOW5v0m', '0s4kXsjYeH0S1xRyVGN4NO', '78rUTD7y6Cy67W1RVzYs7t', '1U1el3k54VvEUzo3ybLPlM', '4Gso3d4CscCijv0lmajZWs', '0EmeFodog0BfCgMzAIvKQp']


# Data lake
# Get all tracks of favourite albums
# Get all tracks of favourite playlists
# Get artists of those tracks

# Data processing
# Get audio features and audio analysis of those tracks
# Get recommendations for similar tracks (based on genre and audio features)
# Get music recommendations based on market, seeds (artists, genres, tracks), audio features (danceability, etc.)

# Data warehouse
# Save recommended tracks in a database
# Can explain later how this can be scaled (loop through artists and tracks and also optimize a little by improving running time ie. when finding max and min values)


recommended_songs = []
top_tracks = []
top_artists = []

tempo_values = []
loudness_values = []
danceability_values = []
energy_values = []
instrumentalness_values = []
valence_values = []


# Going to bias towards higher valence and danceability

# Get album artists and tracks
for album in albums:
    album_endpoint = f'https://api.spotify.com/v1/albums/{album}/tracks'
    r = requests.get(album_endpoint, headers=headers)
    info = r.json()['items']

    artist = info[0]['artists'][0]['id']
    if artist not in top_artists:
        top_artists.append(artist)

    for item in info:
        track_uri = item['uri']
        track = track_uri.split(':')[2]
        if track not in top_tracks:
            top_tracks.append(track)


# Get playlist artists and tracks
for playlist in playlists:
    limit = 10
    playlist_endpoint = f"https://api.spotify.com/v1/playlists/{playlist}/tracks?limit={limit}"
    r = requests.get(playlist_endpoint, headers=headers)
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


# Get audio features of tracks
for track in top_tracks:
    audio_features_endpoint = f"https://api.spotify.com/v1/audio-features/{track}"
    r = requests.get(audio_features_endpoint, headers=headers)

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


# Set audio feature ranges

min_tempo = min(tempo_values)
max_tempo = max(tempo_values)

min_loudness = min(loudness_values)
max_loudness = max(loudness_values)

min_energy = min(energy_values)
max_energy = max(energy_values)

min_instrumentalness = min(instrumentalness_values)
max_instrumentalness = max(instrumentalness_values)

min_danceability = round(sum(danceability_values) / len(danceability_values), 2)
max_danceability = max(danceability_values)

min_valence = round(sum(valence_values) / len(valence_values), 2)
max_valence = max(valence_values)


# Get recommendations based on audio features
limit = 10
seed_artist = top_artists[1]
seed_track = top_tracks[1]
recommendation_endpoint = f"https://api.spotify.com/v1/recommendations?limit=10&market=ES&seed_artists={seed_artist}&seed_genres=pop%2Crap%2Cr-n-b&seed_tracks={seed_track}&min_danceability={min_danceability}&max_danceability={max_danceability}&min_energy={min_energy}&max_energy={max_energy}&min_instrumentalness={min_instrumentalness}&max_instrumentalness={max_instrumentalness}&min_loudness={min_loudness}&max_loudness={max_loudness}&min_tempo={min_tempo}&max_tempo={max_tempo}&min_valence={min_valence}&max_valence={max_valence}"
r = requests.get(recommendation_endpoint, headers=headers)
info = r.json()
for track in info['tracks']:
    song = track['name']
    if song not in recommended_songs:
        recommended_songs.append(song)


# If the above works, clean up and throw into Airflow
# Then figure out Mongo DB
import requests
import base64
import datetime
from urllib.parse import urlencode



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


id = '4aawyAB9vmqN3uQ7FjRGTy'
endpoint = f"https://api.spotify.com/v1/albums/{id}"
headers = generate_auth()

r = requests.get(endpoint, headers=headers)
print(r.json())

import requests
import pandas as pd
from datetime import datetime 


from infos import client_id, client_secret


def run_spotify_etl(templates_dict): 
    # Gets access token 
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "grant_type": "client_credentials", 
        "client_id": client_id, 
        "client_secret": client_secret
    }

    # curl -X POST "https://accounts.spotify.com/api/token" \
    #      -H "Content-Type: application/x-www-form-urlencoded" \
    #      -d "grant_type=client_credentials&client_id=your-client-id&client_secret=your-client-secret"

    url = 'https://accounts.spotify.com/api/token'
    response = requests.post(url, headers=headers, data=data)

    access_token = response.json()['access_token']

    # Adds it to the headers dict 
    headers['Authorization'] = f"Bearer {access_token}"


    # curl --request GET \
    #   --url https://api.spotify.com/v1/browse/new-releases \
    #   --header 'Authorization: Bearer 1POdFZRZbvb...qqillRxMr2z'

    # Gets the 5 new albums releases in france, uk and japan
    url = 'https://api.spotify.com/v1/browse/new-releases'
    params = {
        'country': ['FR', 'UK', 'JP'], 
        'limit': 5
    }

    response = requests.get(url, headers=headers, params=params)

    retrieved_list = []

    # Parses the result
    for album in response.json()['albums']['items']: 

        retrieved_data = {
            'artist': album['artists'][0]['name'],
            'album': album['name'], 
            'release_date': album['release_date'], 
            'url': album['artists'][0]['external_urls']['spotify']
        }

        retrieved_list.append(retrieved_data)

        #print(f"{album['name']}    &    {album['artists'][0]['name']}    &    \
        #        {album['release_date']}    &    {album['artists'][0]['external_urls']['spotify']}")


    df_new_releases = pd.DataFrame(retrieved_list)

    df_new_releases.to_csv(f"s3://marn-github/{'{:%Y_%m_%d}'.format(datetime.now().date())}_{templates_dict['run_id']}_spotify_new_releases.csv")




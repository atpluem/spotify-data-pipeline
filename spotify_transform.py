import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def spotify_extract_data(data):
    album_list = []
    artist_list = []
    song_list = []
    for row in data['items']:
        album_dict = {'album_id': row['track']['album']['id'],
                      'album_name': row['track']['album']['name'].replace(',', ''), 
                      'release_date': row['track']['album']['release_date'],
                      'total_tracks': row['track']['album']['total_tracks'],
                      'url': row['track']['album']['external_urls']['spotify']}
        album_list.append(album_dict)
    
        for artist in row['track']['artists']:
            artist_dict = {'artist_id': artist['id'], 
                            'artist_name': artist['name'].replace(',', ''), 
                            'external_url': artist['href']}
            artist_list.append(artist_dict)
    
        song_dict = {'song_id': row['track']['id'],
                     'song_name': row['track']['name'].replace(',', ''),
                     'duration_ms': row['track']['duration_ms'],
                     'url': row['track']['external_urls']['spotify'],
                     'popularity': row['track']['popularity'],
                     'song_added': row['added_at'],
                     'album_id': row['track']['album']['id'],
                     'artist_id': row['track']['artists'][0]['id']}
        song_list.append(song_dict)
    
    return album_list, artist_list, song_list

def lambda_handler(event, context):
    
    client = boto3.client('s3')
    bucket = 'dw-snowflake-athispat'
    key = 'spotify-data-pipeline/raw-data/to-process/'
    
    spotify_data = []
    spotify_keys = []
    for file in client.list_objects(Bucket=bucket, Prefix=key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = client.get_object(Bucket=bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        album_list, artist_list, song_list = spotify_extract_data(data)
    
        # Convert to Data Frame
        album_df = pd.DataFrame.from_dict(album_list)
        album_df.drop_duplicates(subset=['album_id'], inplace=True)
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df.drop_duplicates(subset=['artist_id'], inplace=True)
        
        song_df = pd.DataFrame.from_dict(song_list)
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        # Put transformed file to s3
        album_key = 'spotify-data-pipeline/transformed-data/album-data/album_' + \
                    str(datetime.now()) + '.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        client.put_object(Bucket=bucket, Key=album_key, Body=album_content)
        
        artist_key = 'spotify-data-pipeline/transformed-data/artist-data/artist_' + \
                    str(datetime.now()) + '.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        client.put_object(Bucket=bucket, Key=artist_key, Body=artist_content)
        
        song_key = 'spotify-data-pipeline/transformed-data/song-data/song_' + \
                    str(datetime.now()) + '.csv'
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        client.put_object(Bucket=bucket, Key=song_key, Body=song_content)
        
    # Move file from folder to-process -> processed
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {'Bucket': bucket, 'Key': key}
        s3_resource.meta.client.copy(copy_source, bucket,
            'spotify-data-pipeline/raw-data/processed/' + key.split('/')[-1])
        s3_resource.Object(bucket, key).delete()
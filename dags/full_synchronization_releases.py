from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task, task_group

import requests
from datetime import datetime
import time
import pandas as pd
import json
import io

OWNER = "JMAKQ"
DAG_ID = "synchronization_dag"

LAYER = 'sync'

SOURCE = "albums"

POSTGRES_CONN_ID = 'postgres_dwh'
SILO_BUCKET_NAME = 'prod'
SILO_CONN_ID = 'silo_s3_conn'
BATCH_SIZE = 2



SEARCH_URL = "https://itunes.apple.com/lookup"

@task
def get_artist_batches(**context):
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = 'SELECT singer_id, artist_api_id FROM dwh.singers'
    records = pg_hook.get_records(sql=sql)
    ids = [r for r in records]
    return [ids[i:i + BATCH_SIZE] for i in range(0, len(ids), BATCH_SIZE)]


@task(pool="sync_itunes_api_pool")
def extract_raw_json_albums(artist_ids, **context):
    
    s3_hook = S3Hook(aws_conn_id=SILO_CONN_ID)
    xcom = []
       
    for artist in artist_ids:
        artist_id = artist[1]
        singer_id = artist[0]
        write_key = f'{LAYER}/raw/itunes/albums/{artist_id}.json'
        
        
        albums_response = requests.get(SEARCH_URL, params={
            'id': artist_id,
            'entity': 'album',
            'limit': 200,
            'sort': 'recent'
        })
        time.sleep(3)
        
        s3_hook.load_bytes(
            bytes_data=albums_response.content,
            key=write_key,
            bucket_name=SILO_BUCKET_NAME,
            replace=True
        )
        
    return artist_ids

@task
def transform_artist_album_names(artist_ids, **context):
    
    s3_hook = S3Hook(aws_conn_id=SILO_CONN_ID)
    
    albums = []
    
    for artist in artist_ids:
        
        artist_id = artist[1]
        singer_id = artist[0]
        read_key = f'{LAYER}/raw/itunes/albums/{artist_id}.json'
    

        data = s3_hook.read_key(
        key=read_key,
        bucket_name=SILO_BUCKET_NAME
        )

        albums_data = json.loads(data)
    
        for item in albums_data['results']:
            
            if item.get('wrapperType') != 'collection':
                continue
            
            stop_flags = ['remix', 'live']
            
            if all(x not in item.get('collectionName').lower() for x in stop_flags):
                
                name = item.get('collectionName')
                if name.endswith(' - Single'):
                    realese_type = 'Single'
                    name = name[:-9]
                elif name.endswith(' - EP'):
                    realese_type = 'EP'
                    name = name[:-5]
                else:
                    realese_type = 'Album'
                    
            albums.append({
                'singer_id': singer_id,
                'release_name': name,
                'genre': item.get('primaryGenreName'),
                'track_count': item.get('trackCount'),
                'release_date': item.get('releaseDate')[:10],
                'release_type': realese_type,
                'release_api_id': item.get('collectionId')
            })
                

    albums_sorted = sorted(albums, key=lambda x: x['release_date'], reverse=True)
    
    data_frame = pd.DataFrame(albums_sorted)

    csv_buffer = io.StringIO()
    data_frame.to_csv(csv_buffer, index=False)
    write_key = f'{LAYER}/stage/itunes/albums/{artist_ids[0][1]}.csv'
    
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=write_key,
        bucket_name=SILO_BUCKET_NAME,
        replace=True,
    )
    
    return write_key

@task
def load_to_postgres_artist_album_names(read_key, **context):
    
    s3_hook = S3Hook(aws_conn_id=SILO_CONN_ID)

    csv_string = s3_hook.read_key(key=read_key, bucket_name=SILO_BUCKET_NAME)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.copy_expert(
        sql="""
            COPY dwh.releases (
                singer_id,
                release_name,
                genre,
                track_count,
                release_date,
                release_type,
                release_api_id
            ) FROM STDIN WITH CSV HEADER DELIMITER AS ','
        """,
        file=io.StringIO(csv_string)
    )
    conn.commit()
    
    cur.close()
    conn.close()
    
@task_group
def etl_per_batch(batch):
    # extract обрабатывает один батч, возвращает "один объект", а не список
    extracted = extract_raw_json_albums(batch)

    transformed_key = transform_artist_album_names(extracted)
    
    load_to_postgres_artist_album_names(transformed_key)

with DAG(
    dag_id=DAG_ID,
    max_active_runs=1,
) as dag:

    batches = get_artist_batches()
    # Разворачиваем всю группу задач, каждый батч создаёт независимый конвейер ETL
    etl_per_batch.expand(batch=batches)
    
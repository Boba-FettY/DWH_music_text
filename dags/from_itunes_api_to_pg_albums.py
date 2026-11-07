from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
from datetime import datetime
import pandas as pd
import json
import io

OWNER = "JMAKQ"
DAG_ID = "from_api_to_s3_to_pg_dag"

LAYER_JSON = "raw"
LAYER_CSV = "stage"

SOURCE = "albums"

POSTGRES_CONN_ID = 'postgres_dwh'
SILO_BUCKET_NAME = 'prod'
SILO_CONN_ID = 'silo_s3_conn'

ACCESS_KEY = Variable.get("accessKey")
SECRET_KEY = Variable.get("secretKey")

ARTIST_ID = 292138099 # Макс Корж - 594629011, Noize MC - 292138099, Anacondaz - 573799679
SEARCH_URL = "https://itunes.apple.com/lookup"

def extract_artist_album_names(**context):
    
    # Получаем все альбомы артиста
    albums_response = requests.get(SEARCH_URL, params={
        'id': ARTIST_ID,
        'entity': 'album',
        'limit': 200,
        'sort': 'recent'
    })
    
    start_date = context['ds']
    
    object_key = f'{LAYER_JSON}/{SOURCE}/{start_date}/{start_date}_00-00-00.json'

    s3_hook = S3Hook(aws_conn_id=SILO_CONN_ID)
    
    s3_hook.load_bytes(
        bytes_data=albums_response.content,
        key=object_key,
        bucket_name=SILO_BUCKET_NAME,
        replace=True  # перезаписывать при совпадении имени
    )

def transform_artist_album_names(**context):
    
    start_date=context['ds']
    
    s3_hook = S3Hook(aws_conn_id=SILO_CONN_ID)
    read_key = f'{LAYER_JSON}/{SOURCE}/{start_date}/{start_date}_00-00-00.json'
    
    data_raw = s3_hook.read_key(
    key=read_key,
    bucket_name=SILO_BUCKET_NAME
    )

    albums_data = json.loads(data_raw)
        
    albums = []
    
    for item in albums_data['results']:
        
        # Пропускаем сам объект артиста
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
                'release_name': name,
                'genre': item.get('primaryGenreName'),
                'track_count': item.get('trackCount'),
                'release_date': item.get('releaseDate')[:10],
                'release_type': realese_type,
                'release_api_id': item.get('collectionId')
            })
            

            

    albums_sorted = sorted(albums, key=lambda x: x['release_date'], reverse=True)
    data_frame = pd.DataFrame(albums_sorted)
    data_frame.insert(0, 'singer_id', '1')

    csv_buffer = io.StringIO()
    data_frame.to_csv(csv_buffer, index=False)
    write_key = f'{LAYER_CSV}/{SOURCE}/{start_date}/{start_date}_00-00-00.csv'
    
    # Сохраняем обратно в S3
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=write_key,
        bucket_name=SILO_BUCKET_NAME,
        replace=True,
    )
    
def load_to_postgres_artist_album_names(**context):
    
    start_date = context['ds']
    s3_hook = S3Hook(aws_conn_id=SILO_CONN_ID)
    read_key = f'{LAYER_CSV}/{SOURCE}/{start_date}/{start_date}_00-00-00.csv'

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


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 11, 3),
    schedule_interval='0 5 * * *',
    max_active_runs=1,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_api_task',
        python_callable=extract_artist_album_names,
    )
    
    transform_task = PythonOperator(
        task_id='transform_s3_json_task',
        python_callable=transform_artist_album_names,
    )
    
    load_task = PythonOperator(
            task_id='load_to_postgres_task',
            python_callable=load_to_postgres_artist_album_names,
        )
    
    extract_task >> transform_task >> load_task

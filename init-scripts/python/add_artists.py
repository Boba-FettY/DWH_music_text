import requests
import psycopg2

ARTIST_NAME = "Борис Гребенщиков" # Сюда вписывать имя исполнителя


SEARCH_URL = "https://itunes.apple.com/search"

import psycopg2

def search_artist(artist_name):
    
    response = requests.get(SEARCH_URL, params={
        'term': ARTIST_NAME,
        'entity': 'musicArtist',
        'limit': 5
    }).json()


    artist = None
    
    for item in response['results']:
        if item['artistName'].lower() == artist_name.lower():
            artist = item
            break

    if not artist:
        print(f"Артист '{artist_name}' не найден")
        if response['results']:
            artist = response['results'][0]['artstName']
            artist_id = artist['artistId']
            print(f"Возможно: {artist['artistName']} (ID: {artist_id})")
        else:
            print('Ничего не найдено')
            
    artist_id = artist['artistId']
    print(f"Найден: {artist['artistName']} (ID: {artist_id})")
    artist_to_tab = {
        'singer_name': artist['artistName'],
        'primary_genre_name': artist['primaryGenreName'],
        'artist_api_id': artist['artistId']
        }
    return artist_to_tab

def load_to_postgres(tab):
    conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
    )

    cur = conn.cursor()

    sql = f"""
    INSERT INTO dwh.singers (singer_name, primary_genre_name, artist_api_id) VALUES 
    ('{tab['singer_name']}', '{tab['primary_genre_name']}', '{tab['artist_api_id']}');
    """
    print('-------------')
    print(sql)
    print('-------------')

    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()

if __name__ == '__main__':
    
    q = search_artist(ARTIST_NAME)
    print(q)
    load_to_postgres(q)
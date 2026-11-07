CREATE SCHEMA IF NOT EXISTS dwh
-- Исполнители
CREATE TABLE IF NOT EXISTS dwh.singers (
    singer_id SERIAL PRIMARY KEY,
    singer_name VARCHAR(255) NOT NULL,
    primary_genre_name VARCHAR(100),
    artist_api_id VARCHAR(100) UNIQUE NOT NULL
);

-- Альбомы
CREATE TABLE IF NOT EXISTS dwh.releases (
    release_id SERIAL PRIMARY KEY,
    singer_id INT REFERENCES dwh.singers(singer_id),
    release_name VARCHAR(100) not null,
    genre VARCHAR(100),
    track_count INT,
    release_date DATE,
    release_type VARCHAR(50),
    release_api_id VARCHAR(100) UNIQUE NOT NULL
);

-- Треки
CREATE TABLE IF NOT EXISTS dwh.tracks (
    track_id SERIAL PRIMARY KEY,
    album_id INT REFERENCES dwh.releases(release_id),
    track_name VARCHAR(255) NOT NULL,
    track_number INT,
    duration INT, -- Длительность в секундах
    track_api_id VARCHAR(100) UNIQUE NOT NULL
);

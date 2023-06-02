import json
import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import time
import sqlite3


class MusicDatabase:
    def __init__(self):
        load_dotenv()
        self.SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
        self.SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
        self.SPOTIPY_REDIRECT_URI = os.getenv('SPOTIPY_REDIRECT_URI')
        self.auth_manager = SpotifyClientCredentials()
        self.sp = spotipy.Spotify(auth_manager=self.auth_manager)
        self.conn = None
        self.cursor = None

    def connect_to_database(self):
        try:
            self.conn = sqlite3.connect('music.db')
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")

    def disconnect_from_database(self):
        if self.cursor and self.conn:
            self.cursor.close()
            self.conn.close()

    def create_tables(self):
        # Create necessary tables in the database if they don't exist
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                artist_name TEXT,
                artist_id TEXT,
                track_name TEXT,
                track_id TEXT
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS searched_and_not_found (
                artist_name TEXT,
                track_name TEXT
            )
        """)
        self.conn.commit()

    def artist_and_track_in_db(self, data):
        # Return if an artist name and track name exist in the database
        query = "SELECT * FROM tracks WHERE artist_name = ? AND track_name = ?"
        self.cursor.execute(query, data)
        result = self.cursor.fetchone()
        return result is not None

    def get_artist_id_from_db(self, artist_name, track_name):
        # Retrieve the artist ID from the database
        query = "SELECT artist_id FROM tracks WHERE artist_name = ? AND track_name = ?"
        self.cursor.execute(query, (artist_name, track_name))
        result = self.cursor.fetchone()
        return result[0] if result else None

    def insert_data_to_db(self, rows):
        # Insert data into the tracks table
        query = "INSERT INTO tracks VALUES (?, ?, ?, ?)"
        self.cursor.executemany(query, rows)
        self.conn.commit()

    def insert_data_to_searched_db(self, rows):
        # Insert data into the searched_and_not_found table
        query = "INSERT INTO searched_and_not_found VALUES (?, ?)"
        self.cursor.executemany(query, rows)
        self.conn.commit()

    def artist_and_track_in_skip_db(self, rows):
        # Check if an artist name and track name exist in the searched_and_not_found table
        query = "SELECT 1 FROM searched_and_not_found WHERE artist_name = ? AND track_name = ?"
        for row in rows:
            self.cursor.execute(query, row)
            result = self.cursor.fetchone()
            if result:
                return True
        return False

    def get_track_ids(self, artist_name, track_name):
        if self.artist_and_track_in_skip_db((artist_name, track_name)):
            return

        if self.artist_and_track_in_db((artist_name, track_name)):
            return

        search_results = self.sp.search(q=artist_name, limit=50, offset=0, type='artist', market=None)
        artist_meta = search_results['artists']['items']
        artist_uris = [x['uri'] for x in artist_meta if x['name'] == artist_name]

        for artist_uri in artist_uris:
            all_albums = self.get_all_albums(artist_uri)
            if all_albums is None:
                continue

            all_album_tracks = []
            for x in range(0, len(all_albums), 20):
                album_meta = self.sp.albums(all_albums[x:x + 20])
                albums = [album['tracks']['items'] for album in album_meta['albums']]
                album_tracks = [song for album in albums for song in album]
                all_album_tracks.extend([album_track['uri'] for album_track in album_tracks])
                insert_data = [(artist_name, artist_uri, track['name'], track['uri']) for track in album_tracks]
                self.insert_data_to_db(insert_data)

                if track_name in [album_track['name'] for album_track in album_tracks]:
                    return

        insert_data = [(artist_name, track_name)]
        self.insert_data_to_searched_db(insert_data)

    def get_all_albums(self, artist_uri):
        try:
            all_albums = []
            offset = 0
            while True:
                current_results = self.sp.artist_albums(artist_id=artist_uri, limit=50, offset=offset)
                current_albums = current_results.get('items', [])
                all_albums.extend([album['uri'] for album in current_albums])
                offset += len(current_albums)
                if len(current_albums) < 50:
                    break
            time.sleep(2)
            return all_albums
        except spotipy.SpotifyException as e:
            print(f"Spotify API error: {e}")
            return None


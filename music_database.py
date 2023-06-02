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


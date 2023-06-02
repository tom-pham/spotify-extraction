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


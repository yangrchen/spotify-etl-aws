import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
import json

s3 = boto3.client("s3")

# Spotify API credentials
client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
client_auth_manager = SpotifyClientCredentials(
    client_id=client_id, client_secret=client_secret
)

# Initialize Spotipy
sp = spotipy.Spotify(client_credentials_manager=client_auth_manager)


def search_artist_id(query: str) -> str:
    q = f"artist:{query}"
    res = sp.search(q, type="artist")
    artist_data = res["artists"]["items"]
    if not artist_data:
        return None

    artist_id = artist_data[0]["id"]
    return artist_id


def search_albums(artist_id: str) -> list[dict]:
    """
    Search for an artist's ID on Spotify based on their name.

    Parameters
    ----------
    query : str
        The name of the artist to search for.

    Returns
    -------
    str
        The Spotify ID of the artist if found, None otherwise.

    Examples
    --------
    >>> search_artist_id('The Beatles')
    '3WrFJ7ztbogyGnTHbHJFl2'
    """
    albums = []
    res = sp.artist_albums(artist_id, album_type="album")
    albums.extend(res["items"])
    while res["next"]:
        res = sp.next(res)
        albums.extend(res["items"])
    return albums


def get_album_tracks(album_id: str) -> list[dict]:
    """
    Retrieve the tracks of an album from Spotify.

    Parameters:
    ----------
    album_id : str
        The ID of the album.

    Returns:
    -------
    list[dict]
        A list of dictionaries representing the tracks of the album.
        Each dictionary contains information about a track, including
        the track name, artist, duration, and more.

    """
    tracks = []
    results = sp.album_tracks(album_id)
    tracks.extend(results["items"])
    while results["next"]:
        results = sp.next(results)
        tracks.extend(results["items"])

    # Tracks don't come with the album id, so we add it here
    for track in tracks:
        track["album_id"] = album_id
    return tracks


def lambda_handler(event, context):
    try:
        # Simple case for now with hard-coded artist query
        artist_id = search_artist_id("oh wonder")
        if not artist_id:
            return {"status": 404, "body": "Artist not found."}

        # Find albums associated with the artist query along with all tracks from each album
        albums = search_albums(artist_id)
        all_tracks = []
        for album in albums:
            tracks = get_album_tracks(album["id"])
            all_tracks.extend(tracks)

        # Write raw data to S3 bucket
        bucket = os.getenv("BUCKET_NAME")
        key = f"raw-data/spotify-raw-{str(datetime.now())}.json"
        body = json.dumps({"albums": albums, "tracks": all_tracks})

        s3.put_object(Bucket=bucket, Key=key, Body=body)

        return {"status": 200, "body": "Inserted raw data into S3 bucket."}
    except Exception as e:
        return {"status": 500, "body": str(e)}

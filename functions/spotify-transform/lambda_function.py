import json
import pandas as pd
from datetime import datetime
import boto3
import os
import urllib.parse

s3 = boto3.client("s3")

BUCKET_NAME = os.getenv("BUCKET_NAME")


def clean_album_data(albums_json: list) -> pd.DataFrame:
    """
    Clean the album data retrieved from Spotify.

    Parameters
    ----------
    albums_json : list
        The list of album data in JSON format retrieved from Spotify.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the cleaned album data.

    Examples
    --------
    >>> albums_json = [{'album_group': 'album', 'album_type': 'album', 'type': 'album', 'artists': [{'name': 'The Beatles', 'external_urls': {'spotify': 'https://open.spotify.com/artist/3WrFJ7ztbogyGnTHbHJFl2'}, 'id': '3WrFJ7ztbogyGnTHbHJFl2'}], 'external_urls': {'spotify': 'https://open.spotify.com/album/3KzAvEXcqJKBF97HrXwlgf'}}]
    >>> clean_album_data(albums_json)
    """
    albums = pd.DataFrame(albums_json)
    albums = albums.drop(["album_group", "album_type", "type"], axis=1)
    albums["artist_names"] = albums["artists"].apply(
        lambda x: [artist["name"] for artist in x]
    )
    albums["artist_urls"] = albums["artists"].apply(
        lambda x: [artist["external_urls"]["spotify"] for artist in x]
    )
    albums["artist_ids"] = albums["artists"].apply(
        lambda x: [artist["id"] for artist in x]
    )
    albums["song_url"] = albums["external_urls"].apply(lambda x: x["spotify"])
    albums = albums.drop(["artists", "external_urls"], axis=1)
    return albums


def clean_track_data(tracks_json: list) -> pd.DataFrame:
    """
    Clean the track data retrieved from Spotify.

    Parameters
    ----------
    tracks_json : list
        The list of track data in JSON format retrieved from Spotify.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the cleaned track data.

    Examples
    --------
    >>> tracks_json = [{'preview_url': 'https://p.scdn.co/mp3-preview/2a3d7df1a5cf8f9b8b8d2d4f2f2a1a2a8a8a2a2a', 'disc_number': 1, 'type': 'track', 'artists': [{'name': 'The Beatles', 'external_urls': {'spotify': 'https://open.spotify.com/artist/3WrFJ7ztbogyGnTHbHJFl2'}, 'id': '3WrFJ7ztbogyGnTHbHJFl2'}], 'external_urls': {'spotify': 'https://open.spotify.com/track/2EqlS6tkEnglzr7tkKAAYD'}}]
    >>> clean_track_data(tracks_json)
    """
    tracks = pd.DataFrame(tracks_json)
    tracks = tracks.drop(["preview_url", "disc_number", "type"], axis=1)
    tracks["artist_names"] = tracks["artists"].apply(
        lambda x: [artist["name"] for artist in x]
    )
    tracks["artist_urls"] = tracks["artists"].apply(
        lambda x: [artist["external_urls"]["spotify"] for artist in x]
    )
    tracks["artist_ids"] = tracks["artists"].apply(
        lambda x: [artist["id"] for artist in x]
    )
    tracks["track_url"] = tracks["external_urls"].apply(lambda x: x["spotify"])
    tracks = tracks.drop(["artists", "external_urls"], axis=1)
    return tracks


def lambda_handler(event, context):
    try:
        event_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        event_object = event["Records"][0]["s3"]["object"]["key"]

        # URL decoding from the event data
        event_object = urllib.parse.unquote_plus(event_object)

        obj = s3.get_object(Bucket=event_bucket, Key=event_object)
        file_content = obj["Body"].read().decode("utf-8")
        raw_data = json.loads(file_content)
        albums = raw_data["albums"]
        tracks = raw_data["tracks"]

        albums = clean_album_data(albums)
        tracks = clean_track_data(tracks)
        album_key = f"transformed-data/albums/spotify-transformed-albums-{str(datetime.now())}.csv"
        tracks_key = f"transformed-data/tracks/spotify-transformed-tracks-{str(datetime.now())}.csv"
        albums.to_csv(f"s3://{BUCKET_NAME}/{album_key}", index=False)
        tracks.to_csv(f"s3://{BUCKET_NAME}/{tracks_key}", index=False)

        return {"status": 200, "body": "Transformed data."}
    except Exception as e:
        return {"status": 500, "body": str(e)}

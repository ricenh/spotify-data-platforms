import json
import requests
from src.extract.utils import get_headers, raw_data_path, write_json

ARTISTS_URL = "https://api.spotify.com/v1/artists"
BATCH_SIZE = 50

def load_artist_ids():
    """Get unique artist IDs from tracks"""
    path = raw_data_path() / "tracks.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    artist_ids = set()
    for track in data["tracks"]:
        for artist in track["artists"]:
            artist_ids.add(artist["id"])
    
    return list(artist_ids)

def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def extract_artists():
    headers = get_headers()
    artist_ids = load_artist_ids()
    artists = []

    for batch in chunks(artist_ids, BATCH_SIZE):
        params = {"ids": ",".join(batch)}
        r = requests.get(ARTISTS_URL, headers=headers, params=params)
        r.raise_for_status()
        artists.extend(r.json()["artists"])

    output = {
        "artist_count": len(artists),
        "artists": artists
    }

    path = raw_data_path() / "artists.json"
    write_json(path, output)
    print(f"Saved {len(artists)} artists → {path}")

if __name__ == "__main__":
    extract_artists()
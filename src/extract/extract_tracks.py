import json
import requests
from pathlib import Path
from src.extract.utils import get_headers, raw_data_path, write_json

TRACKS_URL = "https://api.spotify.com/v1/tracks"
BATCH_SIZE = 50

def load_track_ids():
    path = raw_data_path() / "recently_played.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return list({
        item["track"]["id"]
        for item in data["items"]
        if item["track"]["id"]
    })

def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def extract_tracks():
    headers = get_headers()
    track_ids = load_track_ids()

    tracks = []

    for batch in chunks(track_ids, BATCH_SIZE):
        params = {"ids": ",".join(batch)}
        r = requests.get(TRACKS_URL, headers=headers, params=params)
        r.raise_for_status()
        tracks.extend(r.json()["tracks"])

    output = {
        "track_count": len(tracks),
        "tracks": tracks
    }

    path = raw_data_path() / "tracks.json"
    write_json(path, output)

    print(f"Saved {len(tracks)} tracks → {path}")

if __name__ == "__main__":
    extract_tracks()

import json
import requests
from src.extract.utils import raw_data_path

RECCOBEATS_URL = "https://api.reccobeats.com/v1/audio-features"
BATCH_SIZE = 50


def load_track_ids():
    path = raw_data_path() / "recently_played.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    ids = set()
    for item in data["items"]:
        track = item.get("track")
        if track and track.get("type") == "track" and track.get("id"):
            ids.add(track["id"])

    return list(ids)


def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def extract_audio_features():
    track_ids = load_track_ids()
    features = []

    if not track_ids:
        print("No track IDs found — skipping.")
        return

    for batch in chunks(track_ids, BATCH_SIZE):
        params = {
            "ids": ",".join(batch)
        }

        r = requests.get(
            RECCOBEATS_URL,
            params=params,
            headers={"Accept": "application/json"},
            timeout=30
        )

        print("STATUS:", r.status_code)

        if r.status_code != 200:
            print("ERROR RESPONSE:", r.text)
            raise RuntimeError("ReccoBeats request failed")

        data = r.json()
        features.extend(data.get("audio_features", data))

    output = {
        "feature_count": len(features),
        "audio_features": features
    }

    path = raw_data_path() / "audio_features.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(features)} audio features → {path}")


if __name__ == "__main__":
    extract_audio_features()

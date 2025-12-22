import json
import requests
from src.extract.utils import raw_data_path

RECCOBEATS_URL = "https://api.reccobeats.com/v1/audio-features"
BATCH_SIZE = 40  # ReccoBeats limit


def load_track_ids():
    """
    Load Spotify track IDs from recently_played.json
    """
    path = raw_data_path() / "recently_played.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    ids = set()
    for item in data.get("items", []):
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

    for batch_num, batch in enumerate(chunks(track_ids, BATCH_SIZE), start=1):
        params = {
            "ids": ",".join(batch)
        }

        response = requests.get(
            RECCOBEATS_URL,
            params=params,
            headers={"Accept": "application/json"},
            timeout=30
        )

        print(f"Batch {batch_num} | Status {response.status_code}")

        if response.status_code != 200:
            print("Error response:", response.text)
            raise RuntimeError("ReccoBeats request failed")

        data = response.json()

        # ReccoBeats returns features under `content`
        batch_features = data.get("content", [])
        features.extend(batch_features)

        print(f"  Retrieved {len(batch_features)} features")

    output = {
        "feature_count": len(features),
        "audio_features": features
    }

    out_path = raw_data_path() / "audio_features.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved {len(features)} audio features → {out_path}")


if __name__ == "__main__":
    extract_audio_features()

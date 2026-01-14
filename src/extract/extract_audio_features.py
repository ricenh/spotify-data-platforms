import requests
from src.extract.s3_utils import upload_json_to_s3, download_json_from_s3

RECCOBEATS_URL = "https://api.reccobeats.com/v1/audio-features"
BATCH_SIZE = 40


def load_track_ids():
    # Read from S3
    data = download_json_from_s3("recently_played")

    ids = set()
    for item in data.get("items", []):
        track = item.get("track")
        if track and track.get("type") == "track" and track.get("id"):
            ids.add(track["id"])

    return list(ids)


def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def extract_audio_features():
    track_ids = load_track_ids()
    features = []

    if not track_ids:
        print("No track IDs found — skipping.")
        return

    for batch_num, batch in enumerate(chunks(track_ids, BATCH_SIZE), start=1):
        params = {"ids": ",".join(batch)}

        response = requests.get(
            RECCOBEATS_URL,
            params=params,
            headers={"Accept": "application/json"},
            timeout=30,
        )

        print(f"Batch {batch_num} | Status {response.status_code}")

        if response.status_code != 200:
            print("Error response:", response.text)
            raise RuntimeError("ReccoBeats request failed")

        data = response.json()
        batch_features = data.get("content", [])
        features.extend(batch_features)

        print(f"  Retrieved {len(batch_features)} features")

    output = {"feature_count": len(features), "audio_features": features}

    # Upload to S3
    s3_uri = upload_json_to_s3(output, "audio_features")

    print(f"✅ Saved {len(features)} audio features to S3")
    return s3_uri


if __name__ == "__main__":
    extract_audio_features()

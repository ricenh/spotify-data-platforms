import requests
from src.extract.utils import get_headers
from src.extract.s3_utils import upload_json_to_s3, download_json_from_s3

TRACKS_URL = "https://api.spotify.com/v1/tracks"
BATCH_SIZE = 50


def load_track_ids():
    # Read from S3 instead of local disk
    data = download_json_from_s3("recently_played")

    return list({item["track"]["id"] for item in data["items"] if item["track"]["id"]})


def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def extract_tracks():
    headers = get_headers()
    track_ids = load_track_ids()

    tracks = []

    for batch in chunks(track_ids, BATCH_SIZE):
        params = {"ids": ",".join(batch)}
        r = requests.get(TRACKS_URL, headers=headers, params=params)
        r.raise_for_status()
        tracks.extend(r.json()["tracks"])

    output = {"track_count": len(tracks), "tracks": tracks}

    # Upload to S3
    s3_uri = upload_json_to_s3(output, "tracks")

    print(f"✅ Saved {len(tracks)} tracks to S3")
    return s3_uri


if __name__ == "__main__":
    extract_tracks()

import json
import requests
import boto3
import os
from datetime import datetime
from src.extract.utils import write_json

RECCOBEATS_URL = "https://api.reccobeats.com/v1/audio-features"
BATCH_SIZE = 40

def today_partition():
    return datetime.utcnow().strftime("%Y-%m-%d")

def download_from_s3():
    """Download recently_played.json from S3"""
    s3 = boto3.client('s3')
    bucket = os.getenv('S3_BUCKET')
    partition = today_partition()
    s3_key = f"raw/recently_played/dt={partition}/data.json"
    
    print(f"📥 Downloading from s3://{bucket}/{s3_key}")
    
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    
    print(f"✅ Downloaded from s3://{bucket}/{s3_key}")
    return data

def load_track_ids():
    """Load Spotify track IDs from S3"""
    data = download_from_s3()
    
    ids = set()
    for item in data.get("items", []):
        track = item.get("track")
        if track and track.get("id"):
            ids.add(track["id"])
    
    track_ids = list(ids)
    print(f"📋 Loaded {len(track_ids)} unique track IDs")
    return track_ids

def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def extract_track_id_from_href(href):
    """
    Extract Spotify track ID from href URL
    Example: https://open.spotify.com/track/3pbtBomO4Zt5gGiqsYeiBH -> 3pbtBomO4Zt5gGiqsYeiBH
    """
    if not href:
        return None
    parts = href.rstrip('/').split('/')
    return parts[-1] if parts else None

def upload_to_s3(data):
    """Upload audio features to S3"""
    s3 = boto3.client('s3')
    bucket = os.getenv('S3_BUCKET')
    partition = today_partition()
    s3_key = f"raw/audio_features/dt={partition}/data.json"
    
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(data, indent=2),
        ContentType='application/json'
    )
    
    print(f"✅ Uploaded to s3://{bucket}/{s3_key}")

def extract_audio_features():
    track_ids = load_track_ids()
    features = []

    if not track_ids:
        print("No track IDs found — skipping.")
        return

    print(f"🎵 Extracting audio features for {len(track_ids)} tracks...")

    for batch_num, batch in enumerate(chunks(track_ids, BATCH_SIZE), start=1):
        params = {"ids": ",".join(batch)}

        try:
            response = requests.get(
                RECCOBEATS_URL,
                params=params,
                headers={"Accept": "application/json"},
                timeout=30
            )

            print(f"Batch {batch_num} | Status {response.status_code}")

            if response.status_code != 200:
                print(f"  ⚠️  Error: {response.text}")
                continue

            data = response.json()
            batch_features = data.get("content", [])

            if not batch_features:
                print(f"  ⚠️  No content returned")
                continue

            # Fix track IDs: Replace UUID with real Spotify ID from href
            for feature in batch_features:
                if feature and feature.get("href"):
                    spotify_id = extract_track_id_from_href(feature["href"])
                    if spotify_id:
                        feature["id"] = spotify_id

            # Filter out features without valid IDs
            batch_features = [f for f in batch_features if f and f.get("id")]
            features.extend(batch_features)

            print(f"  ✅ Retrieved {len(batch_features)} features")

        except Exception as e:
            print(f"  ❌ Batch {batch_num} failed: {str(e)}")
            continue

    output = {
        "feature_count": len(features),
        "audio_features": features
    }

    # Upload to S3
    upload_to_s3(output)
    print(f"✅ Saved {len(features)} audio features to S3")

if __name__ == "__main__":
    extract_audio_features()
import json
import boto3
import os
from datetime import datetime
from src.load.db import get_conn

def today_partition():
    return datetime.utcnow().strftime("%Y-%m-%d")

def download_from_s3():
    """Download audio_features.json from S3"""
    s3 = boto3.client('s3')
    bucket = os.getenv('S3_BUCKET')
    partition = today_partition()
    s3_key = f"raw/audio_features/dt={partition}/data.json"
    
    print(f"📥 Downloading from s3://{bucket}/{s3_key}")
    
    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        print(f"✅ Downloaded {data.get('feature_count', 0)} audio features from S3")
        return data
    except Exception as e:
        print(f"❌ Error downloading from S3: {str(e)}")
        return {"audio_features": []}

def load_audio_features():
    data = download_from_s3()
    
    if not data.get("audio_features"):
        print("No audio features to load")
        return

    conn = get_conn()
    cur = conn.cursor()

    loaded_count = 0
    for af in data["audio_features"]:
        try:
            cur.execute(
                """
                INSERT INTO audio_features VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (track_id) DO NOTHING
                """,
                (
                    af["id"],
                    af.get("danceability"),
                    af.get("energy"),
                    af.get("tempo"),
                    af.get("valence"),
                    af.get("loudness")
                )
            )
            loaded_count += 1
        except Exception as e:
            print(f"⚠️  Skipping track {af.get('id')}: {str(e)}")
            continue

    conn.commit()
    cur.close()
    conn.close()
    
    print(f"✅ Loaded {loaded_count} audio features to database")

if __name__ == "__main__":
    load_audio_features()
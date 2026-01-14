from src.load.db_rds import get_rds_conn
from src.extract.s3_utils import download_json_from_s3


def load_audio_features():
    # Read from S3
    data = download_json_from_s3("audio_features")

    conn = get_rds_conn()
    cur = conn.cursor()

    for af in data["audio_features"]:
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
                af.get("loudness"),
            ),
        )

    conn.commit()
    print(f"✅ Loaded {len(data['audio_features'])} audio features to RDS")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_audio_features()

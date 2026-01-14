from src.load.db_rds import get_rds_conn
from src.extract.s3_utils import download_json_from_s3


def load_tracks():
    # Read from S3
    data = download_json_from_s3("tracks")

    conn = get_rds_conn()
    cur = conn.cursor()

    for track in data["tracks"]:
        cur.execute(
            """
            INSERT INTO tracks VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (track_id) DO NOTHING
            """,
            (
                track["id"],
                track["name"],
                track["duration_ms"],
                track["popularity"],
                track["explicit"],
                track["artists"][0]["id"],
            ),
        )

    conn.commit()
    print(f"✅ Loaded {len(data['tracks'])} tracks to RDS")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_tracks()

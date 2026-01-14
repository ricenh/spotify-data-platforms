from src.load.db_rds import get_rds_conn
from src.extract.s3_utils import download_json_from_s3


def load_artists():
    # Read from S3
    data = download_json_from_s3("tracks")

    artists = {}

    for track in data["tracks"]:
        for artist in track["artists"]:
            artists[artist["id"]] = (
                artist["id"],
                artist["name"],
                artist.get("popularity"),
                artist.get("genres", []),
            )

    # Write to RDS
    conn = get_rds_conn()
    cur = conn.cursor()

    for artist in artists.values():
        cur.execute(
            """
            INSERT INTO artists VALUES (%s,%s,%s,%s)
            ON CONFLICT (artist_id) DO NOTHING
            """,
            artist,
        )

    conn.commit()
    print(f"✅ Loaded {len(artists)} artists to RDS")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_artists()

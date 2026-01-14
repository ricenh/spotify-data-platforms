from dateutil.parser import isoparse
from src.load.db_rds import get_rds_conn
from src.extract.s3_utils import download_json_from_s3


def load_plays():
    # Read from S3
    data = download_json_from_s3("recently_played")

    conn = get_rds_conn()
    cur = conn.cursor()

    loaded = 0
    skipped = 0

    for item in data["items"]:
        try:
            cur.execute(
                """
                INSERT INTO plays VALUES (%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (isoparse(item["played_at"]), item["track"]["id"]),
            )
            loaded += 1
        except Exception as e:
            # Skip plays for tracks that don't exist
            skipped += 1
            conn.rollback()
            continue

    conn.commit()
    print(f"✅ Loaded {loaded} plays to RDS")
    if skipped > 0:
        print(f"⚠️  Skipped {skipped} plays (missing track references)")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_plays()

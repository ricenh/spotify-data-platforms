import json
from src.load.db import get_conn
from src.extract.utils import raw_data_path
from dateutil.parser import isoparse

def load_plays():
    path = raw_data_path() / "recently_played.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = get_conn()
    cur = conn.cursor()

    for item in data["items"]:
        cur.execute(
            """
            INSERT INTO plays VALUES (%s,%s)
            ON CONFLICT DO NOTHING
            """,
            (
                isoparse(item["played_at"]),
                item["track"]["id"]
            )
        )

    conn.commit()
    cur.close()
    conn.close()

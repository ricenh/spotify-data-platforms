import json
from src.load.db import get_conn
from src.extract.utils import raw_data_path

def load_tracks():
    path = raw_data_path() / "tracks.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = get_conn()
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
                track["artists"][0]["id"]
            )
        )

    conn.commit()
    cur.close()
    conn.close()

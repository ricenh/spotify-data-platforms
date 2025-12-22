import json
from src.load.db import get_conn
from src.extract.utils import raw_data_path

def load_audio_features():
    path = raw_data_path() / "audio_features.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = get_conn()
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
                af.get("loudness")
            )
        )

    conn.commit()
    cur.close()
    conn.close()

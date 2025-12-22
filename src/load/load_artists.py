import json
from src.load.db import get_conn
from src.extract.utils import raw_data_path

def load_artists():
    path = raw_data_path() / "tracks.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    artists = {}

    for track in data["tracks"]:
        for artist in track["artists"]:
            artists[artist["id"]] = (
                artist["id"],
                artist["name"],
                artist.get("popularity"),
                artist.get("genres", [])
            )

    conn = get_conn()
    cur = conn.cursor()

    for artist in artists.values():
        cur.execute(
            """
            INSERT INTO artists VALUES (%s,%s,%s,%s)
            ON CONFLICT (artist_id) DO NOTHING
            """,
            artist
        )

    conn.commit()
    cur.close()
    conn.close()

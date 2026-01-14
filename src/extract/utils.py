import json
import os
from datetime import datetime
from pathlib import Path
from src.auth.spotify_auth import get_access_token


def get_headers():
    token = get_access_token()
    return {"Authorization": f"Bearer {token}"}


def today_partition():
    return datetime.utcnow().strftime("%Y-%m-%d")


def raw_data_path():
    path = Path("data/raw") / today_partition()
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def get_headers():
    token = get_access_token()
    print("DEBUG access token:", token[:20] if token else token)
    return {"Authorization": f"Bearer {token}"}


def get_reccobeats_headers():
    return {"Content-Type": "application/json"}

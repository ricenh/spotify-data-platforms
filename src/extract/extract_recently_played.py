import requests
from datetime import datetime
from src.extract.utils import get_headers
from src.extract.s3_utils import upload_json_to_s3

URL = "https://api.spotify.com/v1/me/player/recently-played"
LIMIT = 50


def extract_recently_played():
    headers = get_headers()
    params = {"limit": LIMIT}

    all_items = []
    before = None

    while True:
        if before:
            params["before"] = before

        r = requests.get(URL, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()

        items = data.get("items", [])
        if not items:
            break

        all_items.extend(items)
        before = items[-1]["played_at"]

        if len(items) < LIMIT:
            break

    output = {
        "extracted_at": datetime.utcnow().isoformat(),
        "count": len(all_items),
        "items": all_items,
    }

    # Upload to S3 instead of local disk
    s3_uri = upload_json_to_s3(output, "recently_played")

    print(f"✅ Saved {len(all_items)} plays to S3")
    return s3_uri


if __name__ == "__main__":
    extract_recently_played()

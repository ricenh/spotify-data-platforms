import requests
from src.auth.spotify_auth import get_access_token

token = get_access_token()

headers = {"Authorization": f"Bearer {token}"}
r = requests.get(
    "https://api.spotify.com/v1/me/player/recently-played?limit=1", headers=headers
)

print(r.status_code)
print(r.json())

import base64
import json
import requests
from urllib.parse import urlencode
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")

SCOPE = "user-read-recently-played"

AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        code = self.path.split("code=")[1]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"You can close this window.")

        auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

        headers = {
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        }

        r = requests.post(TOKEN_URL, headers=headers, data=data)
        tokens = r.json()

        print("\nSAVE THIS REFRESH TOKEN:\n")
        print(tokens["refresh_token"])
        print("\nDO NOT COMMIT IT\n")


def main():
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPE,
    }

    auth_url = f"{AUTH_URL}?{urlencode(params)}"
    webbrowser.open(auth_url)

    server = HTTPServer(("localhost", 8888), CallbackHandler)
    server.handle_request()


if __name__ == "__main__":
    main()

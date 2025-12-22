from src.load.load_artists import load_artists
from src.load.load_tracks import load_tracks
from src.load.load_plays import load_plays
from src.load.load_audio_features import load_audio_features

def main():
    load_artists()
    load_tracks()
    load_plays()
    load_audio_features()
    print("✅ Load complete")

if __name__ == "__main__":
    main()

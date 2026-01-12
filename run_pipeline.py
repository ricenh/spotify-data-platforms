"""
Spotify Data Pipeline Runner
Runs the complete ETL pipeline: Extract → Load
"""
import sys
from datetime import datetime

def print_header(message):
    """Print a formatted header"""
    print("\n" + "="*60)
    print(f"  {message}")
    print("="*60 + "\n")

def print_step(step_num, total_steps, message):
    """Print a step indicator"""
    print(f"[{step_num}/{total_steps}] {message}...")

def run_pipeline():
    """Run the complete data pipeline"""
    start_time = datetime.now()
    
    print_header("🎵 SPOTIFY DATA PIPELINE")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # ============================================
        # EXTRACT PHASE
        # ============================================
        print_header("📥 EXTRACTION PHASE (Spotify API → S3)")
        
        # Step 1: Extract recently played
        print_step(1, 6, "Extracting recently played tracks")
        from src.extract.extract_recently_played import extract_recently_played
        extract_recently_played()
        
        # Step 2: Extract tracks
        print_step(2, 6, "Extracting track details")
        from src.extract.extract_tracks import extract_tracks
        extract_tracks()
        
        # Step 3: Extract audio features
        print_step(3, 6, "Extracting audio features")
        from src.extract.extract_audio_features import extract_audio_features
        extract_audio_features()
        
        print("\n✅ Extraction phase complete!\n")
        
        # ============================================
        # LOAD PHASE
        # ============================================
        print_header("📤 LOAD PHASE (S3 → RDS)")
        
        # Step 4: Load artists
        print_step(4, 6, "Loading artists")
        from src.load.load_artists import load_artists
        load_artists()
        
        # Step 5: Load tracks
        print_step(5, 6, "Loading tracks")
        from src.load.load_tracks import load_tracks
        load_tracks()
        
        # Step 6: Load plays
        print_step(6, 6, "Loading plays")
        from src.load.load_plays import load_plays
        load_plays()
        
        # Step 7: Load audio features
        print_step(7, 7, "Loading audio features")
        from src.load.load_audio_features import load_audio_features
        load_audio_features()
        
        print("\n✅ Load phase complete!\n")
        
        # ============================================
        # SUMMARY
        # ============================================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print_header("✅ PIPELINE COMPLETE!")
        print(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total duration: {duration:.2f} seconds")
        print("\nNext steps:")
        print("  1. Run dbt transformations: cd dbt && dbt run")
        print("  2. Query analytics tables in RDS")
        print("\n")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR: Pipeline failed!")
        print(f"Error: {str(e)}")
        print("\nPlease check:")
        print("  - AWS credentials in .env")
        print("  - Spotify API credentials in .env")
        print("  - RDS database is running")
        print("  - S3 bucket is accessible")
        return 1

if __name__ == "__main__":
    exit_code = run_pipeline()
    sys.exit(exit_code)
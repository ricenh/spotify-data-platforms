"""
Complete Spotify Data Pipeline with Transformations
Runs: Extract → Load → Transform (dbt)
"""
import sys
import subprocess
from datetime import datetime
from pathlib import Path

def print_header(message):
    """Print a formatted header"""
    print("\n" + "="*60)
    print(f"  {message}")
    print("="*60 + "\n")

def print_step(step_num, total_steps, message):
    """Print a step indicator"""
    print(f"[{step_num}/{total_steps}] {message}...")

def run_dbt():
    """Run dbt transformations"""
    try:
        dbt_dir = Path(__file__).parent / "spotify_analytics"
        
        # Run dbt
        print("Running dbt models...")
        result = subprocess.run(
            ["dbt", "run"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"dbt run failed: {result.stderr}")
            return False
            
        print(result.stdout)
        
        # Run dbt tests
        print("\nRunning dbt tests...")
        result = subprocess.run(
            ["dbt", "test"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"dbt test failed: {result.stderr}")
            return False
            
        print(result.stdout)
        return True
        
    except Exception as e:
        print(f"Error running dbt: {e}")
        return False

def run_full_pipeline():
    """Run the complete pipeline including transformations"""
    start_time = datetime.now()
    
    print_header("🎵 SPOTIFY DATA PIPELINE (FULL)")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # ============================================
        # EXTRACT PHASE
        # ============================================
        print_header("📥 EXTRACTION PHASE (Spotify API → S3)")
        
        print_step(1, 8, "Extracting recently played tracks")
        from src.extract.extract_recently_played import extract_recently_played
        extract_recently_played()
        
        print_step(2, 8, "Extracting track details")
        from src.extract.extract_tracks import extract_tracks
        extract_tracks()
        
        print_step(3, 8, "Extracting audio features")
        from src.extract.extract_audio_features import extract_audio_features
        extract_audio_features()
        
        print("\n✅ Extraction phase complete!\n")
        
        # ============================================
        # LOAD PHASE
        # ============================================
        print_header("📤 LOAD PHASE (S3 → RDS)")
        
        print_step(4, 8, "Loading artists")
        from src.load.load_artists import load_artists
        load_artists()
        
        print_step(5, 8, "Loading tracks")
        from src.load.load_tracks import load_tracks
        load_tracks()
        
        print_step(6, 8, "Loading plays")
        from src.load.load_plays import load_plays
        load_plays()
        
        print_step(7, 8, "Loading audio features")
        from src.load.load_audio_features import load_audio_features
        load_audio_features()
        
        print("\n✅ Load phase complete!\n")
        
        # ============================================
        # TRANSFORM PHASE
        # ============================================
        print_header("🔄 TRANSFORM PHASE (dbt)")
        
        print_step(8, 8, "Running dbt transformations")
        if not run_dbt():
            print("\n⚠️  Warning: dbt transformations failed")
            print("Pipeline data is loaded but analytics tables may not be updated")
        else:
            print("\n✅ Transform phase complete!\n")
        
        # ============================================
        # SUMMARY
        # ============================================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print_header("✅ FULL PIPELINE COMPLETE!")
        print(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total duration: {duration:.2f} seconds")
        print("\nData is ready to query!")
        print("  Query: psql -h $RDS_HOST -U spotify_admin -d spotify")
        print("\n")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR: Pipeline failed!")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = run_full_pipeline()
    sys.exit(exit_code)
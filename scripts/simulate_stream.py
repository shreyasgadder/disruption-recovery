import json
import time
import os
from pathlib import Path
from datetime import datetime
import random

# --- Configuration ---
SOURCE_FILE = Path("data/raw/stream_logs_full.jsonl")
STREAM_DIR = Path("data/streaming/input")
SLEEP_INTERVAL = 2  # Seconds between batches

def clear_stream_dir():
    """Ensure the input directory is empty on start."""
    if STREAM_DIR.exists():
        for f in STREAM_DIR.glob("*.jsonl"):
            f.unlink()
    else:
        STREAM_DIR.mkdir(parents=True, exist_ok=True)

def simulate_stream():
    """Read the 5M row file and trickle it into the stream directory in simple chunks."""
    print("INFO: Starting stream simulation...")
    print(f"INFO: Source: {SOURCE_FILE}")
    print(f"INFO: Mock Stream: {STREAM_DIR}")
    
    if not SOURCE_FILE.exists():
        print(f"ERROR: {SOURCE_FILE} not found!")
        return

    clear_stream_dir()
    
    batch_count = 0
    current_batch = []
    
    try:
        with open(SOURCE_FILE, "r", encoding="utf-8") as f:
            for line in f:
                current_batch.append(line)
                
                # Randomize batch size between 2000 and 5000 as requested
                batch_threshold = random.randint(2000, 5000)
                
                if len(current_batch) >= batch_threshold:
                    batch_count += 1
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                    batch_file = STREAM_DIR / f"batch_{batch_count}_{timestamp}.jsonl"
                    
                    with open(batch_file, "w", encoding="utf-8") as out:
                        out.writelines(current_batch)
                    
                    print(f"SUCCESS: Sent batch {batch_count:,} ({len(current_batch):,} events)")
                    current_batch = []
                    time.sleep(SLEEP_INTERVAL)
                    
            # Send final partial batch
            if current_batch:
                batch_count += 1
                batch_file = STREAM_DIR / f"batch_{batch_count}_final.jsonl"
                with open(batch_file, "w", encoding="utf-8") as f_out:
                    f_out.writelines(current_batch)
                print(f"SUCCESS: Sent final partial batch {batch_count:,}")

    except KeyboardInterrupt:
        print("\nINFO: Simulation stopped by user.")
    except Exception as e:
        print(f"ERROR: Simulation failure: {e}")

if __name__ == "__main__":
    simulate_stream()

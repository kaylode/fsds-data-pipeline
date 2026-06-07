#!/usr/bin/env python3
"""
EHR Live Stream Simulator (Strategy A: Chronological Replay)

This script loads Electronic Health Record (EHR) event logs from a MEDS dataset,
globally sorts all events chronologically, and simulates a live stream replaying 
them into PostgreSQL. Debezium captures the WAL insertions via Change Data Capture (CDC)
and broadcasts them to Kafka.
"""

import os
import sys
import time
import argparse
import datetime
import collections
from tqdm import tqdm
import polars as pl
from loguru import logger
from dotenv import load_dotenv

# Load workspace environment variables
load_dotenv()


# Dynamic sys.path injection to resolve ehr-meds (Vocabulary)
script_dir = os.path.dirname(os.path.abspath(__file__))
fsds_root = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))
parent_source_dir = os.path.abspath(os.path.join(script_dir, "..", "..", "..", ".."))

sys.path.append(os.path.join(parent_source_dir, "ehrfm", "ehr-meds"))
sys.path.append(fsds_root)
import meds_reader
from ehrmeds import Vocabulary

# Import centralized database components
from core.database import get_db_connection, init_db_schema

# --- Command Line Arguments ---
parser = argparse.ArgumentParser(description="EHR Live Stream Simulator")
parser.add_argument("--dataset", type=str, default="eicu", help="Dataset name (eicu or mimiciv)")
parser.add_argument("--num_subjects", type=int, default=100, help="Number of subjects to load for simulation")
parser.add_argument("--speedup", type=float, default=3600.0, help="Replay speedup factor (e.g. 3600 means 1 hour of history replayed in 1 second)")
parser.add_argument("--db-host", type=str, default=os.getenv("POSTGRES_HOST", "localhost"))
parser.add_argument("--db-port", type=int, default=int(os.getenv("POSTGRES_PORT", 5432)))
parser.add_argument("--db-name", type=str, default=os.getenv("POSTGRES_DB", "testdb"))
parser.add_argument("--db-user", type=str, default=os.getenv("POSTGRES_USER", "postgres"))
parser.add_argument("--db-password", type=str, default=os.getenv("POSTGRES_PASSWORD", "postgres"))

class EventReplayItem:
    def __init__(self, subject_id, admission_id, timestamp, code_str, numeric_val, text_val):
        self.subject_id = subject_id
        self.admission_id = admission_id
        self.timestamp = timestamp
        self.code_str = code_str
        self.numeric_val = numeric_val
        self.text_val = text_val

def init_database(args):
    """Ensure target schema exists in PostgreSQL using centralized DDL schema."""
    # Propagate CLI parameter overrides back to the environment variables
    os.environ["POSTGRES_HOST"] = args.db_host
    os.environ["POSTGRES_PORT"] = str(args.db_port)
    os.environ["POSTGRES_DB"] = args.db_name
    os.environ["POSTGRES_USER"] = args.db_user
    os.environ["POSTGRES_PASSWORD"] = args.db_password
    
    conn = get_db_connection()
    init_db_schema(conn)
    return conn

def load_meds_events(args):
    """Loads subjects and flattens their events, sorting them globally by timestamp."""
    path = f"{os.getcwd()}/data/meds/{args.dataset}/reader"
    code_metadata = f"{os.getcwd()}/data/meds/{args.dataset}/transforms/metadata/codes.parquet"
    
    logger.info(f"Loading vocabulary from {code_metadata}...")
    vocab = Vocabulary.from_code_metadata(code_metadata)
    
    logger.info(f"Loading MEDS database from {path}...")
    database = meds_reader.SubjectDatabase(path)
    
    subject_ids = list(database)
    num_to_load = min(args.num_subjects, len(subject_ids))
    logger.info(f"Extracting events for {num_to_load} subjects out of {len(subject_ids)} total...")
    
    all_events = []
    
    for i in tqdm(range(num_to_load), desc="Extracting events"):
        subj_id = subject_ids[i]
        subject = database[subj_id]
        
        for event in subject.events:
            # Decode the event code to string
            code_str = vocab.itos[event.code]
            
            # Capture timestamps (meds-reader event.time is a datetime object)
            t = event.time
            
            all_events.append(
                EventReplayItem(
                    subject_id=int(subject.subject_id),
                    admission_id=int(event.admission_id) if event.admission_id is not None else None,
                    timestamp=t,
                    code_str=code_str,
                    numeric_val=float(event.numeric_value) if event.numeric_value is not None else None,
                    text_val=str(event.text_value) if event.text_value is not None else None
                )
            )
            
    logger.info(f"Total extracted events: {len(all_events)}")
    logger.info("Sorting all events chronologically...")
    all_events.sort(key=lambda x: x.timestamp)
    logger.info("Sorting completed.")
    return all_events


def run_simulation(args, conn, events):
    if not events:
        logger.warning("No events to simulate. Exiting.")
        return
        
    cursor = conn.cursor()
    
    start_hist_time = events[0].timestamp
    end_hist_time = events[-1].timestamp
    total_hist_duration = end_hist_time - start_hist_time
    
    logger.info(
        "\n============================================================\n"
        "🚀 EHR CHRONOLOGICAL REPLAY SIMULATOR READY\n"
        f"📅 Historical Start Time: {start_hist_time}\n"
        f"📅 Historical End Time:   {end_hist_time}\n"
        f"⏳ Total Simulated Span:   {total_hist_duration}\n"
        f"⚡ Replay Speedup Factor: {args.speedup}x\n"
        f"📝 Events Queue Length:   {len(events)}\n"
        "============================================================"
    )
    
    logger.info("Pre-registering subjects...")
    unique_subjects = set(e.subject_id for e in events)
    for s_id in unique_subjects:
        cursor.execute("INSERT INTO subjects (subject_id) VALUES (%s) ON CONFLICT DO NOTHING", (s_id,))
    conn.commit()
    logger.info(f"Registered {len(unique_subjects)} subjects.")
    
    input("Press ENTER to start the streaming simulation...")
    
    # Simulation baseline
    wall_start = time.time()
    sim_start = start_hist_time
    
    event_idx = 0
    total_inserted = 0
    
    logger.info("Streaming initialized. Press Ctrl+C to abort.")
    
    try:
        while event_idx < len(events):
            # Calculate how much historical time has elapsed based on wall-clock time
            wall_elapsed = time.time() - wall_start
            sim_elapsed_seconds = wall_elapsed * args.speedup
            current_sim_time = sim_start + datetime.timedelta(seconds=sim_elapsed_seconds)
            
            # Find and insert all events that occurred up to current_sim_time
            batch_inserts = []
            while event_idx < len(events) and events[event_idx].timestamp <= current_sim_time:
                ev = events[event_idx]
                batch_inserts.append(ev)
                event_idx += 1
                
            if batch_inserts:
                for ev in batch_inserts:
                    cursor.execute("""
                        INSERT INTO patient_events (subject_id, admission_id, historical_time, code, numeric_value, text_value)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (ev.subject_id, ev.admission_id, ev.timestamp, ev.code_str, ev.numeric_val, ev.text_val))
                conn.commit()
                total_inserted += len(batch_inserts)
                
                # Print status using logger
                last_ev = batch_inserts[-1]
                logger.info(
                    f"Simulated {len(batch_inserts)} events. "
                    f"Current Sim Time: {current_sim_time.strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"Last Inserted: Subj={last_ev.subject_id} Code='{last_ev.code_str}' | "
                    f"Total Ingested: {total_inserted}/{len(events)}"
                )
            
            # Dynamic Hybrid Gap-Skipping:
            # If the next event is far in the future (requiring > 1.0 second of real-world wait time),
            # dynamically adjust the wall-clock baseline to fast-forward the clock to 0.5 seconds before the event.
            if event_idx < len(events):
                next_event_time = events[event_idx].timestamp
                next_wall_needed = (next_event_time - sim_start).total_seconds() / args.speedup
                time_remaining = next_wall_needed - (time.time() - wall_start)
                if time_remaining > 1.0:
                    gap_days = (next_event_time - current_sim_time).total_seconds() / 86400.0
                    logger.info(
                        f"⏳ Long inactive span of {gap_days:.1f} days detected. "
                        f"Fast-forwarding simulation clock to {next_event_time.strftime('%Y-%m-%d %H:%M:%S')}..."
                    )
                    # Adjust wall_start so that only 0.5 seconds of real-world delay remains
                    wall_start = time.time() - next_wall_needed + 0.5

            # Sleep slightly to prevent CPU spin
            time.sleep(0.1)

            
    except KeyboardInterrupt:
        logger.warning("Simulation suspended by user.")
    
    logger.info("Simulation completed.")
    logger.info(f"Total replayed events: {total_inserted}")

def main():
    # Configure logger to write to stdout and flush immediately
    logger.remove()
    logger.add(sys.stdout, level="INFO", colorize=True, enqueue=True)

    args = parser.parse_args()
    
    # 1. Initialize Schema in Postgres
    conn = init_database(args)
        
    # 2. Load and Sort MEDS events
    events = load_meds_events(args)
        
    # 3. Stream
    run_simulation(args, conn, events)

if __name__ == "__main__":
    main()

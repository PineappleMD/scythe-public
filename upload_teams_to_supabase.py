import os
import json
from datetime import datetime
from supabase import create_client, Client

# === 🔧 CONFIG: Replace the URL & KEY ===
SUPABASE_URL = "xxx"
SUPABASE_KEY = "xxx"
DATA_FOLDER = "./data"  # Path to folder with .json files

# === Supabase Setup ===
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Track logs ===
skipped_files = []
empty_files = []
skipped_teams = []
duplicate_teams = []
total_uploaded = 0

def process_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            raw = json.load(f)
    except Exception as e:
        skipped_files.append((filepath, str(e)))
        return []

    # Handle object or array structure
    if isinstance(raw, list):
        teams = raw
    elif isinstance(raw, dict) and 'team_ranking_data' in raw:
        teams = raw['team_ranking_data']
    else:
        empty_files.append(filepath)
        return []

    records = []
    for team in teams:
        try:
            record = {
                'id': team['id'],
                'team_name': team['team_name'],
                'total_points': team['total_points'],
                'age': team['age'],
                'gender': team['gender'],
                'national_rank': team.get('national_rank'),
            }
            records.append(record)
        except KeyError as e:
            skipped_teams.append((team.get('id', 'unknown'), str(e)))
            continue

    return records

def deduplicate_records(records):
    """Deduplicate records based on ID, keeping the last occurrence."""
    unique_records = {}
    duplicates = set()
    
    for record in records:
        record_id = record['id']
        if record_id in unique_records:
            duplicates.add(record_id)
        unique_records[record_id] = record
    
    for duplicate_id in duplicates:
        duplicate_teams.append(duplicate_id)
    
    return list(unique_records.values())

def main():
    global total_uploaded

    print("🚀 Starting upload...")
    all_records = []

    for filename in os.listdir(DATA_FOLDER):
        if filename.endswith('.json'):
            path = os.path.join(DATA_FOLDER, filename)
            print(f"📄 Processing: {filename}")
            records = process_file(path)
            all_records.extend(records)

    if not all_records:
        print(⚠️ No valid team data found.")
        return

    print(f"📊 Found {len(all_records)} records total")
    
    # Deduplicate records to avoid the "cannot affect row a second time" error
    deduplicated_records = deduplicate_records(all_records)
    duplicate_count = len(all_records) - len(deduplicated_records)
    
    if duplicate_count > 0:
        print(f"⚠️ Found {duplicate_count} duplicate team IDs (will use latest occurrence)")
    
    print(f"📤 Uploading {len(deduplicated_records)} unique records to Supabase...")
    
    try:
        result = supabase.table("team_ranking_data").upsert(deduplicated_records, on_conflict="id").execute()
        total_uploaded = len(deduplicated_records)
        print("✅ Upload successful!")
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
    
    # Write summary log
    write_log()

def write_log():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = f"upload_log_{timestamp}.txt"

    with open(log_path, 'w', encoding='utf-8') as log:
        log.write(f"Upload Summary ({timestamp})\n")
        log.write("=" * 40 + "\n")
        log.write(f"Total teams uploaded: {total_uploaded}\n\n")

        log.write("Skipped Files:\n")
        for f, reason in skipped_files:
            log.write(f"  {f} - {reason}\n")
        if not skipped_files:
            log.write("  None\n")

        log.write("\nEmpty or unrecognized files:\n")
        for f in empty_files:
            log.write(f"  {f}\n")
        if not empty_files:
            log.write("  None\n")

        log.write("\nTeams skipped due to missing fields:\n")
        for team_id, error in skipped_teams:
            log.write(f"  ID {team_id} - {error}\n")
        if not skipped_teams:
            log.write("  None\n")
            
        log.write("\nDuplicate team IDs (last occurrence used):\n")
        for team_id in duplicate_teams:
            log.write(f"  ID {team_id}\n")
        if not duplicate_teams:
            log.write("  None\n")

    print(f"\n📝 Log saved to {log_path}")

if __name__ == "__main__":
    main()
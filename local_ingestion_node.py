"""
Distributed Audio ETL Pipeline - Local Ingestion Node
Architecture: yt-dlp + Dropbox API + Python
Objective: Automated, fault-tolerant audio extraction that bypasses rate limits and IP blocks, 
syncing massive files directly to intermediary cloud storage.
"""

import sys
import re
import datetime
import os
import time
import glob
import shutil
import yt_dlp
import dropbox
import requests

# Enable ANSI escape sequences for terminal UI
os.system("")

# --- ENVIRONMENT VARIABLES (DO NOT HARDCODE KEYS IN PRODUCTION) ---
APP_KEY = os.getenv("DROPBOX_APP_KEY", "insert_app_key")
APP_SECRET = os.getenv("DROPBOX_APP_SECRET", "insert_app_secret")
REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN", "insert_token")
CLOUD_INGEST_FOLDER = "/Raw_Audio_Ingest"

# --- SYSTEM STATE ---
stats = {'success': 0, 'skipped': 0, 'failed': 0}
file_counts = {}
archive_set = set()
current_date_str = datetime.datetime.now().strftime("%d%m%y")
config = {'min_length_mins': 6, 'min_length_sec': 360}

print("\n[SYSTEM] Initializing Ingestion Node...")

try:
    dbx = dropbox.Dropbox(app_key=APP_KEY, app_secret=APP_SECRET, oauth2_refresh_token=REFRESH_TOKEN)
    account = dbx.users_get_current_account()
    print("[SUCCESS] Cloud API Authenticated.")
except Exception as e:
    sys.exit(f"[FATAL] Cloud Connection Failed: {e}")

# --- HELPER FUNCTIONS ---
def get_bar(percent_float, length=25):
    filled = int((percent_float / 100.0) * length)
    return '█' * filled + '░' * (length - min(length, filled))

def print_inplace(msg):
    term_width = shutil.get_terminal_size().columns
    sys.stdout.write(f"\r\033[K{msg[:max(20, term_width - 2)]}")
    sys.stdout.flush()

def upload_to_cloud(local_file, cloud_dest_path):
    """Robust chunked uploader for massive files bypassing memory constraints."""
    CHUNK_SIZE = 4 * 1024 * 1024 # 4MB chunks
    file_size = os.path.getsize(local_file)
    filename = os.path.basename(local_file)
    
    with open(local_file, "rb") as f:
        if file_size <= CHUNK_SIZE:
            dbx.files_upload(f.read(), cloud_dest_path, mode=dropbox.files.WriteMode.overwrite)
            print_inplace(f"   [UPLOAD] {filename}...[{get_bar(100, 20)}] 100.0%\n")
        else:
            session_start = dbx.files_upload_session_start(f.read(CHUNK_SIZE))
            cursor = dropbox.files.UploadSessionCursor(session_id=session_start.session_id, offset=f.tell())
            commit = dropbox.files.CommitInfo(path=cloud_dest_path, mode=dropbox.files.WriteMode.overwrite)
            
            while f.tell() < file_size:
                pct = (f.tell() / file_size) * 100
                print_inplace(f"[UPLOAD] {filename}...[{get_bar(pct, 20)}] {pct:.1f}%")
                if (file_size - f.tell()) <= CHUNK_SIZE:
                    dbx.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
                else:
                    dbx.files_upload_session_append_v2(f.read(CHUNK_SIZE), cursor)
                    cursor.offset = f.tell()
            
            print_inplace(f"   [UPLOAD] {filename}...[{get_bar(100, 20)}] 100.0%\n")

def clean_ansi(text):
    ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', text)

# --- 1. CLOUD STATE SYNC ---
print("\n[SYNC] Fetching distributed state tracking archive...")
try:
    dbx.files_download_to_file('ingest_archive.txt', f"{CLOUD_INGEST_FOLDER}/ingest_archive.txt")
except dropbox.exceptions.ApiError:
    open('ingest_archive.txt', 'w').close()

with open('ingest_archive.txt', 'r', encoding='utf-8') as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) >= 2: archive_set.add(parts[1])

# --- 2. SELF-HEALING AUTO-SWEEPER ---
# Recovers files interrupted by network crashes during previous sessions
os.makedirs("downloads", exist_ok=True)
stranded_files = glob.glob("downloads/**/*.m4a", recursive=True)

if stranded_files:
    print(f"\n[SWEEP] Recovering {len(stranded_files)} stranded files from previous interrupted session...")
    for local_file in stranded_files:
        filename = os.path.basename(local_file)
        try:
            upload_to_cloud(local_file, f"{CLOUD_INGEST_FOLDER}/{filename}")
            os.remove(local_file)
        except Exception as e:
            print(f"   [ERROR] Recovery failed for {filename}: {e}")

# --- 3. TARGET INGESTION ---
print("\n[INPUT] Enter target source URLs (Press Enter on empty line to execute):")
lines =[]
while True:
    line = input()
    if not line: break
    lines.append(line)

urls = list(set(re.findall(r'(https?://[^\s]+)', " ".join(lines))))
if not urls: sys.exit("[SYSTEM] No targets provided. Terminating.")

# --- 4. PIPELINE EXECUTION ENGINE ---
class PipelineLogger:
    def debug(self, msg): self.process(msg)
    def info(self, msg): self.process(msg)
    def warning(self, msg): pass
    def error(self, msg): stats['failed'] += 1
    def process(self, msg):
        msg_clean = clean_ansi(msg).lower()
        if 'already been downloaded' in msg_clean or 'too short' in msg_clean: stats['skipped'] += 1

def progress_hook(d):
    if d['status'] == 'downloading':
        p = float(clean_ansi(d.get('_percent_str', '0.0%')).replace('%', '') or 0)
        print_inplace(f"  -> [{get_bar(p)}] {p}% | Spd: {clean_ansi(d.get('_speed_str', 'N/A'))}")

def post_process_hook(d):
    """Executes cloud transfer and local cleanup post-download."""
    if d['status'] == 'finished' and d.get('postprocessor') in['Metadata', 'FFmpegMetadata']:
        info = d.get('info_dict', {})
        local_filepath = info.get('filepath') or d.get('filepath')
        if not local_filepath or not os.path.exists(local_filepath): return
        
        filename = os.path.basename(local_filepath)
        try:
            upload_to_cloud(local_filepath, f"{CLOUD_INGEST_FOLDER}/{filename}")
            
            # Atomic state update
            if info.get('id'):
                with open('ingest_archive.txt', 'a') as f: f.write(f"source {info.get('id')}\n")
            with open('ingest_archive.txt', 'rb') as f:
                dbx.files_upload(f.read(), f"{CLOUD_INGEST_FOLDER}/ingest_archive.txt", mode=dropbox.files.WriteMode.overwrite)
            
            # Retrying file deletion bypasses Windows file-lock lag
            for _ in range(5):
                try:
                    os.remove(local_filepath)
                    break
                except Exception: time.sleep(0.5)
            stats['success'] += 1
        except Exception:
            stats['failed'] += 1

def inject_metadata(info):
    """Formats file taxonomy and enforces business logic parameters."""
    dur = info.get('duration')
    if dur and dur < config['min_length_sec']: return "Video too short"
    handle = (info.get('uploader_id') or 'Unknown').replace('@', '')
    mins = max(1, round(dur / 60) if dur else 0)
    
    key = f"{handle}-{current_date_str}-{mins}"
    count = file_counts.get(key, 0)
    info['custom_handle'] = handle
    info['custom_date'] = current_date_str
    info['custom_mins'] = f"{mins}mins{str(count) if count > 0 else ''}"
    if info.get('id') not in archive_set: file_counts[key] = count + 1
    return None 

ydl_opts = {
    'cookiefile': 'cookies.txt', # Bypasses rate-limiting
    'format': 'bestaudio[ext=m4a]/bestaudio/best', 
    'outtmpl': 'downloads/ClientAudio-%(custom_handle)s-%(custom_date)s-%(custom_mins)s.%(ext)s',
    'match_filter': inject_metadata,
    'ignoreerrors': True,
    'download_archive': 'ingest_archive.txt',
    'sleep_interval_requests': 1, # Anti-bot mitigation
    'sleep_interval': 3,          # Anti-bot mitigation
    'logger': PipelineLogger(),
    'progress_hooks':[progress_hook],
    'postprocessor_hooks': [post_process_hook],
    'postprocessors':[{'key': 'FFmpegExtractAudio', 'preferredcodec': 'm4a'}, {'key': 'FFmpegMetadata', 'add_metadata': True}],
}

print("\n[EXECUTE] Starting extraction sequence...")
with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    ydl.download(urls)

print(f"\n[SYSTEM] Run Terminated. Success: {stats['success']} | Skipped: {stats['skipped']} | Failed: {stats['failed']}")

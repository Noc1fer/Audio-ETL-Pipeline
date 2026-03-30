"""
Distributed Audio ETL Pipeline - Local Ingestion Node
Architecture: yt-dlp + Dropbox API + FFmpeg
Objective: Automated extraction of unstructured audio, bypassing rate limits, with self-healing cloud sync.
"""

import sys
import re
import datetime
import os
import time
import glob
import shutil

# 1. DEPENDENCY CHECK
try:
    import yt_dlp
    import dropbox
    import requests
except ImportError:
    sys.exit("[CRITICAL] Missing modules. Run: pip install yt-dlp dropbox requests")

# =======================================================
# ENVIRONMENT VARIABLES (CLOUD AUTH)
# =======================================================
APP_KEY = os.getenv("DROPBOX_APP_KEY", "insert_app_key")
APP_SECRET = os.getenv("DROPBOX_APP_SECRET", "insert_app_secret")
REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN", "insert_token")
DROPBOX_FOLDER = "/Raw_Audio_Ingest"
# =======================================================

try:
    stats = {'success': 0, 'skipped': 0, 'failed': 0}
    file_counts = {}
    archive_set = set()
    current_date_str = datetime.datetime.now().strftime("%d%m%y")
    
    # PRODUCT PARAMETERS: Exclude short-form content
    config = {'min_length_mins': 6, 'min_length_sec': 360}

    print("\n[SYSTEM] Initializing Cloud API Connection...")
    try:
        dbx = dropbox.Dropbox(app_key=APP_KEY, app_secret=APP_SECRET, oauth2_refresh_token=REFRESH_TOKEN)
        account = dbx.users_get_current_account()
        print("[SUCCESS] Secure Cloud Tunnel Established.")
    except Exception as e:
        sys.exit(f"[FATAL] Cloud Connection Failed: {e}")

    # --- HELPER FUNCTIONS ---
    def get_bar(percent_float, length=25):
        filled = int((percent_float / 100.0) * length)
        filled = max(0, min(length, filled)) 
        return '█' * filled + '░' * (length - filled)

    def print_inplace(msg):
        term_width = shutil.get_terminal_size().columns
        max_width = max(20, term_width - 2) 
        safe_msg = msg[:max_width]
        sys.stdout.write(f"\r\033[K{safe_msg}")
        sys.stdout.flush()

    def upload_to_dropbox(local_file, dropbox_dest_path):
        """Robust uploader: Chunks massive files to bypass API timeouts."""
        CHUNK_SIZE = 4 * 1024 * 1024 # 4MB dynamic chunks
        file_size = os.path.getsize(local_file)
        filename = os.path.basename(local_file)
        
        with open(local_file, "rb") as f:
            if file_size <= CHUNK_SIZE:
                dbx.files_upload(f.read(), dropbox_dest_path, mode=dropbox.files.WriteMode.overwrite)
                print_inplace(f"   [UPLOAD] {filename}...[{get_bar(100, 20)}] 100.0%")
                sys.stdout.write("\n")
                sys.stdout.flush()
            else:
                upload_session_start_result = dbx.files_upload_session_start(f.read(CHUNK_SIZE))
                cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id, offset=f.tell())
                commit = dropbox.files.CommitInfo(path=dropbox_dest_path, mode=dropbox.files.WriteMode.overwrite)
                
                while f.tell() < file_size:
                    pct = (f.tell() / file_size) * 100
                    print_inplace(f"[UPLOAD] {filename}...[{get_bar(pct, 20)}] {pct:.1f}%")
                    if (file_size - f.tell()) <= CHUNK_SIZE:
                        dbx.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
                    else:
                        dbx.files_upload_session_append_v2(f.read(CHUNK_SIZE), cursor)
                        cursor.offset = f.tell()
                
                print_inplace(f"   [UPLOAD] {filename}...[{get_bar(100, 20)}] 100.0%")
                sys.stdout.write("\n")
                sys.stdout.flush()

    def clean_ansi(text):
        ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
        return ansi_escape.sub('', text)

    # --- 1. STATE MANAGEMENT (FETCH CLOUD ARCHIVE) ---
    print("\n[SYNC] Fetching persistent memory archive from Cloud...")
    try:
        dbx.files_download_to_file('downloaded_tracks.txt', f"{DROPBOX_FOLDER}/downloaded_tracks.txt")
    except dropbox.exceptions.ApiError:
        open('downloaded_tracks.txt', 'w').close()

    with open('downloaded_tracks.txt', 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2: archive_set.add(parts[1])

    # --- 2. CONFLICT RESOLUTION (SCAN EXISTING CLOUD FILES) ---
    try:
        res = dbx.files_list_folder(DROPBOX_FOLDER)
        entries = res.entries
        while res.has_more:
            res = dbx.files_list_folder_continue(res.cursor)
            entries.extend(res.entries)
            
        pattern = re.compile(r'Ingest-(.+?)-(\d{6})-(\d+)mins(\d*)\.(mp3|m4a|webm)$')
        for entry in entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                match = pattern.match(entry.name)
                if match:
                    key = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
                    count = 0 if match.group(4) == "" else int(match.group(4))
                    file_counts[key] = max(file_counts.get(key, 0), count + 1)
    except Exception as e:
        pass

    # --- 3. TARGET INGESTION ---
    print("Paste target URLs (Press Enter on empty line to execute):")
    lines =[]
    while True:
        line = input()
        if line == "": break
        lines.append(line)
    raw_text = " ".join(lines).replace(',', ' ').replace(';', ' ')
    urls = list(set([url.rstrip('"\'()[]{}<>') for url in re.findall(r'(https?://[^\s]+)', raw_text)]))

    if not urls: sys.exit("[INFO] No targets specified. Terminating.")
    os.makedirs("downloads", exist_ok=True)

    # --- 4. SELF-HEALING SWEEPER (RECOVER INTERRUPTED TRANSFERS) ---
    stranded_files = glob.glob("downloads/**/*.m4a", recursive=True) + glob.glob("downloads/**/*.mp3", recursive=True)
    if stranded_files:
        print(f"\n[SWEEP] Recovering {len(stranded_files)} stranded files from previous interrupted session...")
        for local_file in stranded_files:
            try:
                upload_to_dropbox(local_file, f"{DROPBOX_FOLDER}/{os.path.basename(local_file)}")
                os.remove(local_file)
            except Exception as e:
                pass
        try:
            with open('downloaded_tracks.txt', 'rb') as f:
                dbx.files_upload(f.read(), f"{DROPBOX_FOLDER}/downloaded_tracks.txt", mode=dropbox.files.WriteMode.overwrite)
        except Exception: pass

    # --- 5. DATA PIPELINE LOGIC ---
    class PipelineLogger:
        def debug(self, msg): self.process(msg)
        def info(self, msg): self.process(msg)
        def warning(self, msg): pass
        def error(self, msg):
            if 'HTTP Error 404' in msg: return
            stats['failed'] += 1
        def process(self, msg): pass

    def pp_hook(d):
        if d['status'] == 'finished' and d.get('postprocessor') in ['Metadata', 'FFmpegMetadata']:
            info = d.get('info_dict', {})
            local_filepath = info.get('filepath') or d.get('filepath')
            
            if local_filepath and not local_filepath.endswith('.m4a'):
                base = os.path.splitext(local_filepath)[0]
                if os.path.exists(base + '.m4a'): local_filepath = base + '.m4a'
            
            if not local_filepath or not os.path.exists(local_filepath): return
            
            try:
                upload_to_dropbox(local_filepath, f"{DROPBOX_FOLDER}/{os.path.basename(local_filepath)}")
                
                video_id = info.get('id')
                if video_id:
                    with open('downloaded_tracks.txt', 'a', encoding='utf-8') as f: f.write(f"youtube {video_id}\n")
                
                with open('downloaded_tracks.txt', 'rb') as f:
                    dbx.files_upload(f.read(), f"{DROPBOX_FOLDER}/downloaded_tracks.txt", mode=dropbox.files.WriteMode.overwrite)
                
                for _ in range(5):
                    try:
                        os.remove(local_filepath)
                        break
                    except Exception: time.sleep(0.5)
                stats['success'] += 1
            except Exception as e:
                stats['failed'] += 1

    def inject_custom_metadata(info):
        url = info.get('original_url', '') or info.get('webpage_url', '')
        if '/shorts/' in url: return "SilentSkip-Short"
        dur = info.get('duration')
        if dur is not None and dur < config['min_length_sec']: return "Video too short" 
        handle = info.get('uploader_id') or info.get('channel', 'Unknown').replace(' ', '')
        if handle.startswith('@'): handle = handle[1:]
        mins = round(dur / 60) if dur else 0
        if mins == 0 and dur: mins = 1
        key = f"{handle}-{current_date_str}-{mins}"
        count = file_counts.get(key, 0)
        suffix = "" if count == 0 else str(count)
        if info.get('id') not in archive_set: file_counts[key] = count + 1
        info['custom_handle'] = handle
        info['custom_date'] = current_date_str
        info['custom_mins'] = f"{mins}mins{suffix}"
        return None 

    processed_urls =[]
    base_pattern = re.compile(r'^(https?://(?:www\.)?youtube\.com/(?:@[\w.-]+|c/[\w.-]+|channel/[\w.-]+|user/[\w.-]+))/?(\?.*)?$', re.IGNORECASE)
    for url in urls:
        match = base_pattern.match(url.strip())
        if match: processed_urls.extend([match.group(1) + '/videos', match.group(1) + '/streams'])
        else: processed_urls.append(url)

    # RATE LIMIT & ANTI-BAN PARAMETERS
    ydl_opts = {
        'cookiefile': 'www.youtube.com_cookies.txt', # Bypasses age-gates and regional blocks
        'format': 'bestaudio[ext=m4a]/bestaudio/best', 
        'outtmpl': 'downloads/Ingest-%(custom_handle)s-%(custom_date)s-%(custom_mins)s.%(ext)s',
        'match_filter': inject_custom_metadata,
        'ignoreerrors': True,
        'download_archive': 'downloaded_tracks.txt', 
        'sleep_interval_requests': 1, # Rate limit evasion
        'sleep_interval': 3,          # Rate limit evasion
        'logger': PipelineLogger(),
        'postprocessor_hooks':[pp_hook],
        'postprocessors':[{'key': 'FFmpegExtractAudio', 'preferredcodec': 'm4a'}, {'key': 'FFmpegMetadata', 'add_metadata': True}],
    }

    print("\n[START] Executing extraction pipeline...")
    with yt_dlp.YoutubeDL(ydl_opts) as ydl: ydl.download(processed_urls)
    print(f"\n[SYSTEM] Pipeline execution terminated. Success: {stats['success']}, Failed: {stats['failed']}")

except Exception as e:
    sys.exit(f"\n[FATAL] {e}")

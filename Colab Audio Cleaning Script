"""
Distributed Audio ETL Pipeline - Cloud Processing Node (Google Colab)
Architecture: PyTorch + Pyannote VAD + FFmpeg + Rclone
Objective: Automated extraction, Voice Activity Detection (VAD), scrubbing, and chunking of raw audio.
"""

# ==========================================
# CELL 1: ENVIRONMENT DEPENDENCIES
# ==========================================
# Run this in Colab cell 1, then restart runtime
# !pip install -q "numpy<2.0.0" pyannote.audio
# !apt-get install -q -y rclone ffmpeg

# ==========================================
# CELL 2: ETL PIPELINE EXECUTION
# ==========================================
import os
import sys
import subprocess
import glob
import torch
import torchaudio

# Torchaudio backend patch for Colab compatibility
if not hasattr(torchaudio, 'AudioMetaData'):
    torchaudio.AudioMetaData = type('AudioMetaData', (), {})
if not hasattr(torchaudio, 'list_audio_backends'):
    torchaudio.list_audio_backends = lambda: ['soundfile']

from pyannote.audio import Model
from pyannote.audio.pipelines import VoiceActivityDetection

# --- ENVIRONMENT VARIABLES (DO NOT HARDCODE KEYS IN PRODUCTION) ---
APP_KEY = os.getenv("DROPBOX_APP_KEY", "insert_app_key")
APP_SECRET = os.getenv("DROPBOX_APP_SECRET", "insert_app_secret")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN", "insert_token")
HF_TOKEN = os.getenv("HF_AUTH_TOKEN", "insert_hf_token")

# --- ARCHITECTURE PATHS ---
CLOUD_SOURCE_DIR = "dropbox:Raw_Audio_Ingest/"
CLOUD_DEST_DIR = "dropbox:AI_Processed_Dataset/"
CLOUD_VAULT_DIR = "dropbox:Original_Vault/"
LOCAL_RAW_DIR = "/content/raw_audio"
LOCAL_CHUNK_DIR = "/content/processed_chunks"

# --- 1. CLOUD STORAGE MOUNTING (RCLONE) ---
rclone_conf = f"""[dropbox]
type = dropbox
client_id = {APP_KEY}
client_secret = {APP_SECRET}
token = {{"access_token":"dummy","token_type":"bearer","refresh_token":"{DROPBOX_REFRESH_TOKEN}","expiry":"2020-01-01T00:00:00Z"}}
"""
os.makedirs("/root/.config/rclone", exist_ok=True)
with open("/root/.config/rclone/rclone.conf", "w") as f:
    f.write(rclone_conf)

print("\n[SYSTEM] Establishing secure tunnel to cloud storage...")
test = subprocess.run(['rclone', 'lsf', CLOUD_SOURCE_DIR], capture_output=True, text=True)
if test.returncode != 0:
    sys.exit("[FATAL] Cloud storage bridge failed.")
print("[SUCCESS] Cloud storage mounted.")

# --- 2. AI MODEL INITIALIZATION (VAD) ---
print("\n[SYSTEM] Provisioning Pyannote VAD on T4 GPU...")
try:
    model = Model.from_pretrained("pyannote/segmentation", token=HF_TOKEN)
    pipeline = VoiceActivityDetection(segmentation=model)
    
    # Tuned hyperparameters for distinct human speech isolation
    pipeline.instantiate({
        "onset": 0.8104268538848918,
        "offset": 0.4806866463041527,
        "min_duration_on": 0.05537587440407595,
        "min_duration_off": 0.09791355693027545
    })
    pipeline.to(torch.device("cuda"))
    print("[SUCCESS] GPU Model active.")
except Exception as e:
    sys.exit(f"[FATAL] GPU Initialization failed: {e}")

# --- 3. STATE MANAGEMENT ---
os.makedirs(LOCAL_RAW_DIR, exist_ok=True)
os.makedirs(LOCAL_CHUNK_DIR, exist_ok=True)
processed_set = set()

subprocess.run(['rclone', 'copy', f'{CLOUD_VAULT_DIR}ai_processed_log.txt', '/content/'])
if os.path.exists('/content/ai_processed_log.txt'):
    with open('/content/ai_processed_log.txt', 'r') as f:
        for line in f: processed_set.add(line.strip())

# --- HELPER FUNCTIONS ---
def get_duration(file_path):
    result = subprocess.run(['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path], capture_output=True, text=True)
    try: return float(result.stdout.strip())
    except: return 0.0

def get_suffix(index):
    suffix = ""
    while True:
        suffix = chr(index % 26 + 97) + suffix
        index = index // 26 - 1
        if index < 0: break
    return suffix

# --- 4. CORE PIPELINE EXECUTION ---
raw_files =[f for f in test.stdout.splitlines() if f.endswith(('.m4a', '.mp3', '.webm', '.wav'))]

for filename in raw_files:
    if filename in processed_set: continue

    print(f"\n[PROCESS] Ingesting: {filename}")
    subprocess.run(['rclone', 'copy', f'{CLOUD_SOURCE_DIR}{filename}', LOCAL_RAW_DIR])
    local_file = f"{LOCAL_RAW_DIR}/{filename}"

    try:
        dur = get_duration(local_file)
        if dur == 0: raise Exception("Invalid media format.")

        is_short_file = (dur < 1800) # 30 min threshold

        # STEP A: Audio Pre-Processing & Trimming
        if is_short_file:
            subprocess.run(['ffmpeg', '-nostdin', '-y', '-i', local_file, '-ar', '16000', '-ac', '1', '-loglevel', 'error', 'temp.wav'])
        else:
            trim_dur = dur - 180
            subprocess.run(['ffmpeg', '-nostdin', '-y', '-i', local_file, '-ss', '90', '-t', str(trim_dur), '-ar', '16000', '-ac', '1', '-loglevel', 'error', 'temp.wav'])

        # STEP B: Neural Voice Activity Detection (VAD)
        overlap_annotation = pipeline("temp.wav")
        speech_regions = overlap_annotation.get_timeline().support()

        merged_segments =[]
        for segment in speech_regions:
            if not merged_segments:
                merged_segments.append([segment.start, segment.end])
            else:
                last_start, last_end = merged_segments[-1]
                gap = segment.start - last_end
                if gap <= 20.0:
                    merged_segments[-1][1] = segment.end
                else:
                    merged_segments.append([segment.start, segment.end])

        if not merged_segments: raise Exception("Zero human speech detected.")

        # STEP C: FFmpeg Splicing (Removing non-speech)
        with open('concat.txt', 'w') as f:
            for seg in merged_segments:
                f.write(f"file 'temp.wav'\n")
                f.write(f"inpoint {seg[0]}\n")
                f.write(f"outpoint {seg[1]}\n")

        subprocess.run(['ffmpeg', '-nostdin', '-y', '-f', 'concat', '-safe', '0', '-i', 'concat.txt', '-c:a', 'aac', '-b:a', '64k', '-ac', '1', '-loglevel', 'error', 'scrubbed.m4a'])

        # STEP D: Dynamic Chunking for Annotation UI
        scrubbed_dur = get_duration('scrubbed.m4a')
        chunks_data =[]

        if is_short_file or scrubbed_dur < 900:
            chunks_data.append((0, scrubbed_dur))
        else:
            num_chunks = max(1, round(scrubbed_dur / 1200.0))
            chunk_len = scrubbed_dur / num_chunks
            for i in range(num_chunks):
                chunks_data.append((i * chunk_len, chunk_len))

        base_name = os.path.splitext(filename)[0]
        for f in glob.glob(f'{LOCAL_CHUNK_DIR}/*'): os.remove(f)

        for idx, (start_t, duration_t) in enumerate(chunks_data):
            suffix = get_suffix(idx)
            chunk_name = f"{base_name}-{suffix}.m4a"
            chunk_path = f"{LOCAL_CHUNK_DIR}/{chunk_name}"
            subprocess.run(['ffmpeg', '-nostdin', '-y', '-i', 'scrubbed.m4a', '-ss', str(start_t), '-t', str(duration_t), '-c', 'copy', '-loglevel', 'error', chunk_path])

        # STEP E: Pipeline Output & State Logging
        subprocess.run(['rclone', 'move', LOCAL_CHUNK_DIR, CLOUD_DEST_DIR])
        subprocess.run(['rclone', 'move', f'{CLOUD_SOURCE_DIR}{filename}', CLOUD_VAULT_DIR])

        with open('/content/ai_processed_log.txt', 'a') as f: f.write(filename + '\n')
        subprocess.run(['rclone', 'copy', '/content/ai_processed_log.txt', CLOUD_VAULT_DIR])

    except Exception as e:
        print(f"[ERROR] Pipeline failure on {filename}: {e}")
    finally:
        for temp_file in[local_file, 'temp.wav', 'concat.txt', 'scrubbed.m4a']:
            if os.path.exists(temp_file): os.remove(temp_file)

print("\n[SYSTEM] ETL Pipeline execution completed.")

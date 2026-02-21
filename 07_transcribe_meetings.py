"""
Step 3: Get Video URLs + Transcribe with Whisper
================================================
For each relevant meeting:
  1. Fetches the Mediasite presentation /Info page to get the video URL
  2. Downloads audio using yt-dlp
  3. Transcribes with OpenAI Whisper
  4. Saves transcript to text file

Install deps:
    pip install yt-dlp openai-whisper requests
    # Note: whisper also needs ffmpeg installed
    # Mac:   brew install ffmpeg
    # Linux: sudo apt install ffmpeg
    # Win:   https://ffmpeg.org/download.html
"""

import json
import re
import time
import subprocess
import whisper
import requests
from pathlib import Path

INPUT_JSON   = Path("all_meetings.json")
AUDIO_DIR    = Path("audio")
TRANSCRIPT_DIR = Path("transcripts")

AUDIO_DIR.mkdir(exist_ok=True)
TRANSCRIPT_DIR.mkdir(exist_ok=True)

# Whisper model size:
#   "tiny"   — fastest, least accurate  (~1 min/hr of audio on CPU)
#   "base"   — good balance             (~3 min/hr)
#   "small"  — better accuracy          (~8 min/hr)
#   "medium" — best for city meetings   (~20 min/hr)  ← recommended
WHISPER_MODEL = "medium"


def get_video_url_from_presentation(presentation_url):
    """
    The /Info page is just metadata. yt-dlp needs the actual player page.
    Strip /Info from the end to get the downloadable player URL.
    /Presentation/XXXX/Info  ->  /Presentation/XXXX
    """
    return presentation_url.replace("/Info", "").rstrip("/")


def download_audio(video_url, output_path):
    """Download just the audio track from a video URL using yt-dlp."""
    try:
        result = subprocess.run([
            "yt-dlp",
            "--extract-audio",
            "--audio-format", "mp3",
            "--audio-quality", "5",   # Mid quality — enough for speech
            "--no-warnings",
            "-o", str(output_path),
            video_url
        ], capture_output=True, text=True, timeout=600)  # 10 min timeout

        if result.returncode == 0:
            return True
        else:
            print(f"    [!] yt-dlp error: {result.stderr[:200]}")
            return False
    except subprocess.TimeoutExpired:
        print("    [!] Download timed out")
        return False
    except Exception as e:
        print(f"    [!] Download error: {e}")
        return False


def transcribe_audio(audio_path, model):
    """Run Whisper transcription on an audio file."""
    print(f"    Transcribing {audio_path.name}...")
    try:
        result = model.transcribe(str(audio_path), language="en")
        return result["text"]
    except Exception as e:
        print(f"    [!] Whisper error: {e}")
        return None


def safe_filename(title):
    """Convert a meeting title to a safe filename."""
    safe = re.sub(r'[^\w\s-]', '', title)
    safe = re.sub(r'\s+', '_', safe.strip())
    return safe[:80]  # Truncate long titles


def run_transcription_pipeline():
    if not INPUT_JSON.exists():
        print("[!] Run previous steps first")
        return

    meetings = json.loads(INPUT_JSON.read_text())
    relevant = [m for m in meetings if m.get("relevant")]

    if not relevant:
        print("[!] No relevant meetings found. Run 06_filter_with_claude.py first.")
        return

    print(f"Loading Whisper model ({WHISPER_MODEL})...")
    model = whisper.load_model(WHISPER_MODEL)
    print(f"✓ Whisper model loaded")
    print(f"\nProcessing {len(relevant)} relevant meetings...\n")

    for i, meeting in enumerate(relevant):
        title    = meeting["title"]
        date     = meeting.get("date", "unknown")
        pres_url = meeting["presentation_url"]
        fname    = safe_filename(f"{date}_{title}")

        print(f"[{i+1}/{len(relevant)}] {title}")
        print(f"  Date: {date}")

        transcript_path = TRANSCRIPT_DIR / f"{fname}.txt"

        # Skip if already transcribed
        if transcript_path.exists():
            print(f"  [skip] Already transcribed")
            meeting["transcript_path"] = str(transcript_path)
            continue

        # Step 1: Get video URL
        print(f"  Getting video URL...")
        video_url = get_video_url_from_presentation(pres_url)
        if not video_url:
            print(f"  [!] Could not get video URL, skipping")
            meeting["transcript_path"] = None
            continue

        meeting["video_url"] = video_url

        # Step 2: Download audio
        audio_path = AUDIO_DIR / f"{fname}.mp3"
        print(f"  Downloading audio...")
        success = download_audio(video_url, audio_path)
        if not success or not audio_path.exists():
            # Try downloading directly from presentation URL
            print(f"  Retrying with presentation URL directly...")
            success = download_audio(pres_url, audio_path)

        if not success:
            print(f"  [!] Audio download failed, skipping")
            meeting["transcript_path"] = None
            continue

        # Step 3: Transcribe
        transcript_text = transcribe_audio(audio_path, model)
        if not transcript_text:
            meeting["transcript_path"] = None
            continue

        # Step 4: Save transcript
        transcript_content = f"""MEETING: {title}
DATE: {date}
SOURCE: {pres_url}
{'='*60}

{transcript_text}
"""
        transcript_path.write_text(transcript_content, encoding="utf-8")
        meeting["transcript_path"] = str(transcript_path)
        print(f"  ✓ Transcript saved ({len(transcript_text):,} chars)")

        # Clean up audio to save disk space (transcripts are small)
        audio_path.unlink(missing_ok=True)

        # Save progress after each meeting
        INPUT_JSON.write_text(json.dumps(meetings, indent=2))
        time.sleep(1)

    completed = sum(1 for m in relevant if m.get("transcript_path"))
    print(f"\n✓ Transcribed {completed} / {len(relevant)} meetings")
    print(f"  Transcripts in: {TRANSCRIPT_DIR}/")

    return relevant


if __name__ == "__main__":
    run_transcription_pipeline()
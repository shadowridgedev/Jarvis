import moviepy.editor as mp
import speech_recognition as sr
import os
from pytube import YouTube
from multiprocessing import Pool
import mysql.connector
import re
import time

# New Jarvis
def sanitize_filename(title):
    # Replace invalid file name characters with an underscore
    return re.sub(r'[\\/:*?"<>|\|\t\n\r]', "_", title)


def download_video(video_url, download_path):
    print(f"Downloading video from {video_url}...")
    yt = YouTube(video_url)
    video_title = sanitize_filename(yt.title)
    file_name = f"{video_title}.mp4"
    video = yt.streams.filter(file_extension='mp4').first()
    video.download(output_path=download_path, filename=file_name)
    print("Video downloaded successfully.")
    return os.path.join(download_path, file_name)


def extract_audio(video_path, download_path):
    print(f"Extracting audio from {video_path}")
    video_clip = mp.VideoFileClip(video_path)
    audio_clip = video_clip.audio
    audio_file_name = os.path.basename(video_path).replace('.mp4', '.wav')
    audio_full_path = os.path.join(download_path, audio_file_name)
    audio_clip.write_audiofile(audio_full_path)
    audio_clip.close()
    print("Audio extracted successfully.")
    return audio_full_path


def transcribe_audio(segment):
    audio_path, start_time, duration = segment
    print(f"Transcribing audio from {start_time} to {start_time + duration} seconds...")
    recognizer = sr.Recognizer()
    with sr.AudioFile(audio_path) as source:
        audio_data = recognizer.record(source, duration=duration, offset=start_time)
        try:
            transcription = recognizer.recognize_google(audio_data)
            # Format timestamp as [hh:mm:ss] and append to transcription
            timestamp = f"[{time.strftime('%H:%M:%S', time.gmtime(start_time))}] "
            return timestamp + transcription
        except sr.UnknownValueError:
            return f"[{time.strftime('%H:%M:%S', time.gmtime(start_time))}] [Inaudible]"
        except sr.RequestError as e:
            return f"[{time.strftime('%H:%M:%S', time.gmtime(start_time))}] [Error: {e}]"


def handle_missing_words(full_transcriptions, three_second_transcriptions, overlap):
    corrected_transcripts = []
    for i in range(len(full_transcriptions) - 1):
        current_segment_words = full_transcriptions[i].split()
        next_segment_first_word = three_second_transcriptions[i].split()[0]

        if current_segment_words[-overlap:] != next_segment_first_word:
            if next_segment_first_word not in current_segment_words[-overlap:]:
                corrected_segment = " ".join(current_segment_words) + " " + next_segment_first_word
            else:
                corrected_segment = " ".join(current_segment_words)
        else:
            corrected_segment = " ".join(current_segment_words)

        corrected_transcripts.append(corrected_segment)

    corrected_transcripts.append(full_transcriptions[-1])  # Append the last segment without modification
    return corrected_transcripts


def create_database_and_table(db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS youtube")
    cursor.execute("USE youtube")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS YoutubeData (
            id INT AUTO_INCREMENT PRIMARY KEY,
            video_url TEXT,
            video_path TEXT,
            audio_path TEXT,
            transcript TEXT
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()


def store_data(db_config, video_url, video_path, audio_path, transcript):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("USE youtube")

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS YoutubeData (
            id INT AUTO_INCREMENT PRIMARY KEY,
            video_url TEXT,
            video_path TEXT,
            audio_path TEXT,
            transcript TEXT
        )
    """)

    # Insert data into the table
    cursor.execute("""
        INSERT INTO YoutubeData (video_url, video_path, audio_path, transcript)
        VALUES (%s, %s, %s, %s)
    """, (video_url, video_path, audio_path, transcript))

    conn.commit()
    cursor.close()
    conn.close()


def read_youtube_urls(file_path):
    with open(file_path, 'r') as file:
        urls = file.readlines()
    return [url.strip() for url in urls]


def video_exists(db_config, video_url):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("USE youtube")
    query = "SELECT EXISTS(SELECT 1 FROM YoutubeData WHERE video_url = %s)"
    cursor.execute(query, (video_url,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0]


def process_url(video_url, download_path, db_config):
    if video_exists(db_config, video_url):
        print(f"Video {video_url} already processed. Skipping.")
        return

    print(f"Processing URL: {video_url}")
    video_path = download_video(video_url, download_path)
    audio_path = extract_audio(video_path, download_path)

    # Transcription and processing logic
    segment_duration = 60  # 60 seconds
    overlap = 3  # 3 seconds overlap
    total_duration = mp.VideoFileClip(video_path).duration

    full_segments = [(audio_path, i * segment_duration, segment_duration + overlap) for i in
                     range(int(total_duration / segment_duration))]
    three_second_segments = [(audio_path, i * segment_duration, overlap) for i in
                             range(1, int(total_duration / segment_duration))]

    with Pool(28) as pool:
        full_transcriptions = pool.map(transcribe_audio, full_segments)
        three_second_transcriptions = pool.map(transcribe_audio, three_second_segments)
    # ... rest of the code ...

    corrected_transcripts = handle_missing_words(full_transcriptions, three_second_transcriptions, overlap)

    full_transcript = "\n".join(corrected_transcripts)
    store_data(db_config, video_url, video_path, audio_path, full_transcript)

    # New code to save transcript in the scratch directory
    transcript_file_path = os.path.join(download_path, os.path.basename(video_path).replace('.mp4', '.txt'))
    with open(transcript_file_path, 'w') as transcript_file:
        transcript_file.write(full_transcript)

    print(f"Processing completed for URL: {video_url}")
    print(f"Transcript saved to {transcript_file_path}")


#  test

def main():
    youtube_urls_file = "youtube_urls.txt"
    youtube_urls = read_youtube_urls(youtube_urls_file)
    download_path = "E:/scratch"  # Define download path
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'youtube'
    }
    create_database_and_table(db_config)  # Ensure this is called before processing UR
    for video_url in youtube_urls:
        process_url(video_url, download_path, db_config)


if __name__ == "__main__":
    main()

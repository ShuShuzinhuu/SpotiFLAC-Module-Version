import os
import re
import requests
import time
from typing import Callable
from mutagen.flac import FLAC, Picture
from mutagen.id3 import PictureType

def sanitize_filename(value: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", value).strip()

def safe_int(value) -> int:
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

class SpotiDownloader:

    _cached_token = None

    def __init__(self, timeout: float = 15.0):
        self.session = requests.Session()
        self.session.timeout = timeout
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        self.progress_callback: Callable[[int, int], None] = None

    def set_progress_callback(self, callback: Callable[[int, int], None]) -> None:
        self.progress_callback = callback

    def fetch_token(self) -> str:
        if SpotiDownloader._cached_token:
            return SpotiDownloader._cached_token

        print("Fetching session token from SpotiDownloader...")
        url = "https://spdl.afkarxyz.fun/token"
        
        for attempt in range(1, 4):
            try:
                resp = self.session.get(url, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                token = data.get("token")
                if token:
                    SpotiDownloader._cached_token = token
                    return token
            except Exception as e:
                if attempt == 3:
                    raise Exception(f"Failed to fetch token after 3 attempts: {e}")
                time.sleep(1)
                
        raise Exception("Token not found in response")

    def get_flac_download_link(self, track_id: str, token: str) -> str:
        print(f"Requesting FLAC download link for ID: {track_id}...")
        url = "https://api.spotidownloader.com/download"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Origin": "https://spotidownloader.com",
            "Referer": "https://spotidownloader.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"
        }
        
        payload = {"id": track_id, "flac": True}
        
        resp = self.session.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        if not data.get("success"):
            raise Exception("Download API returned success=false")
            
        flac_link = data.get("linkFlac")
        standard_link = data.get("link")
        
        final_link = None

        if flac_link and ".flac" in flac_link:
            final_link = flac_link
        elif standard_link and ".flac" in standard_link:
            final_link = standard_link
            
        if not final_link:
            raise Exception("SpotiDownloader API did not return a FLAC link for this track (only standard quality available).")
            
        return final_link
            


    def _stream_download(self, url: str, filepath: str, token: str) -> None:
        temp_path = filepath + ".part"
        
        # O download final desta API exige os headers de auth!
        headers = {
            "Authorization": f"Bearer {token}",
            "Origin": "https://spotidownloader.com",
            "Referer": "https://spotidownloader.com/"
        }
        
        with self.session.get(url, headers=headers, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length") or 0)
            downloaded = 0
            with open(temp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=256 * 1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if self.progress_callback:
                            self.progress_callback(downloaded, total)
                            
        os.replace(temp_path, filepath)

    def download_by_spotify_id(self, spotify_track_id, **kwargs):
        output_dir = kwargs.get("output_dir", ".")
        os.makedirs(output_dir, exist_ok=True)
        
        # Limpa o ID caso venha sujo
        track_id = spotify_track_id.split("/")[-1].split("?")[0]
        
        # Formata o nome do arquivo
        safe_title = sanitize_filename(kwargs.get("spotify_track_name", "Unknown"))
        safe_artist = sanitize_filename(kwargs.get("spotify_artist_name", "Unknown").split(",")[0])
        
        # Extensão forçada para FLAC
        expected_filename = f"{safe_artist} - {safe_title}.flac"
        expected_path = os.path.join(output_dir, expected_filename)

        if os.path.exists(expected_path) and os.path.getsize(expected_path) > 0:
            print(f"File already exists: {expected_path}")
            return expected_path

        # Fluxo de requisições
        token = self.fetch_token()
        flac_url = self.get_flac_download_link(track_id, token)

        print("Downloading FLAC file from SpotiDownloader...")
        self._stream_download(flac_url, expected_path, token)
        print() # Quebra de linha após o progresso
        
        # Injeta as tags
        self.embed_metadata(
            expected_path, 
            kwargs.get("spotify_track_name"), kwargs.get("spotify_artist_name"),
            kwargs.get("spotify_album_name"), kwargs.get("spotify_album_artist"),
            kwargs.get("spotify_release_date"), kwargs.get("spotify_track_number", 1),
            kwargs.get("spotify_total_tracks", 1), kwargs.get("spotify_disc_number", 1),
            kwargs.get("spotify_total_discs", 1), kwargs.get("spotify_cover_url"),
            kwargs.get("spotify_copyright"), kwargs.get("spotify_publisher"), kwargs.get("spotify_url")
        )

        return expected_path

    def embed_metadata(self, filepath, title, artist, album, album_artist, date, track_num, total_tracks, 
                       disc_num, total_discs, cover_url, copyright, publisher, url):
        print("Embedding metadata and cover art...")
        try:
            cover_data = None
            if cover_url:
                try: 
                    resp = self.session.get(cover_url, timeout=15)
                    if resp.status_code == 200: cover_data = resp.content
                except Exception as e:
                    print(f"Warning: Could not download cover: {e}")

            t_num = safe_int(track_num) or 1
            t_total = safe_int(total_tracks) or 1
            d_num = safe_int(disc_num) or 1
            d_total = safe_int(total_discs) or 1

            audio = FLAC(filepath)
            audio.delete()
            
            audio["TITLE"] = title
            audio["ARTIST"] = artist
            audio["ALBUM"] = album
            audio["ALBUMARTIST"] = album_artist
            audio["DATE"] = date
            audio["TRACKNUMBER"] = str(t_num)
            audio["TRACKTOTAL"] = str(t_total)
            audio["DISCNUMBER"] = str(d_num)
            audio["DISCTOTAL"] = str(d_total)
            if copyright: audio["COPYRIGHT"] = copyright
            if publisher: audio["ORGANIZATION"] = publisher
            if url: audio["URL"] = url
            audio["DESCRIPTION"] = "https://github.com/ShuShuzinhuu/SpotiFLAC-Module-Version"

            if cover_data:
                pic = Picture()
                pic.data = cover_data
                pic.type = PictureType.COVER_FRONT
                pic.mime = "image/jpeg"
                audio.add_picture(pic)
            
            audio.save()
            print("Metadata embedded successfully")

        except Exception as e:
            print(f"Warning: Failed to embed metadata: {e}")
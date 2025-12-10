import base64
import json
import os
import re
import time
from typing import Callable, Dict, Optional, Tuple

import requests
from mutagen.flac import FLAC, Picture
from mutagen.id3 import PictureType


def _sanitize_filename(value: str, fallback: str = "Unknown") -> str:
    if not value:
        return fallback
    cleaned = re.sub(r'[\\/*?:"<>|]', "", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or fallback


def _check_isrc_exists(directory: str, isrc: str) -> Tuple[Optional[str], bool]:
    if not isrc or not os.path.isdir(directory):
        return None, False

    for entry in os.listdir(directory):
        if not entry.lower().endswith(".flac"):
            continue
        path = os.path.join(directory, entry)
        try:
            audio = FLAC(path)
            if "ISRC" in audio and audio["ISRC"] and audio["ISRC"][0] == isrc:
                return path, True
        except Exception:
            continue
    return None, False


def build_qobuz_filename(
    title: str,
    artist: str,
    track_number: int,
    format_string: str,
    include_track_number: bool,
    position: int,
    use_album_track_number: bool,
) -> str:
    number_to_use = position
    if use_album_track_number and track_number > 0:
        number_to_use = track_number

    if "{" in format_string:
        filename = (
            format_string.replace("{title}", title)
            .replace("{artist}", artist)
        )
        if number_to_use > 0:
            filename = filename.replace("{track}", f"{number_to_use:02d}")
        else:
            filename = re.sub(r"\{track\}\.\s*", "", filename)
            filename = re.sub(r"\{track\}\s*-\s*", "", filename)
            filename = filename.replace("{track}", "")
    else:
        if format_string == "artist-title":
            filename = f"{artist} - {title}"
        elif format_string == "title":
            filename = title
        else:
            filename = f"{title} - {artist}"

        if include_track_number and number_to_use > 0:
            filename = f"{number_to_use:02d}. {filename}"

    return f"{filename}.flac"


class ProgressCallback:
    def __call__(self, current: int, total: int) -> None:
        if total > 0:
            percent = (current / total) * 100
            print(f"\r{percent:.2f}% ({current}/{total})", end="")
        else:
            print(f"\r{current / (1024 * 1024):.2f} MB", end="")


class QobuzDownloader:
    def __init__(self, timeout: float = 60.0, app_id: str = "798273057"):
        self.timeout = timeout
        self.app_id = app_id
        self.session = requests.Session()
        self.session.timeout = timeout
        self.progress_callback: Callable[[int, int], None] = ProgressCallback()

    def set_progress_callback(self, callback: Callable[[int, int], None]) -> None:
        self.progress_callback = callback

    def _search_by_isrc(self, isrc: str) -> Dict:
        api_base = base64.b64decode(
            "aHR0cHM6Ly93d3cucW9idXouY29tL2FwaS5qc29uLzAuMi90cmFjay9zZWFyY2g/cXVlcnk9"
        ).decode()
        url = f"{api_base}{isrc}&limit=1&app_id={self.app_id}"
        resp = self.session.get(url, timeout=self.timeout)
        if resp.status_code != 200:
            raise Exception(f"API returned status {resp.status_code}")
        data = resp.json()
        tracks = data.get("tracks", {}).get("items", [])
        if not tracks:
            raise Exception(f"track not found for ISRC: {isrc}")
        return tracks[0]

    def _get_download_url(self, track_id: int, quality: str = "LOSSLESS") -> str:
        # Qobuz quality codes: 5 (MP3 320), 6 (FLAC 16-bit), 7 (FLAC 24-bit), 27 (Hi-Res)
        quality_code = "27"
        primary_base = base64.b64decode("aHR0cHM6Ly9kYWIueWVldC5zdS9hcGkvc3RyZWFtP3RyYWNrSWQ9").decode()
        primary_url = f"{primary_base}{track_id}&quality={quality_code}"

        def try_url(url: str) -> Optional[str]:
            resp = self.session.get(url, timeout=self.timeout)
            if resp.status_code != 200:
                return None
            body = resp.content
            try:
                parsed = json.loads(body.decode())
            except Exception:
                return None
            return parsed.get("url")

        url = try_url(primary_url)
        if url:
            return url

        fallback_base = base64.b64decode("aHR0cHM6Ly9kYWJtdXNpYy54eXovYXBpL3N0cmVhbT90cmFja0lkPQ==").decode()
        fallback_url = f"{fallback_base}{track_id}&quality={quality_code}"
        url = try_url(fallback_url)
        if not url:
            raise Exception("failed to get download URL")
        return url

    def _stream_download(self, url: str, filepath: str) -> None:
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        temp_path = filepath + ".part"
        try:
            with self.session.get(url, stream=True, timeout=self.timeout) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length") or 0)
                downloaded = 0
                chunk_size = 256 * 1024
                with open(temp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        if self.progress_callback:
                            self.progress_callback(downloaded, total)
            os.replace(temp_path, filepath)
            print("\nDownload complete")
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    def _download_cover_art(self, cover_url: str, filepath: str) -> Optional[str]:
        if not cover_url:
            return None
        try:
            resp = self.session.get(cover_url, timeout=self.timeout)
            if resp.status_code != 200:
                return None
            with open(filepath, "wb") as f:
                f.write(resp.content)
            return filepath
        except Exception:
            return None

    def _embed_metadata(self, filepath: str, metadata: Dict) -> bool:
        try:
            audio = FLAC(filepath)
            audio.clear()
            audio.clear_pictures()

            if metadata.get("Title"):
                audio["TITLE"] = metadata["Title"]
            if metadata.get("Artist"):
                audio["ARTIST"] = metadata["Artist"]
                audio["ALBUMARTIST"] = metadata["Artist"]
            if metadata.get("Album"):
                audio["ALBUM"] = metadata["Album"]
            if metadata.get("Date"):
                audio["DATE"] = metadata["Date"]
            if metadata.get("TrackNumber"):
                audio["TRACKNUMBER"] = str(metadata["TrackNumber"])
            if metadata.get("DiscNumber"):
                audio["DISCNUMBER"] = str(metadata["DiscNumber"])
            if metadata.get("ISRC"):
                audio["ISRC"] = metadata["ISRC"]

            cover_path = metadata.get("CoverPath")
            if cover_path and os.path.exists(cover_path):
                with open(cover_path, "rb") as img:
                    picture = Picture()
                    picture.data = img.read()
                    picture.type = PictureType.COVER_FRONT
                    picture.mime = "image/jpeg"
                    picture.desc = "Cover"
                    audio.add_picture(picture)

            audio.save()
            return True
        except Exception as exc:
            print(f"Error embedding metadata: {exc}")
            return False

    def download_by_isrc(
        self,
        isrc: str,
        output_dir: str,
        quality: str,
        filename_format: str,
        include_track_number: bool,
        position: int,
        spotify_track_name: str,
        spotify_artist_name: str,
        spotify_album_name: str,
        use_album_track_number: bool,
    ) -> str:
        print(f"Fetching track info for ISRC: {isrc}")
        os.makedirs(output_dir, exist_ok=True)

        track = self._search_by_isrc(isrc)

        artists = spotify_artist_name or track.get("performer", {}).get("name") or track.get("album", {}).get("artist", {}).get("name", "")
        track_title = spotify_track_name or track.get("title", "")
        version = track.get("version")
        if not track_title:
            track_title = track.get("title", "")
        if version and version != "null":
            track_title = f"{track_title} ({version})"
        album_title = spotify_album_name or track.get("album", {}).get("title", "")

        print(f"Found track: {artists} - {track_title}")
        print(f"Album: {album_title}")

        download_url = self._get_download_url(track.get("id"), quality)
        if not download_url:
            raise Exception("received empty download URL")
        url_preview = download_url if len(download_url) <= 60 else download_url[:60] + "..."
        print(f"Download URL obtained: {url_preview}")

        safe_artist = _sanitize_filename(artists)
        safe_title = _sanitize_filename(track_title)

        existing_file, exists = _check_isrc_exists(output_dir, track.get("isrc"))
        if exists and existing_file:
            print(f"File with ISRC {track.get('isrc')} already exists: {existing_file}")
            return "EXISTS:" + existing_file

        filename = build_qobuz_filename(
            safe_title,
            safe_artist,
            track.get("track_number", 0),
            filename_format,
            include_track_number,
            position,
            use_album_track_number,
        )
        filepath = os.path.join(output_dir, filename)

        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            print(f"File already exists: {filepath}")
            return "EXISTS:" + filepath

        print(f"Downloading FLAC file to: {filepath}")
        self._stream_download(download_url, filepath)

        cover_path = ""
        cover_url = track.get("album", {}).get("image", {}).get("large") or ""
        if cover_url:
            cover_path_candidate = filepath + ".cover.jpg"
            downloaded_cover = self._download_cover_art(cover_url, cover_path_candidate)
            if downloaded_cover:
                cover_path = downloaded_cover

        release_year = ""
        release_date = track.get("release_date_original") or ""
        if len(release_date) >= 4:
            release_year = release_date[:4]

        track_number_to_embed = 0
        if position > 0:
            if use_album_track_number and track.get("track_number", 0) > 0:
                track_number_to_embed = track.get("track_number", 0)
            else:
                track_number_to_embed = position

        metadata = {
            "Title": track_title,
            "Artist": artists,
            "Album": album_title,
            "Date": release_year,
            "TrackNumber": track_number_to_embed,
            "DiscNumber": track.get("media_number", 0),
            "ISRC": track.get("isrc", ""),
            "CoverPath": cover_path,
        }

        self._embed_metadata(filepath, metadata)

        if cover_path and os.path.exists(cover_path):
            try:
                os.remove(cover_path)
            except Exception:
                pass

        print("Metadata embedded successfully!")
        return filepath

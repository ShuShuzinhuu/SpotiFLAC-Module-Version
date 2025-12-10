import base64
import concurrent.futures
import json
import os
import re
import subprocess
import time
import xml.etree.ElementTree as ET
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests
from mutagen.flac import FLAC, Picture
from mutagen.id3 import PictureType


def _contains_japanese(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]", text))


def _japanese_to_romaji(text: str) -> str:
    # Minimal placeholder: keep text as-is; real romaji conversion would need
    # an external library which we avoid adding here.
    return text or ""


def _clean_to_ascii(text: str) -> str:
    return (text or "").encode("ascii", "ignore").decode().strip()


def _set_download_speed(_mbps: float) -> None:
    # Hook for UI progress; no-op in CLI mode
    return None


def _set_download_progress(_mb_downloaded: float) -> None:
    # Hook for UI progress; no-op in CLI mode
    return None


def _sanitize_filename(value: str, fallback: str = "Unknown") -> str:
    if not value:
        return fallback
    cleaned = re.sub(r'[\\/*?:"<>|]', "", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or fallback


def _build_tidal_filename(
    title: str,
    artist: str,
    track_number: int,
    format_string: str,
    include_track_number: bool,
    position: int,
    use_album_track_number: bool,
) -> str:
    number_to_use = position
    if use_album_track_number and track_number:
        number_to_use = track_number

    if "{" in format_string:
        filename = (
            format_string.replace("{title}", title)
            .replace("{artist}", artist)
        )

        if number_to_use:
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

        if include_track_number and number_to_use:
            filename = f"{number_to_use:02d}. {filename}"

    return f"{filename}.flac"


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


class ProgressCallback:
    def __call__(self, current: int, total: int) -> None:
        if total > 0:
            percent = (current / total) * 100
            print(f"\r{percent:.2f}% ({current}/{total})", end="")
        else:
            print(f"\r{current / (1024 * 1024):.2f} MB", end="")


class TidalDownloader:
    def __init__(self, api_url: Optional[str] = None, timeout: float = 5.0, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries
        self.progress_callback: Callable[[int, int], None] = ProgressCallback()
        self.client_id = base64.b64decode("NkJEU1JkcEs5aHFFQlRnVQ==").decode()
        self.client_secret = base64.b64decode("eGV1UG1ZN25icFo5SUliTEFjUTkzc2hrYTFWTmhlVUFxTjZJY3N6alRHOD0=").decode()

        apis = self.get_available_apis()
        if api_url:
            self.api_url = api_url
        elif apis:
            self.api_url = apis[0]
        else:
            self.api_url = ""
        self.api_list = apis

    def set_progress_callback(self, callback: Callable[[int, int], None]) -> None:
        self.progress_callback = callback

    @staticmethod
    def get_available_apis() -> List[str]:
        try:
            
            url = "https://raw.githubusercontent.com/afkarxyz/SpotiFLAC/refs/heads/main/tidal.json"

            headers = {
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            api_list = response.json()
            return [f"https://{api}" for api in api_list]
        except Exception as exc:
            print(f"Failed to fetch API list: {exc}")
            return []

    def get_access_token(self) -> Optional[str]:
        data = f"client_id={self.client_id}&grant_type=client_credentials"
        auth_url = base64.b64decode("aHR0cHM6Ly9hdXRoLnRpZGFsLmNvbS92MS9vYXV0aDIvdG9rZW4=").decode()
        try:
            resp = requests.post(
                auth_url,
                data=data,
                auth=(self.client_id, self.client_secret),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self.timeout,
            )
            if resp.status_code != 200:
                return None
            return resp.json().get("access_token")
        except Exception:
            return None

    def search_tracks_with_limit(self, query: str, limit: int = 50) -> Dict:
        token = self.get_access_token()
        if not token:
            raise Exception("Failed to get access token")

        search_base = base64.b64decode(
            "aHR0cHM6Ly9hcGkudGlkYWwuY29tL3YxL3NlYXJjaC90cmFja3M/cXVlcnk9"
        ).decode()
        search_url = f"{search_base}{quote(query)}&limit={limit}&offset=0&countryCode=US"
        resp = requests.get(search_url, headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout)
        if resp.status_code != 200:
            raise Exception(f"search failed: HTTP {resp.status_code} - {resp.text}")
        return resp.json()

    def search_tracks(self, query: str) -> Dict:
        return self.search_tracks_with_limit(query, 50)

    def _collect_search_queries(self, track_name: str, artist_name: str) -> List[str]:
        queries: List[str] = []
        if artist_name and track_name:
            queries.append(f"{artist_name} {track_name}")
        if track_name:
            queries.append(track_name)

        if _contains_japanese(track_name) or _contains_japanese(artist_name):
            romaji_track = _japanese_to_romaji(track_name)
            romaji_artist = _japanese_to_romaji(artist_name)
            clean_track = _clean_to_ascii(romaji_track)
            clean_artist = _clean_to_ascii(romaji_artist)

            if clean_artist and clean_track:
                queries.append(f"{clean_artist} {clean_track}")
            if clean_track and clean_track != track_name:
                queries.append(clean_track)
            if artist_name and clean_track:
                queries.append(f"{artist_name} {clean_track}")

        if artist_name:
            artist_only = _clean_to_ascii(_japanese_to_romaji(artist_name))
            if artist_only:
                queries.append(artist_only)

        uniq = []
        seen = set()
        for q in queries:
            q = q.strip()
            if q and q not in seen:
                uniq.append(q)
                seen.add(q)
        return uniq

    def search_track_by_metadata_with_isrc(
        self, track_name: str, artist_name: str, spotify_isrc: str, expected_duration: int
    ) -> Dict:
        queries = self._collect_search_queries(track_name, artist_name)
        all_tracks: List[Dict] = []
        for query in queries:
            print(f"Searching Tidal for: {query}")
            try:
                result = self.search_tracks_with_limit(query, 100)
                items = result.get("items", [])
                if items:
                    print(f"Found {len(items)} results for '{query}'")
                    all_tracks.extend(items)
            except Exception as exc:
                print(f"Search error for '{query}': {exc}")

        if not all_tracks:
            raise Exception("no tracks found for any search query")

        if spotify_isrc:
            print(f"Looking for ISRC match: {spotify_isrc}")
            for track in all_tracks:
                if track.get("isrc") == spotify_isrc:
                    print(
                        f"✓ ISRC match found: {track.get('artist', {}).get('name','?')} - "
                        f"{track.get('title','?')} (ISRC: {spotify_isrc})"
                    )
                    return track
            raise Exception(f"ISRC mismatch: no track found with ISRC {spotify_isrc} on Tidal")

        best_match: Optional[Dict] = None
        if expected_duration:
            tolerance = 3
            matches = []
            for track in all_tracks:
                duration = track.get("duration") or 0
                if abs(duration - expected_duration) <= tolerance:
                    matches.append(track)
            if matches:
                best_match = matches[0]
                for track in matches:
                    tags = (track.get("mediaMetadata") or {}).get("tags") or []
                    if "HIRES_LOSSLESS" in tags:
                        best_match = track
                        break
                return best_match

        best_match = all_tracks[0]
        for track in all_tracks:
            tags = (track.get("mediaMetadata") or {}).get("tags") or []
            if "HIRES_LOSSLESS" in tags:
                best_match = track
                break
        return best_match

    def get_tidal_url_from_spotify(self, spotify_track_id: str) -> str:
        spotify_base = base64.b64decode("aHR0cHM6Ly9vcGVuLnNwb3RpZnkuY29tL3RyYWNrLw==").decode()
        spotify_url = f"{spotify_base}{spotify_track_id}"
        api_base = base64.b64decode("aHR0cHM6Ly9hcGkuc29uZy5saW5rL3YxLWFscGhhLjEvbGlua3M/dXJsPQ==").decode()
        api_url = f"{api_base}{quote(spotify_url)}"
        resp = requests.get(api_url, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        tidal_link = data.get("linksByPlatform", {}).get("tidal", {}).get("url")
        if not tidal_link:
            raise Exception("tidal link not found")
        print(f"Found Tidal URL: {tidal_link}")
        return tidal_link

    @staticmethod
    def get_track_id_from_url(tidal_url: str) -> int:
        parts = tidal_url.split("/track/")
        if len(parts) < 2:
            raise Exception("invalid tidal URL format")
        track_part = parts[1].split("?")[0].strip()
        try:
            return int(track_part)
        except ValueError as exc:
            raise Exception(f"failed to parse track ID: {exc}") from exc

    def get_track_info_by_id(self, track_id: int) -> Dict:
        token = self.get_access_token()
        if not token:
            raise Exception("failed to get access token")
        track_base = base64.b64decode("aHR0cHM6Ly9hcGkudGlkYWwuY29tL3YxL3RyYWNrcy8=").decode()
        track_url = f"{track_base}{track_id}?countryCode=US"
        resp = requests.get(track_url, headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout)
        if resp.status_code != 200:
            raise Exception(f"failed to get track info: HTTP {resp.status_code} - {resp.text}")
        info = resp.json()
        print(f"Found: {info.get('title','?')} ({info.get('audioQuality','?')})")
        return info

    def _request_download_url(self, api_url: str, track_id: int, quality: str) -> Optional[str]:
        url = f"{api_url}/track/?id={track_id}&quality={quality}"
        resp = requests.get(url, timeout=self.timeout)
        if resp.status_code != 200:
            return None
        body = resp.text
        try:
            v2 = resp.json()
        except Exception:
            return None

        if isinstance(v2, dict) and v2.get("data", {}).get("manifest"):
            return "MANIFEST:" + v2["data"]["manifest"]
        if isinstance(v2, list):
            for item in v2:
                if item.get("OriginalTrackUrl"):
                    return item["OriginalTrackUrl"]
        return None

    def get_download_url(self, track_id: int, quality: str = "LOSSLESS") -> str:
        if not self.api_url:
            raise Exception("No API URL configured")
        print("Fetching URL...")
        url = self._request_download_url(self.api_url, track_id, quality)
        if not url:
            raise Exception("download URL not found in response")
        return url

    def _get_download_url_parallel(self, apis: List[str], track_id: int, quality: str) -> Tuple[str, str]:
        if not apis:
            raise Exception("no APIs available")

        def worker(api: str) -> Tuple[str, Optional[str], Optional[str]]:
            try:
                res = self._request_download_url(api, track_id, quality)
                if res:
                    return api, res, None
                return api, None, "no download URL"
            except Exception as exc:
                return api, None, str(exc)

        errors = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(apis))) as pool:
            for api, result, err in pool.map(worker, apis):
                if result:
                    print(f"✓ Got response from: {api}")
                    return api, result
                errors.append(f"{api}: {err}")
        raise Exception(f"all {len(apis)} APIs failed. Errors: {errors[:3]}")

    @staticmethod
    def download_album_art(album_id: str, size: str = "1280x1280") -> Optional[bytes]:
        art_url = f"https://resources.tidal.com/images/{album_id.replace('-', '/')}/{size}.jpg"
        resp = requests.get(art_url, timeout=15)
        if resp.status_code != 200:
            return None
        return resp.content

    def _stream_download(self, url: str, file_obj, show_progress: bool = True) -> None:
        with requests.get(url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length") or 0)
            downloaded = 0
            chunk_size = 256 * 1024
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                file_obj.write(chunk)
                downloaded += len(chunk)
                if show_progress and self.progress_callback:
                    self.progress_callback(downloaded, total)

    def download_file(self, url: str, filepath: str) -> None:
        if url.startswith("MANIFEST:"):
            manifest = url.replace("MANIFEST:", "", 1)
            self.download_from_manifest(manifest, filepath)
            return

        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        temp_path = filepath + ".part"
        try:
            with open(temp_path, "wb") as f:
                self._stream_download(url, f)
            os.replace(temp_path, filepath)
            print("\nDownload complete")
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    def download_from_manifest(self, manifest_b64: str, output_path: str) -> None:
        direct_url, init_url, media_urls = parse_manifest(manifest_b64)
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        if direct_url:
            print("Downloading file...")
            with open(output_path, "wb") as f:
                self._stream_download(direct_url, f)
            print("\nDownload complete")
            return

        temp_path = output_path + ".m4a.tmp"
        with open(temp_path, "wb") as f:
            print("Downloading init segment...")
            self._stream_download(init_url, f, show_progress=False)

            total_bytes = 0
            last_time = time.time()
            last_bytes = 0
            total_segments = len(media_urls)
            for idx, media_url in enumerate(media_urls, start=1):
                self._stream_download(media_url, f, show_progress=False)
                total_bytes = f.tell()
                now = time.time()
                if now - last_time > 0.1:
                    speed = (total_bytes - last_bytes) / (1024 * 1024) / (now - last_time)
                    _set_download_speed(speed)
                    last_bytes = total_bytes
                    last_time = now
                _set_download_progress(total_bytes / (1024 * 1024))
                print(f"\rDownloading: {total_bytes / (1024 * 1024):.2f} MB ({idx}/{total_segments})", end="")

        print()
        print("Converting to FLAC...")
        cmd = ["ffmpeg", "-y", "-i", temp_path, "-vn", "-c:a", "flac", output_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"ffmpeg conversion failed: {result.stderr}")
        try:
            os.remove(temp_path)
        except Exception:
            pass
        print("Download complete")

    def embed_metadata(self, filepath: str, metadata: Dict, search_info: Optional[Dict] = None) -> bool:
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

            cover = metadata.get("CoverPath")
            if cover and os.path.exists(cover):
                with open(cover, "rb") as img:
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

    def download(
        self,
        query: str,
        isrc: Optional[str] = None,
        output_dir: str = ".",
        quality: str = "LOSSLESS",
        is_paused_callback=None,
        is_stopped_callback=None,
        auto_fallback: bool = False,
        filename_format: str = "title-artist",
        include_track_number: bool = False,
        position: int = 0,
        use_album_track_number: bool = False,
    ):
        os.makedirs(output_dir, exist_ok=True)

        try:
            track_info = self.search_track_by_metadata_with_isrc(query, "", isrc or "", 0)
        except Exception as exc:
            raise Exception(f"Error getting track info: {exc}")

        track_id = track_info.get("id")
        if not track_id:
            raise Exception("no track ID found")

        artists = []
        if track_info.get("artists"):
            artists = [a.get("name") for a in track_info["artists"] if a.get("name")]
        elif track_info.get("artist", {}).get("name"):
            artists = [track_info["artist"]["name"]]
        artist_name = _sanitize_filename(", ".join(artists) or "Unknown Artist")
        track_title = _sanitize_filename(track_info.get("title") or f"track_{track_id}")
        album_title = track_info.get("album", {}).get("title", "")

        filename = _build_tidal_filename(
            track_title,
            artist_name,
            track_info.get("trackNumber") or 0,
            filename_format,
            include_track_number,
            position,
            use_album_track_number,
        )
        output_filename = os.path.join(output_dir, filename)

        if os.path.exists(output_filename) and os.path.getsize(output_filename) > 0:
            print(f"File already exists: {output_filename}")
            return output_filename

        existing, exists = _check_isrc_exists(output_dir, track_info.get("isrc"))
        if exists and existing:
            print(f"File with ISRC exists: {existing}")
            return existing

        if auto_fallback and self.api_list:
            api, download_url = self._get_download_url_parallel(self.api_list, track_id, quality)
            downloader = TidalDownloader(api_url=api)
            downloader.set_progress_callback(self.progress_callback)
            downloader.download_file(download_url, output_filename)
        else:
            download_url = self.get_download_url(track_id, quality)
            self.download_file(download_url, output_filename)

        cover_path = ""
        album_cover = track_info.get("album", {}).get("cover")
        if album_cover:
            cover_bytes = self.download_album_art(album_cover)
            if cover_bytes:
                cover_path = output_filename + ".cover.jpg"
                with open(cover_path, "wb") as f:
                    f.write(cover_bytes)

        metadata = {
            "Title": track_title,
            "Artist": artist_name,
            "Album": album_title,
            "Date": (track_info.get("album", {}).get("releaseDate") or "")[:4],
            "TrackNumber": track_info.get("trackNumber", 0),
            "DiscNumber": track_info.get("volumeNumber", 0),
            "ISRC": track_info.get("isrc", ""),
            "CoverPath": cover_path,
        }
        self.embed_metadata(output_filename, metadata, track_info)
        if cover_path and os.path.exists(cover_path):
            try:
                os.remove(cover_path)
            except Exception:
                pass
        print("Done")
        return output_filename


def parse_manifest(manifest_b64: str) -> Tuple[str, str, List[str]]:
    try:
        manifest_bytes = base64.b64decode(manifest_b64)
    except Exception as exc:
        raise Exception(f"failed to decode manifest: {exc}") from exc

    manifest_str = manifest_bytes.decode(errors="ignore").strip()
    if manifest_str.startswith("{"):
        try:
            manifest = json.loads(manifest_str)
            urls = manifest.get("urls") or manifest.get("URLs") or manifest.get("URLs".lower(), [])
            if urls:
                return urls[0], "", []
        except Exception as exc:
            raise Exception(f"failed to parse BTS manifest: {exc}") from exc
        raise Exception("no URLs in BTS manifest")

    try:
        mpd = ET.fromstring(manifest_str)
        ns = {"mpd": mpd.tag.split("}")[0].strip("{")} if "}" in mpd.tag else {}
        seg_template = mpd.find(".//mpd:SegmentTemplate", ns)
        if seg_template is None:
            seg_template = mpd.find(".//SegmentTemplate")
        init_url = seg_template.get("initialization")
        media_template = seg_template.get("media")
        timeline = seg_template.find("mpd:SegmentTimeline", ns) or seg_template.find("SegmentTimeline")
        segments = []
        if timeline is not None:
            for seg in timeline.findall("mpd:S", ns) or timeline.findall("S"):
                repeat = int(seg.get("r") or 0)
                segments.append(repeat + 1)
        segment_count = sum(segments) if segments else 0
        if segment_count == 0:
            segment_count = len(re.findall(r"<S ", manifest_str))
        media_urls = []
        for i in range(1, segment_count + 1):
            media_urls.append(media_template.replace("$Number$", str(i)))
        return "", init_url, media_urls
    except Exception as exc:
        raise Exception(f"failed to parse manifest XML: {exc}") from exc

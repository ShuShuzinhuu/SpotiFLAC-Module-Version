import base64
import json
import os
import random
import re
import time
from typing import Callable, List, Optional
from urllib.parse import quote

import requests


def _sanitize_filename(value: str, fallback: str = "Unknown") -> str:
    if not value:
        return fallback
    cleaned = re.sub(r'[\\/*?:"<>|]', "", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or fallback


def _build_amazon_filename(
    title: str,
    artist: str,
    format_string: str,
    include_track_number: bool,
    position: int,
    use_album_track_number: bool,
    track_number: int = 0,
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


class AmazonDownloader:
    def __init__(self, timeout: float = 120.0):
        self.timeout = timeout
        self.client = requests.Session()
        self.client.timeout = timeout
        self.regions: List[str] = ["us", "eu"]
        self.last_api_call_time = 0.0
        self.api_call_count = 0
        self.api_call_reset_time = time.time()
        self.progress_callback: Callable[[int, int], None] = ProgressCallback()

    def set_progress_callback(self, callback: Callable[[int, int], None]) -> None:
        self.progress_callback = callback

    @staticmethod
    def _random_user_agent() -> str:
        return (
            f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{random.randint(11,15)}_{random.randint(4,8)}) "
            f"AppleWebKit/{random.randint(530,536)}.{random.randint(30,36)} (KHTML, like Gecko) "
            f"Chrome/{random.randint(80,104)}.0.{random.randint(3000,4500)}.{random.randint(60,120)} "
            f"Safari/{random.randint(530,536)}.{random.randint(30,36)}"
        )

    def _respect_rate_limit(self) -> None:
        now = time.time()
        if now - self.api_call_reset_time >= 60:
            self.api_call_count = 0
            self.api_call_reset_time = now

        if self.api_call_count >= 9:
            wait_time = 60 - (now - self.api_call_reset_time)
            if wait_time > 0:
                print(f"Rate limit reached, waiting {int(wait_time)}s...")
                time.sleep(wait_time)
                self.api_call_count = 0
                self.api_call_reset_time = time.time()

        if self.last_api_call_time:
            elapsed = now - self.last_api_call_time
            min_delay = 7
            if elapsed < min_delay:
                wait_time = min_delay - elapsed
                print(f"Rate limiting: waiting {int(wait_time)}s...")
                time.sleep(wait_time)

    def get_amazon_url_from_spotify(self, spotify_track_id: str) -> str:
        self._respect_rate_limit()
        spotify_base = base64.b64decode("aHR0cHM6Ly9vcGVuLnNwb3RpZnkuY29tL3RyYWNrLw==").decode()
        spotify_url = f"{spotify_base}{spotify_track_id}"
        api_base = base64.b64decode("aHR0cHM6Ly9hcGkuc29uZy5saW5rL3YxLWFscGhhLjEvbGlua3M/dXJsPQ==").decode()
        api_url = f"{api_base}{quote(spotify_url)}"

        headers = {"User-Agent": self._random_user_agent()}

        max_retries = 3
        for attempt in range(max_retries):
            resp = self.client.get(api_url, headers=headers, timeout=self.timeout)
            self.last_api_call_time = time.time()
            self.api_call_count += 1
            if resp.status_code == 429 and attempt < max_retries - 1:
                print("Rate limited by API, waiting 15s before retry...")
                time.sleep(15)
                continue
            if resp.status_code != 200:
                raise Exception(f"API returned status {resp.status_code}")
            data = resp.json()
            amazon_link = data.get("linksByPlatform", {}).get("amazonMusic", {}).get("url")
            if not amazon_link:
                raise Exception("amazon Music link not found")

            if "trackAsin=" in amazon_link:
                parts = amazon_link.split("trackAsin=")
                if len(parts) > 1:
                    track_asin = parts[1].split("&")[0]
                    music_base = base64.b64decode("aHR0cHM6Ly9tdXNpYy5hbWF6b24uY29tL3RyYWNrcy8=").decode()
                    amazon_link = f"{music_base}{track_asin}?musicTerritory=US"
            print(f"Found Amazon URL: {amazon_link}")
            return amazon_link
        raise Exception("Failed to get Amazon URL after retries")

    def _stream_download(self, url: str, filepath: str) -> None:
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        temp_path = filepath + ".part"
        try:
            with self.client.get(url, stream=True, timeout=self.timeout) as resp:
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

    def download_from_service(self, amazon_url: str, output_dir: str) -> str:
        last_error: Optional[Exception] = None
        for region in self.regions:
            print(f"\nTrying region: {region}...")
            service_base = base64.b64decode("aHR0cHM6Ly8=").decode()
            service_domain = base64.b64decode("LmRvdWJsZWRvdWJsZS50b3A=").decode()
            base_url = f"{service_base}{region}{service_domain}"

            submit_url = f"{base_url}/dl?url={quote(amazon_url)}"
            headers = {"User-Agent": self._random_user_agent()}
            try:
                resp = self.client.get(submit_url, headers=headers, timeout=self.timeout)
                if resp.status_code != 200:
                    last_error = Exception(f"submit failed with status {resp.status_code}")
                    continue
                submit_resp = resp.json()
                download_id = submit_resp.get("id") if submit_resp.get("success") else ""
                if not download_id:
                    last_error = Exception("submit request failed")
                    continue
            except Exception as exc:
                last_error = exc
                continue

            status_url = f"{base_url}/dl/{download_id}"
            print("Waiting for download to complete...")
            max_wait = 300
            elapsed = 0
            poll_interval = 3
            file_url = ""
            track_name = ""
            artist = ""
            while elapsed < max_wait:
                time.sleep(poll_interval)
                elapsed += poll_interval
                try:
                    status_resp = self.client.get(status_url, headers={"User-Agent": self._random_user_agent()}, timeout=self.timeout)
                    if status_resp.status_code != 200:
                        print(f"\rStatus check failed (status {status_resp.status_code}), retrying...", end="")
                        continue
                    status = status_resp.json()
                except Exception:
                    print("\rStatus check failed, retrying...", end="")
                    continue

                state = status.get("status")
                if state == "done":
                    file_url = status.get("url", "")
                    if file_url.startswith("./"):
                        file_url = f"{base_url}/{file_url[2:]}"
                    elif file_url.startswith("/"):
                        file_url = f"{base_url}{file_url}"
                    track_name = status.get("current", {}).get("name", "")
                    artist = status.get("current", {}).get("artist", "")
                    print("\nDownload ready!")
                    break
                if state == "error":
                    msg = status.get("friendlyStatus") or "Unknown error"
                    last_error = Exception(f"processing failed: {msg}")
                    break
                friendly = status.get("friendlyStatus") or state or "processing"
                print(f"\r{friendly}...", end="")

            if not file_url:
                last_error = Exception("download timeout or failed")
                print(f"\nError with {region} region: {last_error}")
                continue

            filename = f"{artist} - {track_name}.flac"
            for char in '<>:"/\\|?*':
                filename = filename.replace(char, "")
            filename = filename.strip()
            filepath = os.path.join(output_dir, filename)

            try:
                print("Downloading...")
                self._stream_download(file_url, filepath)
                print("Download complete!")
                return filepath
            except Exception as exc:
                last_error = exc
                print(f"Error with {region} region: {last_error}")
                continue

        raise Exception(f"all regions failed. Last error: {last_error}")

    def download_by_url(
        self,
        amazon_url: str,
        output_dir: str,
        filename_format: str,
        include_track_number: bool,
        position: int,
        spotify_track_name: str,
        spotify_artist_name: str,
        spotify_album_name: str,
        use_album_track_number: bool,
    ) -> str:
        os.makedirs(output_dir, exist_ok=True)

        if spotify_track_name and spotify_artist_name:
            expected = _build_amazon_filename(
                _sanitize_filename(spotify_track_name),
                _sanitize_filename(spotify_artist_name),
                filename_format,
                include_track_number,
                position,
                use_album_track_number,
            )
            expected_path = os.path.join(output_dir, expected)
            if os.path.exists(expected_path) and os.path.getsize(expected_path) > 0:
                print(f"File already exists: {expected_path}")
                return "EXISTS:" + expected_path

        print(f"Using Amazon URL: {amazon_url}")
        file_path = self.download_from_service(amazon_url, output_dir)

        if spotify_track_name and spotify_artist_name:
            safe_artist = _sanitize_filename(spotify_artist_name)
            safe_title = _sanitize_filename(spotify_track_name)
            new_filename = _build_amazon_filename(
                safe_title,
                safe_artist,
                filename_format,
                include_track_number,
                position,
                use_album_track_number,
            )
            new_file_path = os.path.join(output_dir, new_filename)
            if file_path != new_file_path:
                try:
                    os.replace(file_path, new_file_path)
                    file_path = new_file_path
                    print(f"Renamed to: {new_filename}")
                except Exception as exc:
                    print(f"Warning: Failed to rename file: {exc}")

        print("✓ Downloaded successfully from Amazon Music")
        return file_path

    def download_by_spotify_id(
        self,
        spotify_track_id: str,
        output_dir: str,
        filename_format: str,
        include_track_number: bool,
        position: int,
        spotify_track_name: str,
        spotify_artist_name: str,
        spotify_album_name: str,
        use_album_track_number: bool,
    ) -> str:
        amazon_url = self.get_amazon_url_from_spotify(spotify_track_id)
        return self.download_by_url(
            amazon_url,
            output_dir,
            filename_format,
            include_track_number,
            position,
            spotify_track_name,
            spotify_artist_name,
            spotify_album_name,
            use_album_track_number,
        )


if __name__ == "__main__":
    # Simple manual test placeholder
    print("Amazon downloader module")

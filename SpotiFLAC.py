import os
import re
import time
import argparse
import asyncio

from dataclasses import dataclass
from getMetadata import get_filtered_data, parse_uri, SpotifyInvalidUrlException
from tidalDL import TidalDownloader
from deezerDL import DeezerDownloader
from qobuzDL import QobuzDownloader

@dataclass
class Config:
    url: str
    output_dir: str
    service: list = None
    filename_format: str = "title_artist"
    use_track_numbers: bool = False
    use_artist_subfolders: bool = False
    use_album_subfolders: bool = False
    is_album: bool = False
    is_playlist: bool = False
    is_single_track: bool = False
    album_or_playlist_name: str = ""
    tracks = []
    worker = None
    loop: int = 3600
    start_time: float = 0.0
    end_time: float = 0.0

@dataclass
class Track:
    external_urls: str
    title: str
    artists: str
    album: str
    track_number: int
    duration_ms: int
    id: str
    isrc: str = ""
    downloaded : bool = False

def get_metadata(url):
    try:
        metadata = get_filtered_data(url)
        if "error" in metadata:
            print("Error fetching metadata:", metadata["error"])
        else:
            print("Metadata fetched successfully.")
            return metadata
    except SpotifyInvalidUrlException as e:
        print("Invalid URL:", str(e))
    except Exception as e:
        print("An error occurred while fetching metadata:", str(e))


def fetch_tracks(url):
    if not url:
        print('Warning: Please enter a Spotify URL.')
        return

    try:
        print('Just a moment. Fetching metadata...')

        metadata = get_metadata(url)
        on_metadata_fetched(metadata)

    except Exception as e:
        print(f'Error: Failed to start metadata fetch: {str(e)}')


def on_metadata_fetched(metadata):
    try:
        url_info = parse_uri(config.url)

        if url_info["type"] == "track":
            handle_track_metadata(metadata["track"])
        elif url_info["type"] == "album":
            handle_album_metadata(metadata)
        elif url_info["type"] == "playlist":
            handle_playlist_metadata(metadata)

    except Exception as e:
        print(f'Error: {str(e)}')


def handle_track_metadata(track_data):
    track_id = track_data["external_urls"].split("/")[-1]

    if any(t.id == track_id for t in config.tracks):
        return

    track = Track(
        external_urls=track_data["external_urls"],
        title=track_data["name"],
        artists=track_data["artists"],
        album=track_data["album_name"],
        track_number=1,
        duration_ms=track_data.get("duration_ms", 0),
        id=track_id,
        isrc=track_data.get("isrc", "")
    )

    config.tracks = [track]
    config.is_single_track = True
    config.is_album = config.is_playlist = False
    config.album_or_playlist_name = f"{config.tracks[0].title} - {config.tracks[0].artists}"


def handle_album_metadata(album_data):
    config.album_or_playlist_name = album_data["album_info"]["name"]

    for track in album_data["track_list"]:
        track_id = track["external_urls"].split("/")[-1]

        if any(t.id == track_id for t in config.tracks):
            continue

        config.tracks.append(Track(
            external_urls=track["external_urls"],
            title=track["name"],
            artists=track["artists"],
            album=config.album_or_playlist_name,
            track_number=track["track_number"],
            duration_ms=track.get("duration_ms", 0),
            id=track_id,
            isrc=track.get("isrc", "")
        ))

    config.is_album = True
    config.is_playlist = config.is_single_track = False


def handle_playlist_metadata(playlist_data):
    config.album_or_playlist_name = playlist_data["playlist_info"]["owner"]["name"]

    for track in playlist_data["track_list"]:
        track_id = track["external_urls"].split("/")[-1]

        if any(t.id == track_id for t in config.tracks):
            continue

        config.tracks.append(Track(
            external_urls=track["external_urls"],
            title=track["name"],
            artists=track["artists"],
            album=track["album_name"],
            track_number=track.get("track_number", len(config.tracks) + 1),
            duration_ms=track.get("duration_ms", 0),
            id=track_id,
            isrc=track.get("isrc", "")
        ))

    config.is_playlist = True
    config.is_album = config.is_single_track = False


def download_tracks(indices):
    raw_outpath = config.output_dir
    outpath = os.path.normpath(raw_outpath)
    if not os.path.exists(outpath):
        print('Warning: Invalid output directory. Please check if the folder exists.')
        return

    tracks_to_download = config.tracks if config.is_single_track else [config.tracks[i] for i in indices]

    if config.is_album or config.is_playlist:
        name = config.album_or_playlist_name.strip()
        folder_name = re.sub(r'[<>:"/\\|?*]', '_', name)
        outpath = os.path.join(outpath, folder_name)
        os.makedirs(outpath, exist_ok=True)

    try:
        start_download_worker(tracks_to_download, outpath)
    except Exception as e:
        print(f"Error: An error occurred while starting the download: {str(e)}")


def start_download_worker(tracks_to_download, outpath):
    config.worker = DownloadWorker(
        tracks_to_download,
        outpath,
        config.is_single_track,
        config.is_album,
        config.is_playlist,
        config.album_or_playlist_name,
        config.filename_format,
        config.use_track_numbers,
        config.use_artist_subfolders,
        config.use_album_subfolders,
        config.service,
    )
    config.worker.run()


def on_download_finished(success, message, failed_tracks, total_elapsed=None):
    if success:
        print(f"\n=======================================")
        print(f"\nStatus: {message}")
        if failed_tracks:
            print("\nFailed downloads:")
            for title, artists, error in failed_tracks:
                print(f"• {title} - {artists}")
                print(f"  Error: {error}\n")
    else:
        print(f"Error: {message}")

    if total_elapsed is not None:
        print(f"\nElapsed time for this download loop: {format_seconds(total_elapsed)}")

    if config.loop is not None:
        print(f"\nDownload starting again in: {format_minutes(config.loop)}")
        print(f"\n=======================================")
        time.sleep(config.loop * 60)
        fetch_tracks(config.url)
        download_tracks(range(len(config.tracks)))


def update_progress(message):
    print(message)


def format_minutes(minutes):
    if minutes < 60:
        return f"{minutes} minutes"
    elif minutes < 1440:
        hours = minutes // 60
        mins = minutes % 60
        return f"{hours} hours {mins} minutes"
    else:
        days = minutes // 1440
        hours = (minutes % 1440) // 60
        mins = minutes % 60
        return f"{days} days {hours} hours {mins} minutes"

def format_seconds(seconds: float) -> str:
    seconds = int(round(seconds))

    days, rem = divmod(seconds, 86400)
    hrs, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hrs:
        parts.append(f"{hrs}h")
    if mins:
        parts.append(f"{mins}m")
    if secs or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


class DownloadWorker:
    def __init__(self, tracks, outpath, is_single_track=False, is_album=False, is_playlist=False,
                 album_or_playlist_name='', filename_format='title_artist', use_track_numbers=True,
                 use_artist_subfolders=False, use_album_subfolders=False, services=["tidal"]):
        super().__init__()
        self.tracks = tracks
        self.outpath = outpath
        self.is_single_track = is_single_track
        self.is_album = is_album
        self.is_playlist = is_playlist
        self.album_or_playlist_name = album_or_playlist_name
        self.filename_format = filename_format
        self.use_track_numbers = use_track_numbers
        self.use_artist_subfolders = use_artist_subfolders
        self.use_album_subfolders = use_album_subfolders
        self.services = services
        self.failed_tracks = []

    def get_formatted_filename(self, track):
        if self.filename_format == "artist_title":
            filename = f"{track.artists} - {track.title}.flac"
        elif self.filename_format == "title_only":
            filename = f"{track.title}.flac"
        else:
            filename = f"{track.title} - {track.artists}.flac"
        return re.sub(r'[<>:"/\\|?*]', lambda m: "'" if m.group() == '"' else '_', filename)

    def run(self):
        try:

            total_tracks = len(self.tracks)

            start = time.perf_counter()

            def progress_update(current, total):
                if total <= 0:
                    update_progress("Processing metadata...")

            for i, track in enumerate(self.tracks):

                if track.downloaded:
                    continue

                update_progress(f"[{i + 1}/{total_tracks}] Starting download: {track.title} - {track.artists}")

                if self.is_playlist:
                    track_outpath = self.outpath

                    if self.use_artist_subfolders:
                        artist_name = track.artists.split(", ")[0] if ", " in track.artists else track.artists
                        artist_folder = re.sub(r'[<>:"/\\|?*]', lambda m: "'" if m.group() == "\"" else "_",
                                               artist_name)
                        track_outpath = os.path.join(track_outpath, artist_folder)

                    if self.use_album_subfolders:
                        album_folder = re.sub(r'[<>:"/\\|?*]', lambda m: "'" if m.group() == "\"" else "_", track.album)
                        track_outpath = os.path.join(track_outpath, album_folder)

                    os.makedirs(track_outpath, exist_ok=True)

                else:
                    track_outpath = self.outpath

                if (self.is_album or self.is_playlist) and self.use_track_numbers:
                    new_filename = f"{track.track_number:02d} - {self.get_formatted_filename(track)}"
                else:
                    new_filename = self.get_formatted_filename(track)

                new_filename = re.sub(r'[<>:"/\\|?*]', lambda m: "'" if m.group() == "\"" else "_", new_filename)
                new_filepath = os.path.join(track_outpath, new_filename)

                if os.path.exists(new_filepath) and os.path.getsize(new_filepath) > 0:
                    update_progress(f"File already exists: {new_filename}. Skipping download.")
                    track.downloaded = True
                    continue

                download_success = False
                last_error = None

                for svc in self.services:
                    update_progress(f"Trying service: {svc}")

                    if svc == "tidal":
                        downloader = TidalDownloader()
                    elif svc == "deezer":
                        downloader = DeezerDownloader()
                    elif svc == "qobuz":
                        downloader = QobuzDownloader()
                    else:
                        downloader = TidalDownloader()

                    downloader.set_progress_callback(progress_update)

                    try:
                        if not track.isrc:
                            raise Exception("No ISRC available")

                        if svc == "tidal":
                            update_progress(
                                f"Searching and downloading from Tidal for ISRC: {track.isrc} - {track.title} - {track.artists}"
                            )

                            result = downloader.download(
                                query=f"{track.title} {track.artists}",
                                isrc=track.isrc,
                                output_dir=track_outpath,
                                quality="LOSSLESS",
                            )

                            if isinstance(result, str) and os.path.exists(result):
                                downloaded_file = result

                            elif isinstance(result, dict) and result.get("success") == False:
                                if result.get("error") == "Download stopped by user":
                                    update_progress(f"Download stopped by user for: {track.title}")
                                    return
                                raise Exception(result.get("error", "Tidal download failed"))

                            elif isinstance(result, dict) and result.get("status") in ("all_skipped", "skipped_exists"):
                                downloaded_file = new_filepath

                            else:
                                raise Exception(f"Unexpected Tidal result: {result}")

                        elif svc == "deezer":
                            update_progress(f"Downloading from Deezer with ISRC: {track.isrc}")

                            ok = asyncio.run(downloader.download_by_isrc(track.isrc, track_outpath))

                            if not ok:
                                raise Exception("Deezer download failed")

                            import glob
                            flac_files = glob.glob(os.path.join(track_outpath, "*.flac"))
                            if not flac_files:
                                raise Exception("No FLAC file found after Deezer download")

                            downloaded_file = max(flac_files, key=os.path.getctime)

                        elif svc == "qobuz":
                            update_progress(f"Downloading from Qobuz with ISRC: {track.isrc}")
                            format_map = {
                                "title_artist": "title-artist",
                                "artist_title": "artist-title",
                                "title_only": "title",
                            }
                            qb_format = format_map.get(self.filename_format, self.filename_format.replace("_", "-"))
                            downloaded_file = downloader.download_by_isrc(
                                isrc=track.isrc,
                                output_dir=track_outpath,
                                quality="LOSSLESS",
                                filename_format=qb_format,
                                include_track_number=self.use_track_numbers,
                                position=track.track_number or i + 1,
                                spotify_track_name=track.title,
                                spotify_artist_name=track.artists,
                                spotify_album_name=track.album,
                                use_album_track_number=self.use_track_numbers,
                            )

                        else:
                            track_id = track.id
                            update_progress(f"Getting track info for ID: {track_id} from {svc}")

                            try:
                                loop = asyncio.get_event_loop()
                                if loop.is_closed():
                                    loop = asyncio.new_event_loop()
                                    asyncio.set_event_loop(loop)
                            except RuntimeError:
                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)

                            metadata = loop.run_until_complete(
                                downloader.get_track_info(track_id, svc)
                            )

                            downloaded_file = downloader.download(metadata, track_outpath)

                        if downloaded_file and os.path.exists(downloaded_file):
                            if downloaded_file != new_filepath:
                                try:
                                    os.rename(downloaded_file, new_filepath)
                                    update_progress(f"File renamed to: {new_filename}")
                                except OSError as e:
                                    update_progress(
                                        f"[X] Warning: Could not rename file {downloaded_file} → {new_filepath}: {e}"
                                    )
                            update_progress(f"Successfully downloaded using: {svc}")
                            track.downloaded = True
                            download_success = True
                            break

                        else:
                            raise Exception("Downloaded file missing or invalid")

                    except Exception as e:
                        last_error = str(e)
                        update_progress(f"[X] {svc} failed: {e}")
                        continue

                if not download_success:
                    self.failed_tracks.append((track.title, track.artists, last_error))
                    update_progress(f"[X] Failed all services for: {track.title}")
                    continue

            total_elapsed = time.perf_counter() - start

            msg = "Download completed!"
            if self.failed_tracks:
                msg += f"\n\nFailed downloads: {len(self.failed_tracks)}"

            on_download_finished(True, msg, self.failed_tracks, total_elapsed)

        except Exception as e:
            total_elapsed = time.perf_counter() - start
            on_download_finished(False, str(e), self.failed_tracks, total_elapsed)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="Spotify URL")
    parser.add_argument("output_dir", help="Output directory")
    parser.add_argument(
        "--service",
        choices=["tidal", "deezer", "qobuz"],
        nargs="+",
        default=["tidal"],
        help="One or more services to try in order (e.g. --service tidal deezer qobuz)",
    )
    parser.add_argument("--filename-format", choices=["title_artist","artist_title","title_only"], default="title_artist")
    parser.add_argument("--use-track-numbers", action="store_true")
    parser.add_argument("--use-artist-subfolders", action="store_true")
    parser.add_argument("--use-album-subfolders", action="store_true")
    parser.add_argument("--loop", type=int, help="Loop delay in minutes")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    config = Config(**vars(args))

    try:
        fetch_tracks(config.url)
        download_tracks(range(len(config.tracks)))

    except KeyboardInterrupt:
        print("\nDownload stopped.")
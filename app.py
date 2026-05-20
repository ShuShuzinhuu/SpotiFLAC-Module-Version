import webview
import threading
import json
import os
import logging

# ── Default download folder ───────────────────────────────────────────────────
DEFAULT_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Music", "SpotiFLAC")

# ── Logging handler → UI ─────────────────────────────────────────────────────
class UILogHandler(logging.Handler):
    """Captures SpotiFLAC module logs and forwards them to the UI."""
    def __init__(self, api):
        super().__init__()
        self.api = api

    def emit(self, record):
        try:
            level = record.levelname
            msg   = self.format(record)
            ltype = "error" if level == "ERROR" else ("info" if level == "INFO" else "")
            self.api.log(msg, ltype)
        except Exception:
            pass


class SpotiFLAC_API:
    def __init__(self):
        self._window        = None  # Changed to private to prevent PyWebView inspection recursion
        self._stop_anim     = threading.Event()
        self._anim_thread   = None
        self.download_dir   = DEFAULT_DOWNLOAD_DIR
        self.current_tracks = []
        self.current_url    = ""

    def set_window(self, window):
        self._window = window
        self._on_loaded()

    def _on_loaded(self):
        self.log("Python Backend connected.", "info")
        self.log(f"Default download folder: {self.download_dir}", "info")

    # ── UI communication ──────────────────────────────────────────────────────

    def log(self, message, type=""):
        safe = json.dumps(str(message))
        safe_type = json.dumps(type)
        try:
            if self._window:
                self._window.evaluate_js(f"window.app_log({safe}, {safe_type});")
        except Exception:
            pass

    def set_progress(self, pct, label=""):
        safe_label = json.dumps(label)
        try:
            if self._window:
                self._window.evaluate_js(f"window.app_set_progress({pct}, {safe_label});")
        except Exception:
            pass

    def set_metadata(self, title, artist, cover="", quality="FLAC"):
        data = json.dumps({"title": title, "artist": artist,
                           "cover": cover, "quality": quality})
        try:
            if self._window:
                self._window.evaluate_js(f"window.app_set_metadata({data});")
        except Exception:
            pass

    # ── Window and folder controls ────────────────────────────────────────────

    def WindowMinimise(self):
        if webview.windows:
            threading.Thread(target=webview.windows[0].minimize, daemon=True).start()

    def WindowToggleMaximise(self):
        if webview.windows:
            threading.Thread(target=webview.windows[0].toggle_fullscreen, daemon=True).start()

    def Quit(self):
        if webview.windows:
            threading.Thread(target=webview.windows[0].destroy, daemon=True).start()

    def choose_folder(self):
        """Opens the folder dialog to choose the download directory."""
        if result and len(result) > 0:
            self.download_dir = result[0]
            self.log(f"Download folder changed: {self.download_dir}", "ok")

    # ── Phase 1: Metadata and track lookup ───────────────────────────────────

    def fetch_metadata(self, url, include_featuring=False):
        self.current_url = url
        threading.Thread(
            target=self._fetch_metadata_task,
            args=(url, include_featuring),
            daemon=True,
        ).start()

    def _fetch_metadata_task(self, url, include_featuring=False):
        try:
            self.set_progress(15, "Fetching metadata…")
            self.log(f"Analysing URL: {url}", "info")

            if "tidal.com" in url:
                from SpotiFLAC.providers.tidal_metadata import TidalMetadataClient
                client = TidalMetadataClient()
            elif "music.apple.com" in url:
                from SpotiFLAC.providers.apple_music_metadata import AppleMusicMetadataClient
                client = AppleMusicMetadataClient()
            else:
                from SpotiFLAC.providers.spotify_metadata import SpotifyMetadataClient
                client = SpotifyMetadataClient()

            collection_name, tracks = client.get_url(url, include_featuring=include_featuring)

            if not tracks:
                self.log("No tracks found at this URL.", "error")
                return

            self.current_tracks = tracks
            track_data = []

            for i, t in enumerate(tracks):
                track_data.append({
                    "index": i,
                    "title": getattr(t, 'title', f'Track {i+1}'),
                    "artist": getattr(t, 'artists', '')
                })

            badge = f"FLAC — {len(tracks)} tracks" if len(tracks) > 1 else "FLAC"
            self.set_metadata(tracks[0].title, tracks[0].artists, tracks[0].cover_url or "", badge)

            self.log(f"Found: {collection_name} ({len(tracks)} track(s)). Choose the songs to download.", "ok")
            self.set_progress(100, "Pronto per il download.")

        except Exception as e:
            self.log(f"Error fetching metadata: {str(e)}", "error")
            self.set_progress(0, "Error.")

    # ── Phase 2: Actual download ──────────────────────────────────────────────

    def download_tracks(self, selected_indices, config):
        """Starts the download in the background based on selected indices and settings."""
        threading.Thread(target=self._download_task, args=(selected_indices, config), daemon=True).start()

    def _download_task(self, selected_indices, config):
        sf_logger = logging.getLogger("SpotiFLAC")
        handler   = UILogHandler(self)
        handler.setFormatter(logging.Formatter("[%(name)s] %(message)s"))
        sf_logger.addHandler(handler)

        # Log level
        log_level_str = config.get("log_level", "INFO")
        current_log_level = logging.DEBUG if log_level_str == "DEBUG" else logging.INFO
        sf_logger.setLevel(current_log_level)

        try:
            os.makedirs(self.download_dir, exist_ok=True)

            # ── Extract all options from UI ───────────────────────────────────
            quality              = config.get("quality", "LOSSLESS")
            allow_fallback       = config.get("allow_fallback", True)
            embed_lyrics         = config.get("lyrics", True)
            enrich_metadata      = config.get("enrich_metadata", True)
            services             = config.get("services", ["tidal", "qobuz", "deezer"])

            filename_format      = config.get("filename_format", "{title} - {artist}")
            use_track_numbers    = config.get("use_track_numbers", False)
            use_album_track_numbers = config.get("use_album_track_numbers", False)
            use_artist_subfolders = config.get("use_artist_subfolders", False)
            use_album_subfolders  = config.get("use_album_subfolders", False)
            first_artist_only    = config.get("first_artist_only", False)
            include_featuring    = config.get("include_featuring", False)

            lyrics_providers     = config.get("lyrics_providers") or ["spotify", "apple", "musixmatch", "lrclib", "amazon"]
            enrich_providers     = config.get("enrich_providers") or ["deezer", "apple", "qobuz", "tidal", "soundcloud"]

            track_max_retries    = int(config.get("track_max_retries", 0))
            post_download_action = config.get("post_download_action", "none")
            post_download_command = config.get("post_download_command", "")
            qobuz_token          = config.get("qobuz_token") or None

            loop_val             = config.get("loop", None)
            loop_minutes         = int(loop_val) if loop_val else None

            # ── Validate services ─────────────────────────────────────────────
            if not services:
                self.log("Error: you must select at least one service/source.", "error")
                return

            # ── Build list of URLs to download ───────────────────────────────
            if len(selected_indices) == len(self.current_tracks):
                urls_to_download = [self.current_url]
                self.log("Starting download of the entire album/playlist…", "info")
            else:
                urls_to_download = []
                for i in selected_indices:
                    t = self.current_tracks[i]
                    t_url = getattr(t, 'url', None) or getattr(t, 'link', None)
                    if not t_url and getattr(t, 'track_id', None):
                        if "spotify" in self.current_url:
                            t_url = f"https://open.spotify.com/track/{t.track_id}"
                        elif "tidal" in self.current_url:
                            t_url = f"https://tidal.com/browse/track/{t.track_id}"
                        elif "apple" in self.current_url:
                            t_url = f"https://music.apple.com/track/{t.track_id}"
                    if t_url:
                        urls_to_download.append(t_url)
                    else:
                        self.log(f"Could not resolve URL for '{t.title}'. It will be skipped.", "error")

            if not urls_to_download:
                self.log("No valid URLs to download.", "error")
                return

            # ── Progress bar animation + download ────────────────────────────
            self.set_progress(25, "Initialising services…")
            self._start_progress_animation(25, 95, f"Downloading ({quality})…")

            from SpotiFLAC import SpotiFLAC

            for u in urls_to_download:
                SpotiFLAC(
                    url                     = u,
                    output_dir              = self.download_dir,
                    services                = services,
                    quality                 = quality,
                    allow_fallback          = allow_fallback,
                    filename_format         = filename_format,
                    use_track_numbers       = use_track_numbers,
                    use_album_track_numbers = use_album_track_numbers,
                    use_artist_subfolders   = use_artist_subfolders,
                    use_album_subfolders    = use_album_subfolders,
                    first_artist_only       = first_artist_only,
                    include_featuring       = include_featuring,
                    embed_lyrics            = embed_lyrics,
                    lyrics_providers        = lyrics_providers,
                    enrich_metadata         = enrich_metadata,
                    enrich_providers        = enrich_providers,
                    qobuz_token             = qobuz_token,
                    track_max_retries       = track_max_retries,
                    post_download_action    = post_download_action,
                    post_download_command   = post_download_command,
                    log_level               = current_log_level,
                    loop                    = loop_minutes,
                )

            self._stop_progress_animation()
            self.set_progress(100, "Complete!")
            self.log(f"All selected tracks saved to: {self.download_dir}", "ok")

        except Exception as e:
            self._stop_progress_animation()
            self.log(f"Download error: {str(e)}", "error")
            self.set_progress(0, "Error.")
        finally:
            sf_logger.removeHandler(handler)

    # ── Health Check ─────────────────────────────────────────────────────────

    def run_health_check(self, services):
        threading.Thread(
            target=self._health_check_task,
            args=(services,),
            daemon=True,
        ).start()

    def _health_check_task(self, services):
        try:
            import importlib
            hc_module = importlib.import_module("SpotiFLAC.health_check")
            hc_run = getattr(hc_module, "run_health_check")
            self.log(f"Health check started for: {', '.join(services)}", "info")
            results = hc_run(services)
            data = [
                {
                    "provider": r.provider,
                    "method":   r.method,
                    "url":      r.url,
                    "ok":       r.ok,
                    "latency":  round(r.latency) if r.latency >= 0 else -1,
                    "detail":   r.detail,
                }
                for r in results
            ]
            ok_providers = [r.provider for r in results if r.ok]
            self.log(
                f"Health check complete — {len([r for r in results if r.ok])}/{len(results)} endpoints OK.",
                "ok" if ok_providers else "error",
            )
        except ImportError:
            self.log("health_check module not found. Make sure SpotiFLAC is installed.", "error")
        except Exception as e:
            self.log(f"Health check error: {str(e)}", "error")

    # ── Progress bar animation ────────────────────────────────────────────────

    def _start_progress_animation(self, start_pct, end_pct, label):
        self._stop_anim.clear()
        import time
        def _animate():
            pct, step = float(start_pct), (end_pct - start_pct) / 180
            while not self._stop_anim.is_set() and pct < end_pct:
                self.set_progress(int(pct), label)
                pct += step
                time.sleep(1)
        self._anim_thread = threading.Thread(target=_animate, daemon=True)
        self._anim_thread.start()

    def _stop_progress_animation(self):
        self._stop_anim.set()
        if self._anim_thread:
            self._anim_thread.join(timeout=2)
        self._anim_thread = None


if __name__ == '__main__':
    api = SpotiFLAC_API()
    html_path = os.path.join(os.path.dirname(__file__), 'index.html')
    window = webview.create_window(
        'SpotiFLAC', url=html_path, js_api=api,
        width=750, height=730, min_size=(650, 580),
        frameless=True, background_color='#0a0a0a'
    )
    api.set_window(window)
    webview.start(http_server=True)
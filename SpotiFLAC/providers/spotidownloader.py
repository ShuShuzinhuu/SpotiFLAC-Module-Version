###########################################################################
#Spotidownloader return
####################

from __future__ import annotations

import logging
import threading
import time

from ..core.models import TrackMetadata, DownloadResult
from ..core.errors import SpotiflacError, AuthError, TrackNotFoundError
from ..core.tagger import embed_metadata
from .base import BaseProvider

logger = logging.getLogger(__name__)

_API_BASE = "https://api.spotidownloader.com"
_ORIGIN   = "https://spotidownloader.com"
_SESSION_URL = f"{_API_BASE}/session"

class SpotiDownloaderProvider(BaseProvider):
    name = "spotidownloader"

    _token: str = ""
    _token_exp: float = 0.0
    _bootstrap_token: str = ""
    _lock = threading.Lock()

    def __init__(self, timeout_s: int = 30):
        super().__init__(timeout_s=timeout_s)

    # ---------------------------------------------------------
    # BOOTSTRAP TOKEN SCRAPER (fallback real do site)
    # ---------------------------------------------------------
    
    def _fetch_bootstrap_token(self) -> str:
        try:
            resp = self._http.post_json(
                _SESSION_URL,
                json={"token": self._bootstrap_token or "init"},
                headers={
                    "Origin": _ORIGIN,
                    "Referer": _ORIGIN,
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                },
            )
        except Exception as exc:
            raise AuthError(self.name, f"session request failed: {exc}")

        if not resp or not resp.get("success"):
            raise AuthError(self.name, "bootstrap session failed")

        token = resp.get("token")
        if not token:
            raise AuthError(self.name, "no token in session response")

        return token

    # ---------------------------------------------------------
    # TOKEN CACHE
    # ---------------------------------------------------------

    def _get_token(self) -> str:
        now = time.monotonic()

        if self._token and now < self._token_exp:
            return self._token

        with self._lock:
            now = time.monotonic()

            if self._token and now < self._token_exp:
                return self._token

            token = self._fetch_bootstrap_token()

            self._token = token
            self._token_exp = now + (55 * 60)

            logger.info("[%s] token atualizado via /session", self.name)
            return token

    def invalidate_token(self):
        with self._lock:
            self._token = ""
            self._token_exp = 0.0

    # ---------------------------------------------------------
    # RESOLVE FLAC
    # ---------------------------------------------------------

    def _get_flac_url(self, spotify_id: str, token: str) -> str:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Origin": _ORIGIN,
            "Referer": f"{_ORIGIN}/",
        }

        payload = {"id": spotify_id, "flac": True}

        try:
            data = self._http.post_json(
                f"{_API_BASE}/download",
                json=payload,
                headers=headers,
            )
        except Exception:
            self.invalidate_token()
            raise

        if not data or not data.get("success"):
            raise TrackNotFoundError(self.name, spotify_id)

        flac = data.get("linkFlac")
        normal = data.get("link")

        for url in (flac, normal):
            if url and ".flac" in url:
                return url

        raise TrackNotFoundError(self.name, "no FLAC available")

    # ---------------------------------------------------------
    # DOWNLOAD PIPELINE (FIX CRÍTICO)
    # ---------------------------------------------------------

    def download_track(
        self,
        metadata: TrackMetadata,
        output_dir: str,
        *,
        filename_format: str = "{title} - {artist}",
        position: int = 1,
        include_track_num: bool = False,
        use_album_track_num: bool = False,
        first_artist_only: bool = False,
        allow_fallback: bool = True,
        quality: str = "LOSSLESS",
        embed_lyrics: bool = False,
        lyrics_providers=None,
        lyrics_spotify_token: str = "",
        enrich_metadata: bool = False,
        enrich_providers=None,
        **kwargs,   # <- FIX: evita crash de "unexpected keyword"
    ) -> DownloadResult:

        try:
            dest = self._build_output_path(
                metadata,
                output_dir,
                filename_format,
                position,
                include_track_num,
                use_album_track_num,
                first_artist_only,
            )

            if self._file_exists(dest):
                return DownloadResult.ok(self.name, str(dest))

            token = self._get_token()
            url = self._get_flac_url(metadata.id, token)

            self._http.stream_to_file(
                url,
                str(dest),
                progress_cb=self._progress_cb,
                extra_headers={
                    "Authorization": f"Bearer {token}",
                    "Origin": _ORIGIN,
                    "Referer": f"{_ORIGIN}/",
                },
            )

            embed_metadata(
                dest,
                metadata,
                first_artist_only=first_artist_only,
                cover_url=metadata.cover_url,
                embed_lyrics=embed_lyrics,
                lyrics_providers=lyrics_providers,
                lyrics_spotify_token=lyrics_spotify_token,
                enrich=enrich_metadata,
                enrich_providers=enrich_providers,
            )

            return DownloadResult.ok(self.name, str(dest))

        except SpotiflacError as exc:
            logger.error("[%s] %s", self.name, exc)
            return DownloadResult.fail(self.name, str(exc))

        except Exception as exc:
            logger.exception("[%s] crash", self.name)
            return DownloadResult.fail(self.name, f"unexpected: {exc}")
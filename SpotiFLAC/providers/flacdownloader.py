from __future__ import annotations

import logging
import os
import random
import time
import urllib.parse
from typing import Any

import httpx

from .base import BaseProvider
from ..core.console import print_source_banner
from ..core.http import NetworkManager, RetryConfig
from ..core.models import TrackMetadata, DownloadResult
from ..core.errors import SpotiflacError, ErrorKind, TrackNotFoundError
from ..core.tagger import embed_metadata, EmbedOptions, _print_mb_summary
from ..core.download_validation import validate_downloaded_track
from ..core.musicbrainz import AsyncMBFetch, mb_result_to_tags

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_API_BASE   = "https://flacdownloader.com/flac"
_DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/149.0.0.0 Safari/537.36"
)

# Header "magico" per bypassare i check del server WAF di flacdownloader
_AUTH_HEADERS = {
    "Accept": "*/*",
    "User-Agent": _DEFAULT_UA,
    "Content-Type": "application/json",
    "X-Download-Access": "l@p*gute)77=g5clebcp4lz#=x%(*rwg+ku0_)bh=&%6wg!a"
}

_MAX_RETRIES        = 2
_RETRY_BASE_DELAY_S = 1.0
_RETRY_MAX_DELAY_S  = 16.0
_RETRY_JITTER       = 0.25


def _backoff_delay(attempt: int) -> float:
    """Calcola il delay per il retry esponenziale."""
    base = min(_RETRY_BASE_DELAY_S * (2 ** (attempt - 1)), _RETRY_MAX_DELAY_S)
    jitter = base * _RETRY_JITTER * (2 * random.random() - 1)
    return max(0.1, base + jitter)


# ---------------------------------------------------------------------------
# FlacDownloaderProvider
# ---------------------------------------------------------------------------

class FlacDownloaderProvider(BaseProvider):
    """
    Provider per FlacDownloader (flacdownloader.com).
    Adotta i pattern avanzati di backoff, validazione ciclo di download, 
    ed espulsione dei file FLAC corrotti/falsi allineandosi allo standard QobuzProvider.
    """

    name = "flacdownloader"

    def __init__(self, timeout_s: int = 30) -> None:
        super().__init__(
            timeout_s=timeout_s,
            retry=RetryConfig(max_attempts=2),
            headers=_AUTH_HEADERS.copy()
        )
        # Client HTTP ottimizzato (sfrutta Connection Pooling)
        self._session = NetworkManager.get_sync_client()

    # ------------------------------------------------------------------
    # Helper Interni API
    # ------------------------------------------------------------------

    def _get_deezer_id(self, metadata: TrackMetadata) -> str:
        """
        Risolve l'ID Deezer richiesto da flacdownloader tramite ISRC o testo.
        """
        if metadata.extra_info and "deezer_id" in metadata.extra_info:
            return str(metadata.extra_info["deezer_id"])

        # Ricerca per ISRC (molto precisa)
        if metadata.isrc:
            try:
                resp = self._session.get(f"https://api.deezer.com/track/isrc:{metadata.isrc}", timeout=8)
                if resp.status_code == 200:
                    data = resp.json()
                    if "id" in data:
                        return str(data["id"])
            except Exception as exc:
                logger.debug("[%s] Ricerca Deezer ISRC %s fallita: %s", self.name, metadata.isrc, exc)

        # Fallback Ricerca Testuale
        query = f"{metadata.title} {metadata.first_artist}".strip()
        try:
            resp = self._session.get("https://api.deezer.com/search", params={"q": query, "limit": 1}, timeout=8)
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                if data:
                    return str(data[0]["id"])
        except Exception as exc:
            logger.debug("[%s] Ricerca testuale Deezer fallita per '%s': %s", self.name, query, exc)

        return ""

    def _get_download_url(self, deezer_id: str) -> str:
        """
        Richiede il token temporaneo con Retry Esponenziale e genera il link di download.
        """
        url_token = f"{_API_BASE}/download-token?t={deezer_id}&f=FLAC"
        last_err: Exception = RuntimeError("Nessun tentativo effettuato")

        for attempt in range(_MAX_RETRIES + 1):
            if attempt > 0:
                delay = _backoff_delay(attempt)
                logger.debug("[%s] Retry %d/%d token fetch dopo %.2fs", self.name, attempt, _MAX_RETRIES, delay)
                time.sleep(delay)

            try:
                resp = self._session.get(url_token, headers=_AUTH_HEADERS, timeout=12)
                resp.raise_for_status()
                data = resp.json()
                
                token = data.get("token")
                expires = data.get("expires")

                if token and expires:
                    encoded_token = urllib.parse.quote_plus(str(token))
                    return f"{_API_BASE}/download?t={deezer_id}&f=FLAC&token={encoded_token}&expires={expires}"
                
                raise RuntimeError("Token o Expires mancanti dal payload JSON")
                
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    last_err = RuntimeError("Rate limited (HTTP 429)")
                    continue
                last_err = exc
                break  # Errori drastici, interrompe 
            except Exception as exc:
                last_err = exc
                continue

        raise SpotiflacError(ErrorKind.UNAVAILABLE, f"Impossibile ottenere il token: {last_err}", self.name)

    # ------------------------------------------------------------------
    # Download Core Logic (Allineato a qobuz.py)
    # ------------------------------------------------------------------

    def download_track(
            self,
            metadata:            TrackMetadata,
            output_dir:          str,
            *,
            filename_format:     str              = "{title} - {artist}",
            position:            int              = 1,
            include_track_num:   bool             = False,
            use_album_track_num: bool             = False,
            first_artist_only:   bool             = False,
            allow_fallback:      bool             = True,
            embed_genre:         bool             = True,
            embed_lyrics:        bool             = False,
            lyrics_providers:    list[str] | None = None,
            enrich_metadata:     bool             = False,
            enrich_providers:    list[str] | None = None,
            is_album:            bool             = False,
            **kwargs:            Any,
    ) -> DownloadResult:

        try:
            # ── 1. Preparazione & ID Risoluzione
            deezer_id = self._get_deezer_id(metadata)
            if not deezer_id:
                raise TrackNotFoundError(self.name, f"Impossibile mappare la traccia all'ID Deezer: {metadata.title}")

            dest = self._build_output_path(
                metadata, output_dir, filename_format,
                position, include_track_num, use_album_track_num, first_artist_only,
                extension=".flac",
            )

            if self._file_exists(dest):
                return DownloadResult.skipped_result(self.name, str(dest), fmt="flac")

            # ── 2. Async MusicBrainz Tagger (Parallel Execution)
            mb_fetcher = None
            if (enrich_metadata or embed_genre) and metadata.isrc:
                mb_fetcher = AsyncMBFetch(metadata.isrc)

            # ── 3. Risoluzione Metadati MusicBrainz
            mb_tags: dict[str, str] = {}
            if mb_fetcher:
                res = mb_fetcher.future.result()
                mb_tags = mb_result_to_tags(res)
                _print_mb_summary(mb_tags)

            # ── 4. Setup Ciclo di Download Antistrappo
            expected_s = int(metadata.duration_seconds)
            valid = False
            last_err = None
            max_download_attempts = 3
            current_attempt = 0

            print_source_banner("flacdownloader", "flacdownloader.com", "FLAC Lossless")

            # ── 5. Ciclo While Not Valid
            while not valid and current_attempt < max_download_attempts:
                current_attempt += 1

                try:
                    dl_url = self._get_download_url(deezer_id)
                    logger.info("[%s] Downloading '%s' (ID=%s) [Try %d/%d]", 
                                self.name, metadata.title, deezer_id, current_attempt, max_download_attempts)

                    self._http.stream_to_file(dl_url, str(dest), self._progress_cb, extra_headers=_AUTH_HEADERS)

                    valid, err_msg = validate_downloaded_track(str(dest), expected_s)
                    if not valid:
                        logger.warning("[%s] File non valido scaricato: %s. Riprovo...", self.name, err_msg)
                        last_err = err_msg
                        if dest.exists():
                            try:
                                os.remove(str(dest))
                                logger.debug("[%s] Rimozione file invalido effettuata", self.name)
                            except OSError as e:
                                logger.error("[%s] Impossibile rimuovere file invalido: %s", self.name, e)
                        continue 

                    # Embedded Tag & Identificazione file Fake FLAC
                    opts = EmbedOptions(
                        first_artist_only  = first_artist_only,
                        cover_url          = metadata.cover_url,
                        extra_tags         = mb_tags,
                        embed_lyrics       = embed_lyrics,
                        lyrics_providers   = lyrics_providers or [],
                        enrich             = enrich_metadata,
                        enrich_providers   = enrich_providers,
                        is_album           = is_album,
                    )
                    
                    try:
                        embed_metadata(str(dest), metadata, opts, session=self._session)
                    except SpotiflacError as exc:
                        message = str(exc).lower()
                        if exc.kind == ErrorKind.FILE_IO and "not a valid flac file" in message:
                            logger.warning("[%s] FLAC header corrotto o finto. Eccezione: %s. Riprovo...", self.name, exc)
                            last_err = exc
                            valid = False
                            if dest.exists():
                                try:
                                    os.remove(str(dest))
                                except OSError as e:
                                    logger.error("[%s] Impossibile rimuovere file finto FLAC: %s", self.name, e)
                            continue 
                        raise 
                    break

                except SpotiflacError as exc:
                    last_err = exc
                    if current_attempt >= max_download_attempts:
                        raise exc

            if not valid:
                return DownloadResult.fail(self.name, f"Tutti i tentativi falliti. Ultimo errore: {last_err}")

            return DownloadResult.ok(self.name, str(dest), fmt="flac")

        except SpotiflacError as exc:
            logger.error("[%s] %s", self.name, exc)
            return DownloadResult.fail(self.name, str(exc))
        except Exception as exc:
            logger.exception("[%s] Errore imprevisto", self.name)
            return DownloadResult.fail(self.name, f"Errore imprevisto: {exc}")
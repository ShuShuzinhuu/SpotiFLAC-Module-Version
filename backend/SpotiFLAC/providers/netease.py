from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .base import BaseProvider
from ..core.http import NetworkManager
from ..core.models import TrackMetadata, DownloadResult
from ..core.errors import SpotiflacError, ErrorKind, TrackNotFoundError
from ..core.tagger import embed_metadata, EmbedOptions
from ..core.download_validation import validate_downloaded_track
from ..core.musicbrainz import AsyncMBFetch, mb_result_to_tags

logger = logging.getLogger(__name__)

_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

class NeteaseProvider(BaseProvider):
    name = "netease"

    def __init__(self, timeout_s: int = 30) -> None:
        super().__init__(timeout_s=timeout_s)
        self._session = NetworkManager.get_sync_client()
        self._session.headers.update({"User-Agent": _DEFAULT_UA})
        self.api_base = "https://music-api.gdstudio.xyz/api.php"

    def _search_track(self, title: str, artist: str) -> str | None:
        """Cerca la traccia su NetEase e restituisce l'ID."""
        query = quote(f"{title} {artist}")
        search_url = f"{self.api_base}?types=search&source=netease&name={query}"
        
        try:
            resp = self._session.get(search_url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            # GD Studio restituisce un array di risultati
            if isinstance(data, list) and len(data) > 0:
                first_result = data[0]
                return str(first_result.get("id"))
        except Exception as e:
            logger.debug(f"[netease] Errore durante la ricerca: {e}")
            
        return None

    def _get_download_url(self, track_id: str, quality: str) -> tuple[str, str]:
        """Ottiene il link al file audio."""
        # Mappiamo la qualità richiesta su quella supportata da GD Studio
        br = "999" if quality.upper() in ("LOSSLESS", "HI_RES", "HI_RES_LOSSLESS") else "320"
        
        url = f"{self.api_base}?types=url&source=netease&id={track_id}&br={br}"
        
        try:
            resp = self._session.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            dl_url = data.get("url")
            if not dl_url:
                raise SpotiflacError(ErrorKind.UNAVAILABLE, "NetEase non ha restituito un URL (possibile blocco VIP).", self.name)
                
            # Determiniamo l'estensione in base al link o al bitrate restituito
            actual_br = str(data.get("br", ""))
            ext = "flac" if actual_br in ("740", "999") or ".flac" in dl_url.lower() else "mp3"
            
            return dl_url, ext
        except SpotiflacError:
            raise
        except Exception as e:
            raise SpotiflacError(ErrorKind.NETWORK_ERROR, f"Errore API GD Studio: {e}", self.name)

    def download_track(
            self,
            metadata: TrackMetadata,
            output_dir: str,
            *,
            quality: str = "LOSSLESS",
            filename_format: str = "{title} - {artist}",
            position: int = 1,
            include_track_num: bool = False,
            use_album_track_num: bool = False,
            first_artist_only: bool = False,
            allow_fallback: bool = True,
            embed_lyrics: bool = False,
            lyrics_providers: list[str] | None = None,
            enrich_metadata: bool = False,
            enrich_providers: list[str] | None = None,
            qobuz_token: str | None = None,
            is_album: bool = False,
            **kwargs: Any,
    ) -> DownloadResult:

        try:
            # 1. Cerca l'ID della traccia
            track_id = self._search_track(metadata.title, metadata.artists)
            if not track_id:
                raise TrackNotFoundError(self.name, f"Traccia non trovata su NetEase: {metadata.title}")

            # 2. Ottieni l'URL di download
            dl_url, ext = self._get_download_url(track_id, quality)

            # 3. Costruisci il percorso di salvataggio
            dest = Path(self._build_output_path(
                metadata, output_dir, filename_format,
                position, include_track_num, use_album_track_num, first_artist_only,
                extension=f".{ext}"
            ))

            if dest.exists():
                return DownloadResult.skipped_result(self.name, str(dest), fmt=ext)

            # Eseguiamo il fetch da MusicBrainz per i metadati arricchiti in background
            mb_fetcher = AsyncMBFetch(metadata.isrc) if metadata.isrc else None

            # 4. Scarica il file
            logger.info(f"[netease] Scaricamento in corso ({ext})...")
            self._http.stream_to_file(dl_url, str(dest), self._progress_cb)

            # 5. Validazione
            expected_s = metadata.duration_ms // 1000
            valid, err_msg = validate_downloaded_track(str(dest), expected_s)
            if not valid:
                if dest.exists():
                    os.remove(str(dest))
                return DownloadResult.fail(self.name, f"File invalido: {err_msg}")

            # 6. Tagging (Inserimento metadati)
            mb_tags: dict[str, str] = {}
            if mb_fetcher:
                res = mb_fetcher.future.result()
                mb_tags = mb_result_to_tags(res)

            opts = EmbedOptions(
                first_artist_only=first_artist_only,
                cover_url=metadata.cover_url,
                extra_tags=mb_tags,
                embed_lyrics=embed_lyrics,
                lyrics_providers=lyrics_providers or [],
                enrich=enrich_metadata,
                enrich_providers=enrich_providers,
                enrich_qobuz_token=qobuz_token or "",
                is_album=is_album,
            )
            embed_metadata(str(dest), metadata, opts, session=self._session)

            return DownloadResult.ok(self.name, str(dest), fmt=ext)

        except SpotiflacError as exc:
            logger.error(f"[{self.name}] {exc}")
            return DownloadResult.fail(self.name, str(exc))
        except Exception as exc:
            logger.exception(f"[{self.name}] Errore inaspettato")
            return DownloadResult.fail(self.name, f"Inaspettato: {exc}")
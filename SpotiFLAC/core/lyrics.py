# SpotiFLAC/core/lyrics.py
"""
Multi-provider lyrics fetcher.

Ordine di tentativo (configurabile):
  1. Spotify Web  — testo sincronizzato LRC (richiede sp_dc cookie)
  2. Musixmatch   — testo sincronizzato / plain (token configurabile)
  3. Amazon Music — testo plain via afkar API
  4. LRCLIB       — testo sincronizzato / plain (nessuna auth)
"""
from __future__ import annotations

import logging
import time
import urllib.parse
from typing import Any

import requests

logger = logging.getLogger(__name__)

_LRCLIB          = "https://lrclib.net/api"
_MUSIXMATCH_BASE = "https://apic-desktop.musixmatch.com/ws/1.1"
_SPOTIFY_LYRICS  = "https://spclient.wg.spotify.com/color-lyrics/v2/track"
_AMAZON_API_BASE = "https://amzn.afkarxyz.qzz.io/api"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "Chrome/145.0.0.0 Safari/537.36"
)

_DEFAULT_PROVIDERS = ["spotify", "musixmatch", "amazon", "lrclib"]


# --------------------------------------------------------------------------- #
# Provider 1 — Spotify Web                                                     #
# --------------------------------------------------------------------------- #

def _fetch_spotify(
        track_id: str,
        sp_dc_token: str,
        timeout: int = 10,
) -> str:
    if not track_id or not sp_dc_token:
        return ""
    try:
        client_token = _spotify_client_token(sp_dc_token, timeout)
        if not client_token:
            return ""

        r = requests.get(
            f"{_SPOTIFY_LYRICS}/{track_id}",
            params={"format": "json", "market": "from_token"},
            headers={
                "Authorization": f"Bearer {client_token}",
                "App-Platform":  "WebPlayer",
                "User-Agent":    _UA,
            },
            timeout=timeout,
        )
        if r.status_code != 200:
            logger.debug("[lyrics/spotify] HTTP %s for track %s", r.status_code, track_id)
            return ""

        data  = r.json()
        lines = data.get("lyrics", {}).get("lines", [])
        if not lines:
            return ""

        sync_type = data.get("lyrics", {}).get("syncType", "")
        if sync_type == "LINE_SYNCED":
            lrc_lines = []
            for line in lines:
                ms   = int(line.get("startTimeMs", 0))
                m, s = divmod(ms // 1000, 60)
                cs   = (ms % 1000) // 10
                words = line.get("words", "")
                lrc_lines.append(f"[{m:02d}:{s:02d}.{cs:02d}]{words}")
            return "\n".join(lrc_lines)

        return "\n".join(line.get("words", "") for line in lines)

    except Exception as exc:
        logger.debug("[lyrics/spotify] %s", exc)
        return ""


def _spotify_client_token(sp_dc: str, timeout: int) -> str:
    """
    Scambia il cookie sp_dc con un Bearer token per le API client Spotify.

    FIX: integra spotify_totp.py come header aggiuntivo.
    Le versioni recenti di Spotify validano le richieste tramite TOTP;
    senza di esso il server risponde 401 anche con sp_dc valido.
    """
    # Genera il codice TOTP (spotify_totp.py era definito ma mai usato)
    totp_headers: dict[str, str] = {}
    try:
        from .spotify_totp import generate_spotify_totp
        totp_code, totp_version = generate_spotify_totp()
        if totp_code:
            totp_headers["Spotify-TOTP"]    = totp_code
            totp_headers["Spotify-TOTP-V2"] = f"{totp_code}:{totp_version}"
            logger.debug("[lyrics/spotify] TOTP v%d added to token request", totp_version)
    except Exception as exc:
        logger.debug("[lyrics/spotify] TOTP generation skipped: %s", exc)

    try:
        r = requests.get(
            "https://open.spotify.com/get_access_token",
            params={"reason": "transport", "productType": "web_player"},
            headers={
                "Cookie":     f"sp_dc={sp_dc}",
                "User-Agent": _UA,
                **totp_headers,
            },
            timeout=timeout,
        )
        if r.ok:
            return r.json().get("accessToken", "")
    except Exception as exc:
        logger.debug("[lyrics/spotify] token exchange failed: %s", exc)
    return ""


# --------------------------------------------------------------------------- #
# Provider 2 — Musixmatch                                                      #
# --------------------------------------------------------------------------- #

_MUSIXMATCH_DEFAULT_TOKEN = ""

def _fetch_musixmatch(
        track_name:  str,
        artist_name: str,
        album_name:  str = "",
        duration_s:  int = 0,
        token:       str = "",
        timeout:     int = 12,
) -> str:
    used_token = token or _MUSIXMATCH_DEFAULT_TOKEN
    if not used_token:
        return ""

    try:
        result = _musixmatch_subtitles(track_name, artist_name, album_name, duration_s, used_token, timeout)
        if result:
            return result
        return _musixmatch_plain(track_name, artist_name, used_token, timeout)
    except Exception as exc:
        logger.debug("[lyrics/musixmatch] %s", exc)
        return ""


def _musixmatch_base_params(token: str) -> dict:
    return {
        "format":    "json",
        "app_id":    "web-desktop-app-v1.0",
        "usertoken": token,
    }


def _musixmatch_subtitles(
        title:    str,
        artist:   str,
        album:    str,
        duration: int,
        token:    str,
        timeout:  int,
) -> str:
    params = {
        **_musixmatch_base_params(token),
        "namespace":      "lyrics_richsynced",
        "optional_calls": "track.richsync",
        "q_track":        title,
        "q_artist":       artist,
    }
    if album:    params["q_album"]    = album
    if duration: params["q_duration"] = duration

    r = requests.get(
        f"{_MUSIXMATCH_BASE}/macro.subtitles.get",
        params=params,
        headers={"User-Agent": _UA},
        timeout=timeout,
    )
    if not r.ok:
        return ""

    body  = r.json().get("message", {}).get("body", {})
    macro = body.get("macro_calls", {})

    richsync = (
        macro.get("track.richsync.get", {})
        .get("message", {})
        .get("body", {})
        .get("richsync", {})
    )
    if richsync.get("richsync_body"):
        return _richsync_to_lrc(richsync["richsync_body"])

    subtitle = (
        macro.get("track.subtitles.get", {})
        .get("message", {})
        .get("body", {})
        .get("subtitle_list", [{}])[0]
        .get("subtitle", {})
    )
    return subtitle.get("subtitle_body", "")


def _musixmatch_plain(title: str, artist: str, token: str, timeout: int) -> str:
    params = {
        **_musixmatch_base_params(token),
        "q_track":  title,
        "q_artist": artist,
    }
    r = requests.get(
        f"{_MUSIXMATCH_BASE}/matcher.lyrics.get",
        params=params,
        headers={"User-Agent": _UA},
        timeout=timeout,
    )
    if not r.ok:
        return ""
    return (
        r.json()
        .get("message", {})
        .get("body", {})
        .get("lyrics", {})
        .get("lyrics_body", "")
        .replace("******* This Lyrics is NOT for Commercial use *******", "")
        .strip()
    )


def _richsync_to_lrc(richsync_body: str) -> str:
    import json as _json
    try:
        lines = _json.loads(richsync_body)
        lrc: list[str] = []
        for entry in lines:
            ts  = float(entry.get("ts", 0))
            m   = int(ts // 60)
            s   = int(ts % 60)
            cs  = int((ts % 1) * 100)
            text = "".join(w.get("c", "") for w in entry.get("l", []))
            if text.strip():
                lrc.append(f"[{m:02d}:{s:02d}.{cs:02d}]{text}")
        return "\n".join(lrc)
    except Exception:
        return ""


# --------------------------------------------------------------------------- #
# Provider 3 — Amazon Music                                                    #
# --------------------------------------------------------------------------- #

def _fetch_amazon(
        track_name:  str,
        artist_name: str,
        isrc:        str = "",
        timeout:     int = 15,
) -> str:
    if not isrc:
        return ""
    try:
        r = requests.get(
            f"{_AMAZON_API_BASE}/lyrics/{isrc}",
            headers={"User-Agent": _UA},
            timeout=timeout,
        )
        if not r.ok:
            return ""
        data  = r.json()
        lines = data.get("lines") or data.get("lyrics", [])
        if not lines:
            return ""
        if isinstance(lines[0], dict):
            lrc: list[str] = []
            for line in lines:
                ts   = int(line.get("startTime", 0))
                m    = ts // 60000
                s    = (ts % 60000) // 1000
                cs   = (ts % 1000) // 10
                text = line.get("text", "")
                lrc.append(f"[{m:02d}:{s:02d}.{cs:02d}]{text}")
            return "\n".join(lrc)
        return "\n".join(str(l) for l in lines)
    except Exception as exc:
        logger.debug("[lyrics/amazon] %s", exc)
        return ""


# --------------------------------------------------------------------------- #
# Provider 4 — LRCLIB                                                          #
# --------------------------------------------------------------------------- #

def _fetch_lrclib(
        track_name:  str,
        artist_name: str,
        album_name:  str = "",
        duration_s:  int = 0,
        timeout:     int = 10,
) -> str:
    result = _lrclib_exact(track_name, artist_name, album_name, duration_s, timeout)
    if result:
        return result
    if album_name:
        result = _lrclib_exact(track_name, artist_name, "", duration_s, timeout)
        if result:
            return result
    return _lrclib_search(track_name, artist_name, timeout)


def _lrclib_exact(track: str, artist: str, album: str, duration: int, timeout: int) -> str:
    params: dict[str, Any] = {"artist_name": artist, "track_name": track}
    if album:    params["album_name"] = album
    if duration: params["duration"]   = duration
    try:
        r = requests.get(f"{_LRCLIB}/get", params=params, timeout=timeout)
        if r.status_code != 200:
            return ""
        d = r.json()
        return d.get("syncedLyrics") or d.get("plainLyrics") or ""
    except Exception:
        return ""


def _lrclib_search(track: str, artist: str, timeout: int) -> str:
    try:
        r = requests.get(
            f"{_LRCLIB}/search",
            params={"artist_name": artist, "track_name": track},
            timeout=timeout,
        )
        if r.status_code != 200:
            return ""
        results = r.json()
        if not results:
            return ""
        for item in results:
            if item.get("syncedLyrics"):
                return item["syncedLyrics"]
        return results[0].get("plainLyrics", "")
    except Exception:
        return ""


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #

def fetch_lyrics(
        track_name:       str,
        artist_name:      str,
        album_name:       str  = "",
        duration_s:       int  = 0,
        track_id:         str  = "",
        isrc:             str  = "",
        providers:        list[str] | None = None,
        spotify_token:    str  = "",
        musixmatch_token: str  = "",
) -> str:
    if providers is None:
        providers = _DEFAULT_PROVIDERS

    for provider in providers:
        result = ""
        try:
            if provider == "spotify":
                result = _fetch_spotify(track_id, spotify_token)
            elif provider == "musixmatch":
                result = _fetch_musixmatch(
                    track_name, artist_name, album_name, duration_s,
                    token=musixmatch_token,
                )
            elif provider == "amazon":
                result = _fetch_amazon(track_name, artist_name, isrc=isrc)
            elif provider == "lrclib":
                result = _fetch_lrclib(track_name, artist_name, album_name, duration_s)
            else:
                logger.warning("[lyrics] unknown provider: %s", provider)
        except Exception as exc:
            logger.debug("[lyrics/%s] unexpected error: %s", provider, exc)

        if result and result.strip():
            logger.debug("[lyrics] found via %s (%d chars)", provider, len(result))
            return result.strip()

    logger.debug("[lyrics] not found for '%s' by '%s'", track_name, artist_name)
    return ""


def set_musixmatch_token(token: str) -> None:
    global _MUSIXMATCH_DEFAULT_TOKEN
    _MUSIXMATCH_DEFAULT_TOKEN = token
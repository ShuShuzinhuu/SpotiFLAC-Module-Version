from __future__ import annotations
import sys
from tqdm import tqdm 

_BANNER_WIDTH = 60

def print_track_header(position: int, total: int, title: str, artists: str, album: str) -> None:
    pos = f"[{position}/{total}]"
    summary = f"Track {pos} {title[:40]!s} — {artists[:40]!s} ({album[:32]!s})"
    with tqdm.get_lock():
        tqdm.write(summary, file=sys.stderr)

def print_source_banner(provider: str, api: str, quality: str) -> None:
    label = _shorten_api(api)
    line  = f"[SOURCE] {provider.upper()} · {label} · {quality}"
    with tqdm.get_lock():
        tqdm.write(line, file=sys.stderr)

def print_official_source(provider: str, quality: str) -> None:
    line = f"[SOURCE] {provider.upper()} · Official API · {quality}"
    with tqdm.get_lock():
        tqdm.write(line, file=sys.stderr)

def print_summary(total: int, succeeded: int, failed: list[tuple[str, str, str]], elapsed_s: float) -> None:
    bar = "═" * _BANNER_WIDTH
    summary = f"\n╔{bar}╗\n"
    summary += f"║  SESSION SUMMARY{'':<43}║\n"
    summary += f"╠{bar}╣\n"
    summary += f"║  Total Tracks  : {total:<42}║\n"
    summary += f"║  Successful    : {succeeded:<42}║\n"
    summary += f"║  Failed        : {len(failed):<42}║\n"
    summary += f"║  Time Elapsed  : {_fmt_seconds(elapsed_s):<42}║"
    
    if failed:
        summary += f"\n╠{bar}╣\n"
        summary += f"║  ✗ FAILURES{'':<47}║\n"
        for title, artists, err in failed:
            short_err = _clean_error(err)[:18]
            short = f"{title[:20]} — {artists[:14]}: {short_err}"
            summary += f"\n║    {short:<56}║"
    summary += f"\n╚{bar}╝"
    with tqdm.get_lock():
        tqdm.write(summary, file=sys.stderr)

def print_api_failure(provider: str, api: str, reason: str) -> None:
    with tqdm.get_lock():
        tqdm.write(f"  ✗  {provider}  ·  {_shorten_api(api)}  ·  {_clean_error(reason)}", file=sys.stderr)

def print_quality_fallback(provider: str, from_q: str, to_q: str) -> None:
    with tqdm.get_lock():
        tqdm.write(f"  ⬇  {provider}: quality {from_q} unavailable — falling back to {to_q}", file=sys.stderr)

def _shorten_api(url: str) -> str:
    return url.removeprefix("https://").removeprefix("http://").split("/")[0]

def _fmt_seconds(s: float) -> str:
    s = int(round(s))
    parts = []
    for unit, div in [("h", 3600), ("m", 60), ("s", 1)]:
        val, s = divmod(s, div)
        if val:
            parts.append(f"{val}{unit}")
    return " ".join(parts) or "0s"

def _clean_error(err: str) -> str:
    err_str = str(err)
    if "Max retries exceeded" in err_str or "NameResolutionError" in err_str:
        return "Connection timeout / Unreachable"
    if "Read timed out" in err_str:
        return "Read timed out"
    if "403 Client Error: Forbidden" in err_str:
        return "HTTP 403 Forbidden (Cloudflare/WAF blocked)"
    if "Expecting value: line 1" in err_str or "invalid JSON" in err_str.lower():
        return "Invalid JSON response"
    return err_str.split('\n')[0][:60]
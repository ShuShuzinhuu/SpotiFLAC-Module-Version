from __future__ import annotations
import io
import logging
import sys
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any
from tqdm import tqdm

# Sincronizzazione visiva centralizzata sul core di tqdm.
_CONSOLE_LOCK = threading.RLock()
tqdm.set_lock(_CONSOLE_LOCK)


def safe_print(*args: object, **kwargs: Any) -> None:
    content = " ".join(str(a) for a in args)
    with tqdm.get_lock():
        tqdm.write(content, file=kwargs.get("file", sys.stdout))


def safe_tqdm_write(msg: str, file: io.TextIOBase | None = None) -> None:
    with tqdm.get_lock():
        tqdm.write(msg, file=file or sys.stdout)


class TqdmLoggingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self._message_cache: dict[str, float] = {}
        self._cache_ttl = 0.5  # 500ms deduplication window

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            now = time.time()
            
            # Deduplication: skip if same message logged recently
            if msg in self._message_cache:
                if now - self._message_cache[msg] < self._cache_ttl:
                    return
            
            # Update cache and write
            self._message_cache[msg] = now
            
            # Cleanup old entries (keep cache small)
            self._message_cache = {
                k: v for k, v in self._message_cache.items()
                if now - v < self._cache_ttl * 2
            }
            
            with tqdm.get_lock():
                tqdm.write(msg, file=sys.stderr)
        except Exception:
            self.handleError(record)


class _TqdmTextIOProxy(io.TextIOBase):
    def __init__(self, original: io.TextIOBase) -> None:
        self._original = original
        self._buf = ""

    def write(self, s: str) -> int:
        with tqdm.get_lock():
            s = s.replace("\r", "")
            self._buf += s
            while "\n" in self._buf:
                line, self._buf = self._buf.split("\n", 1)
                tqdm.write(line, file=self._original)
        return len(s)
    def flush(self) -> None:
        with tqdm.get_lock():
            if self._buf:
                tqdm.write(self._buf, file=self._original)
                self._buf = ""
            try:
                self._original.flush()
            except Exception:
                pass

    @property
    def encoding(self) -> str:
        return getattr(self._original, "encoding", "utf-8")

    def fileno(self) -> int:
        return self._original.fileno()

    def isatty(self) -> bool:
        return getattr(self._original, "isatty", lambda: False)()


def install_console_interception() -> None:
    if not isinstance(sys.stdout, _TqdmTextIOProxy):
        sys.stdout = _TqdmTextIOProxy(sys.__stdout__)
    if not isinstance(sys.stderr, _TqdmTextIOProxy):
        sys.stderr = _TqdmTextIOProxy(sys.__stderr__)

    root = logging.getLogger()
    for handler in list(root.handlers):
        if isinstance(handler, logging.StreamHandler):
            root.removeHandler(handler)

    # Some SpotiFLAC loggers may have their own StreamHandler attached,
    # which would duplicate warnings and info messages along with the root handler.
    for name, logger in list(logging.Logger.manager.loggerDict.items()):
        if isinstance(logger, logging.Logger) and (name == "SpotiFLAC" or name.startswith("SpotiFLAC.")):
            for handler in list(logger.handlers):
                if isinstance(handler, logging.StreamHandler):
                    logger.removeHandler(handler)
            logger.propagate = True

    new_handler = TqdmLoggingHandler()
    new_handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
    new_handler.setLevel(root.level or logging.WARNING)
    root.addHandler(new_handler)


def uninstall_console_interception() -> None:
    if isinstance(sys.stdout, _TqdmTextIOProxy):
        sys.stdout = sys.__stdout__
    if isinstance(sys.stderr, _TqdmTextIOProxy):
        sys.stderr = sys.__stderr__


class DownloadStatus(Enum):
    QUEUED      = "queued"
    DOWNLOADING = "downloading"
    COMPLETED   = "completed"
    FAILED      = "failed"
    SKIPPED     = "skipped"

@dataclass
class DownloadItem:
    id:            str
    track_name:    str
    artist_name:   str
    album_name:    str
    spotify_id:    str
    status:        DownloadStatus = DownloadStatus.QUEUED
    progress:      float          = 0.0
    total_size:    float          = 0.0
    speed:         float          = 0.0
    start_time:    float          = 0.0
    end_time:      float          = 0.0
    error_message: str            = ""
    file_path:     str            = ""

class DownloadManager:
    _instance: "DownloadManager | None" = None
    _creation_lock = threading.Lock()

    def __new__(cls) -> "DownloadManager":
        with cls._creation_lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._init_state()
                cls._instance = inst
        return cls._instance

    def _init_state(self) -> None:
        self._lock            = threading.RLock()
        self._queue:    list[DownloadItem] = []
        self.is_downloading   = False
        self.current_speed    = 0.0
        self.total_downloaded = 0.0
        self.current_item_id  = ""
        self.session_start    = 0.0

    def add_to_queue(self, item_id: str, track_name: str, artist_name: str, album_name: str, spotify_id: str) -> None:
        with self._lock:
            self._queue.append(DownloadItem(id=item_id, track_name=track_name, artist_name=artist_name, album_name=album_name, spotify_id=spotify_id))
            if self.session_start == 0.0: self.session_start = time.time()

    def start_download(self, item_id: str) -> None:
        with self._lock:
            for item in self._queue:
                if item.id == item_id:
                    item.status, item.start_time, item.progress = DownloadStatus.DOWNLOADING, time.time(), 0.0
                    break
            self.current_item_id, self.is_downloading = item_id, True

    def update_progress(self, item_id: str, progress_mb: float, speed_mbps: float) -> None:
        with self._lock:
            for item in self._queue:
                if item.id == item_id:
                    item.progress, item.speed = progress_mb, speed_mbps
                    break

    def complete_download(self, item_id: str, filepath: str, final_size_mb: float) -> None:
        with self._lock:
            for item in self._queue:
                if item.id == item_id:
                    item.status, item.end_time, item.file_path, item.progress, item.total_size = DownloadStatus.COMPLETED, time.time(), filepath, final_size_mb, final_size_mb
                    self.total_downloaded += final_size_mb
                    break
            self.is_downloading = False

    def fail_download(self, item_id: str, error_msg: str) -> None:
        with self._lock:
            for item in self._queue:
                if item.id == item_id:
                    item.status, item.end_time, item.error_message = DownloadStatus.FAILED, time.time(), error_msg
                    break
            self.is_downloading = False

    def skip_download(self, item_id: str) -> None:
        with self._lock:
            for item in self._queue:
                if item.id == item_id:
                    item.status, item.end_time = DownloadStatus.SKIPPED, time.time()
                    break
            self.is_downloading = False

    def get_stats(self) -> dict:
        with self._lock:
            queued    = sum(1 for item in self._queue if item.status == DownloadStatus.QUEUED)
            completed = sum(1 for item in self._queue if item.status == DownloadStatus.COMPLETED)
            failed    = sum(1 for item in self._queue if item.status == DownloadStatus.FAILED)
            skipped   = sum(1 for item in self._queue if item.status == DownloadStatus.SKIPPED)
            active_bytes = sum(item.progress for item in self._queue if item.status == DownloadStatus.DOWNLOADING)
            return {
                "is_downloading":   self.is_downloading,
                "current_speed":    self.current_speed,
                "total_downloaded": self.total_downloaded + active_bytes,
                "queued":           queued,
                "completed":        completed,
                "failed":           failed,
                "skipped":          skipped,
                "queue": [{"id": i.id, "track_name": i.track_name, "artist_name": i.artist_name, "album_name": i.album_name, "spotify_id": i.spotify_id, "status": i.status.value, "progress": i.progress, "total_size": i.total_size, "speed": i.speed, "file_path": i.file_path} for i in self._queue],
            }

    def reset(self) -> None:
        with self._lock: self._init_state()


class ProgressManager:
    _bars: dict[str, tqdm] = {}
    _slot_map: dict[str, int] = {}
    _master_bar: tqdm | None = None
    _master_enabled: bool = False

    @classmethod
    def _allocate_slot(cls, item_id: str) -> int:
        if item_id in cls._slot_map:
            return cls._slot_map[item_id]

        used_slots = set(cls._slot_map.values())
        slot = 0
        while slot in used_slots:
            slot += 1

        cls._slot_map[item_id] = slot
        return slot

    @classmethod
    def get_effective_position(cls, slot: int) -> int:
        return slot + (1 if cls._master_enabled else 0)

    @classmethod
    def create_bar(cls, item_id: str, track_name: str, total_bytes: int | None) -> tqdm:
        if item_id in cls._bars:
            return cls._bars[item_id]

        slot = cls._allocate_slot(item_id)
        display_name = track_name.strip()
        if len(display_name) > 18:
            display_name = display_name[:15] + "..."

        bar = tqdm(
            total        = total_bytes if total_bytes and total_bytes > 0 else None,
            unit         = "B",
            unit_scale   = True,
            unit_divisor = 1024,
            desc         = f"Track: {display_name:<18}",
            leave        = False,
            position     = cls.get_effective_position(slot),
            dynamic_ncols= True,
            miniters     = 1,
            smoothing    = 0.2,
            file         = sys.__stderr__,
        )

        cls._bars[item_id] = bar
        return bar

    @classmethod
    def release_bar(cls, item_id: str) -> None:
        bar = cls._bars.pop(item_id, None)
        if bar is None:
            cls._slot_map.pop(item_id, None)
            return

        try:
            bar.clear()
            bar.close()
        except Exception:
            pass
        cls._slot_map.pop(item_id, None)

    @classmethod
    def clear_item(cls, item_id: str) -> None:
        with tqdm.get_lock():
            cls.release_bar(item_id)

    @classmethod
    def clear_all(cls) -> None:
        with tqdm.get_lock():
            for item_id in list(cls._bars):
                cls.release_bar(item_id)
            cls._slot_map.clear()
            cls.clear_master_bar()

    @classmethod
    def initialize_master_bar(cls, total_items: int, description: str = "Batch", at_top: bool = True) -> None:
        if not at_top:
            raise ValueError("Only top-aligned master bar is supported by ProgressManager at this time.")

        with tqdm.get_lock():
            cls.clear_master_bar()
            cls._master_enabled = True
            cls._master_bar = tqdm(
                total        = total_items,
                desc         = description,
                leave        = True,
                position     = 0,
                dynamic_ncols= True,
                miniters     = 1,
                file         = sys.__stderr__,
            )

    @classmethod
    def clear_master_bar(cls) -> None:
        with tqdm.get_lock():
            if cls._master_bar is None:
                cls._master_enabled = False
                return

            try:
                cls._master_bar.clear()
                cls._master_bar.close()
            except Exception:
                pass
            cls._master_bar = None
            cls._master_enabled = False

    @classmethod
    def increment_master(cls, step: int = 1) -> None:
        with tqdm.get_lock():
            if cls._master_bar is None:
                return

            cls._master_bar.update(step)
            cls._master_bar.refresh()

    @classmethod
    def reset_master_total(cls, total_items: int) -> None:
        with tqdm.get_lock():
            if cls._master_bar is None:
                return

            cls._master_bar.reset(total=total_items)
            cls._master_bar.refresh()


class ProgressCallback:
    _bytes_since_refresh: int
    _last_refresh_time: float
    _last_reported_bytes: int

    def __init__(self, item_id: str = "", track_name: str = "") -> None:
        self._item_id = item_id
        self._track_name = track_name
        self._bytes_since_refresh = 0
        self._last_refresh_time = 0.0
        self._last_reported_bytes = 0

    def __call__(self, current_bytes: int, total_bytes: int) -> None:
        current_bytes = max(0, current_bytes)
        total_bytes = total_bytes if total_bytes > 0 else None
        now = time.time()

        with tqdm.get_lock():
            bar = ProgressManager.create_bar(self._item_id, self._track_name, total_bytes)
            newly_created = bar.n == 0 and self._last_refresh_time == 0.0
            reset_needed = current_bytes < self._last_reported_bytes

            if total_bytes != bar.total:
                bar.total = total_bytes

            if reset_needed:
                bar.reset(total=total_bytes)
                self._bytes_since_refresh = 0
                self._last_refresh_time = now
                self._last_reported_bytes = 0

            delta = current_bytes - self._last_reported_bytes
            if delta > 0:
                self._bytes_since_refresh += delta

            self._last_reported_bytes = current_bytes
            is_complete = total_bytes is not None and current_bytes >= total_bytes
            do_refresh = newly_created or reset_needed or (now - self._last_refresh_time >= 0.15) or is_complete

            if do_refresh:
                if self._bytes_since_refresh > 0:
                    bar.update(self._bytes_since_refresh)
                    self._bytes_since_refresh = 0
                else:
                    bar.refresh()
                self._last_refresh_time = now

                if is_complete:
                    ProgressManager.release_bar(self._item_id)

    @classmethod
    def clear_item(cls, item_id: str) -> None:
        ProgressManager.clear_item(item_id)


RichProgressCallback = ProgressCallback
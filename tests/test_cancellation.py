import time
import threading

from SpotiFLAC.downloader import download_one, DownloadOptions
from SpotiFLAC.core.models import TrackMetadata
from SpotiFLAC.core.models import DownloadResult


class DummyProvider:
    name = "dummy"

    def __init__(self):
        self._progress_cb = None
        self._stop_event = None

    def set_progress_callback(self, cb):
        self._progress_cb = cb

    def set_stop_event(self, ev):
        self._stop_event = ev

    def download_track(self, metadata, output_dir, **kwargs):
        # Simulate long-running download that checks for stop_event
        for i in range(10):
            if self._stop_event and self._stop_event.is_set():
                return DownloadResult.fail(self.name, "Cancelled")
            time.sleep(0.2)
        return DownloadResult.ok(self.name, "/tmp/fake.flac")


def test_download_one_timeout_triggers_cancellation():
    opts = DownloadOptions(output_dir="/tmp", track_max_retries=0, timeout_s=1)
    meta = TrackMetadata(id="t1", title="T1", artists="A", album="Album", album_artist="A")
    provider = DummyProvider()
    result = download_one(meta, "/tmp", [provider], opts)
    assert not result.success
    assert "timed out" in (result.error or "").lower() or "cancel" in (result.error or "").lower()

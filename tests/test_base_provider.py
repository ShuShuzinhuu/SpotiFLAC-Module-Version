import unittest
import asyncio
from unittest.mock import patch, AsyncMock
from SpotiFLAC.providers.base import BaseProvider


class DummyProvider(BaseProvider):
    name = "dummy"

    async def download_track_async(self, metadata, output_dir, **kwargs):
        pass


class BaseProviderTests(unittest.TestCase):
    @patch("asyncio.create_subprocess_exec")
    def test_run_ffprobe_executes_successfully(self, mock_exec):
        mock_proc = AsyncMock()
        mock_proc.communicate.return_value = (b"some stdout", b"some stderr")
        mock_proc.returncode = 0
        mock_exec.return_value = mock_proc

        provider = DummyProvider()
        rc, stdout, stderr = asyncio.run(provider._run_ffprobe("ffprobe", "-version"))

        self.assertEqual(rc, 0)
        self.assertEqual(stdout, "some stdout")
        self.assertEqual(stderr, "some stderr")
        mock_exec.assert_called_once_with(
            "ffprobe",
            "-version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    @patch("asyncio.create_subprocess_exec")
    def test_run_ffmpeg_executes_successfully(self, mock_exec):
        mock_proc = AsyncMock()
        mock_proc.communicate.return_value = (b"some ffmpeg output", b"")
        mock_proc.returncode = 0
        mock_exec.return_value = mock_proc

        provider = DummyProvider()
        rc, stdout, stderr = asyncio.run(provider._run_ffmpeg("ffmpeg", "-version"))

        self.assertEqual(rc, 0)
        self.assertEqual(stdout, "some ffmpeg output")
        self.assertEqual(stderr, "")
        mock_exec.assert_called_once_with(
            "ffmpeg",
            "-version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )


if __name__ == "__main__":
    unittest.main()

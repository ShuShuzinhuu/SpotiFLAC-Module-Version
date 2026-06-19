import logging
import re
from typing import Optional
from ..core.http import HttpClient

logger = logging.getLogger(__name__)

class SoundplateProvider:
    """Risolve ISRC tramite l'endpoint HTML di Soundplate."""

    API_URL = "https://soundplate.com/isrc-finder?track="

    def __init__(self, http_client: HttpClient):
        self.http = http_client

    def get_isrc(self, track_id: str) -> Optional[str]:
        try:
            url = f"{self.API_URL}{track_id}"
            resp = self.http.get(url, follow_redirects=True)
            match = re.search(r'(?i)\bISRC\b[^A-Z0-9]*([A-Z]{2}[A-Z0-9]{10})', resp.text)
            if match:
                return match.group(1).upper()
            return None
        except Exception as e:
            logger.debug("[soundplate] Failed: %s", e)
            return None
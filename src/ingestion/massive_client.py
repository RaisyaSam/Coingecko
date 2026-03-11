from __future__ import annotations
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from datetime import date
from typing import Any, Dict, Optional

from src.utils.rate_limiter import RateLimiter


class MassiveClient:
    def __init__(self, base_url: str, api_key: str, rpm: int = 5) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.limiter = RateLimiter(max_per_minute=rpm)
        self.session = requests.Session()

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
        }

    def _auth_params(self) -> Dict[str, str]:
        return {
            "apiKey": self.api_key,
        }

    def _massive_symbol(self, symbol: str, kind: str) -> str:
        if kind == "fx":
            if symbol.startswith("C:"):
                return symbol
            return f"C:{symbol}"
        return symbol

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def get_time_series(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        kind: str,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self.limiter.wait()

        ticker = self._massive_symbol(symbol, kind)

        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}"

        params: Dict[str, Any] = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": self.api_key,
        }

        if extra_params:
            params.update(extra_params)

        resp = self.session.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
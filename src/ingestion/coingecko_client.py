from __future__ import annotations
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from datetime import datetime, date
from typing import Dict, Any

from src.utils.rate_limiter import RateLimiter


class CoinGeckoClient:
    def __init__(self, base_url: str, api_key: str = "", rpm: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.limiter = RateLimiter(max_per_minute=rpm)
        self.session = requests.Session()

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}

        if self.api_key:
            if "pro-api.coingecko.com" in self.base_url:
                headers["x-cg-pro-api-key"] = self.api_key
            else:
                headers["x-cg-demo-api-key"] = self.api_key

        return headers

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def market_chart_range(
        self,
        coingecko_id: str,
        vs_currency: str,
        start_date: date,
        end_date: date,
    ) -> Dict[str, Any]:

        self.limiter.wait()

        start_ts = int(datetime.combine(start_date, datetime.min.time()).timestamp())
        end_ts = int(datetime.combine(end_date, datetime.max.time()).timestamp())

        url = f"{self.base_url}/coins/{coingecko_id}/market_chart/range"

        params = {
            "vs_currency": vs_currency.lower(),
            "from": start_ts,
            "to": end_ts,
        }

        resp = self.session.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=30,
        )

        resp.raise_for_status()

        return resp.json()
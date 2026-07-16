"""Small YouTube Data API client used by the ingestion Lambda."""

import json
import logging
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import Settings

logger = logging.getLogger()


def fetch_trending_videos(region_code: str, settings: Settings) -> dict[str, Any]:
    """Call videos.list for the most popular videos in one region."""
    return call_youtube_api(
        "videos",
        {
            "part": "snippet,statistics,contentDetails",
            "chart": "mostPopular",
            "regionCode": region_code,
            "maxResults": str(settings.max_results),
        },
        settings,
    )


def fetch_video_categories(region_code: str, settings: Settings) -> dict[str, Any]:
    """Call videoCategories.list for category lookup data in one region."""
    return call_youtube_api(
        "videoCategories",
        {
            "part": "snippet",
            "regionCode": region_code,
        },
        settings,
    )


def call_youtube_api(
    endpoint: str,
    params: dict[str, str],
    settings: Settings,
) -> dict[str, Any]:
    """Make an HTTPS GET request to the YouTube Data API."""
    query_params = {**params, "key": settings.youtube_api_key}
    url = f"{settings.api_base_url}/{endpoint}?{urlencode(query_params)}"
    request = Request(url, headers={"Accept": "application/json"})

    try:
        with urlopen(request, timeout=30) as response:
            response_body = response.read().decode("utf-8")
            return json.loads(response_body)
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        logger.error("YouTube API returned HTTP %s for endpoint=%s", exc.code, endpoint)
        raise RuntimeError(f"YouTube API request failed: {error_body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not connect to YouTube API: {exc.reason}") from exc

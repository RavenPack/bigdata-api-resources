"""
Batch file upload to Bigdata using the REST API.

Flow: POST /documents → PUT file to presigned URL → poll GET /documents/{id}
until status is "completed". A thread-safe rate limiter is used to avoid
being blocked.
"""

import concurrent.futures
import csv
import logging
import os
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from timeit import default_timer as timer

from dotenv import load_dotenv
import requests

# Load .env from this script's directory so it works when run from anywhere
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# -----------------------------------------------------------------------------
# Constants (from environment with defaults)
# -----------------------------------------------------------------------------
API_BASE_URL = os.getenv("BIGDATA_API_BASE_URL", "https://api.bigdata.com")
DOCUMENTS_PATH = "/contents/v1/documents"


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        return default


RATE_LIMIT_PER_MINUTE = _env_int("BIGDATA_RATE_LIMIT_PER_MINUTE", 500)
RATE_LIMIT_SAFETY_MARGIN = _env_int("BIGDATA_RATE_LIMIT_SAFETY_MARGIN", 20)
MAX_REQUESTS_PER_MINUTE = max(1, RATE_LIMIT_PER_MINUTE - RATE_LIMIT_SAFETY_MARGIN)
POLL_INTERVAL_SEC = _env_float("BIGDATA_POLL_INTERVAL_SEC", 10.0)
UPLOAD_MAX_RETRIES = _env_int("BIGDATA_UPLOAD_MAX_RETRIES", 5)

UPLOAD_DONE = "UPLOAD_DONE"
UPLOAD_ERROR = "UPLOAD_ERROR"


def configure_logging(workdir: str):
    log_file_path = os.path.join(
        workdir, f"bigdata_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler(),
        ],
    )


# -----------------------------------------------------------------------------
# Rate limiter (thread-safe, sliding window)
# -----------------------------------------------------------------------------
class RateLimiter:
    """
    Ensures we do not exceed MAX_REQUESTS_PER_MINUTE requests in any
    rolling 60-second window. All requests to the REST API (POST + GET)
    count toward the limit.
    """

    def __init__(self, max_per_minute: int = MAX_REQUESTS_PER_MINUTE):
        self.max_per_minute = max_per_minute
        self._timestamps: list[float] = []
        self._lock = threading.Lock()

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            # Drop timestamps older than 60 seconds
            self._timestamps = [t for t in self._timestamps if now - t < 60.0]
            if len(self._timestamps) >= self.max_per_minute:
                # Wait until oldest request exits the window
                sleep_time = 60.0 - (now - self._timestamps[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                now = time.monotonic()
                self._timestamps = [t for t in self._timestamps if now - t < 60.0]
            self._timestamps.append(now)


# -----------------------------------------------------------------------------
# REST API helpers
# -----------------------------------------------------------------------------
def _api_headers(api_key: str) -> dict:
    return {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }


def _post_document(
    api_key: str,
    file_name: str,
    rate_limiter: RateLimiter,
    published_ts: str | None = None,
    tags: list[str] | None = None,
    share_with_org: bool = True,
) -> tuple[dict | None, int]:
    """POST to create document and get upload URL and id. Returns (json_response, status_code)."""
    rate_limiter.acquire()
    url = f"{API_BASE_URL.rstrip('/')}{DOCUMENTS_PATH}"
    payload = {
        "file_name": file_name,
        "published_ts": published_ts or datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "tags": tags or [],
        "share_with_org": share_with_org,
    }
    resp = requests.post(url, json=payload, headers=_api_headers(api_key), timeout=30)
    try:
        data = resp.json() if resp.text else None
    except Exception:
        data = None
    return data, resp.status_code


def _put_file_to_url(upload_url: str, file_path: str) -> tuple[bool, int]:
    """
    PUT file contents to the presigned S3 URL. Do not send any headers so the
    request matches how the URL was signed (signature is sensitive to headers).
    """
    try:
        with open(file_path, "rb") as f:
            payload = f.read()
        response = requests.put(upload_url, data=payload, headers={}, timeout=120)
        if response.status_code >= 400:
            logging.warning(
                "PUT response status=%s body=%s",
                response.status_code,
                (response.text or "")[:300],
            )
        return 200 <= response.status_code < 300, response.status_code
    except requests.RequestException as e:
        logging.debug("PUT exception: %s", e)
        return False, 0


def _get_document_status(
    api_key: str, content_id: str, rate_limiter: RateLimiter
) -> tuple[dict | None, int]:
    """GET document by id. Returns (json_response, status_code)."""
    rate_limiter.acquire()
    url = f"{API_BASE_URL.rstrip('/')}{DOCUMENTS_PATH}/{content_id}"
    resp = requests.get(url, headers={"X-API-KEY": api_key}, timeout=30)
    try:
        data = resp.json() if resp.text else None
    except Exception:
        data = None
    return data, resp.status_code


def _poll_until_completed(
    api_key: str,
    content_id: str,
    rate_limiter: RateLimiter,
    interval_sec: float = POLL_INTERVAL_SEC,
    file_path: str | None = None,
) -> bool:
    """Poll GET document until status is 'completed' or a terminal error. Returns True if completed."""
    logging.info(
        "Waiting for document %s to complete (polling every %.0fs)...",
        content_id,
        interval_sec,
    )
    while True:
        data, status_code = _get_document_status(api_key, content_id, rate_limiter)
        if status_code == 429:
            logging.info("Rate limited (429), backing off...")
            time.sleep(min(interval_sec * 2, 60))
            continue
        if status_code != 200 or not data:
            logging.warning("Document %s: GET failed status=%s", content_id, status_code)
            return False
        status = data.get("status", "?")
        logging.info("Document %s: status=%s", content_id, status)
        if status == "completed":
            return True
        if status and status not in ("processing", "pending"):
            logging.warning("Document %s: terminal status=%s", content_id, status)
            return False
        time.sleep(interval_sec)


# -----------------------------------------------------------------------------
# Upload (single file)
# -----------------------------------------------------------------------------
def upload_file(
    api_key: str,
    rate_limiter: RateLimiter,
    file_path: str,
    published_ts: str | None = None,
    tags: list[str] | None = None,
    share_with_org: bool = True,
) -> tuple[str, str, str]:
    """
    Upload one file via REST API: POST → PUT → poll until completed.
    Returns (file_path, content_id, status) where status is UPLOAD_DONE or UPLOAD_ERROR.
    """
    file_name = os.path.basename(file_path)
    if not os.path.isfile(file_path):
        logging.error(f"File not found: {file_path}")
        return file_path, "", UPLOAD_ERROR

    logging.info("Uploading file: %s", file_path)
    for attempt in range(UPLOAD_MAX_RETRIES):
        try:
            # 1) POST to get upload URL and id
            logging.info("POST create document for %s...", file_name)
            data, status_code = _post_document(
                api_key=api_key,
                file_name=file_name,
                rate_limiter=rate_limiter,
                published_ts=published_ts,
                tags=tags,
                share_with_org=share_with_org,
            )
            if status_code == 429:
                time.sleep(min(2 ** (attempt + 1), 10))
                continue
            if status_code != 200 or not data or "url" not in data or "id" not in data:
                if status_code >= 500:
                    time.sleep(min(2 ** (attempt + 1), 10))
                    continue
                logging.error(f"Error uploading file {file_path}: POST failed status={status_code}")
                return file_path, "", UPLOAD_ERROR

            upload_url = data["url"]
            content_id = data["id"]
            logging.info("Got upload URL for document %s, sending file...", content_id)

            # 2) PUT file to presigned URL (no headers so signature matches)
            ok, put_status = _put_file_to_url(upload_url, file_path)
            if not ok:
                if put_status in (429, 500, 502, 503):
                    time.sleep(min(2 ** (attempt + 1), 10))
                    continue
                logging.error(f"Error uploading file {file_path}: PUT failed status={put_status}")
                return file_path, "", UPLOAD_ERROR

            logging.info("PUT done for %s, checking processing status...", file_name)
            # 3) Poll until processing is completed (no timeout; same behavior as original SDK)
            if not _poll_until_completed(api_key, content_id, rate_limiter, file_path=file_path):
                logging.error(
                    f"Error uploading file {file_path}: document {content_id} did not complete (status or API error)"
                )
                return file_path, content_id, UPLOAD_ERROR

            logging.info("Document %s completed.", content_id)
            return file_path, content_id, UPLOAD_DONE

        except requests.RequestException as e:
            logging.warning(f"Attempt {attempt + 1} failed for {file_path}: {e}")
            time.sleep(min(2 ** (attempt + 1), 10))
        except Exception as e:
            logging.exception(f"Unexpected error for {file_path}: {e}")
            return file_path, "", UPLOAD_ERROR

    logging.error(f"Error uploading file {file_path}: max retries reached")
    return file_path, "", UPLOAD_ERROR


# -----------------------------------------------------------------------------
# Bulk upload
# -----------------------------------------------------------------------------
def bulk_upload_files(
    api_key: str,
    rate_limiter: RateLimiter,
    workdir: str,
    max_concurrency: int,
    upload_txt_filename: str,
    result_csv_filename: str,
    published_ts: str | None = None,
    tags: list[str] | None = None,
    share_with_org: bool = False,
):
    with (
        open(upload_txt_filename, "r") as upload_txt,
        open(result_csv_filename, "w+", newline="") as result_csv,
        ThreadPoolExecutor(max_workers=max_concurrency) as executor,
    ):
        writer = csv.writer(result_csv)
        future_to_file: dict[Future, str] = {}
        for line in upload_txt:
            raw = line.strip()
            if not raw:
                continue
            # Resolve relative to workdir so entries like "10k_png.PDF" find files in workdir
            file_path = os.path.join(workdir, raw) if not os.path.isabs(raw) else raw
            future = executor.submit(
                upload_file,
                api_key,
                rate_limiter,
                file_path,
                published_ts=published_ts,
                tags=tags,
                share_with_org=share_with_org,
            )
            future_to_file[future] = file_path

        for future in concurrent.futures.as_completed(fs=future_to_file):
            file_path = future_to_file[future]
            file_id = ""
            upload_status = UPLOAD_ERROR
            try:
                file_path, file_id, upload_status = future.result()
                if upload_status == UPLOAD_DONE:
                    logging.info(f"Success uploading file {file_path}")
                # UPLOAD_ERROR: reason already logged in upload_file (e.g. timeout, PUT failed)
            except Exception as e:
                logging.error(f"Error uploading file {file_path}", exc_info=e)
            finally:
                writer.writerow([file_id, upload_status, file_path])


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    params = {param.split("=")[0]: param.split("=")[1] for param in sys.argv[1:]}
    workdir = params["workdir"]
    upload_txt_filename = params["upload_txt_filename"]
    upload_result_csv_filename = (
        f"uploaded_file_ids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    max_concurrency = params["max_concurrency"]

    configure_logging(workdir)
    concurrency = int(max_concurrency)

    api_key = os.getenv("BIGDATA_API_KEY")
    if not api_key:
        logging.error("BIGDATA_API_KEY is not set. Set it in .env or the environment.")
        sys.exit(1)

    rate_limiter = RateLimiter()
    upload_txt_path = os.path.join(workdir, upload_txt_filename)
    upload_result_csv_path = os.path.join(workdir, upload_result_csv_filename)
    upload_start = timer()
    bulk_upload_files(
        api_key=api_key,
        rate_limiter=rate_limiter,
        workdir=workdir,
        max_concurrency=concurrency,
        upload_txt_filename=upload_txt_path,
        result_csv_filename=upload_result_csv_path,
    )
    upload_end = timer()
    total_upload_time = upload_end - upload_start
    logging.info(f"UPLOAD TIME: {total_upload_time}")

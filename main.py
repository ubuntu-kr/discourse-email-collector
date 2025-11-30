import os
import time
import uvloop
import asyncio
import httpx
import re
import json
import pandas as pd
import pytz
import calendar
import logging
from rich.logging import RichHandler
from dateutil import parser as date_parser
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%Y-%m-%d %H:%M:%S]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger("discourse-email-collector")

ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010\013-\014\016-\037]')


def clean_illegal_chars(value):
    if isinstance(value, str):
        return ILLEGAL_CHARACTERS_RE.sub("", value)
    return value


def created_at_to_utc_midnight_ts(created_at: str) -> int:
    parsed_date = date_parser.parse(created_at)
    year, month, day = str(parsed_date.date()).split("-")
    return calendar.timegm(
        datetime(
            int(year),
            int(month),
            int(day),
            tzinfo=pytz.utc
        ).timetuple()
    )


class Discourse:
    def __init__(self, username: str, base_url: str):
        self.logger = logger
        self.base_url = base_url
        self.headers = {
            "User-Agent": "discourse-email-collector/0.0.1",
            "Api-Key": os.environ["DISCOURSE_API_KEY"],
            "Api-Username": username
        }
        self.client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            headers=self.headers,
            http2=True,
            timeout=httpx.Timeout(20.0, connect=10.0),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        )
        self.logger.info(
            f"Initialized Discourse client with user: {self.headers['Api-Username']}"
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.client is not None:
            await self.client.aclose()
            self.logger.info("Closed Discourse client")

    async def get_list_of_users_email(
        self,
        flag: str,
        _utc_timestemp: int | None = None,
        concurrency: int = 8,
    ) -> list[dict]:
        if self.client is None:
            raise RuntimeError("AsyncClient is not initialized. Use 'async with Discourse(...)'.")

        log_info = self.logger.info
        log_warn = self.logger.warning

        json_data: list[dict] = []
        page = 1

        if os.path.exists("discourse_users.json"):
            log_info("Found existing discourse_users.json — skipping API fetch")
            with open("discourse_users.json", "r", encoding="utf-8") as f:
                json_data = json.load(f)
            log_info(f"Loaded {len(json_data)} entries from discourse_users.json")
        else:
            while True:
                log_info(f"Requesting page {page}")
                response = await self.client.get(
                    f"https://{self.base_url}/admin/users/list/{flag}.json",
                    params={"show_emails": True, "page": page},
                )
                try:
                    resp = response.json()
                    if isinstance(resp, dict) and resp.get("errors"):
                        wait_seconds = resp["extras"]["wait_seconds"]
                        log_warn(f"Rate limit: waiting {wait_seconds}s")
                        await asyncio.sleep(wait_seconds)
                        continue
                    if not resp:
                        break
                except json.decoder.JSONDecodeError:
                    lines = response.text.split("\n")
                    wait_seconds = int(re.findall(r"\d+", lines[1])[0])
                    log_warn(f"JSONDecodeError — waiting {wait_seconds}s")
                    await asyncio.sleep(wait_seconds)
                    continue

                json_data.extend(resp)
                log_info(f"Fetched total entries: {len(json_data)}")
                page += 1

            with open("discourse_users.json", "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=4, ensure_ascii=False)
            log_info("Saved fetched data to discourse_users.json")

        if not json_data:
            log_info("No users found.")
            return []

        sem = asyncio.Semaphore(concurrency)
        now_ts = int(time.time())
        cutoff_ts = _utc_timestemp if _utc_timestemp is not None else now_ts

        async def fetch_user_detail_and_filter(index: dict) -> dict | None:
            username = index["username"]
            user_id = index["id"]

            while True:
                async with sem:
                    log_info(f"User: {index.get('name')} ({username})")
                    try:
                        resp = await self.client.get(
                            f"https://{self.base_url}/admin/users/{user_id}.json"
                        )
                    except TypeError as e:
                        self.logger.error(f"TypeError for {username}: {e}")
                        return None

                try:
                    status = resp.json()
                except json.decoder.JSONDecodeError:
                    lines = resp.text.split("\n")
                    wait_seconds = int(re.findall(r"\d+", lines[1])[0])
                    log_warn(f"JSONDecodeError for {username} — waiting {wait_seconds}s")
                    await asyncio.sleep(wait_seconds)
                    continue

                if status.get("error_type") == "rate_limit":
                    wait_seconds = status["extras"]["wait_seconds"]
                    log_warn(f"Rate limited for {username} — waiting {wait_seconds}s")
                    await asyncio.sleep(wait_seconds)
                    continue

                external_ids = status.get("external_ids") or {}
                penalty_counts = status.get("penalty_counts") or {}

                silenced = penalty_counts.get("silenced", 0)
                suspended = penalty_counts.get("suspended", 0)

                if silenced != 0 and suspended != 0:
                    log_warn(f"User {username} silenced={silenced}, suspended={suspended}")
                    return None

                created_at = status["created_at"]
                ts = created_at_to_utc_midnight_ts(created_at)

                if ts > cutoff_ts:
                    log_info(f"PASS (new user): {username}")
                    return None

                if not external_ids:
                    log_info(f"PASS (no external_ids): {username}")
                    return None

                log_info(f"ADD: {username}")
                return {
                    "username": username,
                    "name": index["name"],
                    "email": index["email"],
                    "oidc": external_ids.get("oidc"),
                    "created_at": created_at,
                }

        tasks = [
            asyncio.create_task(fetch_user_detail_and_filter(index))
            for index in json_data
        ]

        results = await asyncio.gather(*tasks, return_exceptions=False)
        listed = [r for r in results if r is not None]
        listed.sort(key=lambda x: x["created_at"], reverse=True)

        log_info(f"Final listed count: {len(listed)}")
        return listed


async def main():
    _base_url = input("Enter Discourse base URL: ")
    _if_utc_timestemp = int(
        input("Enter 1 to specify a custom UTC timestamp, or 0 otherwise: ")
    )
    _utc_timestemp: int | None = None

    if _if_utc_timestemp == 0:
        _utc_timestemp = None
    elif _if_utc_timestemp == 1:
        __utc_timestemp = int(time.time())
        _utc_timestemp = int(
            input(f"Enter the UTC timestamp (current UTC Timestemp {__utc_timestemp}): ")
        )
    else:
        logger.error("Invalid input for UTC timestamp. Please enter 0 or 1.")
        raise SystemExit(1)

    async with Discourse("system", _base_url) as discourse:
        data = await discourse.get_list_of_users_email(
            "active",
            _utc_timestemp,
            concurrency=8,
        )

    df = pd.json_normalize(data)
    df = df.applymap(clean_illegal_chars)
    df.columns = [clean_illegal_chars(c) for c in df.columns]

    filename = f"{_base_url.replace('.', '_')}_email_list.xlsx"
    logger.info(f"Saving {filename}")
    df.to_excel(filename, index=False)
    logger.info("Excel export complete")


if __name__ == "__main__":
    uvloop.run(main())

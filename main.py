import os
import time
import httpx
import re
import json
import pandas as pd
import pytz
import calendar
import logging
from dateutil import parser as date_parser
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)
logger = logging.getLogger("discourse-email-collector")

ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010\013-\014\016-\037]')

def clean_illegal_chars(value):
    if isinstance(value, str):
        return ILLEGAL_CHARACTERS_RE.sub("", value)
    return value

class Discourse:
    def __init__(self, username: str):
        self.logger = logger
        self.base_url = "discourse.ubuntu-kr.org"
        self.headers = {
            "User-Agent": "ubuntu-kr-discourse-email-collector/0.0.1",
            "Api-Key": os.environ["DISCOURSE_API_KEY"],
            "Api-Username": username
        }
        self.client = httpx.Client(headers=self.headers)
        self.logger.info(f"Initialized Discourse client with user: {username}")

    def get_list_of_users_email(self, flag: str) -> list[dict]:
        self.logger.info(f"Fetching user list (flag={flag})")
        listed = []
        json_data = []
        page = 1
        status = True

        if os.path.exists("discourse_users.json"):
            self.logger.info("Found existing discourse_users.json — skipping API fetch")
            status = False

        while status:
            self.logger.info(f"Requesting page {page}")
            response = self.client.get(
                f"https://{self.base_url}/admin/users/list/{flag}.json",
                params={"show_emails": True, "page": page}
            )

            try:
                resp = response.json()
                if isinstance(resp, dict) and resp.get("errors"):
                    errors = resp["errors"][0]
                    wait_seconds = resp["extras"]["wait_seconds"]
                    self.logger.warning(f"Rate limit: {errors}. Waiting {wait_seconds}s")
                    time.sleep(wait_seconds)
                    continue

                if not resp or page == 76:
                    self.logger.info("Pagination stopped")
                    status = False

            except json.decoder.JSONDecodeError:
                wait_seconds = int(re.findall(r'\d+', response.text.split("\n")[1])[0])
                self.logger.warning(f"JSONDecodeError — waiting {wait_seconds}s")
                time.sleep(wait_seconds)
                continue

            json_data.extend(resp)
            self.logger.info(f"Fetched total entries: {len(json_data)}")
            page += 1

        if not os.path.exists("discourse_users.json"):
            with open("discourse_users.json", "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=4, ensure_ascii=False)
            self.logger.info("Saved fetched data to discourse_users.json")
        else:
            with open("discourse_users.json", "r", encoding="utf-8") as f:
                json_data = json.load(f)
            self.logger.info(f"Loaded {len(json_data)} entries from discourse_users.json")

        for index in json_data:
            self.logger.info(f"User: {index.get('name')} ({index['username']})")

            try:
                resp = self.client.get(
                    f"https://{self.base_url}/admin/users/{index['id']}.json"
                )
            except TypeError as e:
                self.logger.error(f"TypeError: {e}")
                continue

            if resp.status_code != 200:
                self.logger.warning(f"Non-200 status: {resp.status_code}")

            try:
                status = resp.json()
            except json.decoder.JSONDecodeError:
                wait_seconds = int(re.findall(r'\d+', resp.text.split("\n")[1])[0])
                self.logger.warning(f"JSONDecodeError — waiting {wait_seconds}s")
                time.sleep(wait_seconds)
                continue

            if status.get("error_type") == "rate_limit":
                wait_seconds = status["extras"]["wait_seconds"]
                self.logger.warning(f"Rate limited — waiting {wait_seconds}s")
                time.sleep(wait_seconds)
                continue

            external_ids = status["external_ids"]
            penalty_counts = status["penalty_counts"]

            if penalty_counts["silenced"] == 0 or penalty_counts["suspended"] == 0:
                created_at = status["created_at"]
                parsed_date = date_parser.parse(created_at)
                year, month, day = str(parsed_date.date()).split("-")

                ts = calendar.timegm(
                    datetime(int(year), int(month), int(day), tzinfo=pytz.utc).timetuple()
                )

                if ts <= 1764336976:
                    if external_ids:
                        self.logger.info(f"ADD: {index['username']}")
                        listed.append({
                            "username": index["username"],
                            "name": index["name"],
                            "email": index["email"],
                            "oidc": external_ids.get("oidc"),
                            "created_at": created_at,
                        })
                    else:
                        self.logger.info(f"PASS (no external_ids): {index['username']}")
                else:
                    self.logger.info(f"PASS (new user): {index['username']}")
            else:
                self.logger.warning(
                    f"User {index['username']} silenced={penalty_counts['silenced']}, suspended={penalty_counts['suspended']}"
                )

        listed.sort(key=lambda x: x["created_at"], reverse=True)
        self.logger.info(f"Final listed count: {len(listed)}")
        return listed


if __name__ == "__main__":
    discourse = Discourse("system")
    data = discourse.get_list_of_users_email("active")
    df = pd.json_normalize(data)
    df = df.applymap(clean_illegal_chars)
    df.columns = [clean_illegal_chars(c) for c in df.columns]

    logger.info("Saving ubuntu-kr-discourse.xlsx")
    df.to_excel("ubuntu-kr-discourse.xlsx", index=False)
    logger.info("Excel export complete")

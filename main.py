import os
import time
import httpx
import re
import json
import pandas as pd
import pytz
import calendar
from dateutil import parser as date_parser
from datetime import datetime

ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010\013-\014\016-\037]')

def clean_illegal_chars(value):
    if isinstance(value, str):
        return ILLEGAL_CHARACTERS_RE.sub("", value)
    return value


class Discourse:
    def __init__(self, username: str):
        self.base_url = "discourse.ubuntu-kr.org"
        self.headers = {
            "User-Agent": "ubuntu-kr-discourse-email-collector/0.0.1",
            "Api-Key": os.environ["DISCOURSE_API_KEY"],
            "Api-Username": username
        }
        self.client = httpx.Client(
            headers=self.headers,
        )

    def json_process(self):
        with open("discourse_users.json", "r", encoding="utf-8") as f:
            json_data = json.load(f)

    def get_list_of_users_email(self, flag: str) -> list[dict]:
        """
        Get list of users email.

        Args:
            flag (str): "active" "new" "staff" "suspended" "blocked" "suspect"
        """
        listed = []
        json_data = []
        page = 1
        status = True

        if os.path.exists("discourse_users.json"):
            status = False

        while status:
            print(f"page: {page}")
            response = self.client.get(
                f"https://{self.base_url}/admin/users/list/{flag}.json",
                params={"show_emails": True, "page": page}
            )
            try:
                resp = response.json()

                # rate limit 처리
                if isinstance(resp, dict):
                    errors = resp["errors"][0]
                    wait_seconds = resp["extras"]["wait_seconds"]
                    print(errors)
                    time.sleep(wait_seconds)
                    continue

                if not resp or page == 76:
                    status = False

            except json.decoder.JSONDecodeError:
                wait_seconds = int(re.findall(r'\d+', response.text.split("\n")[1])[0])
                print(f"{wait_seconds}초후 다시 시도합니다")
                time.sleep(wait_seconds)
                continue

            json_data.extend(resp)
            page += 1
            print("range: ", len(json_data))

        # 첫 실행 시 파일 저장
        if not os.path.exists("discourse_users.json"):
            with open("discourse_users.json", "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=4, ensure_ascii=False)
        else:
            with open("discourse_users.json", "r", encoding="utf-8") as f:
                json_data = json.load(f)

        print("range: ", len(json_data))

        for index in json_data:
            print("REGISTER: " + str(index.get("name")) + " " + index['username'])
            try:
                resp = self.client.get(f"https://{self.base_url}/admin/users/{index['id']}.json")
            except TypeError as e:
                print(e)
                continue

            print(f"HTTP STATUS CODE: {resp.status_code}")

            try:
                status = resp.json()
            except json.decoder.JSONDecodeError:
                wait_seconds = int(re.findall(r'\d+', resp.text.split("\n")[1])[0])
                print(f"{wait_seconds}초후 다시 시도합니다")
                time.sleep(wait_seconds)
                continue

            if status.get("error_type") == "rate_limit":
                wait_seconds = status["extras"]["wait_seconds"]
                print(f"{wait_seconds}초후 다시 시도합니다")
                time.sleep(wait_seconds)
                continue

            external_ids = status["external_ids"]
            penalty_counts = status["penalty_counts"]

            # suspension/silenced 체크
            if penalty_counts["silenced"] == 0 or penalty_counts["suspended"] == 0:

                created_at = status["created_at"]
                parsed_date = date_parser.parse(created_at)
                year, month, day = str(parsed_date.date()).split("-")
                ts = calendar.timegm(datetime(int(year), int(month), int(day), tzinfo=pytz.utc).timetuple())

                # 특정 날짜 이하만 수집
                if 1764336976 >= ts:
                    if external_ids:
                        listed.append({
                            "username": index["username"],
                            "name": index["name"],
                            "email": index["email"],
                            "oidc": external_ids.get("oidc"),
                            "created_at": created_at,
                        })

        listed.sort(key=lambda x: x["created_at"], reverse=True)
        return listed


if __name__ == "__main__":
    discourse = Discourse("system")
    data = discourse.get_list_of_users_email("active")

    df = pd.json_normalize(data)
    df = df.applymap(clean_illegal_chars)
    df.columns = [clean_illegal_chars(c) for c in df.columns]

    print(df)

    df.to_excel("ubuntu-kr-discourse.xlsx", index=False)

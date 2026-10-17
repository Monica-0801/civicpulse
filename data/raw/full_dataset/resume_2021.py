
import csv
import io
import time
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
FILE = Path.home() / "civicpulse/data/raw/full_dataset/311_2021.csv.part"
PAGE_SIZE = 2000
MAX_RETRIES = 8
END_DATE = "2022-01-01T00:00:00"

with FILE.open("r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    fields = reader.fieldnames
    last = None
    for row in reader:
        last = row

if not fields or not last:
    raise SystemExit("Partial file is empty or has no valid header.")

last_date = last["created_date"]
last_key = last["unique_key"]

print("Resuming from:", last_date, last_key)

while True:
    where = (
        f"created_date >= '2021-01-01T00:00:00' "
        f"AND created_date < '{END_DATE}' "
        f"AND (created_date > '{last_date}' "
        f"OR (created_date = '{last_date}' "
        f"AND unique_key > '{last_key}'))"
    )

    params = {
        "$where": where,
        "$order": "created_date,unique_key",
        "$limit": str(PAGE_SIZE),
    }
    url = BASE + "?" + urllib.parse.urlencode(params)
    content = None

    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(
            url, headers={"User-Agent": "CivicPulse student project"}
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as response:
                content = response.read().decode("utf-8-sig")
            break
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            OSError,
        ) as exc:
            delay = min(5 * (2 ** attempt), 120)
            print(f"Request failed ({attempt + 1}/{MAX_RETRIES}): {exc}")
            if attempt + 1 < MAX_RETRIES:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)

    if content is None:
        print("Retries exhausted. Partial data is preserved.")
        print("Run this resume script again later.")
        break

    reader = csv.DictReader(io.StringIO(content))
    if reader.fieldnames != fields:
        raise SystemExit("CSV headers differ. Nothing appended.")

    rows = list(reader)
    if not rows:
        print("No more records returned for 2021.")
        break

    previous = (last_date, int(last_key))

    for row in rows:
        date = row.get("created_date", "")
        key = row.get("unique_key", "")

        if not date or not key:
            raise SystemExit("Missing date or key. Nothing appended.")

        current = (date, int(key))
        if current <= previous:
            raise SystemExit("Unexpected record ordering. Nothing appended.")

        if not ("2021-01-01T00:00:00" <= date < END_DATE):
            raise SystemExit(f"Record outside 2021: {date}")

        previous = current

    with FILE.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=fields, extrasaction="ignore"
        )
        writer.writerows(rows)

    last_date = rows[-1]["created_date"]
    last_key = rows[-1]["unique_key"]

    print(f"Appended {len(rows):,}; checkpoint {last_date}, {last_key}")
    time.sleep(1)

    if len(rows) < PAGE_SIZE:
        print("Reached the end of available 2021 records.")
        break


import csv
import io
import time
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
FILE = Path.home() / "civicpulse/data/raw/full_dataset/311_2020.csv.part"
PAGE_SIZE = 2000
MAX_RETRIES = 8

# Read the last saved record.
with FILE.open("r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    fields = reader.fieldnames
    last = None
    for row in reader:
        last = row

if not fields or not last:
    raise SystemExit("No valid header or data records found.")

last_date = last["created_date"]
last_key = last["unique_key"]

print("Resume checkpoint:", last_date, last_key)
print("Page size:", PAGE_SIZE)

while True:
    where = (
        f"created_date >= '{last_date}' "
        f"AND created_date < '2021-01-01T00:00:00' "
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
            url,
            headers={"User-Agent": "CivicPulse student project"}
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
            print(
                f"Request failed ({attempt + 1}/{MAX_RETRIES}): "
                f"{exc}. Waiting {delay}s."
            )
            if attempt + 1 < MAX_RETRIES:
                time.sleep(delay)

    if content is None:
        print("Retries exhausted. Existing data remains saved.")
        print("Run this script again later to resume.")
        break

    reader = csv.DictReader(io.StringIO(content))

    if reader.fieldnames != fields:
        raise SystemExit(
            "CSV headers differ from the saved file. Nothing appended."
        )

    rows = list(reader)

    if not rows:
        print("No more records returned for 2020.")
        break

    # Validate the page before appending it.
    previous = (last_date, int(last_key))

    for row in rows:
        if not row.get("created_date") or not row.get("unique_key"):
            raise SystemExit("Invalid row detected. Nothing appended.")
        current = (row["created_date"], int(row["unique_key"]))
        if current <= previous:
            raise SystemExit(
                "Non-increasing checkpoint detected. Nothing appended."
            )
        if row["created_date"] >= "2021-01-01T00:00:00":
            raise SystemExit("Record outside 2020 detected. Nothing appended.")
        previous = current

    # Append only after the entire page passes validation.
    with FILE.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=fields, extrasaction="ignore"
        )
        writer.writerows(rows)

    last_date = rows[-1]["created_date"]
    last_key = rows[-1]["unique_key"]

    print(
        f"Appended {len(rows):,}; "
        f"checkpoint {last_date}, {last_key}"
    )

    time.sleep(1)

    if len(rows) < PAGE_SIZE:
        print("Reached the end of available 2020 records.")
        break


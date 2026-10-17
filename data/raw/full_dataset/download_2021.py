
import csv
import io
import time
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
OUT = Path.home() / "civicpulse/data/raw/full_dataset/311_2021.csv"
PART = Path(str(OUT) + ".part")

PAGE_SIZE = 2000
MAX_RETRIES = 8

if OUT.exists():
    raise SystemExit(f"{OUT.name} already exists. Refusing to overwrite it.")

if PART.exists():
    raise SystemExit(
        f"{PART.name} already exists. Inspect it before starting; "
        "this script will not overwrite or append blindly."
    )

start_date = "2021-01-01T00:00:00"
end_date = "2022-01-01T00:00:00"
last_date = None
last_key = None
fields = None
total = 0

while True:
    conditions = [
        f"created_date >= '{start_date}'",
        f"created_date < '{end_date}'",
    ]

    if last_date is not None:
        conditions.append(
            f"(created_date > '{last_date}' OR "
            f"(created_date = '{last_date}' AND unique_key > {last_key}))"
        )

    params = {
        "$where": " AND ".join(conditions),
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
        print("Retries exhausted. Partial data remains saved.")
        print(f"Resume checkpoint: {last_date}, {last_key}")
        print(f"Partial file: {PART}")
        break

    reader = csv.DictReader(io.StringIO(content))

    if not reader.fieldnames:
        raise SystemExit("Response has no CSV header; stopping.")

    if fields is None:
        fields = reader.fieldnames
    elif reader.fieldnames != fields:
        raise SystemExit("CSV headers changed; stopping before appending.")

    rows = list(reader)

    if not rows:
        if not PART.exists():
            with PART.open("w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(fields)
        PART.rename(OUT)
        print(f"Completed {OUT.name}: {total:,} records")
        break

    previous = (
        (last_date, int(last_key))
        if last_date is not None else None
    )

    for row in rows:
        date = row.get("created_date", "")
        key = row.get("unique_key", "")

        if not date or not key:
            raise SystemExit("Missing date or ID; stopping.")

        current = (date, int(key))

        if previous is not None and current <= previous:
            raise SystemExit("Ordering/checkpoint error; stopping.")

        if not (start_date <= date < end_date):
            raise SystemExit(f"Record outside 2021 detected: {date}")

        previous = current

    # Write the header only once, then append validated batches.
    write_header = not PART.exists()
    with PART.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    last_date = rows[-1]["created_date"]
    last_key = rows[-1]["unique_key"]
    total += len(rows)

    print(
        f"Downloaded this session: {total:,} records; "
        f"checkpoint {last_date}, {last_key}"
    )
    time.sleep(1)

    if len(rows) < PAGE_SIZE:
        PART.rename(OUT)
        print(f"Completed {OUT.name}: {total:,} records")
        break

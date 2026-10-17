import csv
import io
import json
import math
import time
import calendar
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path
from datetime import date

ROOT = Path.home() / "civicpulse/data/raw/full_dataset"
BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
TARGET = 200_000
FIELDS = [
    "unique_key", "created_date", "agency", "agency_name",
    "complaint_type", "descriptor", "location_type",
    "incident_zip", "city", "borough", "status"
]

TODAY = date.today()

def fetch_month(year, month, limit):
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)

    params = {
        "$select": ",".join(FIELDS),
        "$where": (
            f"created_date >= '{start.isoformat()}T00:00:00' "
            f"AND created_date < '{end.isoformat()}T00:00:00'"
        ),
        "$order": "created_date,unique_key",
        "$limit": str(limit),
    }
    url = BASE + "?" + urllib.parse.urlencode(params)

    for attempt in range(1, 7):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "CivicPulseStudentProject/1.0"}
            )
            with urllib.request.urlopen(req, timeout=120) as response:
                text = response.read().decode("utf-8-sig")
            return list(csv.DictReader(io.StringIO(text)))
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            print(f"Request failed ({attempt}/6): {exc}", flush=True)
            if attempt == 6:
                raise
            time.sleep(min(5 * (2 ** (attempt - 1)), 60))

def count_and_keys(path):
    keys = set()
    count = 0
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != FIELDS:
            raise RuntimeError(f"Unexpected CSV columns in {path.name}")
        for row in reader:
            key = (row.get("unique_key") or "").strip()
            if not key:
                continue
            keys.add(key)
            count += 1
    return count, keys

def prepare_year(year):
    output = ROOT / f"311_{year}_sample_recovered.csv"
    state = ROOT / f"311_{year}_sample_recovered.state.json"

    if not output.exists():
        with output.open("w", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=FIELDS)
            writer.writeheader()

            # Reuse existing 2023 partial data without changing the source.
            if year == 2023:
                source = ROOT / "311_2023_sample.csv.part"
                if source.exists():
                    print("Recovering existing 2023 records...", flush=True)
                    with source.open(newline="", encoding="utf-8-sig") as inp:
                        reader = csv.DictReader(inp)
                        if reader.fieldnames != FIELDS:
                            raise RuntimeError(
                                "2023 source columns do not match expected schema"
                            )
                        seen = set()
                        for row in reader:
                            key = (row.get("unique_key") or "").strip()
                            if key and key not in seen:
                                writer.writerow({
                                    field: row.get(field, "") or ""
                                    for field in FIELDS
                                })
                                seen.add(key)
                    print(f"Copied {len(seen):,} unique 2023 records.", flush=True)

    count, keys = count_and_keys(output)

    if state.exists():
        next_month = json.loads(state.read_text()).get("next_month", 1)
    else:
        next_month = 1

    return output, state, count, keys, next_month

def main():
    for year in range(2023, min(2026, TODAY.year) + 1):
        output, state, count, keys, next_month = prepare_year(year)
        last_month = TODAY.month if year == TODAY.year else 12

        print(
            f"\n{year}: starting with {count:,} records; "
            f"next month {next_month}",
            flush=True
        )

        while count < TARGET and next_month <= last_month:
            months_left = last_month - next_month + 1
            limit = min(20_000, max(1, math.ceil((TARGET - count) / months_left)))

            print(
                f"{year}-{next_month:02d}: requesting up to {limit:,} rows",
                flush=True
            )
            rows = fetch_month(year, next_month, limit)
            added = 0

            with output.open("a", newline="", encoding="utf-8") as out:
                writer = csv.DictWriter(out, fieldnames=FIELDS)
                for row in rows:
                    key = (row.get("unique_key") or "").strip()
                    if not key or key in keys:
                        continue
                    if count >= TARGET:
                        break
                    writer.writerow({
                        field: row.get(field, "") or ""
                        for field in FIELDS
                    })
                    keys.add(key)
                    count += 1
                    added += 1
                out.flush()

            # Save progress only after the month's rows have been written.
            next_month += 1
            state.write_text(json.dumps({"next_month": next_month}))
            print(
                f"{year}: added {added:,}; total {count:,}/{TARGET:,}",
                flush=True
            )
            time.sleep(0.5)

        if count >= TARGET:
            state.unlink(missing_ok=True)
            print(f"{year}: target reached.", flush=True)
        else:
            print(
                f"{year}: currently {count:,} records. "
                "This year may have fewer records available so far.",
                flush=True
            )

        print(f"Saved: {output}", flush=True)

if __name__ == "__main__":
    main()

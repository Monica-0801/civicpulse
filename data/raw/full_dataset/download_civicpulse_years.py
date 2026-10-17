import csv
import io
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
ROOT = Path.home() / "civicpulse/data/raw/full_dataset"
YEARS = range(2022, 2027)
TARGET = 200_000
PAGE = 2_000
FIELDS = [
    "unique_key", "created_date", "agency", "agency_name",
    "complaint_type", "descriptor", "location_type",
    "incident_zip", "city", "borough", "status"
]

def fetch(start, end, offset):
    params = {
        "$select": ",".join(FIELDS),
        "$where": (
            f"created_date >= '{start}-01-01T00:00:00' "
            f"AND created_date < '{end}-01-01T00:00:00'"
        ),
        "$order": "created_date,unique_key",
        "$limit": str(PAGE),
        "$offset": str(offset),
    }
    url = BASE + "?" + urllib.parse.urlencode(params)
    for attempt in range(1, 7):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "CivicPulseStudentProject/1.0"}
            )
            with urllib.request.urlopen(req, timeout=120) as response:
                return list(csv.DictReader(
                    io.StringIO(response.read().decode("utf-8-sig"))
                ))
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            print(f"Request failed ({attempt}/6): {exc}", flush=True)
            if attempt == 6:
                raise
            time.sleep(min(5 * 2 ** (attempt - 1), 60))

def main():
    for year in YEARS:
        output = ROOT / f"311_{year}_sample.csv"
        state = ROOT / f"311_{year}_sample.checkpoint.json"

        if output.exists() and not state.exists():
            print(f"SKIP {year}: {output.name} exists without checkpoint.")
            print("Preserving it; inspect it manually before restarting.")
            continue

        if state.exists():
            progress = json.loads(state.read_text())
            offset = progress["offset"]
            print(f"\nResuming {year} from offset {offset}", flush=True)
        else:
            offset = 0
            with output.open("w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=FIELDS).writeheader()
            state.write_text(json.dumps({"offset": 0}))

        count = sum(1 for _ in csv.DictReader(
            output.open(newline="", encoding="utf-8")
        ))

        while count < TARGET:
            end_year = year + 1
            rows = fetch(year, end_year, offset)
            if not rows:
                print(f"{year}: no more rows returned.", flush=True)
                break

            remaining = TARGET - count
            rows = rows[:remaining]

            with output.open("a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                for row in rows:
                    writer.writerow({k: row.get(k, "") or "" for k in FIELDS})

            count += len(rows)
            offset += len(rows)
            state.write_text(json.dumps({"offset": offset}))
            print(f"{year}: {count:,}/{TARGET:,} records", flush=True)
            time.sleep(0.3)

        print(f"Saved {year}: {output} ({count:,} records)", flush=True)

        if count >= TARGET:
            state.unlink(missing_ok=True)
            print(f"{year} sample complete.", flush=True)

if __name__ == "__main__":
    main()

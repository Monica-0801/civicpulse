import csv
import io
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import date, timedelta
from pathlib import Path

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
ROOT = Path.home() / "civicpulse/data/raw/full_dataset"
YEARS = range(2022, 2027)
TARGET_PER_YEAR = 200_000
PAGE_SIZE = 2_000

FIELDS = [
    "unique_key", "created_date", "agency", "agency_name",
    "complaint_type", "descriptor", "location_type",
    "incident_zip", "city", "borough", "status"
]

def fetch(start, end, offset, limit):
    params = {
        "$select": ",".join(FIELDS),
        "$where": (
            f"created_date >= '{start}' AND created_date < '{end}'"
        ),
        "$order": "created_date,unique_key",
        "$limit": str(limit),
        "$offset": str(offset),
    }
    url = BASE + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(
        url, headers={"User-Agent": "CivicPulseStudentProject/1.0"}
    )

    for attempt in range(1, 7):
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return list(csv.DictReader(
                    io.StringIO(response.read().decode("utf-8-sig"))
                ))
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            print(f"Request failed ({attempt}/6): {exc}", flush=True)
            if attempt == 6:
                raise
            time.sleep(min(5 * attempt, 30))

def download_year(year):
    final_path = ROOT / f"311_{year}_sample.csv"
    part_path = ROOT / f"311_{year}_sample.csv.part"
    state_path = ROOT / f"311_{year}_sample.state.json"

    if final_path.exists():
        print(f"{final_path.name} already exists; skipping.")
        return

    start = f"{year}-01-01T00:00:00"
    end = f"{year + 1}-01-01T00:00:00"
    today = date.today()
    year_end = min(date(year + 1, 1, 1), today + timedelta(days=1))
    year_start = date(year, 1, 1)

    # Divide the year into 10-day periods, taking a small quota from each.
    periods = []
    cursor = year_start
    while cursor < year_end:
        period_end = min(cursor + timedelta(days=10), year_end)
        periods.append((cursor.isoformat() + "T00:00:00",
                        period_end.isoformat() + "T00:00:00"))
        cursor = period_end

    quota = max(1, TARGET_PER_YEAR // len(periods))
    print(f"\nYear {year}: {len(periods)} periods, about {quota} rows per period.")

    state = {"period": 0, "offset": 0, "count": 0}
    if state_path.exists():
        state = json.loads(state_path.read_text())
        print(f"Resuming at period {state['period'] + 1}, "
              f"saved rows: {state['count']}")

    mode = "a" if part_path.exists() else "w"
    existing_keys = set()
    if part_path.exists():
        with part_path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("unique_key"):
                    existing_keys.add(row["unique_key"])

    with part_path.open(mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if mode == "w":
            writer.writeheader()

        for pi in range(state["period"], len(periods)):
            pstart, pend = periods[pi]
            offset = state["offset"] if pi == state["period"] else 0
            period_count = 0

            while period_count < quota:
                limit = min(PAGE_SIZE, quota - period_count)
                rows = fetch(pstart, pend, offset, limit)
                if not rows:
                    break

                added = 0
                for row in rows:
                    key = (row.get("unique_key") or "").strip()
                    if not key or key in existing_keys:
                        continue
                    writer.writerow({field: row.get(field, "") or ""
                                     for field in FIELDS})
                    existing_keys.add(key)
                    added += 1

                f.flush()
                period_count += len(rows)
                offset += len(rows)
                state = {"period": pi, "offset": offset,
                         "count": len(existing_keys)}
                state_path.write_text(json.dumps(state))
                print(f"{year}: period {pi+1}/{len(periods)}, "
                      f"saved {len(existing_keys)} unique rows", flush=True)
                if len(rows) < limit:
                    break
                time.sleep(0.2)

            state = {"period": pi + 1, "offset": 0,
                     "count": len(existing_keys)}
            state_path.write_text(json.dumps(state))

    print(f"{year}: saved {len(existing_keys)} rows to {part_path.name}")
    print("Check the saved file before renaming it to the final .csv name.")

def main():
    for year in YEARS:
        try:
            download_year(year)
        except Exception as exc:
            print(f"\nStopped during {year}: {exc}")
            print("Run this script again later to resume.")
            return

if __name__ == "__main__":
    main()

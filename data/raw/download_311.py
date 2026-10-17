
import csv
import io
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
OUT = Path.home() / "civicpulse/data/raw/full_dataset"
OUT.mkdir(parents=True, exist_ok=True)

PAGE_SIZE = 50000
START_YEAR = 2020
END_YEAR = 2026  # Includes 2026; data may be updated during download.

for year in range(START_YEAR, END_YEAR + 1):
    output = OUT / f"311_{year}.csv"
    if output.exists():
        print(f"Skipping existing file: {output.name}")
        continue

    total = 0
    offset = 0
    first_page = True
    temp = OUT / f"311_{year}.csv.part"

    try:
        with temp.open("w", encoding="utf-8", newline="") as dest:
            while True:
                params = {
                    "$where": (
                        f"created_date >= '{year}-01-01T00:00:00' "
                        f"AND created_date < '{year + 1}-01-01T00:00:00'"
                    ),
                    "$limit": str(PAGE_SIZE),
                    "$offset": str(offset),
                    "$order": "created_date,unique_key",
                }
                url = BASE + "?" + urllib.parse.urlencode(params)
                request = urllib.request.Request(
                    url, headers={"User-Agent": "CivicPulse student project"}
                )

                with urllib.request.urlopen(request, timeout=180) as response:
                    content = response.read().decode("utf-8-sig")

                reader = csv.reader(io.StringIO(content))
                header = next(reader, None)
                rows = list(reader)

                if header is None:
                    break

                writer = csv.writer(dest)
                if first_page:
                    writer.writerow(header)
                    first_page = False

                writer.writerows(rows)
                dest.flush()

                count = len(rows)
                total += count
                print(f"{year}: {total:,} records downloaded")

                if count < PAGE_SIZE:
                    break

                offset += PAGE_SIZE
                time.sleep(0.25)

        temp.replace(output)
        print(f"Completed {year}: {total:,} records -> {output.name}")

    except Exception as exc:
        print(f"Stopped during {year} at offset {offset}: {exc}")
        print(f"Partial download retained at {temp}")
        break

print("Download script finished.")

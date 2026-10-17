# CivicPulse

CivicPulse analyzes recurring patterns in public-service requests using the NYC
311 Open Data API, Java, HDFS, Apache Pig, MongoDB, and Apache Hive.

## Milestone 1: Java API ingestion

The Java 17 collector retrieves a date-bounded set of fields from the NYC 311
SODA API and writes a raw CSV compatible with `CivicPulseCsvCleaner`. It
validates the response schema and counts records with missing required fields,
invalid dates, or duplicate IDs while preserving all returned rows in the raw
file. Duplicate removal belongs to the cleaning stage.

Compile and run from the project root:

```text
javac -d build src/CivicPulseIngestor.java
java -cp build CivicPulseIngestor 2026-10-12 2026-10-13 data/raw/311_api_sample.csv 100 100
```

Arguments are start date, exclusive end date, output CSV path, optional page
size (default `1000`, maximum `50000`), and optional maximum records (default
`10000`). The collector only fetches records created in the specified range.
It retries rate-limit and server errors a limited number of times. An optional
Socrata app token can be provided through the `SOCRATA_APP_TOKEN` environment
variable; no token is embedded in the project.

The raw file has these columns, in order:

```text
unique_key, created_date, agency, agency_name, complaint_type, descriptor,
location_type, incident_zip, city, borough, status
```

## Milestone 2: Pig cleaning and persistent hotspots

`scripts/complaint_patterns.pig` loads the four 11-column source extracts under
`/civicpulse/raw/yearly_samples/`, skips their headers, validates required
fields, normalizes selected categorical fields, deduplicates by `unique_key`,
and writes tab-separated clean records into HDFS. It then groups located
complaints by borough, ZIP code, category, and calendar month. A persistent
hotspot is an area/category pair with complaints in at least two distinct
months.

The script uses Piggybank's `CSVExcelStorage` so quoted CSV values are parsed
correctly. Change the `REGISTER` path if Piggybank is installed elsewhere.
Output paths must not already exist; use new run-specific directories for each
execution. Example for the installed `/opt/pig` setup:

```text
/opt/pig/bin/pig \
  -param INPUT=/civicpulse/raw/yearly_samples/311_20*_sample.csv \
  -param CLEANED_OUT=/civicpulse/processed/pig_clean_RUN_ID \
  -param PATTERNS_OUT=/civicpulse/analytics/persistent_hotspots_RUN_ID \
  -f scripts/complaint_patterns.pig
```

Both outputs are tab-separated without headers. The clean output fields follow
the 11-column input order. The hotspot output fields are `area`,
`complaint_type`, `months_observed`, and `total_complaints`. These thresholds
identify recurring reports only; they do not establish that an infrastructure
failure exists.

### Verified Pig run

On the four HDFS extracts under `/civicpulse/raw/yearly_samples/`, Pig 0.17.0
produced:

- 800,004 input lines, including four CSV headers.
- 800,000 cleaned records in
  `/civicpulse/processed/pig_clean_20261013_v2/`.
- 15,215 persistent area/category summaries in
  `/civicpulse/analytics/persistent_hotspots_20261013_v2/`.

The row-count check matches all 800,000 data lines after excluding the four
headers. The hotspot count is the result of this run and should not be treated
as a universal or independently validated infrastructure-risk measure. A
read-only HDFS recount on 2026-10-14 reconfirmed 800,000 clean rows and 15,215
pattern rows. The dashboard now uses this verified 800,000-row run as its
active analytical source. The separate 15,000-row API extract remains in HDFS
as an unchanged rollback option.

## Milestone 3: Hive tables and analysis

Hive 4.0.1 is installed in the current WSL environment, and its MySQL-backed
metastore schema reports version 4.0.0. `hive/schema.hql` defines external
tables over the Pig tab-separated files in HDFS; Hive reads those files in
place, so no second copy of the complaint dataset is made. `hive/
analysis_queries.hql` includes total-count, category, monthly-trend, hotspot,
and status queries.

Run from WSL after HDFS is available. Start the metastore and HiveServer2 in
two separate terminals. On this Java 17/Hive 4 installation, the module-open
options are required. Disabling notification polling avoids an unused metastore
notification API path; disabling `doAs` is appropriate only for this
single-user local demo and should not be copied to a multi-user deployment.

```text
HADOOP_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED" \
  /opt/hive/bin/hive --service metastore

HADOOP_HEAPSIZE=2048 \
HADOOP_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED" \
  /opt/hive/bin/hive --service hiveserver2 \
  --hiveconf hive.notification.event.poll.interval=-1 \
  --hiveconf hive.server2.enable.doAs=false \
  --hiveconf mapreduce.task.io.sort.mb=32

/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000/default \
  -n "$USER" -f /home/monica/civicpulse/hive/schema.hql
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000/default \
  -n "$USER" -f /home/monica/civicpulse/hive/analysis_queries.hql
```

### Verified Hive run

Hive 4.0.1 successfully registered both external tables and ran every query in
`hive/analysis_queries.hql` using local MapReduce. The Hive count was **800,000**,
matching the Pig-cleaned HDFS record count. The query returned 20 category
counts (top 20), 46 monthly totals, the top 100 Pig hotspot summaries, and a
seven-row status breakdown.

These are results for the four staged yearly extracts currently in HDFS, not
counts for all NYC 311 requests. The staged sample has a nearly even number of
records per month in several periods, so these observed rankings and counts
are pipeline-validation outputs, not population-level conclusions.

## Milestone 4: MongoDB patterns and evidence

`src/CivicPulseMongoImporter.java` reads the Pig hotspot output and streams the
clean HDFS complaint output to find up to five evidence complaints for each of
the top selected patterns. It stores only those pattern summaries and evidence
documents in MongoDB, not all 800,000 complaint rows. Mongo IDs include the
run ID, making a repeated import with the same run ID and input selection
upserts the same documents.

The MongoDB Java Sync Driver 5.5.1 dependencies are in `lib/`. Compile from
PowerShell in the project root with Java 17:

```powershell
javac -Xlint:all -cp "lib\*" -d build src\CivicPulseMongoImporter.java
java -cp "build;lib\*" CivicPulseMongoImporter `
  run_20261013 `
  /civicpulse/processed/pig_clean_20261013_v2 `
  /civicpulse/analytics/persistent_hotspots_20261013_v2 `
  100
```

The current Windows MongoDB service binds to Windows localhost, so run the Java
process in Windows PowerShell. The importer streams HDFS files through WSL;
`CIVICPULSE_WSL_DISTRO` can override the default `Ubuntu` distribution name.
For an environment where MongoDB is reachable from the same Linux environment,
run the same Java command there; it invokes `hdfs dfs -cat` directly. The
MongoDB URI defaults to `mongodb://127.0.0.1:27017` and can be overridden by
`CIVICPULSE_MONGODB_URI`. The current local MongoDB service has no configured
authentication, so do not expose it beyond the local development machine.

The database is `civicpulse`:

- `patterns`: `_id`, `runId`, `area`, `borough`, `incidentZip`,
  `complaintType`, `monthsObserved`, `totalComplaints`, and `evidenceCount`.
- `complaint_evidence`: `_id`, `runId`, `patternId`, `requestId`,
  `createdDate`, `borough`, `incidentZip`, `complaintType`, `descriptor`,
  `agency`, and `status`.

Evidence deliberately excludes address, coordinates, names, and other
unnecessary identifying details.

### Verified MongoDB run

The importer scanned all **800,000** Pig-cleaned HDFS rows with **zero
malformed rows**, selected the top **100** hotspot summaries, and wrote **500**
evidence complaint documents (up to five per pattern). MongoDB read-back
verified 100 patterns and 500 evidence records, including retrieval of a
pattern and its linked request evidence. No full complaint dataset was copied
into MongoDB.

## Milestone 5: Analytical dashboard

`src/CivicPulseDashboardServer.java` serves `dashboard/index.html` from a
Java 17 HTTP server bound to `127.0.0.1:8088`. Hive analytics run through
Beeline in WSL, while the Windows Java service reads the selected pattern
summaries and evidence from the local MongoDB service:

```text
NYC open data -> Java ingestion -> HDFS -> Pig -> clean HDFS files
    -> Hive aggregates -------> dashboard charts and KPIs
    -> MongoDB patterns/evidence -> recurring-area table and drill-down
```

Hive external tables read Pig's HDFS outputs in place. The dashboard executes
its Hive aggregates through one Beeline launch and caches up to eight results
for two minutes, keyed by the complete date, borough, and complaint-type
filter. A cache miss still runs Hive; an exact-filter repeat during the TTL
reuses the result, and expiration or a server restart clears it. Complaint
browsing remains paginated and retrieves its page and total in one Beeline
launch. Date, borough, and complaint-type filters are applied in Hive before
the charts and counts are returned. MongoDB supplies selected pattern
summaries and up to five stored evidence records per pattern; it does not
duplicate the full complaint dataset.

On the 2026-10-14 local 15,000-row API run, a cold `/api/dashboard` request on
port 8088 took 53.4 seconds. Exact-filter cache hits took 38-133 milliseconds in subsequent
repeat checks; one request in an earlier pair unexpectedly took 53.6 seconds,
which was not reproduced in the later isolated repeats. A new filter
combination or expired cache entry still incurs Hive query time. A paginated
request for 10 complaint records returned those 10 of 15,000 in 18.8 seconds.
These measurements are not timings for the active 800,000-row source and are
not performance guarantees; Hive remains the main cold-query delay.

Build and start the dashboard from PowerShell in the project root after
HDFS, HiveServer2, and MongoDB are running:

```powershell
javac -Xlint:all -cp "lib\*" -d build src\CivicPulseDashboardServer.java
java -cp "build;lib\*" CivicPulseDashboardServer
```

Open `http://127.0.0.1:8088/`. The dashboard displays the full observed data
date range and has date, borough, and complaint-type filters. The monthly,
category, borough-level, and persistent-pattern aggregates respond to those
filters. Its JSON routes are `GET /api/dashboard` and
`GET /api/patterns/{patternId}/evidence`; both accept `from`, `to`, `borough`,
and `complaintType` query parameters. Date filters use inclusive dates in the
UI and Hive applies an exclusive upper bound on the next day. The additional
`GET /api/complaints` route accepts those filters plus `search`, `status`,
`page`, and `pageSize`. The `GET /api/zip-hotspots` and
`GET /api/zip-details?zip=10001` routes return filter-aware Hive aggregates;
the details route requires a five-digit ZIP and includes the leading complaint
types, status counts, and borough context. Clicking or keyboard-activating a
map region opens that breakdown in an accessible popup. Demo submissions use
`GET /api/demo-reports` and
`POST /api/demo-reports`, stored only in MongoDB's `demo_submissions`
collection. The dashboard's defaults match the local setup; override them with `CIVICPULSE_HIVE_URL`,
`CIVICPULSE_HIVE_USER`, `CIVICPULSE_WSL_DISTRO`, `CIVICPULSE_MONGODB_URI`,
`CIVICPULSE_MONGO_RUN_ID`, or `CIVICPULSE_DASHBOARD_PORT` as needed. The Hive
JDBC URL is used by Beeline inside WSL, not by a Windows JDBC driver.

The dashboard UI uses HTML, CSS, and vanilla JavaScript; it is served by the
Java HTTP server and is not a React application or a static screenshot. Its
left navigation separates Home, Big Data Insights, Hotspot Explorer, Recurring
Patterns, Browse Complaints, and Report an Issue. Home shows Hive-derived totals,
the leading category and borough, top-five categories, and monthly trends.
Big Data Insights retains the service-status chart and adds the top 10 complaint
categories, monthly complaint volume, borough comparison, and filtered Hive KPIs
for reports, distinct categories, closed share, and months with reports. Its
month-over-month indicator is shown only when the two latest reported months are
consecutive. The current API does not return a category-by-borough cross-tab, so
the optional heatmap is not displayed; supporting it requires a Hive
`GROUP BY complaint_type, borough` aggregate and a corresponding API field.
Hotspot Explorer shows Hive-filtered Queens completed (source status CLOSED)
and in-progress counts, plus a ZIP-region choropleth. The Queens chart follows
the selected date and issue-type filters but remains scoped to Queens regardless
of the borough dropdown selection.
The dashboard does not claim ward or street-level precision that the extract
cannot support. Clicking a ZIP region shows complaint totals and breakdowns
from Hive under the active filters.

Browse Complaints queries Hive with the dashboard's date, borough, and category
filters plus search and source-status filters; it returns 25 rows per page.
Report an Issue is a prototype only: submissions go to the separate MongoDB
`demo_submissions` collection and do not enter public-data Hive analytics or
contact NYC 311. It accepts category, borough, date, and description; image
uploads and personally identifying details are not supported. Refreshing or
filtering the dashboard changes the displayed analytics, while changing the
underlying public records requires rerunning ingestion and the HDFS/Pig/Hive/
MongoDB pipeline.

### Verified dashboard run

With HiveServer2 configured with a 2 GB heap, the original staged-data API
returned HTTP 200 with
**800,000** Hive complaint rows, **198** distinct complaint categories, **46**
monthly aggregates, **100** MongoDB patterns, and **500** evidence documents.
The dashboard charts the top 15 categories. The pattern evidence route
returned five matching MongoDB records for a selected pattern. The dashboard
page loaded in the browser, and the Java source has no reported diagnostics.

An earlier default HiveServer2 process used a 256 MB heap and failed during a
local MapReduce scan. The `HADOOP_HEAPSIZE=2048` setting above is needed for
the local 800,000-row demonstration; lower-memory environments may need a
smaller staged input. This 800,000-row run was over the staged yearly extracts.
As throughout the project, the dashboard labels staged-data metrics as
pipeline results, not population estimates, and recurring complaints are not
proof of confirmed infrastructure faults.

The active dashboard source is the staged 800,000-row run `run_20261013`. For
comparison, the separate 15,000-row API integration run from **2024-01-01
through 2024-03-01** had 130 categories and three month groups. An
applied filter for **2024-01-01 through 2024-02-29**, borough **BROOKLYN**, and
category **ILLEGAL PARKING** returned 688 complaints, one category, January
and February monthly buckets, 37 matching persistent groups in Hive, and
22 displayed patterns with selected MongoDB evidence. The evidence endpoint
returned five records for a selected filtered pattern. It remains preserved
in HDFS but is not the active dashboard source. The dashboard also
shows Hive borough-level totals and the actual month list observed for each
displayed pattern. No street-level precision is implied.

## Milestone 6: Fresh API-to-dashboard integration

A new end-to-end run was completed using fresh API downloads for
**2024-01-01 through 2024-04-01**, capped at 5,000 records per request. The
January file came from a January-to-April request ordered by creation date and
the cap returned January records; February and March were collected with
month-bounded requests. The three files contain 15,000 API rows in total.
Every collection reported zero required-field/date validation failures and
zero duplicate IDs within its own request. HDFS keeps the three raw files under
`/civicpulse/raw/api_monthly_e2e_20261013/`.

Pig processed the combined monthly files into
`/civicpulse/processed/pig_clean_monthly_e2e_20261013/` and generated 1,505
area/category patterns in
`/civicpulse/analytics/persistent_hotspots_monthly_e2e_20261013/`. Hive
external table locations were moved to these run-scoped paths; Hive counted
15,000 clean rows, 1,505 patterns, 130 categories, and records from all three
months. The MongoDB importer selected 100 patterns and linked 500 evidence
records with zero malformed clean rows. This documents the historical 2024
integration run and is not the active dashboard source.

This run validates that the source API batch can proceed through Java, HDFS,
Pig, Hive, MongoDB, and the dashboard. It is a bounded sample of up to the
first 5,000 records returned in each request, not a representative sample of
NYC 311. It remains preserved but is not the active dashboard source.

The three bounded Java requests used for this run were:

```powershell
java -cp "build" CivicPulseIngestor 2024-01-01 2024-04-01 `
  data/raw/311_api_e2e_20261013.csv 1000 5000
java -cp "build" CivicPulseIngestor 2024-02-01 2024-03-01 `
  data/raw/311_api_e2e_feb_20261013.csv 1000 5000
java -cp "build" CivicPulseIngestor 2024-03-01 2024-04-01 `
  data/raw/311_api_e2e_mar_20261013.csv 1000 5000
```

### Preserved bounded public-data API run

On 2026-10-14, Java fetched three current NYC Open Data API extracts, each
capped at 5,000 rows, for August, September, and October 2026. Pig cleaned
15,000 rows and generated 1,790 recurring borough/ZIP/category patterns.
MongoDB stores the selected 100 patterns and 500 linked evidence records.
At the time, Hive external tables pointed to these HDFS run directories and
the dashboard Mongo run ID was `run_live_20261014`. These 15,000 Pig-cleaned
rows remain preserved in HDFS as a rollback option; they are not the active
dashboard source. The API-reported data coverage is
**2026-08-01 through 2026-10-01**; the October request was bounded through
October 15, but the dashboard reports the actual minimum/maximum dates in the
returned rows. Counts remain a bounded sample, not a full or representative
NYC population. Dashboard complaint totals and chart values are displayed as
whole report counts with thousands separators; category, pattern, and
evidence-document counts remain item counts.

The Hotspot Explorer uses Leaflet with OpenStreetMap basemap tiles and shades
ZIP regions using filtered counts queried from Hive. Its bundled GeoJSON
contains 2020 Census ZIP Code Tabulation Areas (ZCTAs) returned for ZIP codes
in the preserved bounded API extract from the [U.S. Census Bureau TIGERweb ZCTA
layer](https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/2).
Selecting a region opens its filtered complaint total, categories, statuses,
and borough context from Hive. Internet access is required for the Leaflet CDN
and OpenStreetMap tiles; the ZIP boundary geometry is served locally. ZCTAs
approximate ZIP areas; they are not ward boundaries and do not represent
geocoded complaint points. The 800,000-row run may contain ZIPs without a
bundled shape, so unmapped areas remain included in Hive totals but cannot be
shaded on the map. The map integration does not require re-ingesting complaint
records.

For the historical 2024 integration run, the raw files were uploaded to
`/civicpulse/raw/api_monthly_e2e_20261013/`. Its repeat commands are:

```text
/opt/pig/bin/pig \
  -param INPUT=/civicpulse/raw/api_monthly_e2e_20261013/311_api_e2e_*.csv \
  -param CLEANED_OUT=/civicpulse/processed/pig_clean_monthly_e2e_20261013 \
  -param PATTERNS_OUT=/civicpulse/analytics/persistent_hotspots_monthly_e2e_20261013 \
  -f scripts/complaint_patterns.pig
```

Then, from Windows PowerShell, import selected patterns and verify the
dashboard integration:

```powershell
java -cp "build;lib\*" CivicPulseMongoImporter `
  run_api_e2e_20261013 `
  /civicpulse/processed/pig_clean_monthly_e2e_20261013 `
  /civicpulse/analytics/persistent_hotspots_monthly_e2e_20261013 `
  100
```

Hive table locations must match the Pig paths before querying. Run
`hive/schema.hql` when registering the tables for the first time; for existing
external tables use `ALTER TABLE ... SET LOCATION` if the run paths changed.
The active dashboard source is the 800,000-row historical run. To roll back to
the preserved 15,000-row API extract, set `civicpulse.complaint_clean` to
`/civicpulse/processed/pig_clean_live_3m_20261014` and
`civicpulse.persistent_hotspots` to
`/civicpulse/analytics/persistent_hotspots_live_3m_20261014`, then restart the
dashboard with `CIVICPULSE_MONGO_RUN_ID=run_live_20261014`.
With HDFS, HiveServer2, and MongoDB running, start
`CivicPulseDashboardServer` and verify `/api/dashboard` and a selected
`/api/patterns/{patternId}/evidence` route.

The current Hive definitions in `hive/schema.hql` point at
`pig_clean_20261013_v2` and `persistent_hotspots_20261013_v2`. Existing
external tables require `ALTER TABLE ... SET LOCATION` when switching runs;
re-running `CREATE EXTERNAL TABLE IF NOT EXISTS` alone does not change their
locations.

### Repeatable validation

`scripts/validate_pipeline.ps1` cross-checks the HDFS clean/pattern line counts
against Hive, verifies every stored pattern spans at least two months, checks
Hive category/month totals against the dashboard, and requests evidence for a
returned MongoDB pattern. Run it from an authorized PowerShell session after
HDFS, HiveServer2, MongoDB, and the dashboard server are available:

```powershell
.\scripts\validate_pipeline.ps1
```

Parameters such as `-RunId`, `-CleanHdfsPath`, `-PatternsHdfsPath`,
`-DashboardUrl`, `-WslDistro`, `-HiveUrl`, and `-HiveUser` allow validation of
another already-registered run. A successful run prints the compared counts
and exits normally; any mismatch or unavailable service raises an error.

## Academic deliverable

The report, including the abstract, objectives, architecture, module
descriptions, measured evaluation checks, limitations, references, and short
faculty presentation, is in
[`docs/CivicPulse_Project_Report.md`](docs/CivicPulse_Project_Report.md).

The timed faculty walkthrough, run checklist, validated demo outputs, and
likely-question answers are in
[`docs/CivicPulse_Demo_Guide.md`](docs/CivicPulse_Demo_Guide.md).
#   C i v i c P u l s e  
 
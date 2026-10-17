# CivicPulse Faculty Demonstration Guide

## Demonstration goal

Show that CivicPulse is a connected data pipeline, not just a dashboard:

```text
NYC 311 API -> Java -> HDFS -> Pig -> Hive + MongoDB -> Java dashboard
```

Use the completed staged historical run `run_20261013`. Do not rerun ingestion
or Pig during a short live presentation. The active dashboard reads its
800,000-row HDFS output through Hive and reads selected pattern details from
MongoDB; the 15,000-row bounded API extract remains preserved separately.

## Before the presentation

Use the completed 800,000-row HDFS outputs; **do not rerun ingestion or Pig**
before a short presentation. Leave each long-running service terminal open.

1. In WSL Ubuntu, start HDFS if it is not already active:

   ```bash
   start-dfs.sh
   ```

2. In a separate WSL terminal, start the metastore:

   ```bash
   export HADOOP_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
   /opt/hive/bin/hive --service metastore
   ```

3. In a third WSL terminal, start HiveServer2 with the heap used in the
   successful local run:

   ```bash
   export HADOOP_HEAPSIZE=2048
   export HADOOP_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
   /opt/hive/bin/hive --service hiveserver2 \
     --hiveconf hive.notification.event.poll.interval=-1 \
     --hiveconf hive.server2.enable.doAs=false \
     --hiveconf mapreduce.task.io.sort.mb=32
   ```

   The `doAs=false` setting is only for this single-user local demo, not a
   multi-user deployment. The smaller MapReduce sort buffer prevents the
   local HiveServer2 JVM from running out of heap while aggregating the
   800,000-row dataset; the dashboard also applies this setting to each
   Beeline session.

4. In Windows PowerShell, check MongoDB and start it only if needed:

   ```powershell
   Get-Service MongoDB
   # If stopped and your account has permission:
   Start-Service MongoDB
   ```

5. In Windows PowerShell at the project root, compile and start the dashboard
   only if port 8088 is not already serving it:

   ```powershell
   javac -Xlint:all -cp "lib\*" -d build src\CivicPulseDashboardServer.java
   java -cp "build;lib\*" CivicPulseDashboardServer
   ```

6. Run the validator from a separate PowerShell prompt. The workspace is
   hosted under a WSL UNC path and Windows `RemoteSigned` policy blocked
   direct execution in this environment; this in-session form was tested
   without changing execution policy:

   ```powershell
   $script = Get-Content '.\scripts\validate_pipeline.ps1' -Raw
   Invoke-Expression $script
   ```

7. Keep the successful validation output visible. If startup cannot be
   completed in time, use the verified run paths and report rather than
   claiming a live test.

## Suggested 8-minute walkthrough

### 0:00-0:45 — Problem and scope

Introduce CivicPulse as descriptive analysis of public NYC 311 service
requests. Emphasize at the outset that recurrence means repeat reports in the
selected data, not a confirmed infrastructure fault or a prediction.

### 0:45-1:30 — Data source and Java

Show `src/CivicPulseIngestor.java` and the official NYC Open Data dataset/API
reference in the project report. Explain date-bounded API requests,
pagination, selected fields, validation, and the CSV written by Java. For the
integration run, show the three retained raw files in
`/civicpulse/raw/api_monthly_e2e_20261013/`.

### 1:30-2:30 — HDFS and Pig

Show the raw HDFS path and the `scripts/complaint_patterns.pig` pipeline.
Explain quoted-CSV parsing, required-field filtering, normalization,
deduplication by `unique_key`, and month grouping. Use this definition:

> A pattern is a borough/ZIP and complaint-category group with reports in at
> least two distinct calendar months.

Show the active clean and pattern output paths:

- `/civicpulse/processed/pig_clean_20261013_v2/`
- `/civicpulse/analytics/persistent_hotspots_20261013_v2/`

### 2:30-3:30 — Hive

Explain that Hive external tables read Pig's tab-separated HDFS output in
place; they do not duplicate the clean dataset. Show the schema and analysis
queries. For this run, Hive should return 800,000 clean rows and 15,215 pattern rows.
The staged extracts cover 46 observed months and 198 complaint categories.

### 3:30-4:30 — MongoDB

Explain that MongoDB contains only the 100 selected pattern documents and up
to five supporting evidence records per pattern, rather than all complaint
rows. The importer scanned 800,000 clean rows, skipped zero malformed records,
and verified 100 patterns and 500 evidence documents.

### 4:30-6:30 — Dashboard

Open `http://127.0.0.1:8088/` and demonstrate:

1. The overview cards: 800,000 reports from Hive, the returned category and
   recurring-pattern counts, and MongoDB's 100 selected pattern documents
   with 500 linked evidence records. This is the staged historical dataset,
   not a current or representative NYC 311 population.
2. The dataset date coverage, then apply a date range, borough, and complaint
   type filter. Explain that the aggregate results are recalculated in Hive.
3. Open Big Data Insights and show service-request status, the most persistent
   area/issue patterns (from the stored sample of up to 100), the
   category-concentration Pareto chart, and borough
   comparison. Explain that the Pareto bars show category counts while the
   connected line shows their cumulative share of all filtered reports.
   The top-five share KPI makes that concentration easy to summarize. Pattern
   persistence shows observed months and matching report counts for the
   MongoDB-backed patterns, using Hive metrics for the active filters. These
   are descriptive results for the staged extract, not forecasts. The optional
   category-by-borough heatmap is omitted because the current API does not
   expose that cross-tab.
4. The Home issue-category chart and regional pie, then open Hotspot Explorer.
   Show the Leaflet map with OpenStreetMap basemap tiles, ZIP-region overlay,
   and ranked ZIP counts. Explain that Hive supplies the counts and the shaded
   shapes are approximate 2020 Census ZIP Code Tabulation Areas, not complaint
   coordinates or wards. Select a region to show its filtered complaint total,
   leading issue types, statuses, and borough context. Internet access is needed
   to load Leaflet from its CDN and retrieve OpenStreetMap tiles.
5. The Queens completed / in-progress counts and recurring-pattern table,
   including observed month periods, frequency,
   and its bounded sample of MongoDB-backed patterns.
6. A pattern's “View details” drawer and linked example records.

### 6:30-7:30 — Validation

Run or show `scripts/validate_pipeline.ps1`. Its defaults target `run_20261013`
and check HDFS line counts,
Hive counts, the recurrence minimum, dashboard aggregates, and linked MongoDB
evidence. The verified run passed with:

```text
Clean rows: HDFS=800000, Hive=800000, Dashboard=800000
Patterns: HDFS=15215, Hive=15215; minimum months=2
Hive categories=198; months=46
MongoDB patterns=100; evidence=500
Evidence lookup returned 5 records
```

After switching the source, test date, borough, and complaint-type filters and
open a pattern detail. The API should report the filtered Hive totals and
MongoDB evidence for `run_20261013`. The older 15,000-row filter measurements
document a different run and should not be presented as tests of this source.

### 7:30-8:00 — Limitations and close

State that the active dataset consists of staged historical extracts, is not
representative of all NYC 311, and reported recurrence does not establish
fault, cause, or risk.
Resolution duration is not analyzed because this implementation does not
establish reliable resolution timestamp semantics. The separate 15,000-row API run is preserved as an integration example, not
the source of the active dashboard.

## Likely faculty questions

### Why use Pig if Hive can clean data?

Pig is used as the required transformation stage for parsing, normalization,
validation, deterministic de-duplication, and recurring-pattern generation.
Hive then exposes the resulting HDFS files as analytical tables and runs
descriptive queries. The tools have separate, visible responsibilities.

### Why use both HDFS and MongoDB?

HDFS retains the raw and processed file datasets for batch processing. MongoDB
is used selectively for flexible pattern summaries and linked evidence
documents used in the dashboard. The complete complaint set is not copied
into MongoDB.

### What does “persistent hotspot” mean here?

It is a chosen descriptive label for a borough/ZIP and complaint category
reported in at least two distinct months. It does not mean that the system
confirmed a hazardous location or infrastructure defect.

### Is the sample representative of NYC?

No. The live API run is capped at 5,000 records per request, and API ordering
and those caps do not create a representative sample. The 800,000-row run
uses staged extracts with quota-like monthly balance. Results are evidence of
pipeline execution and must not be presented as population estimates.

### How did you validate correctness?

The fresh run collected 15,000 rows. Pig wrote 15,000 clean rows and 1,505
patterns; Hive independently counted the same clean and pattern rows. Every
pattern had at least two months. The Mongo importer scanned the 15,000 clean
rows with zero malformed rows and verified 100 patterns and 500 evidence
records. The dashboard returned matching counts and a selected evidence
lookup returned five records.

### Why not report resolution time?

The implementation has creation time and current status, but it does not
establish a reliable resolution timestamp/history for calculating duration.
Reporting such a measure without verifying source semantics would be
misleading.

### What would you improve next?

Collect a larger explicitly documented sample, evaluate sampling effects,
preserve source/run metadata, and validate category/location quality. Only
consider prediction after defining an outcome and obtaining labeled data for
held-out evaluation. A map would require careful location quality and privacy
review.

## Reference files

- Full report: [`CivicPulse_Project_Report.md`](./CivicPulse_Project_Report.md)
- Pipeline instructions and measured run details: [`../README.md`](../README.md)
- Validator: [`../scripts/validate_pipeline.ps1`](../scripts/validate_pipeline.ps1)

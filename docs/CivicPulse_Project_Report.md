# CivicPulse: A Big Data-Based Public Complaint Pattern Intelligence System

## Abstract

CivicPulse demonstrates a Big Data pipeline for descriptive analysis of public
service complaints. It uses NYC 311 Service Requests as the data source and
connects Java ingestion, Hadoop Distributed File System (HDFS), Apache Pig,
MongoDB, Apache Hive, and an analytical dashboard. Java retrieves a bounded
sample from the public Socrata API and writes CSV. Pig parses and normalizes
the source, rejects incomplete records, deduplicates requests by source ID,
and groups located reports into recurring area/category patterns. The clean
files remain in HDFS and are queried through Hive external tables. MongoDB
stores selected pattern summaries and a limited number of example records,
not a duplicate of the complete dataset.

The distinguishing analysis defines a persistent complaint pattern as the
same borough/ZIP area and complaint category receiving reports in at least two
distinct calendar months. It measures recurrence in reports; it does not
confirm a physical fault or predict an infrastructure failure. In a verified
staged-data run, Pig produced 800,000 cleaned records and 15,215 pattern
summaries, and Hive returned a matching count. A separate fresh API integration
run collected 15,000 records from three date-bounded API queries covering
January through March 2024; Pig produced 1,505 patterns, Hive matched the
15,000 clean rows, and MongoDB stored 100 selected patterns with 500 evidence
documents. The dashboard combined Hive aggregates with MongoDB evidence.
These are implementation checks on bounded inputs, not estimates for all NYC
311 requests. No predictive accuracy or infrastructure-failure detection is
claimed.

## 1. Problem statement and motivation

Municipal service-request datasets contain reports distributed across dates,
issue categories, and locations. A raw list does not readily show whether
complaints recur in an area, how reported volumes vary by month, or which
selected patterns can be examined through example records. CivicPulse addresses
the engineering problem of building a repeatable pipeline to process public
complaint records and present descriptive patterns for review.

The analysis is limited to reported service requests. Reports may be
incomplete, duplicated, unevenly sampled, or affected by changes in reporting
and agency practice. A recurring group therefore means repeated reports in the
selected data; it does not establish an underlying fault, risk, or cause.

## 2. Objectives

1. Collect a bounded set of public NYC 311 records using Java.
2. Store raw and transformed data in HDFS.
3. Use Pig to parse CSV, normalize fields, reject incomplete rows, deduplicate
   by request ID, and generate recurring-pattern summaries.
4. Define Hive external tables over Pig outputs and run descriptive queries.
5. Store selected patterns and bounded evidence in MongoDB rather than copying
   all complaint documents.
6. Present totals, category/month aggregates, patterns, and linked examples in
   a dashboard.
7. Verify record-count consistency and the integrated dashboard routes.

## 3. Scope and assumptions

### In scope

- NYC 311 Service Requests from 2020 to Present.
- Java 17 API collection for explicit date ranges and record limits.
- HDFS raw and processed storage; Pig cleaning and recurrence analysis.
- Hive analytics over processed HDFS files.
- Run-scoped MongoDB pattern/evidence documents.
- A local dashboard with category/month charts and pattern drill-down.

### Out of scope

- Exhaustive ingestion of the entire NYC 311 population.
- Private municipal databases or private user information.
- Geocoding or street-level mapping.
- Predicting infrastructure failures or assessing complaint validity.
- Causal analysis or claims about service quality.

### Assumptions

- `unique_key` identifies source requests for deduplication.
- The `created_date` value provides a usable month where it is present.
- Borough and ZIP code are adequate for a coarse area grouping, not a precise
  location.
- Staged yearly extracts demonstrate the pipeline but are not a
  representative statistical sample of the full dataset.

## 4. Dataset and source

**Dataset:** NYC Open Data, *311 Service Requests from 2020 to Present*  
**Official dataset page:**  
<https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9>  
**Socrata CSV endpoint:**  
<https://data.cityofnewyork.us/resource/erm2-nwe9.csv>

The Java collector requests selected columns for a bounded date range from the
public CSV endpoint, paginating with `$limit` and `$offset`. It can optionally
use a Socrata app token provided through an environment variable; no token is
embedded in the project. Requests may be throttled or API behavior may change,
so the collector uses bounded retries and each run should record the requested
dates and record limit. The data is publicly accessible; consult the current
NYC Open Data terms and dataset metadata for reuse conditions and update notes
before redistributing data.

### Selected schema

| Field | Use |
|---|---|
| `unique_key` | Source identifier and Pig deduplication key |
| `created_date` | Request creation time and month grouping input |
| `agency`, `agency_name` | Agency labels |
| `complaint_type` | Normalized issue category |
| `descriptor` | Source description of the issue |
| `location_type` | Source location-type field |
| `incident_zip`, `city`, `borough` | Coarse location grouping and display |
| `status` | Descriptive status analysis |

The selected fields include creation date and current status, but this
implementation does not rely on a universally reliable resolution timestamp.
Resolution-duration analysis is therefore not reported. Address and
coordinates are not included in MongoDB evidence documents.

## 5. Existing-system limitations and proposed approach

### Limitations addressed

- Large raw request lists are difficult to inspect manually.
- Overall counts alone do not summarize recurrence across place and time.
- Copying every complaint into a document database would be unnecessary for
  this demonstration.
- Aggregate results are easier to review when selected examples are available.

### Proposed approach

- Use HDFS as the file-storage layer for raw and transformed data.
- Perform explicit parsing, validation, normalization, and deduplication in
  Pig.
- Define recurrence in interpretable terms: same borough/ZIP, category, and at
  least two distinct months.
- Use Hive for file-based aggregate queries; use MongoDB for selected flexible
  summaries and bounded examples.
- Present descriptive measures with clear caveats and linked evidence.

## 6. Architecture and workflow

```text
NYC Open Data SODA API
        |
        v
Java collector (selected fields, validation, CSV)
        |
        v
HDFS raw files
        |
        v
Apache Pig (CSV parsing, normalization, deduplication)
        |                                  |
        v                                  v
HDFS clean TSV                    HDFS recurring-pattern TSV
        |                                  |
        v                                  v
Hive external tables             Java importer -> MongoDB
        |                                  |
        +----------------+-----------------+
                         v
                Java dashboard service
                         |
                         v
                Browser charts and details
```

1. **Collection — Java 17.** `src/CivicPulseIngestor.java` requests selected
   fields from the SODA CSV API for an explicit date range. It validates
   required fields/dates, counts duplicate IDs in the API collection run, and
   writes a header plus CSV rows. The raw result is placed in HDFS.
2. **Raw storage — HDFS.** Raw data is kept under `/civicpulse/raw/`; the
   verified multi-year sample extracts are under
   `/civicpulse/raw/yearly_samples/`.
3. **Transformation — Pig.** `scripts/complaint_patterns.pig` uses Piggybank
   `CSVExcelStorage` to read quoted CSV. It rejects headers and incomplete
   rows, trims and normalizes selected fields, and retains one deterministic
   record per `unique_key`. It writes clean headerless TSV to
   `/civicpulse/processed/`. It also groups by `borough|incident_zip`,
   `complaint_type`, and calendar month, emitting summaries with at least two
   months under `/civicpulse/analytics/`.
4. **Document store — MongoDB.** `src/CivicPulseMongoImporter.java` reads the
   Pig pattern output and streams clean HDFS rows to choose up to five
   examples for each of the selected top 100 patterns. Documents are
   run-scoped. The full clean dataset is not copied to MongoDB.
5. **Analytics — Hive.** `hive/schema.hql` defines external tables over Pig TSV
   directories. Hive reads the HDFS files in place. The
   `hive/analysis_queries.hql` file includes total, category, monthly,
   pattern, and status queries.
6. **Presentation — Java dashboard.** `src/CivicPulseDashboardServer.java`
   serves `dashboard/index.html` and JSON endpoints. It obtains aggregates
   through Beeline in WSL and selected patterns/evidence from MongoDB. Date,
   borough, and category filters are applied in Hive. The UI displays the
   observed data coverage, category/month trends, borough-level totals,
   recurring-pattern period lists, and a pattern-evidence drawer. Its
   geographic aggregate is borough-level; pattern groups use borough and ZIP.

### Recurrence algorithm

- **Group key:** normalized borough, ZIP, and complaint category.
- **Time unit:** the `yyyy-MM` prefix of `created_date`.
- **Rule:** count distinct months per group; include the group when
  `months_observed >= 2`.
- **Output:** `months_observed` and `total_complaints`.
- **Interpretation:** descriptive recurrence in the selected records only.
- **Limitations:** missing/incorrect ZIP or borough values affect grouping;
  categories may be broad; sampling changes counts; recurrence is not a
  confirmed fault or a prediction.

## 7. Modules

| Module | Responsibility | Main artifact |
|---|---|---|
| Java API ingestor | Fetch, validate, and write bounded API data | `src/CivicPulseIngestor.java` |
| HDFS | Retain raw and transformed files | `/civicpulse/raw`, `/processed`, `/analytics` |
| Pig | Clean, deduplicate, normalize, and derive patterns | `scripts/complaint_patterns.pig` |
| Hive | Define external tables and run aggregates | `hive/schema.hql`, `hive/analysis_queries.hql` |
| Mongo importer | Store selected patterns and limited examples | `src/CivicPulseMongoImporter.java` |
| Dashboard | Combine Hive aggregates and Mongo drill-down | `src/CivicPulseDashboardServer.java`, `dashboard/index.html` |

## 8. Hardware and software requirements

### Software observed in the local setup

| Component | Version/configuration |
|---|---|
| Java | OpenJDK/Temurin 17 |
| Hadoop/HDFS | Hadoop 3.4.2, local WSL setup |
| Apache Pig | 0.17.0 |
| Apache Hive | 4.0.1; MySQL metastore schema 4.0.0 |
| MongoDB | 8.3 local Windows service |
| MongoDB Java Sync Driver | 5.5.1 |
| Operating environment | Windows with WSL Ubuntu |

Hive 4 and Hadoop 3 need Java 17 module-open options in this setup. A 2 GB
HiveServer2 heap was used for the 800,000-row local MapReduce demonstration.
Disabling `hive.server2.enable.doAs` was used only for a single-user local
demonstration and is not a multi-user security configuration. The dashboard
binds to loopback; do not expose the services without appropriate
authentication and network security.

Allow sufficient host memory for WSL, HDFS, HiveServer2, local MapReduce,
MongoDB, and the browser concurrently. The 2 GB Hive heap is an observed
configuration, not a universal minimum. Smaller-memory systems can reduce the
input sample; cluster requirements depend on deployment and data size.

## 9. Evaluation and observed test results

These are implementation checks, not statistical model-accuracy measures.

| Check | Acceptance condition | Observed result |
|---|---|---|
| API ingestion | Bounded response is written and required fields/date validation is reported | 50 records fetched in the documented sample run; 0 failed required-field/date validation |
| Fresh API integration | Date-bounded collections produce records with validation reported | 15,000 rows collected by three date-bounded queries covering Jan-Mar 2024; every request reported 0 validation failures and 0 duplicate IDs within that request |
| Pig output | Source headers removed and normalized clean records written | 800,000 clean records from four files with 800,004 input lines including headers |
| Pattern generation | Every emitted group occurs in at least two distinct months | 15,215 summaries emitted; script filter is `months_observed >= 2` |
| Pig/Hive consistency | Hive clean count equals Pig clean output count | Both reported 800,000 |
| Hive aggregates | Category and monthly queries return results | Analysis query returned the top 20 category rows and 46 monthly totals |
| MongoDB selection | Store selected summaries and bounded evidence only | 100 pattern docs and 500 evidence docs; importer scanned 800,000 clean rows and reported zero malformed rows |
| Dashboard API | Hive and Mongo data are returned together | HTTP 200; 800,000 records, 198 distinct categories, 46 months, 100 patterns, 500 evidence documents |
| Dashboard chart/drill-down | Top-category chart and linked evidence render | 15 categories charted; browser check showed 100 pattern buttons and five evidence cards for a selected pattern |
| Fresh-run dashboard | Dashboard reports the fresh API run's Hive and Mongo data | HTTP 200; run `run_api_e2e_20261013`, 15,000 clean records, 130 category groups, 3 months, 100 patterns, 500 evidence records; evidence route returned five linked records |
| Repeatable pipeline validator | HDFS, Hive, MongoDB, and dashboard counts and recurrence criteria agree | `scripts/validate_pipeline.ps1` passed: HDFS/Hive/dashboard clean counts 15,000; HDFS/Hive patterns 1,505; minimum observed months 2; 130 categories; 3 months; MongoDB 100 patterns / 500 evidence; evidence lookup returned five |
| Dashboard filters and geography | Date, borough, and category filters change Hive totals, charts, and recurring patterns | Brooklyn + ILLEGAL PARKING + 2024-01-01 through 2024-02-29 returned 688 rows, 1 category, 2 months, 37 Hive recurring groups; UI shows borough-level totals |
| Pattern periods | Show frequency and months for displayed groups | Hive recomputes total reports and sorted observed month list per currently filtered recurring group |

Hive found 198 distinct category groups for this staged run. The analysis
query shows the top 20 and the dashboard charts the top 15; its category KPI
counts all 198 groups. Re-run checks after changing source extracts, run IDs,
Hive tables, or MongoDB data.

The fresh API integration run had 15,000 Pig-clean records, 1,505 Pig
area/category patterns, and Hive counts of 15,000 clean rows, 1,505 pattern
rows, 130 categories, and three calendar months. MongoDB verified 100 selected
patterns and 500 evidence documents after scanning 15,000 records with zero
malformed rows. The running dashboard is configured to this integration run;
its API returned HTTP 200 and a selected evidence lookup returned five linked
records.

### Repeatable evaluation procedure

For each run, record requested API dates, page size and maximum, fetched/raw
row counts, invalid and duplicate counts, HDFS paths, Pig clean/pattern
counts, Hive clean count, and Mongo pattern/evidence counts. Compare the Hive
clean count to the Pig count for the same paths. Inspect duplicate source IDs
to confirm deterministic deduplication and check examples around month
boundaries and records with missing area fields. The project has no labeled
ground-truth set; precision, recall, prediction accuracy, and infrastructure
risk are not evaluated.

## 10. Expected outcomes

- A runnable Java-to-HDFS-to-Pig transformation.
- Hive SQL over external tables without duplicating the clean dataset.
- MongoDB containing a bounded subset of patterns and examples.
- A dashboard for descriptive counts and recurring groups.
- A repeatable validation procedure that separates pipeline correctness from
  analytical validity.

The observed runs achieved these outcomes on both staged data and one fresh,
bounded API extraction. Repeating the end-to-end procedure with new date
windows remains useful for operational reliability and broader validation.

## 11. Limitations and future enhancements

- The 800,000-row multi-year input is a staged sample, not the full NYC 311
  population. It is approximately quota-balanced by month in several periods
  and does not support population-level conclusions.
- The 50-record API smoke test and the 15,000-record integration run are
  bounded downloads; neither is a representative sample of NYC 311.
- Current status is not a status-transition history; this implementation does
  not calculate resolution duration.
- Borough and ZIP are coarse. The dashboard map shades 2020 Census ZIP Code
  Tabulation Areas with Hive complaint counts; these approximate ZIP regions,
  not ward boundaries or individually geocoded complaint points.
- A two-month recurrence rule is an interpretable project choice, not an
  optimized statistical threshold.
- Complaint volumes can reflect reporting behavior, access, agency policy,
  seasonality, or extraction choices as well as underlying issues.
- The local setup uses separate Windows/WSL services and does not include
  production authentication, orchestration, monitoring, or fault tolerance.

Future work could examine larger documented API windows, preserve run/source
metadata, assess sampling effects, add a map only where location quality and
privacy permit, and compare complaint categories using reviewed labels.
Resolution-duration analysis should be added only after timestamp semantics
are verified. Any predictive extension would need an explicit target, labeled
data, a baseline, and held-out evaluation before performance claims.

## 12. Short faculty presentation

> CivicPulse is a Big Data pipeline for descriptive analysis of public NYC 311
> complaints. Java collects a bounded API sample, HDFS stores raw and
> processed files, and Pig cleans and deduplicates records while finding
> borough/ZIP and issue groups reported in at least two different months. Hive
> runs the aggregates, MongoDB stores selected patterns and a few evidence
> records, and a Java dashboard brings these results together. In our staged
> run, Pig and Hive counts both reached 800,000, with 15,215 recurring-pattern
> summaries. These are pipeline-validation results, not estimates for all NYC
> complaints, and repeated reports are not proof of a confirmed infrastructure
> fault.

## References

1. NYC Open Data, *311 Service Requests from 2020 to Present*, dataset page:
   <https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9>.
2. NYC Open Data/Socrata CSV resource:
   <https://data.cityofnewyork.us/resource/erm2-nwe9.csv>.
3. Cite the official Apache Hadoop, Pig, Hive, and MongoDB documentation
   matching the versions installed for the final academic submission.

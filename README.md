# CivicPulse - A Big Data-Based Public Complaint Pattern Intelligence System

## Overview

CivicPulse is a Big Data project that analyzes public service complaints from New York City's 311 Service Requests dataset. It identifies frequently reported complaint categories, geographic concentrations, monthly trends, and recurring complaint patterns to support data-driven public-service planning.

The system processes structured complaint data using Java, Hadoop HDFS, Apache Pig, Apache Hive, and MongoDB. An interactive dashboard presents the analytical results through charts, tables, geographic visualizations, and filters.

## Problem Statement

New York City receives millions of public service requests related to potholes, illegal parking, sanitation, noise, street conditions, and other civic issues. Analyzing these large volumes of records manually makes it difficult to identify frequently reported problems, areas with high complaint concentrations, and recurring reporting patterns over time.

CivicPulse addresses this challenge by using Big Data technologies to store, process, and analyze NYC 311 complaint records and present meaningful insights through an interactive dashboard.

## Objectives

- Process large volumes of public complaint data efficiently.
- Identify the most frequently reported complaint categories.
- Analyze complaint distribution across boroughs and geographic areas.
- Examine monthly complaint trends.
- Detect recurring complaint patterns across multiple months.
- Retrieve supporting complaint records for detected patterns.
- Present analytical results through an interactive dashboard.

## Dataset

**Dataset:** NYC 311 Service Requests from 2020 to Present

**Source:** NYC Open Data

**Official dataset:** https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data

The dataset contains structured records of service requests submitted through New York City's 311 system.

Important attributes include:

- `unique_key` — Unique identifier for a service request
- `created_date` — Date and time the request was created
- `complaint_type` — Type of reported issue
- `descriptor` — Additional description of the issue
- `agency` — Responsible city agency
- `status` — Current or recorded request status
- `borough` — Borough associated with the request

The dataset is updated regularly. The records used in an analysis depend on the downloaded data and the selected date range.

## Technologies Used

| Technology | Purpose |
|---|---|
| Java | Data ingestion, cleaning, validation, and backend processing |
| Hadoop HDFS | Distributed storage of raw and processed data |
| Apache Pig | Data transformation, filtering, grouping, and aggregation |
| Apache Hive | SQL-based querying and analytical reporting |
| MongoDB | Storage and retrieval of complaint patterns and supporting records |
| HTML, CSS, JavaScript | Dashboard interface and visualizations |

## System Architecture

```text
NYC 311 Open Data
        |
        v
 Java Data Ingestion
        |
        v
 Hadoop HDFS
  (Raw Dataset)
        |
        v
 Java Data Cleaning
        |
        v
 Apache Pig Processing
        |
        v
 Hadoop HDFS
 (Processed Dataset)
        |
        +------------------+
        |                  |
        v                  v
   Apache Hive          MongoDB
   Aggregations      Patterns and Evidence
        |                  |
        +---------+--------+
                  |
                  v
          Java Backend API
                  |
                  v
        Interactive Dashboard
```

## Key Features

### 1. Overview Dashboard
Provides a summary of complaint activity and key analytical indicators.

### 2. Big Data Insights
Displays complaint category distributions, monthly trends, borough comparisons, and category-wise analysis.

### 3. Hotspot Explorer
Helps explore geographic concentrations of complaints using available location information.

### 4. Recurring Pattern Detection
Identifies complaint categories repeatedly reported in the same area across multiple months.

### 5. Complaint Browser
Allows users to examine individual complaint records associated with reported issues.

### 6. Interactive Filters
Enables users to explore results by date range, borough, and complaint category.

### 7. Supporting Evidence
Links detected complaint patterns to relevant records for further investigation.

## Expected Outputs

- Cleaned and validated complaint data stored in HDFS.
- Category-wise complaint counts and rankings.
- Borough-wise and geographic complaint analysis.
- Monthly complaint trend visualizations.
- Recurring complaint pattern reports.
- Supporting complaint records retrieved from MongoDB.
- Interactive dashboard for exploring analytical results.

## Installation and Execution

The following steps describe the general workflow. Update the commands and configuration according to your local environment.

1. Install and configure Java, Hadoop, Apache Pig, Apache Hive, and MongoDB.
2. Start Hadoop services and verify that HDFS is accessible.
3. Start MongoDB and configure the database connection.
4. Upload the raw dataset to the designated HDFS directory.
5. Run the Java data-cleaning and validation program.
6. Execute the Apache Pig processing scripts.
7. Create or update Hive tables and run the analytical queries.
8. Store the detected patterns and supporting records in MongoDB.
9. Compile and run the Java dashboard backend.
10. Open the dashboard in a browser using the configured local URL.

**Note:** Exact commands, configuration files, and script names depend on the implementation in the repository.

## Project Outcomes

CivicPulse demonstrates how Big Data technologies can transform large-scale civic complaint records into useful analytical insights. It helps users understand which complaint categories are frequently reported, where complaints are concentrated, and which reporting patterns recur over time.

The results can support further investigation and public-service prioritization. Repeated complaints are indicators of reported concerns and do not, by themselves, confirm that an underlying physical fault exists.

## Future Enhancements

- Incremental ingestion of newly published complaint records.
- Scheduled data processing and dashboard refreshes.
- Improved geographic hotspot visualization.
- Advanced complaint classification using machine learning.
- Alerts for emerging or increasing complaint patterns.
- Performance optimization for large datasets.

## Author

Developed as an academic Big Data Computing project.

## License

Specify an appropriate license before distributing or reusing this project.

-- Run with -param CLEANED_OUT=... -param PATTERNS_OUT=...
-- CSVExcelStorage handles quoted fields and fields containing commas.
REGISTER /opt/pig/lib/piggybank.jar;

%default INPUT '/civicpulse/raw/yearly_samples/311_20*_sample.csv'
%default CLEANED_OUT '/civicpulse/processed/pig_clean_20261013'
%default PATTERNS_OUT '/civicpulse/analytics/persistent_hotspots_20261013'

raw_complaints = LOAD '$INPUT'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage(',')
    AS (
        unique_key:chararray,
        created_date:chararray,
        agency:chararray,
        agency_name:chararray,
        complaint_type:chararray,
        descriptor:chararray,
        location_type:chararray,
        incident_zip:chararray,
        city:chararray,
        borough:chararray,
        status:chararray
    );

-- Every source file has a header. Reject it along with malformed and
-- incomplete rows before normalization.
valid_complaints = FILTER raw_complaints BY
    unique_key IS NOT NULL
    AND unique_key != ''
    AND unique_key != 'unique_key'
    AND created_date IS NOT NULL
    AND created_date MATCHES '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*'
    AND complaint_type IS NOT NULL
    AND TRIM(complaint_type) != '';

normalized_complaints = FOREACH valid_complaints GENERATE
    TRIM(unique_key) AS unique_key,
    TRIM(created_date) AS created_date,
    UPPER(TRIM(agency)) AS agency,
    TRIM(agency_name) AS agency_name,
    UPPER(TRIM(complaint_type)) AS complaint_type,
    TRIM(descriptor) AS descriptor,
    TRIM(location_type) AS location_type,
    TRIM(incident_zip) AS incident_zip,
    UPPER(TRIM(city)) AS city,
    UPPER(TRIM(borough)) AS borough,
    UPPER(TRIM(status)) AS status;

-- Collapse repeated source IDs by selecting one deterministic complete row.
by_request_id = GROUP normalized_complaints BY unique_key;
deduplicated_complaints = FOREACH by_request_id {
    ordered_records = ORDER normalized_complaints BY
        created_date ASC, agency ASC, agency_name ASC, complaint_type ASC,
        descriptor ASC, location_type ASC, incident_zip ASC, city ASC,
        borough ASC, status ASC;
    first_record = LIMIT ordered_records 1;
    GENERATE FLATTEN(first_record);
};

STORE deduplicated_complaints
    INTO '$CLEANED_OUT'
    USING PigStorage('\t');

located_complaints = FILTER deduplicated_complaints BY
    borough IS NOT NULL
    AND borough != ''
    AND incident_zip IS NOT NULL
    AND incident_zip != ''
    AND complaint_type IS NOT NULL
    AND complaint_type != '';

complaints_with_period = FOREACH located_complaints GENERATE
    CONCAT(borough, '|', incident_zip) AS area,
    complaint_type AS complaint_type,
    SUBSTRING(created_date, 0, 7) AS complaint_month;

monthly_groups = GROUP complaints_with_period
    BY (area, complaint_type, complaint_month);
monthly_counts = FOREACH monthly_groups GENERATE
    group.area AS area,
    group.complaint_type AS complaint_type,
    group.complaint_month AS complaint_month,
    COUNT(complaints_with_period) AS monthly_complaint_count;

area_issue_groups = GROUP monthly_counts BY (area, complaint_type);
area_issue_summaries = FOREACH area_issue_groups GENERATE
    group.area AS area,
    group.complaint_type AS complaint_type,
    COUNT(monthly_counts) AS months_observed,
    SUM(monthly_counts.monthly_complaint_count) AS total_complaints;

-- A persistent hotspot has reports in at least two distinct calendar months.
persistent_hotspots = FILTER area_issue_summaries BY months_observed >= 2;
ranked_hotspots = ORDER persistent_hotspots BY
    months_observed DESC, total_complaints DESC;

STORE ranked_hotspots
    INTO '$PATTERNS_OUT'
    USING PigStorage('\t');

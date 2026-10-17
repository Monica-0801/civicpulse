USE civicpulse;

-- Processed rows should match the Pig clean output record count.
SELECT COUNT(*) AS processed_complaint_count
FROM complaint_clean;

-- Most frequently reported categories.
SELECT complaint_type, COUNT(*) AS complaint_count
FROM complaint_clean
GROUP BY complaint_type
ORDER BY complaint_count DESC
LIMIT 20;

-- Monthly trends using the yyyy-MM prefix of created_date.
SELECT SUBSTR(created_date, 1, 7) AS complaint_month,
       COUNT(*) AS complaint_count
FROM complaint_clean
WHERE created_date IS NOT NULL
  AND LENGTH(created_date) >= 7
GROUP BY SUBSTR(created_date, 1, 7)
ORDER BY complaint_month;

-- Areas and issue categories recurring across multiple months,
-- ordered by months observed and total complaint records.
SELECT area, complaint_type, months_observed, total_complaints
FROM persistent_hotspots
ORDER BY months_observed DESC, total_complaints DESC
LIMIT 100;

-- Status distribution for service requests.
SELECT status, COUNT(*) AS request_count
FROM complaint_clean
GROUP BY status
ORDER BY request_count DESC;

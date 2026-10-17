import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Indexes;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class CivicPulseDashboardServer {

    private static final String HIVE_URL = System.getenv().getOrDefault(
            "CIVICPULSE_HIVE_URL", "jdbc:hive2://localhost:10000/default");
    private static final String HIVE_USER = System.getenv().getOrDefault(
            "CIVICPULSE_HIVE_USER", "monica");
    private static final String WSL_DISTRO = System.getenv().getOrDefault(
            "CIVICPULSE_WSL_DISTRO", "Ubuntu");
    private static final String MONGO_URI = System.getenv().getOrDefault(
            "CIVICPULSE_MONGODB_URI", "mongodb://127.0.0.1:27017");
    private static final String RUN_ID = System.getenv().getOrDefault(
            "CIVICPULSE_MONGO_RUN_ID", "run_20261013");
    private static final String DASHBOARD_FILE =
            "dashboard/index.html";
    private static final String ZCTA_FILE =
            "dashboard/nyc_zctas_2020.geojson";
    private static final long CACHE_TTL_MILLIS = 120_000;
    private static final int ANALYTICS_CACHE_MAX_ENTRIES = 8;
    private static final long HIVE_TIMEOUT_MILLIS = 180_000;
    private static final int MAX_HIVE_OUTPUT_CHARS = 2_000_000;

    private final MongoClient mongoClient;
    private final MongoCollection<Document> patterns;
    private final MongoCollection<Document> evidence;
    private final MongoCollection<Document> demoReports;
    private final Map<String, CachedAnalytics> analyticsCache =
            new LinkedHashMap<>(16, 0.75f, true);

    private CivicPulseDashboardServer(String mongoUri) {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(
                        new com.mongodb.ConnectionString(mongoUri))
                .applyToClusterSettings(builder ->
                        builder.serverSelectionTimeout(8, TimeUnit.SECONDS))
                .build();
        mongoClient = MongoClients.create(settings);
        patterns = mongoClient.getDatabase("civicpulse")
                .getCollection("patterns");
        evidence = mongoClient.getDatabase("civicpulse")
                .getCollection("complaint_evidence");
        demoReports = mongoClient.getDatabase("civicpulse")
                .getCollection("demo_submissions");
    }

    public static void main(String[] args) throws Exception {
        int port = args.length == 0
                ? Integer.parseInt(System.getenv().getOrDefault(
                        "CIVICPULSE_DASHBOARD_PORT", "8088"))
                : Integer.parseInt(args[0]);
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535.");
        }

        CivicPulseDashboardServer app =
                new CivicPulseDashboardServer(MONGO_URI);
        HttpServer server = HttpServer.create(
                new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/", app::handle);
        server.setExecutor(Executors.newFixedThreadPool(8));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(1);
            app.mongoClient.close();
        }));
        server.start();
        System.out.println("CivicPulse dashboard: http://127.0.0.1:" + port);
        System.out.println("MongoDB run ID: " + RUN_ID);
        System.out.println("HiveServer2: " + HIVE_URL);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if ("/".equals(path) && "GET".equals(method)) {
                sendFile(exchange);
            } else if ("/nyc_zctas_2020.geojson".equals(path)
                    && "GET".equals(method)) {
                sendStaticFile(exchange, ZCTA_FILE,
                        "application/geo+json; charset=utf-8");
            } else if ("/api/dashboard".equals(path) && "GET".equals(method)) {
                sendJson(exchange, 200, dashboardJson(
                        DashboardFilters.parse(
                                exchange.getRequestURI().getRawQuery())));
            } else if ("/api/zip-hotspots".equals(path)
                    && "GET".equals(method)) {
                sendJson(exchange, 200, zipHotspotsJson(
                        exchange.getRequestURI().getRawQuery()));
            } else if ("/api/zip-details".equals(path)
                    && "GET".equals(method)) {
                sendJson(exchange, 200, zipDetailsJson(
                        exchange.getRequestURI().getRawQuery()));
            } else if ("/api/complaints".equals(path) && "GET".equals(method)) {
                sendJson(exchange, 200,
                        complaintsJson(exchange.getRequestURI().getRawQuery()));
            } else if ("/api/demo-reports".equals(path)
                    && "GET".equals(method)) {
                sendJson(exchange, 200, demoReportsJson());
            } else if ("/api/demo-reports".equals(path)
                    && "POST".equals(method)) {
                sendJson(exchange, 201, saveDemoReport(exchange));
            } else if (path.startsWith("/api/patterns/")
                    && "GET".equals(method)
                    && path.endsWith("/evidence")) {
                String encodedId = path.substring(
                        "/api/patterns/".length(),
                        path.length() - "/evidence".length());
                sendJson(exchange, 200, evidenceJson(
                        decodePath(encodedId),
                        DashboardFilters.parse(
                                exchange.getRequestURI().getRawQuery())));
            } else {
                if (!List.of("GET", "POST").contains(method)) {
                    sendJson(exchange, 405,
                            "{\"error\":\"Method not allowed.\"}");
                    return;
                }
                sendJson(exchange, 404, "{\"error\":\"Not found.\"}");
            }
        } catch (Exception ex) {
            String message = ex.getMessage() == null
                    ? ex.getClass().getSimpleName() : ex.getMessage();
            System.err.println("Dashboard request failed: " + message);
            int status = ex instanceof IllegalArgumentException ? 400 : 503;
            sendJson(exchange, status, "{\"error\":"
                    + jsonString(message) + "}");
        } finally {
            exchange.close();
        }
    }

    private String zipHotspotsJson(String rawQuery) throws Exception {
        DashboardFilters filters = DashboardFilters.parse(rawQuery);
        List<String[]> rows = runHiveQuery(
                "SELECT TRIM(incident_zip) AS label, COUNT(*) AS metric "
                + "FROM civicpulse.complaint_clean"
                + filters.hiveWhereClause(
                        "incident_zip IS NOT NULL "
                        + "AND TRIM(incident_zip) != ''")
                + "GROUP BY TRIM(incident_zip) "
                + "ORDER BY metric DESC, label ASC LIMIT 500",
                "label\tmetric");
        List<Metric> hotspots = new ArrayList<>();
        for (String[] row : rows) {
            if (row.length != 2 || !row[0].matches("[0-9]{5}")) {
                throw new IOException("Hive returned an invalid ZIP hotspot row.");
            }
            hotspots.add(new Metric(
                    row[0], parseMetric(row[1], "ZIP complaint count")));
        }
        return "{\"zipHotspots\":" + monthArray(hotspots) + "}";
    }

    private String zipDetailsJson(String rawQuery) throws Exception {
        Map<String, String> query = parseQuery(rawQuery);
        for (String key : query.keySet()) {
            if (!List.of("zip", "from", "to", "borough", "complaintType")
                    .contains(key)) {
                throw new IllegalArgumentException(
                        "Unsupported ZIP detail filter: " + key);
            }
        }
        String zip = query.get("zip");
        if (zip == null || !zip.matches("[0-9]{5}")) {
            throw new IllegalArgumentException(
                    "ZIP must contain exactly five digits.");
        }
        DashboardFilters filters = DashboardFilters.parse(
                onlyDashboardFilters(query));
        String where = filters.hiveWhereClause(
                "incident_zip IS NOT NULL AND TRIM(incident_zip) = "
                + DashboardFilters.sqlString(zip));
        List<List<String[]>> resultSets = runHiveQueries(
                List.of(
                        "SELECT COUNT(*) AS total "
                        + "FROM civicpulse.complaint_clean" + where,
                        "SELECT complaint_type AS label, COUNT(*) AS metric "
                        + "FROM civicpulse.complaint_clean" + where
                        + "GROUP BY complaint_type "
                        + "ORDER BY metric DESC, label ASC LIMIT 5",
                        "SELECT status AS label, COUNT(*) AS metric "
                        + "FROM civicpulse.complaint_clean"
                        + filters.hiveWhereClause(
                                "incident_zip IS NOT NULL "
                                + "AND TRIM(incident_zip) = "
                                + DashboardFilters.sqlString(zip) + " "
                                + "AND status IS NOT NULL AND status != ''")
                        + "GROUP BY status ORDER BY metric DESC",
                        "SELECT borough AS label, COUNT(*) AS metric "
                        + "FROM civicpulse.complaint_clean"
                        + filters.hiveWhereClause(
                                "incident_zip IS NOT NULL "
                                + "AND TRIM(incident_zip) = "
                                + DashboardFilters.sqlString(zip) + " "
                                + "AND borough IS NOT NULL AND borough != ''")
                        + "GROUP BY borough ORDER BY metric DESC"),
                List.of("total", "label\tmetric", "label\tmetric",
                        "label\tmetric"));
        List<String[]> totalRows = resultSets.get(0);
        if (totalRows.size() != 1 || totalRows.get(0).length != 1) {
            throw new IOException("Hive returned an invalid ZIP total.");
        }
        long total = parseMetric(totalRows.get(0)[0], "ZIP complaint count");
        return "{\"zip\":" + jsonString(zip)
                + ",\"total\":" + total
                + ",\"categories\":" + metricArray(
                        resultSets.get(1), "ZIP category count")
                + ",\"statuses\":" + metricArray(
                        resultSets.get(2), "ZIP status count")
                + ",\"boroughs\":" + metricArray(
                        resultSets.get(3), "ZIP borough count")
                + "}";
    }

    private static String metricArray(List<String[]> rows, String description)
            throws IOException {
        List<Metric> metrics = new ArrayList<>();
        for (String[] row : rows) {
            if (row.length != 2 || row[0].isBlank()) {
                throw new IOException("Hive returned an invalid " + description + ".");
            }
            metrics.add(new Metric(
                    row[0], parseMetric(row[1], description)));
        }
        return monthArray(metrics);
    }

    private String complaintsJson(String rawQuery) throws Exception {
            Map<String, String> query = parseQuery(rawQuery);
            for (String key : query.keySet()) {
                if (!List.of("from", "to", "borough", "complaintType",
                        "status", "search", "page", "pageSize").contains(key)) {
                    throw new IllegalArgumentException(
                            "Unsupported complaint filter: " + key);
                }
            }
            DashboardFilters filters = DashboardFilters.parse(
                    onlyDashboardFilters(query));
            String status = boundedText(query.get("status"), "status", 50);
            String search = boundedText(query.get("search"), "search", 100);
            int page = parsePositiveInt(query.get("page"), 1, 1_000_000, "page");
            int pageSize = parsePositiveInt(
                    query.get("pageSize"), 25, 100, "pageSize");
            List<String> clauses = new ArrayList<>();
            if (!filters.hiveWhereClause().isBlank()) {
                clauses.add(filters.hiveWhereClause().substring(7).trim());
            }
            if (status != null) {
                clauses.add("status = " + DashboardFilters.sqlString(status));
            }
            if (search != null) {
                String term = DashboardFilters.sqlString("%" + search + "%");
                clauses.add("(unique_key LIKE " + term
                        + " OR complaint_type LIKE " + term
                        + " OR descriptor LIKE " + term
                        + " OR borough LIKE " + term
                        + " OR incident_zip LIKE " + term + ")");
            }
            String where = clauses.isEmpty() ? ""
                    : " WHERE " + String.join(" AND ", clauses);
            long offset = (long) (page - 1) * pageSize;
            String countQuery =
                    "SELECT COUNT(*) AS total FROM civicpulse.complaint_clean"
                    + where;
            String pageQuery =
                    "SELECT unique_key AS label, created_date AS created, "
                    + "complaint_type AS category, borough, incident_zip AS zip, "
                    + "descriptor, status FROM civicpulse.complaint_clean"
                    + where + " ORDER BY created_date DESC LIMIT " + pageSize
                    + " OFFSET " + offset;
            List<List<String[]>> queryResults = runHiveQueries(
                    List.of(countQuery, pageQuery),
                    List.of("total",
                            "label\tcreated\tcategory\tborough\tzip\tdescriptor\tstatus"));
            List<String[]> countRows = queryResults.get(0);
            if (countRows.size() != 1 || countRows.get(0).length != 1) {
                throw new IOException("Hive returned an invalid complaint count.");
            }
            long total = parseMetric(countRows.get(0)[0], "complaint count");
            List<String[]> rows = queryResults.get(1);
            List<Document> records = new ArrayList<>();
            for (String[] row : rows) {
                if (row.length != 7) {
                    throw new IOException("Hive returned an invalid complaint row.");
                }
                records.add(new Document("requestId", row[0])
                        .append("createdDate", row[1])
                        .append("complaintType", row[2])
                        .append("borough", row[3])
                        .append("incidentZip", row[4])
                        .append("descriptor", row[5])
                        .append("status", row[6]));
            }
            return "{\"total\":" + total + ",\"page\":" + page
                    + ",\"pageSize\":" + pageSize
                    + ",\"records\":" + documentArray(records) + "}";
        }

    private static String onlyDashboardFilters(Map<String, String> query) {
            List<String> pairs = new ArrayList<>();
            for (String key : List.of("from", "to", "borough", "complaintType")) {
                if (query.containsKey(key)) {
                    pairs.add(key + "=" + java.net.URLEncoder.encode(
                            query.get(key), StandardCharsets.UTF_8));
                }
            }
            return String.join("&", pairs);
        }

    private String demoReportsJson() {
            List<Document> reports = demoReports.find()
                    .sort(Sorts.descending("submittedAt"))
                    .limit(50)
                    .into(new ArrayList<>());
            return "{\"count\":" + demoReports.countDocuments()
                    + ",\"reports\":" + documentArray(reports) + "}";
        }

    private String saveDemoReport(HttpExchange exchange) throws IOException {
            byte[] body = exchange.getRequestBody().readNBytes(16_385);
            if (body.length > 16_384) {
                throw new IllegalArgumentException("Submission is too large.");
            }
            Document submitted;
            try {
                submitted = Document.parse(
                        new String(body, StandardCharsets.UTF_8));
            } catch (org.bson.json.JsonParseException ex) {
                throw new IllegalArgumentException("Submission must be valid JSON.");
            }
            String category = requiredText(submitted, "category", 100);
            String borough = requiredText(submitted, "borough", 50);
            String description = requiredText(submitted, "description", 1000);
            String date = requiredText(submitted, "date", 10);
            try {
                LocalDate.parse(date);
            } catch (java.time.format.DateTimeParseException ex) {
                throw new IllegalArgumentException("Date must use YYYY-MM-DD.");
            }
            if (description.length() < 10) {
                throw new IllegalArgumentException(
                        "Description must contain at least 10 characters.");
            }
            Document report = new Document("category", category)
                    .append("borough", borough)
                    .append("description", description)
                    .append("date", date)
                    .append("source", "DEMO_SUBMISSION")
                    .append("submittedAt", Instant.now().toString());
            demoReports.insertOne(report);
            return report.toJson();
        }

    private static String requiredText(Document document, String key, int max) {
            return boundedText(document.getString(key), key, max, true);
        }

    private static String boundedText(String value, String name, int max) {
            return boundedText(value, name, max, false);
        }

    private static String boundedText(
                String value, String name, int max, boolean required) {
            if (value == null || value.isBlank()) {
                if (required) {
                    throw new IllegalArgumentException(name + " is required.");
                }
                return null;
            }
            String normalized = value.trim();
            if (normalized.length() > max || normalized.chars().anyMatch(
                    Character::isISOControl)) {
                throw new IllegalArgumentException("Invalid " + name + ".");
            }
            return normalized;
        }

    private static int parsePositiveInt(
                String value, int defaultValue, int maximum, String name) {
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            try {
                int parsed = Integer.parseInt(value);
                if (parsed < 1 || parsed > maximum) {
                    throw new IllegalArgumentException(
                            name + " must be between 1 and " + maximum + ".");
                }
                return parsed;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(name + " must be an integer.");
            }
        }

    private String dashboardJson(DashboardFilters filters) throws Exception {
        CachedAnalytics analytics = getAnalytics(filters);
        List<Document> storedPatterns = patterns.find(
                        Filters.and(
                                Filters.eq("runId", RUN_ID),
                                filters.mongoPatternFilter()))
                .sort(Sorts.orderBy(
                        Sorts.descending("monthsObserved"),
                        Sorts.descending("totalComplaints")))
                .limit(5000)
                .into(new ArrayList<>());
        Map<String, Metric> filteredPatternMetrics = new LinkedHashMap<>();
        for (Metric metric : analytics.patterns) {
            filteredPatternMetrics.put(metric.label, metric);
        }
        List<Document> patternDocs = new ArrayList<>();
        for (Document stored : storedPatterns) {
            Metric metric = filteredPatternMetrics.get(stored.getString("_id"));
            if (metric != null) {
                Document filtered = new Document(stored);
                filtered.put("monthsObserved", metric.monthsObserved);
                filtered.put("totalComplaints", metric.value);
                filtered.put("periods", metric.periods);
                patternDocs.add(filtered);
            }
        }
        long evidenceCount = 0;
        if (!patternDocs.isEmpty()) {
            List<String> matchingPatternIds = patternDocs.stream()
                    .map(pattern -> pattern.getString("_id"))
                    .toList();
            evidenceCount = evidence.countDocuments(Filters.and(
                    Filters.eq("runId", RUN_ID),
                    Filters.in("patternId", matchingPatternIds),
                    filters.mongoEvidenceFilter()));
        }

        return "{\"generatedAt\":" + jsonString(Instant.now().toString())
                + ",\"runId\":" + jsonString(RUN_ID)
                + ",\"sourceRecordCount\":" + analytics.recordCount
                + ",\"categoryCount\":" + analytics.categories.size()
                + ",\"patternCount\":" + analytics.patternCount
                + ",\"storedPatternCount\":" + patterns.countDocuments(
                        Filters.eq("runId", RUN_ID))
                + ",\"evidenceCount\":" + evidenceCount
                + ",\"dateRange\":{\"min\":" + jsonString(analytics.minDate)
                + ",\"max\":" + jsonString(analytics.maxDate)
                + ",\"from\":" + jsonString(filters.fromDate == null
                        ? analytics.minDate : filters.fromDate.toString())
                + ",\"to\":" + jsonString(filters.toDate == null
                        ? analytics.maxDate : filters.toDate.toString()) + "}"
                + ",\"boroughOptions\":" + stringArray(analytics.boroughOptions)
                + ",\"categoryOptions\":" + stringArray(analytics.categoryOptions)
                + ",\"categories\":" + categoryArray(analytics.categories)
                + ",\"monthly\":" + monthArray(analytics.months)
                + ",\"boroughs\":" + monthArray(analytics.boroughs)
                + ",\"statuses\":" + monthArray(analytics.statuses)
                + ",\"queensStatuses\":" + monthArray(analytics.queensStatuses)
                + ",\"patterns\":" + documentArray(patternDocs)
                + "}";
    }

    private String evidenceJson(String patternId, DashboardFilters filters)
            throws Exception {
        if (patternId.isBlank() || patternId.length() > 300
                || patternId.contains("/") || patternId.contains("\\")
                || patternId.contains("\0")) {
            throw new IllegalArgumentException("Invalid pattern ID.");
        }
        Document pattern = patterns.find(Filters.and(
                Filters.eq("_id", patternId),
                Filters.eq("runId", RUN_ID))).first();
        if (pattern == null) {
            throw new IllegalArgumentException("Pattern not found for run "
                    + RUN_ID + ".");
        }
        CachedAnalytics analytics = getAnalytics(filters);
        Metric filteredMetrics = analytics.patterns.stream()
                .filter(metric -> metric.label.equals(patternId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Pattern does not recur within the selected filters."));
        Document filteredPattern = new Document(pattern)
                .append("monthsObserved", filteredMetrics.monthsObserved)
                .append("totalComplaints", filteredMetrics.value)
                .append("periods", filteredMetrics.periods);
        List<Document> records = evidence.find(Filters.and(
                        Filters.eq("patternId", patternId),
                        Filters.eq("runId", RUN_ID),
                        filters.mongoEvidenceFilter()))
                .sort(Sorts.ascending("createdDate"))
                .limit(10)
                .into(new ArrayList<>());
        return "{\"pattern\":" + filteredPattern.toJson()
                + ",\"evidence\":" + documentArray(records) + "}";
    }

    private CachedAnalytics getAnalytics(DashboardFilters filters)
            throws Exception {
        String filterKey = filters.cacheKey();
        synchronized (this) {
            CachedAnalytics cached = analyticsCache.get(filterKey);
            long now = System.currentTimeMillis();
            if (cached != null
                    && now - cached.loadedAt < CACHE_TTL_MILLIS) {
                return cached;
            }
            analyticsCache.remove(filterKey);
            CachedAnalytics refreshed = queryHive(filters);
            analyticsCache.put(filterKey, refreshed);
            while (analyticsCache.size() > ANALYTICS_CACHE_MAX_ENTRIES) {
                String oldestKey = analyticsCache.keySet().iterator().next();
                analyticsCache.remove(oldestKey);
            }
            return refreshed;
        }
    }

    private CachedAnalytics queryHive(DashboardFilters filters)
            throws Exception {
        CachedAnalytics result = new CachedAnalytics();
        result.filterKey = filters.cacheKey();
        String filterSql = filters.hiveWhereClause();
        String categoryQuery = "SELECT label, metric, "
                + "SUM(metric) OVER () AS total "
                + "FROM (SELECT complaint_type AS label, COUNT(*) AS metric "
                + "FROM civicpulse.complaint_clean" + filterSql + " "
                + "GROUP BY complaint_type) category_counts "
                + "ORDER BY metric DESC";
        String monthQuery = "SELECT SUBSTR(created_date, 1, 7) AS label, "
                + "COUNT(*) AS metric "
                + "FROM civicpulse.complaint_clean "
                + filters.hiveWhereClause(
                        "created_date IS NOT NULL AND LENGTH(created_date) >= 7")
                + "GROUP BY SUBSTR(created_date, 1, 7) "
                + "ORDER BY label";
        String boroughQuery = "SELECT borough AS label, COUNT(*) AS metric "
                + "FROM civicpulse.complaint_clean"
                + filters.hiveWhereClause(
                        "borough IS NOT NULL AND borough != ''")
                + "GROUP BY borough ORDER BY metric DESC";
        String statusQuery = "SELECT status AS label, COUNT(*) AS metric "
                + "FROM civicpulse.complaint_clean"
                + filters.hiveWhereClause(
                        "status IS NOT NULL AND status != ''")
                + "GROUP BY status ORDER BY metric DESC";
        String queensStatusQuery = "SELECT "
                + "COALESCE(SUM(CASE WHEN UPPER(TRIM(status)) = 'CLOSED' "
                + "THEN 1 ELSE 0 END), 0) AS completed, "
                + "COALESCE(SUM(CASE WHEN UPPER(TRIM(status)) = 'IN PROGRESS' "
                + "THEN 1 ELSE 0 END), 0) AS in_progress "
                + "FROM civicpulse.complaint_clean"
                + filters.hiveWhereClauseWithoutBorough(
                        "UPPER(TRIM(borough)) = 'QUEENS' "
                        + "AND status IS NOT NULL");
        String patternQuery = "SELECT label, category, months_observed, "
                + "total_complaints, periods, COUNT(*) OVER () AS pattern_count "
                + "FROM (SELECT "
                + "CONCAT(UPPER(TRIM(borough)), '|', TRIM(incident_zip)) AS label, "
                + "UPPER(TRIM(complaint_type)) AS category, "
                + "COUNT(DISTINCT SUBSTR(created_date, 1, 7)) AS months_observed, "
                + "COUNT(*) AS total_complaints, "
                + "CONCAT_WS(',', SORT_ARRAY(COLLECT_SET("
                + "SUBSTR(created_date, 1, 7)))) AS periods "
                + "FROM civicpulse.complaint_clean"
                + filters.hiveWhereClause(
                        "borough IS NOT NULL AND borough != '' "
                        + "AND incident_zip IS NOT NULL AND incident_zip != '' "
                        + "AND complaint_type IS NOT NULL AND complaint_type != '' "
                        + "AND created_date IS NOT NULL "
                        + "AND LENGTH(created_date) >= 7")
                + "GROUP BY CONCAT(UPPER(TRIM(borough)), '|', TRIM(incident_zip)), "
                + "UPPER(TRIM(complaint_type)) "
                + "HAVING COUNT(DISTINCT SUBSTR(created_date, 1, 7)) >= 2"
                + ") recurring "
                + "ORDER BY months_observed DESC, total_complaints DESC, "
                + "label ASC, category ASC LIMIT 500";
        String categoryOptionsQuery = "SELECT complaint_type AS label, COUNT(*) AS metric "
                + "FROM civicpulse.complaint_clean "
                + "GROUP BY complaint_type ORDER BY label";
        String dateRangeQuery = "SELECT MIN(SUBSTR(created_date, 1, 10)) AS label, "
                + "MAX(SUBSTR(created_date, 1, 10)) AS metric "
                + "FROM civicpulse.complaint_clean "
                + "WHERE created_date IS NOT NULL AND LENGTH(created_date) >= 10";

        List<List<String[]>> queryResults = runHiveQueries(
                List.of(categoryQuery, monthQuery, boroughQuery,
                        statusQuery, queensStatusQuery, patternQuery,
                        categoryOptionsQuery, dateRangeQuery),
                List.of("label\tmetric\ttotal", "label\tmetric", "label\tmetric",
                        "label\tmetric", "completed\tin_progress",
                        "label\tcategory\tmonths_observed\ttotal_complaints\tperiods\tpattern_count",
                        "label\tmetric", "label\tmetric"));

        List<String[]> categoryRows = queryResults.get(0);
        Long categoryTotal = null;
        for (String[] row : categoryRows) {
            if (row.length != 3 || row[0].isBlank()) {
                throw new IOException("Hive returned an invalid category row.");
            }
            long total = parseMetric(row[2], "record count");
            if (categoryTotal != null && categoryTotal != total) {
                throw new IOException("Hive returned inconsistent category totals.");
            }
            categoryTotal = total;
            result.categories.add(
                    new Metric(row[0], parseMetric(row[1], "category count")));
        }
        result.recordCount = categoryTotal == null ? 0 : categoryTotal;

        List<String[]> monthRows = queryResults.get(1);
        for (String[] row : monthRows) {
            if (row.length != 2
                    || !row[0].matches("[0-9]{4}-[0-9]{2}")) {
                throw new IOException("Hive returned an invalid monthly row.");
            }
            result.months.add(
                    new Metric(row[0], parseMetric(row[1], "monthly count")));
        }

        List<String[]> boroughRows = queryResults.get(2);
        for (String[] row : boroughRows) {
            if (row.length != 2 || row[0].isBlank()) {
                throw new IOException("Hive returned an invalid borough row.");
            }
            result.boroughs.add(
                    new Metric(row[0], parseMetric(row[1], "borough count")));
            result.boroughOptions.add(row[0]);
        }

        List<String[]> statusRows = queryResults.get(3);
        for (String[] row : statusRows) {
            if (row.length != 2 || row[0].isBlank()) {
                throw new IOException("Hive returned an invalid status row.");
            }
            result.statuses.add(
                    new Metric(row[0], parseMetric(row[1], "status count")));
        }

        List<String[]> queensStatusRows = queryResults.get(4);
        if (queensStatusRows.size() != 1 || queensStatusRows.get(0).length != 2) {
            throw new IOException("Hive returned invalid Queens status counts.");
        }
        result.queensStatuses.add(new Metric("Completed",
                parseMetric(queensStatusRows.get(0)[0], "Queens completed count")));
        result.queensStatuses.add(new Metric("In progress",
                parseMetric(queensStatusRows.get(0)[1], "Queens in-progress count")));

        List<String[]> patternRows = queryResults.get(5);
        for (String[] row : patternRows) {
            if (row.length != 6 || row[0].isBlank() || row[1].isBlank()) {
                throw new IOException("Hive returned an invalid recurring pattern.");
            }
            int months;
            long total;
            long patternCount;
            try {
                months = Integer.parseInt(row[2].trim());
                total = Long.parseLong(row[3].trim());
                patternCount = Long.parseLong(row[5].trim());
            } catch (NumberFormatException ex) {
                throw new IOException("Hive returned invalid pattern metrics.", ex);
            }
            if (!result.patterns.isEmpty()
                    && result.patternCount != patternCount) {
                throw new IOException("Hive returned inconsistent pattern counts.");
            }
            result.patternCount = patternCount;
            String patternId = RUN_ID + "::" + row[0] + "::" + row[1];
            result.patterns.add(new Metric(
                    patternId, total, months, row[4].trim()));
        }

        List<String[]> categoryOptionRows = queryResults.get(6);
        for (String[] row : categoryOptionRows) {
            if (row.length == 2 && !row[0].isBlank()) {
                result.categoryOptions.add(row[0]);
            }
        }

        List<String[]> rangeRows = queryResults.get(7);
        if (rangeRows.size() != 1 || rangeRows.get(0).length != 2
                || rangeRows.get(0)[0].isBlank()
                || rangeRows.get(0)[1].isBlank()) {
            throw new IOException("Hive returned an invalid dataset date range.");
        }
        result.minDate = rangeRows.get(0)[0].trim();
        result.maxDate = rangeRows.get(0)[1].trim();

        result.loadedAt = System.currentTimeMillis();
        return result;
    }

    private List<String[]> runHiveQuery(String query) throws Exception {
        return runHiveQuery(query, "total");
    }

    private List<String[]> runHiveQuery(String query, String expectedHeader)
            throws Exception {
        return runHiveQueries(List.of(query), List.of(expectedHeader))
                .get(0);
    }

    private List<List<String[]>> runHiveQueries(
            List<String> queries, List<String> expectedHeaders)
            throws Exception {
        if (queries.isEmpty() || queries.size() != expectedHeaders.size()) {
            throw new IllegalArgumentException(
                    "Hive queries and result headers must have the same non-zero size.");
        }
        List<String> command = new ArrayList<>();
        command.add("wsl.exe");
        command.add("--distribution");
        command.add(WSL_DISTRO);
        command.add("--");
        command.add("/opt/hive/bin/beeline");
        command.add("-u");
        command.add(HIVE_URL);
        command.add("--silent=true");
        command.add("--showHeader=true");
        command.add("--outputformat=tsv2");
        command.add("--hiveconf");
        command.add("mapreduce.task.io.sort.mb=32");
        command.add("-n");
        command.add(HIVE_USER);
        command.add("-e");
        command.add(String.join("; ", queries));

        Process process = new ProcessBuilder(command).start();
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();
        Thread stdoutReader = drain(process.getInputStream(), stdout,
                "hive-stdout", MAX_HIVE_OUTPUT_CHARS);
        Thread stderrReader = drain(process.getErrorStream(), stderr,
                "hive-stderr", 256_000);
        boolean finished = process.waitFor(
                HIVE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IOException("Hive dashboard query exceeded "
                    + (HIVE_TIMEOUT_MILLIS / 1000) + " seconds.");
        }
        stdoutReader.join();
        stderrReader.join();
        if (process.exitValue() != 0) {
            throw new IOException("Hive dashboard query failed with exit code "
                    + process.exitValue() + ": "
                    + compact(stderr + "\n" + stdout));
        }

        List<List<String[]>> results = new ArrayList<>();
        int resultIndex = -1;
        for (int i = 0; i < queries.size(); i++) {
            results.add(new ArrayList<>());
        }
        for (String line : stdout.toString().split("\\R")) {
            if (line.isBlank() || line.startsWith("SLF4J:")
                    || line.startsWith("Picked up JAVA_TOOL_OPTIONS:")) {
                continue;
            }
            String trimmed = line.trim();
            if (resultIndex + 1 < expectedHeaders.size()
                    && trimmed.equals(expectedHeaders.get(resultIndex + 1))) {
                resultIndex++;
                continue;
            }
            if (resultIndex >= 0) {
                String[] fields = line.split("\t", -1);
                if (fields.length > 0) {
                    results.get(resultIndex).add(fields);
                }
            }
        }
        if (resultIndex != queries.size() - 1) {
            throw new IOException("Hive returned only " + (resultIndex + 1)
                    + " result sets for " + queries.size() + " dashboard queries: "
                    + compact(stderr.toString()));
        }
        return results;
    }

    private static long parseMetric(String value, String description)
            throws IOException {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            throw new IOException("Hive returned an invalid " + description
                    + ": " + value, ex);
        }
    }

    private static Thread drain(java.io.InputStream stream,
            StringBuilder destination, String name, int maximumCharacters) {
        Thread thread = new Thread(() -> {
            try (stream) {
                byte[] buffer = new byte[4096];
                int read;
                while ((read = stream.read(buffer)) != -1) {
                    synchronized (destination) {
                        if (destination.length() < maximumCharacters) {
                            destination.append(new String(
                                    buffer, 0, read, StandardCharsets.UTF_8));
                        }
                    }
                }
            } catch (IOException ex) {
                synchronized (destination) {
                    destination.append("\n").append(ex.getMessage());
                }
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static String categoryArray(List<Metric> metrics) {
        StringBuilder json = new StringBuilder("[");
        int count = Math.min(15, metrics.size());
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(metrics.get(i).toJson());
        }
        return json.append(']').toString();
    }

    private static String monthArray(List<Metric> metrics) {
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < metrics.size(); i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(metrics.get(i).toJson());
        }
        return json.append(']').toString();
    }

    private static String stringArray(List<String> values) {
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(jsonString(values.get(i)));
        }
        return json.append(']').toString();
    }

    private static String documentArray(List<Document> documents) {
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < documents.size(); i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(documents.get(i).toJson());
        }
        return json.append(']').toString();
    }

    private static String decodePath(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static String compact(String value) {
        String cleaned = value.replaceAll("\\s+", " ").trim();
        return cleaned.length() > 2_500
                ? cleaned.substring(cleaned.length() - 2_500) : cleaned;
    }

    private static String jsonString(String value) {
        return new Document("value", value).toJson()
                .replaceFirst("^\\{\\\"value\\\":", "")
                .replaceFirst("\\}$", "");
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> parameters = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return parameters;
        }
        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 2
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            if (parameters.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException(
                        "Duplicate query parameter: " + key);
            }
        }
        return parameters;
    }

    private static void sendFile(HttpExchange exchange) throws IOException {
        sendStaticFile(exchange, DASHBOARD_FILE, "text/html; charset=utf-8");
    }

    private static void sendStaticFile(
            HttpExchange exchange, String file, String contentType)
            throws IOException {
        Path path = Path.of(file);
        if (!Files.isRegularFile(path)) {
            sendJson(exchange, 404, "{\"error\":\"Dashboard asset is missing.\"}");
            return;
        }
        byte[] body = Files.readAllBytes(path);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Cache-Control",
                contentType.startsWith("text/html")
                        ? "no-store" : "public, max-age=86400");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
    }

    private static void sendJson(HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set(
                "Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static final class Metric {
        private final String label;
        private final long value;
        private final int monthsObserved;
        private final String periods;

        private Metric(String label, long value) {
            this(label, value, 0, "");
        }

        private Metric(String label, long value, int monthsObserved) {
            this(label, value, monthsObserved, "");
        }

        private Metric(String label, long value, int monthsObserved,
                String periods) {
            this.label = label;
            this.value = value;
            this.monthsObserved = monthsObserved;
            this.periods = periods;
        }

        private String toJson() {
            return "{\"label\":" + jsonString(label)
                    + ",\"value\":" + value + "}";
        }
    }

    private static final class CachedAnalytics {
        private long loadedAt;
        private long recordCount;
        private long patternCount;
        private String filterKey;
        private String minDate;
        private String maxDate;
        private final List<Metric> categories = new ArrayList<>();
        private final List<Metric> months = new ArrayList<>();
        private final List<Metric> boroughs = new ArrayList<>();
        private final List<Metric> statuses = new ArrayList<>();
        private final List<Metric> queensStatuses = new ArrayList<>();
        private final List<Metric> patterns = new ArrayList<>();
        private final List<String> boroughOptions = new ArrayList<>();
        private final List<String> categoryOptions = new ArrayList<>();
    }

    private static final class DashboardFilters {
        private final LocalDate fromDate;
        private final LocalDate toDate;
        private final String borough;
        private final String complaintType;

        private DashboardFilters(LocalDate fromDate, LocalDate toDate,
                String borough, String complaintType) {
            this.fromDate = fromDate;
            this.toDate = toDate;
            this.borough = borough;
            this.complaintType = complaintType;
        }

        private static DashboardFilters parse(String rawQuery) {
            Map<String, String> query = parseQuery(rawQuery);
            for (String key : query.keySet()) {
                if (!List.of("from", "to", "borough", "complaintType")
                        .contains(key)) {
                    throw new IllegalArgumentException(
                            "Unsupported dashboard filter: " + key);
                }
            }
            LocalDate from = parseDate(query.get("from"), "from");
            LocalDate to = parseDate(query.get("to"), "to");
            if (from != null && to != null && from.isAfter(to)) {
                throw new IllegalArgumentException(
                        "The start date must not be after the end date.");
            }
            return new DashboardFilters(from, to,
                    validateDimension(query.get("borough"), "borough"),
                    validateDimension(
                            query.get("complaintType"), "complaint type"));
        }

        private static LocalDate parseDate(String value, String name) {
            if (value == null || value.isBlank()) {
                return null;
            }
            try {
                return LocalDate.parse(value);
            } catch (java.time.format.DateTimeParseException ex) {
                throw new IllegalArgumentException(
                        "Filter '" + name + "' must use YYYY-MM-DD.");
            }
        }

        private static String validateDimension(String value, String name) {
            if (value == null || value.isBlank()) {
                return null;
            }
            if (value.length() > 100 || value.chars().anyMatch(
                    Character::isISOControl)) {
                throw new IllegalArgumentException(
                        "Invalid " + name + " filter.");
            }
            return value;
        }

        private String cacheKey() {
            return String.join("|",
                    fromDate == null ? "" : fromDate.toString(),
                    toDate == null ? "" : toDate.toString(),
                    borough == null ? "" : borough,
                    complaintType == null ? "" : complaintType);
        }

        private String hiveWhereClause() {
            return hiveWhereClause(null);
        }

        private String hiveWhereClause(String condition) {
            return hiveWhereClause(condition, true);
        }

        private String hiveWhereClauseWithoutBorough(String condition) {
            return hiveWhereClause(condition, false);
        }

        private String hiveWhereClause(String condition, boolean includeBorough) {
            List<String> clauses = new ArrayList<>();
            if (condition != null && !condition.isBlank()) {
                clauses.add(condition);
            }
            if (fromDate != null) {
                clauses.add("created_date >= "
                        + sqlString(fromDate + "T00:00:00"));
            }
            if (toDate != null) {
                clauses.add("created_date < "
                        + sqlString(toDate.plusDays(1) + "T00:00:00"));
            }
            if (includeBorough && borough != null) {
                clauses.add("borough = " + sqlString(borough));
            }
            if (complaintType != null) {
                clauses.add("complaint_type = " + sqlString(complaintType));
            }
            return clauses.isEmpty() ? "" : " WHERE "
                    + String.join(" AND ", clauses) + " ";
        }

        private Bson mongoPatternFilter() {
            List<Bson> clauses = new ArrayList<>();
            if (borough != null) {
                clauses.add(Filters.eq("borough", borough));
            }
            if (complaintType != null) {
                clauses.add(Filters.eq("complaintType", complaintType));
            }
            return clauses.isEmpty() ? new Document()
                    : Filters.and(clauses);
        }

        private Bson mongoEvidenceFilter() {
            List<Bson> clauses = new ArrayList<>();
            if (borough != null) {
                clauses.add(Filters.eq("borough", borough));
            }
            if (complaintType != null) {
                clauses.add(Filters.eq("complaintType", complaintType));
            }
            if (fromDate != null) {
                clauses.add(Filters.gte(
                        "createdDate", fromDate + "T00:00:00"));
            }
            if (toDate != null) {
                clauses.add(Filters.lt("createdDate",
                        toDate.plusDays(1) + "T00:00:00"));
            }
            return clauses.isEmpty() ? new Document()
                    : Filters.and(clauses);
        }

        private static String sqlString(String value) {
            return "'" + value.replace("'", "''") + "'";
        }
    }
}

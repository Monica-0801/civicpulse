import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class CivicPulseMongoImporter {

    private static final String DEFAULT_CLEAN_HDFS =
            "/civicpulse/processed/pig_clean_monthly_e2e_20261013";
    private static final String DEFAULT_HOTSPOTS_HDFS =
            "/civicpulse/analytics/persistent_hotspots_monthly_e2e_20261013";
    private static final String DEFAULT_MONGO_URI =
            "mongodb://127.0.0.1:27017";
    private static final int BATCH_SIZE = 500;

    private CivicPulseMongoImporter() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 4) {
            System.err.println(
                    "Usage: CivicPulseMongoImporter <run-id> "
                    + "[clean-hdfs-dir] [hotspots-hdfs-dir] [max-patterns]");
            System.exit(2);
        }

        String runId = args[0];
        if (!runId.matches("[A-Za-z0-9_-]{1,80}")) {
            throw new IllegalArgumentException(
                    "run-id may contain only letters, digits, '_' and '-'.");
        }
        String cleanDir = args.length >= 2 ? args[1] : DEFAULT_CLEAN_HDFS;
        String hotspotDir = args.length >= 3 ? args[2] : DEFAULT_HOTSPOTS_HDFS;
        int maxPatterns = args.length >= 4
                ? Integer.parseInt(args[3]) : 100;
        validateHdfsDirectory(cleanDir, "/civicpulse/processed/");
        validateHdfsDirectory(hotspotDir, "/civicpulse/analytics/");
        if (maxPatterns < 1 || maxPatterns > 5_000) {
            throw new IllegalArgumentException(
                    "max-patterns must be between 1 and 5000.");
        }

        String mongoUri = System.getenv().getOrDefault(
                "CIVICPULSE_MONGODB_URI", DEFAULT_MONGO_URI);
        runImport(runId, cleanDir, hotspotDir, maxPatterns, mongoUri);
    }

    private static void runImport(String runId, String cleanDir,
            String hotspotDir, int maxPatterns, String mongoUri)
            throws Exception {
        Map<String, PatternRecord> patterns = loadPatterns(
                hotspotDir, maxPatterns, runId);
        if (patterns.isEmpty()) {
            throw new IOException("No valid hotspot rows found in " + hotspotDir);
        }

        long[] scanStats = loadEvidence(cleanDir, patterns);
        List<Document> patternDocuments = new ArrayList<>();
        List<Document> evidenceDocuments = new ArrayList<>();
        for (PatternRecord pattern : patterns.values()) {
            patternDocuments.add(pattern.toDocument(runId));
            evidenceDocuments.addAll(pattern.evidence);
        }

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(
                        new com.mongodb.ConnectionString(mongoUri))
                .applyToClusterSettings(builder ->
                        builder.serverSelectionTimeout(8, TimeUnit.SECONDS))
                .build();

        try (MongoClient client = MongoClients.create(settings)) {
            MongoCollection<Document> patternCollection = client
                    .getDatabase("civicpulse")
                    .getCollection("patterns");
            MongoCollection<Document> evidenceCollection = client
                    .getDatabase("civicpulse")
                    .getCollection("complaint_evidence");

            patternCollection.createIndex(Indexes.ascending("runId"));
            evidenceCollection.createIndex(Indexes.ascending("runId"));
            evidenceCollection.createIndex(Indexes.ascending("patternId"));

            int patternWrites = upsertDocuments(
                    patternCollection, patternDocuments);
            int evidenceWrites = upsertDocuments(
                    evidenceCollection, evidenceDocuments);

            long savedPatterns = patternCollection.countDocuments(
                    new Document("runId", runId));
            long savedEvidence = evidenceCollection.countDocuments(
                    new Document("runId", runId));
            if (savedPatterns < patternDocuments.size()
                    || savedEvidence < evidenceDocuments.size()) {
                throw new IOException("MongoDB verification count did not "
                        + "match the records submitted for run " + runId + ".");
            }

            Document samplePattern = patternCollection.find(
                    Filters.eq("runId", runId)).first();
            if (samplePattern == null) {
                throw new IOException("Could not retrieve an imported pattern "
                        + "for run " + runId + ".");
            }
            Document sampleEvidence = evidenceCollection.find(
                    Filters.eq("patternId", samplePattern.getString("_id"))).first();
            if (sampleEvidence == null) {
                throw new IOException("Could not retrieve evidence for pattern "
                        + samplePattern.getString("_id") + ".");
            }

            System.out.println("CivicPulse MongoDB import completed.");
            System.out.println("Run ID: " + runId);
            System.out.println("HDFS complaint rows scanned: " + scanStats[0]);
            System.out.println("Malformed complaint rows skipped: " + scanStats[1]);
            System.out.println("Selected hotspots: " + patterns.size());
            System.out.println("Evidence complaint documents: "
                    + evidenceDocuments.size());
            System.out.println("Pattern documents upserted: " + patternWrites);
            System.out.println("Evidence documents upserted: " + evidenceWrites);
            System.out.println("MongoDB verified: " + savedPatterns
                    + " patterns, " + savedEvidence + " evidence documents.");
            System.out.println("Retrieved sample pattern: "
                    + samplePattern.getString("area") + " / "
                    + samplePattern.getString("complaintType"));
            System.out.println("Retrieved linked evidence request: "
                    + sampleEvidence.getString("requestId"));
        }
    }

    private static Map<String, PatternRecord> loadPatterns(
            String hdfsDir, int maxPatterns, String runId) throws Exception {
        Map<String, PatternRecord> patterns = new HashMap<>();
        try (HdfsLines lines = new HdfsLines(hdfsDir)) {
            String line;
            while (patterns.size() < maxPatterns && (line = lines.readLine()) != null) {
                String[] fields = line.split("\t", -1);
                if (fields.length != 4) {
                    continue;
                }

                String[] areaFields = fields[0].split("\\|", 2);
                if (areaFields.length != 2
                        || fields[1].isBlank()
                        || fields[2].isBlank()
                        || fields[3].isBlank()) {
                    continue;
                }

                int months;
                long total;
                try {
                    months = Integer.parseInt(fields[2]);
                    total = Long.parseLong(fields[3]);
                } catch (NumberFormatException ex) {
                    continue;
                }
                if (months < 2 || total < 1) {
                    continue;
                }

                PatternRecord pattern = new PatternRecord(
                        runId, fields[0], areaFields[0], areaFields[1],
                        fields[1], months, total);
                patterns.put(pattern.key(), pattern);
            }
        }
        return patterns;
    }

    private static long[] loadEvidence(String hdfsDir,
            Map<String, PatternRecord> patterns) throws Exception {
        long scanned = 0;
        long malformed = 0;
        try (HdfsLines lines = new HdfsLines(hdfsDir)) {
            String line;
            while ((line = lines.readLine()) != null) {
                String[] fields = line.split("\t", -1);
                if (fields.length != 11) {
                    malformed++;
                    continue;
                }
                scanned++;

                String area = fields[9] + "|" + fields[7];
                PatternRecord pattern = patterns.get(area + "\u0000" + fields[4]);
                if (pattern == null || pattern.evidence.size() >= 5) {
                    continue;
                }
                pattern.evidence.add(new Document("_id",
                                pattern.runScopedId + "::" + fields[0])
                        .append("runId", pattern.runId)
                        .append("patternId", pattern.runScopedId)
                        .append("requestId", fields[0])
                        .append("createdDate", fields[1])
                        .append("borough", fields[9])
                        .append("incidentZip", fields[7])
                        .append("complaintType", fields[4])
                        .append("descriptor", fields[5])
                        .append("agency", fields[2])
                        .append("status", fields[10]));
            }
        }
        return new long[] {scanned, malformed};
    }

    private static int upsertDocuments(MongoCollection<Document> collection,
            List<Document> documents) {
        int submitted = 0;
        ReplaceOptions options = new ReplaceOptions().upsert(true);
        for (int start = 0; start < documents.size(); start += BATCH_SIZE) {
            int end = Math.min(start + BATCH_SIZE, documents.size());
            List<WriteModel<Document>> writes = new ArrayList<>(end - start);
            for (Document document : documents.subList(start, end)) {
                writes.add(new ReplaceOneModel<>(
                        new Document("_id", document.get("_id")),
                        document, options));
            }
            collection.bulkWrite(writes);
            submitted += writes.size();
        }
        return submitted;
    }

    private static void validateHdfsDirectory(String path, String prefix) {
        if (!path.startsWith(prefix)
                || !path.matches("/[A-Za-z0-9_/-]+")
                || path.contains("..")) {
            throw new IllegalArgumentException(
                    "Invalid HDFS directory for this pipeline: " + path);
        }
    }

    private static final class PatternRecord {
        private final String area;
        private final String borough;
        private final String zip;
        private final String category;
        private final int monthsObserved;
        private final long totalComplaints;
        private final List<Document> evidence = new ArrayList<>();
        private String runId;
        private String runScopedId;

        private PatternRecord(String runId, String area, String borough, String zip,
                String category, int monthsObserved, long totalComplaints) {
            this.runId = runId;
            this.area = area;
            this.borough = borough;
            this.zip = zip;
            this.category = category;
            this.monthsObserved = monthsObserved;
            this.totalComplaints = totalComplaints;
            this.runScopedId = runId + "::" + area + "::" + category;
        }

        private String key() {
            return area + "\u0000" + category;
        }

        private Document toDocument(String currentRunId) {
            return new Document("_id", runScopedId)
                    .append("runId", currentRunId)
                    .append("area", area)
                    .append("borough", borough)
                    .append("incidentZip", zip)
                    .append("complaintType", category)
                    .append("monthsObserved", monthsObserved)
                    .append("totalComplaints", totalComplaints)
                    .append("evidenceCount", evidence.size());
        }
    }

    private static final class HdfsLines implements AutoCloseable {
        private final Process process;
        private final BufferedReader output;
        private final StringBuilder errors = new StringBuilder();
        private final Thread errorReader;
        private boolean finished;

        private HdfsLines(String hdfsDir) throws IOException {
            String patternPath = hdfsDir + "/part-*";
            ProcessBuilder builder;
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                String command = "hdfs dfs -cat '" + patternPath + "'";
                String wslDistro = System.getenv().getOrDefault(
                        "CIVICPULSE_WSL_DISTRO", "Ubuntu");
                builder = new ProcessBuilder(
                        "wsl.exe", "--distribution", wslDistro,
                        "--", "bash", "-lc", command);
            } else {
                builder = new ProcessBuilder(
                        "hdfs", "dfs", "-cat", patternPath);
            }
            process = builder.start();
            output = new BufferedReader(new InputStreamReader(
                    process.getInputStream(), StandardCharsets.UTF_8));
            errorReader = new Thread(
                    () -> copyErrors(process, errors), "hdfs-stderr");
            errorReader.setDaemon(true);
            errorReader.start();
        }

        private String readLine() throws IOException {
            String line = output.readLine();
            if (line == null) {
                finish();
            }
            return line;
        }

        private void finish() throws IOException {
            if (finished) {
                return;
            }
            finished = true;
            try {
                int status = process.waitFor();
                errorReader.join();
                if (status != 0) {
                    throw new IOException("HDFS stream command failed (exit "
                            + status + "): " + errors);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading HDFS output.", ex);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                output.close();
            } finally {
                if (!finished) {
                    process.destroyForcibly();
                }
            }
        }

        private static void copyErrors(Process process, StringBuilder errors) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            process.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (errors) {
                        if (errors.length() < 8_000) {
                            errors.append(line).append(System.lineSeparator());
                        }
                    }
                }
            } catch (IOException ex) {
                synchronized (errors) {
                    errors.append("Could not read HDFS command diagnostics: ")
                            .append(ex.getMessage());
                }
            }
        }
    }
}

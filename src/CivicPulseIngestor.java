import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class CivicPulseIngestor {

    private static final String API =
            "https://data.cityofnewyork.us/resource/erm2-nwe9.csv";
    private static final String[] FIELDS = {
        "unique_key", "created_date", "agency", "agency_name",
        "complaint_type", "descriptor", "location_type",
        "incident_zip", "city", "borough", "status"
    };
    private static final int DEFAULT_PAGE_SIZE = 1_000;
    private static final int DEFAULT_MAX_RECORDS = 10_000;
    private static final int MAX_PAGE_SIZE = 50_000;
    private static final int MAX_RETRIES = 3;

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();

    private CivicPulseIngestor() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 5) {
            System.err.println(
                    "Usage: CivicPulseIngestor <start-date> "
                    + "<end-date-exclusive> <output.csv> "
                    + "[page-size] [max-records]");
            System.exit(2);
        }

        LocalDate startDate = LocalDate.parse(args[0]);
        LocalDate endDate = LocalDate.parse(args[1]);
        if (!startDate.isBefore(endDate)) {
            throw new IllegalArgumentException(
                    "start-date must be earlier than end-date-exclusive.");
        }

        int pageSize = args.length >= 4
                ? Integer.parseInt(args[3]) : DEFAULT_PAGE_SIZE;
        int maxRecords = args.length >= 5
                ? Integer.parseInt(args[4]) : DEFAULT_MAX_RECORDS;
        if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "page-size must be between 1 and " + MAX_PAGE_SIZE + ".");
        }
        if (maxRecords < 1) {
            throw new IllegalArgumentException("max-records must be positive.");
        }

        Path output = Path.of(args[2]).toAbsolutePath().normalize();
        Path parent = output.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("Output must have a parent directory.");
        }
        Files.createDirectories(parent);

        new CivicPulseIngestor().collect(
                startDate, endDate, output, pageSize, maxRecords);
    }

    private void collect(LocalDate startDate, LocalDate endDate, Path output,
            int pageSize, int maxRecords) throws Exception {
        Path temporaryOutput = output.resolveSibling(output.getFileName() + ".part");
        long fetched = 0;
        long invalid = 0;
        long duplicateIds = 0;
        long offset = 0;
        Set<String> seenIds = new HashSet<>();
        boolean completed = false;

        try (BufferedWriter writer = Files.newBufferedWriter(
                temporaryOutput, StandardCharsets.UTF_8)) {
            writeRow(writer, List.of(FIELDS));

            while (fetched < maxRecords) {
                int limit = (int) Math.min(pageSize, maxRecords - fetched);
                String responseBody = fetchPage(
                        buildUri(startDate, endDate, limit, offset));
                List<List<String>> rows = parseCsv(responseBody);
                if (rows.isEmpty()) {
                    break;
                }

                List<String> header = rows.get(0);
                if (!header.equals(List.of(FIELDS))) {
                    throw new IOException("Unexpected API CSV header: " + header);
                }

                int pageRecords = 0;
                for (int rowIndex = 1; rowIndex < rows.size(); rowIndex++) {
                    List<String> row = rows.get(rowIndex);
                    if (row.size() != FIELDS.length) {
                        throw new IOException("API returned " + row.size()
                                + " columns; expected " + FIELDS.length
                                + " at offset " + offset
                                + ", row " + rowIndex + ".");
                    }

                    fetched++;
                    pageRecords++;
                    if (!isValid(row, startDate, endDate)) {
                        invalid++;
                    }
                    if (!seenIds.add(row.get(0))) {
                        duplicateIds++;
                    }
                    writeRow(writer, row);
                }
                writer.flush();

                System.out.printf("Fetched %,d records (offset %,d).%n",
                        fetched, offset);
                if (pageRecords < limit) {
                    break;
                }
                offset += pageRecords;
            }

            if (fetched == 0) {
                throw new IOException("The API returned no records for the date range.");
            }
            completed = true;
        } finally {
            if (!completed) {
                Files.deleteIfExists(temporaryOutput);
            }
        }

        moveAtomically(temporaryOutput, output);
        System.out.println("NYC 311 collection completed.");
        System.out.println("Date range: [" + startDate + ", " + endDate + ")");
        System.out.println("Records written: " + fetched);
        System.out.println("Rows failing required-field/date validation: " + invalid);
        System.out.println("Duplicate IDs observed (preserved in raw output): "
                + duplicateIds);
        System.out.println("Raw CSV: " + output);
    }

    private URI buildUri(LocalDate startDate, LocalDate endDate,
            int limit, long offset) {
        String where = "created_date >= '" + startDate
                + "T00:00:00' AND created_date < '" + endDate + "T00:00:00'";
        String query = "$select=" + String.join(",", FIELDS)
                + "&$where=" + encode(where)
                + "&$order=" + encode("created_date,unique_key")
                + "&$limit=" + limit
                + "&$offset=" + offset;
        return URI.create(API + "?" + query);
    }

    private String fetchPage(URI uri) throws Exception {
        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            HttpRequest.Builder request = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(90))
                    .header("Accept", "text/csv")
                    .header("User-Agent", "CivicPulse student project")
                    .GET();
            String appToken = System.getenv("SOCRATA_APP_TOKEN");
            if (appToken != null && !appToken.isBlank()) {
                request.header("X-App-Token", appToken);
            }

            HttpResponse<String> response = client.send(
                    request.build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return response.body();
            }

            if ((status == 429 || status >= 500) && attempt < MAX_RETRIES) {
                long delaySeconds = Math.min(1L << attempt, 8L);
                System.err.println("SODA API returned HTTP " + status
                        + "; retrying in " + delaySeconds + " second(s).");
                Thread.sleep(Duration.ofSeconds(delaySeconds).toMillis());
                continue;
            }
            throw new IOException("SODA API request failed with HTTP "
                    + status + ": " + response.body());
        }
        throw new IOException("SODA API request failed after retries.");
    }

    private static boolean isValid(List<String> row,
            LocalDate startDate, LocalDate endDate) {
        if (row.get(0).isBlank() || row.get(4).isBlank()) {
            return false;
        }
        try {
            LocalDate createdDate = LocalDate.parse(row.get(1).substring(0, 10));
            return !createdDate.isBefore(startDate)
                    && createdDate.isBefore(endDate);
        } catch (DateTimeParseException | IndexOutOfBoundsException ex) {
            return false;
        }
    }

    private static List<List<String>> parseCsv(String text) throws IOException {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;

        for (int i = 0; i < text.length(); i++) {
            char current = text.charAt(i);
            if (quoted) {
                if (current == '"') {
                    if (i + 1 < text.length() && text.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    field.append(current);
                }
            } else if (current == '"' && field.length() == 0) {
                quoted = true;
            } else if (current == ',') {
                row.add(field.toString());
                field.setLength(0);
            } else if (current == '\n' || current == '\r') {
                if (current == '\r' && i + 1 < text.length()
                        && text.charAt(i + 1) == '\n') {
                    i++;
                }
                row.add(field.toString());
                field.setLength(0);
                if (!(row.size() == 1 && row.get(0).isEmpty())) {
                    rows.add(row);
                }
                row = new ArrayList<>();
            } else {
                field.append(current);
            }
        }

        if (quoted) {
            throw new IOException("Malformed API CSV: unterminated quoted field.");
        }
        if (field.length() > 0 || !row.isEmpty()) {
            row.add(field.toString());
            rows.add(row);
        }
        return rows;
    }

    private static void writeRow(BufferedWriter writer, List<String> fields)
            throws IOException {
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                writer.write(',');
            }
            writer.write('"');
            writer.write(fields.get(i).replace("\"", "\"\""));
            writer.write('"');
        }
        writer.newLine();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static void moveAtomically(Path source, Path destination)
            throws IOException {
        try {
            Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ex) {
            Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}

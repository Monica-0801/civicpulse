import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

public class CivicPulseCleaner {

    private static String clean(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return "";
        }
        return value.asText().trim()
                .replaceAll("[\\r\\n\\t]+", " ")
                .replaceAll("\\s+", " ");
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    public static void main(String[] args) throws IOException {
        Path input = Path.of(args.length > 0
                ? args[0]
                : System.getProperty("user.home")
                    + "/civicpulse/data/raw/sample.json");

        Path output = Path.of(args.length > 1
                ? args[1]
                : System.getProperty("user.home")
                    + "/civicpulse/data/processed/complaints_clean.csv");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode records = mapper.readTree(input.toFile());

        if (records == null || !records.isArray()) {
            throw new IllegalArgumentException(
                    "Input must be a JSON array of complaint records.");
        }

        Files.createDirectories(output.getParent());

        String[] fields = {
            "unique_key", "created_date", "complaint_type",
            "descriptor", "borough", "status", "agency"
        };

        int processed = 0;
        int skipped = 0;

        try (BufferedWriter writer = Files.newBufferedWriter(
                output, StandardCharsets.UTF_8)) {

            for (int i = 0; i < fields.length; i++) {
                if (i > 0) writer.write(",");
                writer.write(fields[i]);
            }
            writer.newLine();

            for (JsonNode record : records) {
                if (!record.isObject()
                        || clean(record, "unique_key").isEmpty()
                        || clean(record, "complaint_type").isEmpty()) {
                    skipped++;
                    continue;
                }

                for (int i = 0; i < fields.length; i++) {
                    if (i > 0) writer.write(",");

                    String value = clean(record, fields[i]);

                    if (fields[i].equals("borough")
                            || fields[i].equals("status")
                            || fields[i].equals("agency")
                            || fields[i].equals("complaint_type")) {
                        value = value.toUpperCase(Locale.ROOT);
                    }

                    writer.write(csv(value));
                }

                writer.newLine();
                processed++;
            }
        }

        System.out.println("CivicPulse cleaning completed.");
        System.out.println("Input records: " + records.size());
        System.out.println("Processed records: " + processed);
        System.out.println("Skipped records: " + skipped);
        System.out.println("Output file: " + output);
    }
}

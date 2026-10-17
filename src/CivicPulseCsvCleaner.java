import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class CivicPulseCsvCleaner {

    private static final String[] FIELDS = {
        "unique_key", "created_date", "agency", "agency_name",
        "complaint_type", "descriptor", "location_type",
        "incident_zip", "city", "borough", "status"
    };

    private static String[] parseCsvLine(String line) {
        String[] values = new String[FIELDS.length];
        StringBuilder value = new StringBuilder();
        boolean quoted = false;
        int column = 0;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (quoted && i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {
                    value.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if (c == ',' && !quoted) {
                if (column < values.length) {
                    values[column] = value.toString();
                }
                column++;
                value.setLength(0);
            } else {
                value.append(c);
            }
        }

        if (column < values.length) {
            values[column] = value.toString();
        }

        for (int i = 0; i < values.length; i++) {
            values[i] = values[i] == null ? "" :
                values[i].trim().replaceAll("[\\r\\n\\t]+", " ")
                               .replaceAll("\\s+", " ");
        }
        return values;
    }

    private static String csv(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println(
                "Usage: CivicPulseCsvCleaner <input.csv> <output.csv>");
            System.exit(1);
        }

        Path input = Path.of(args[0]);
        Path output = Path.of(args[1]);

        Files.createDirectories(output.toAbsolutePath().getParent());

        int processed = 0;
        int skipped = 0;
        int duplicates = 0;
        Set<String> seenKeys = new HashSet<>();

        try (BufferedReader reader = Files.newBufferedReader(
                    input, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(
                    output, StandardCharsets.UTF_8)) {

            String header = reader.readLine();
            if (header == null) {
                throw new IOException("Input CSV is empty: " + input);
            }

            String[] actualHeader = parseCsvLine(header);
            for (int i = 0; i < FIELDS.length; i++) {
                if (!FIELDS[i].equals(actualHeader[i])) {
                    throw new IOException(
                        "Unexpected CSV header at column " + (i + 1)
                        + ". Expected " + FIELDS[i]
                        + ", found " + actualHeader[i]);
                }
            }

            for (int i = 0; i < FIELDS.length; i++) {
                if (i > 0) writer.write(",");
                writer.write(FIELDS[i]);
            }
            writer.newLine();

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    skipped++;
                    continue;
                }

                String[] row = parseCsvLine(line);

                if (row[0].isEmpty() || row[4].isEmpty()
                        || row[1].isEmpty()) {
                    skipped++;
                    continue;
                }

                if (!seenKeys.add(row[0])) {
                    duplicates++;
                    continue;
                }

                for (int i = 0; i < row.length; i++) {
                    if (i > 0) writer.write(",");

                    String value = row[i];
                    if (i == 0 || i == 7) {
                        value = value.trim();
                    }

                    if (i == 2 || i == 4 || i == 9 || i == 10) {
                        value = value.toUpperCase(Locale.ROOT);
                    }

                    writer.write(csv(value));
                }

                writer.newLine();
                processed++;
            }
        }

        System.out.println("CivicPulse CSV cleaning completed.");
        System.out.println("Processed records: " + processed);
        System.out.println("Skipped records: " + skipped);
        System.out.println("Duplicate IDs skipped: " + duplicates);
        System.out.println("Output file: " + output);
    }
}

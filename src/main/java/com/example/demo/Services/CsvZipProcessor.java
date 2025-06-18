package com.example.demo.Services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Service
public class CsvZipProcessor {

    private final FieldMetaService fieldMetaService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public CsvZipProcessor(FieldMetaService fieldMetaService) {
        this.fieldMetaService = fieldMetaService;
    }

    // #7: processes a zip file containing a CSV, extracts the CSV
    public List<String> processZipCsv(String zipPath, int resourceId) throws IOException {
        List<String> jsonList = new ArrayList<>();

        try (ZipFile zipFile = new ZipFile(zipPath)) {
            ZipEntry csvEntry = findCsvEntry(zipFile);
            Map<String, FieldMetaService.FieldMeta> fieldMetaMap = fieldMetaService.getFieldMeta(resourceId);

            try (InputStream is = zipFile.getInputStream(csvEntry);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

                String headerLine = reader.readLine();
                if (headerLine == null) return jsonList;

                List<CompletableFuture<String>> futures = processCsvLinesAsync(reader);

                // now we convert the list of completable futures to an array of completable futures
                // because CompletableFuture.allOf requires an array, so we use toArray method
                // and wait for all futures to complete with .join() method.
                try {
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                    for (CompletableFuture<String> future : futures) {
                        jsonList.add(future.join());
                    }
                } catch (CompletionException e) {
                    System.err.println("CSV parsing failed: " + e.getCause().getMessage());
                    return Collections.emptyList();
                }
            }
        }

        return jsonList;
    }

    // #8: finds the first CSV entry in the zip file.
    private ZipEntry findCsvEntry(ZipFile zipFile) throws IOException {
        return zipFile.stream()
                .filter(e -> e.getName().endsWith(".csv"))
                .findFirst()
                .orElseThrow(() -> new FileNotFoundException("CSV not found in zip"));
    }

    // #10: processes CSV lines asynchronously, converting each line to JSON, using DB field order.
    private List<CompletableFuture<String>> processCsvLinesAsync(
            BufferedReader reader
    ) {
        List<CompletableFuture<String>> futures = new ArrayList<>();
        reader.lines().forEach(line ->
                futures.add(CompletableFuture.supplyAsync(() ->
                        processCsvLine(line))));
        return futures;
    }

    private String processCsvLine(
            String line
    ) {
        String[] values = line.split(",");
        if (values.length < 2) throw new RuntimeException("Invalid CSV line: " + line);

        int resourceId = Integer.parseInt(values[0]);
        String resourceName = values[1];

        // Get ordered field names for this resourceId (excluding resourceId and resourceName)
        List<String> fieldNames = fieldMetaService.getPropsFieldNames(resourceId);

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("resourceId", resourceId);
        jsonMap.put("resourceName", resourceName);

        // Map CSV values to field names (starting from index 2)
        for (int i = 0; i < fieldNames.size() && (i + 2) < values.length; i++) {
            String fieldName = fieldNames.get(i);
            String value = values[i + 2];
            FieldMetaService.FieldMeta meta = fieldMetaService.getFieldMeta(resourceId).get(fieldName);
            Object castedValue = castValue(value, meta);
            jsonMap.put(fieldName, castedValue);
        }

        try {
            return objectMapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse line: " + line, e);
        }
    }

    private Object castValue(String value, FieldMetaService.FieldMeta meta) {
        if (meta == null || value == null) return value;
        return switch (meta.type()) {
            case "float" -> {
                try {
                    yield Float.parseFloat(value);
                } catch (Exception e) {
                    yield value;
                }
            }
            case "boolean" -> {
                try {
                    yield Boolean.parseBoolean(value);
                } catch (Exception e) {
                    yield value;
                }
            }
            case "int", "integer" -> {
                try {
                    yield Integer.parseInt(value);
                } catch (Exception e) {
                    yield value;
                }
            }
            default -> value;
        };
    }
}

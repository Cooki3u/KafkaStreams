package com.example.demo.Services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Service
public class ZipProcessor {

    private final ResourceDescriptionService resourceDescriptionService;
    private final PropsDataService propsDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public ZipProcessor(ResourceDescriptionService resourceDescriptionService, PropsDataService propsDataService) {
        this.resourceDescriptionService = resourceDescriptionService;
        this.propsDataService = propsDataService;
    }

    public List<String> processZipJson(String zipPath) throws IOException {
        List<String> jsonList = new ArrayList<>();
        try (ZipFile zipFile = new ZipFile(zipPath)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            ObjectMapper mapper = new ObjectMapper();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                String name = entry.getName();
                if (name.endsWith(".json") && !name.equals("metadata.json")) {
                    try (InputStream is = zipFile.getInputStream(entry)) {
                        JsonNode node = mapper.readTree(is);
                        if (node.isArray()) {
                            for (JsonNode obj : node) {
                                jsonList.add(mapper.writeValueAsString(obj));
                            }
                        } else if (node.isObject()) {
                            jsonList.add(mapper.writeValueAsString(node));
                        }
                    }
                }
            }
        }
        return jsonList;
    }

    // #7: processes a zip file containing a CSV, extracts the CSV
    public List<String> processZipCsv(String zipPath, int resourceId) throws IOException {
        List<String> jsonList = new ArrayList<>();

        try (ZipFile zipFile = new ZipFile(zipPath)) {
            ZipEntry csvEntry = findCsvEntry(zipFile);

            Map<String, PropsDataService.PropsData> propsDataMap = propsDataService.getPropsData(resourceId);
            List<String> fieldNames = propsDataService.getPropsFieldNames(resourceId);
            ResourceDescriptionService.CsvFormat csvFormat = resourceDescriptionService.getCsvFormat(resourceId);

            try (InputStream is = zipFile.getInputStream(csvEntry);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

                reader.readLine();
                List<CompletableFuture<String>> futures = processCsvLinesAsync(reader, propsDataMap, fieldNames, csvFormat);

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

    private ZipEntry findCsvEntry(ZipFile zipFile) throws IOException {
        return zipFile.stream()
                .filter(e -> e.getName().endsWith(".csv"))
                .findFirst()
                .orElseThrow(() -> new FileNotFoundException("CSV not found in zip"));
    }

    // #10: processes CSV lines asynchronously, converting each line to JSON, using DB field order.
    private List<CompletableFuture<String>> processCsvLinesAsync(
            BufferedReader reader, Map<String, PropsDataService.PropsData> propsDataMap
            , List<String> fieldNames, ResourceDescriptionService.CsvFormat csvFormat
    ) {
        List<CompletableFuture<String>> futures = new ArrayList<>();
        reader.lines().forEach(line ->
                futures.add(CompletableFuture.supplyAsync(() ->
                        processCsvLine(line, propsDataMap, fieldNames, csvFormat))));
        return futures;
    }

    private String processCsvLine(
            String line, Map<String, PropsDataService.PropsData> propsDataMap
            , List<String> fieldNames, ResourceDescriptionService.CsvFormat csvFormat
    ) {
        String delimiter = Pattern.quote(csvFormat.delimiter());
        String[] values = line.split(delimiter, -1);
        values[values.length - 1] = values[values.length - 1].replace(csvFormat.endLine(), "");

        // Pad values if missing columns, for example, if phone number is empty
        if (values.length < fieldNames.size()) {
            values = Arrays.copyOf(values, fieldNames.size());
        }

        Map<String, Object> jsonMap = new HashMap<>();

        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            String value = values[i];
            PropsDataService.PropsData meta = propsDataMap.get(fieldName);
            Object castedValue = castValue(value, meta);
            jsonMap.put(fieldName, castedValue);
        }

        try {
            return objectMapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse line: " + line, e);
        }
    }

    private Object castValue(String value, PropsDataService.PropsData meta) {
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

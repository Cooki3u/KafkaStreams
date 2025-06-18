package com.example.demo.Config;

import com.example.demo.Services.ResourceDescriptionCacheService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.example.demo.Services.CsvZipProcessor;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Configuration
public class KafkaStreamsConfig {

    public static final String RESET = "\u001B[0m";
    public static final String RED = "\u001B[31m";
    public static final String GREEN = "\u001B[32m";
    public static final String YELLOW = "\u001B[33m";
    public static final String BLUE = "\u001B[34m";

    private final ResourceDescriptionCacheService cacheService;
    private final CsvZipProcessor csvZipProcessor;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaStreamsConfig(ResourceDescriptionCacheService cacheService, CsvZipProcessor csvZipProcessor, KafkaTemplate<String, String> kafkaTemplate) {
        this.cacheService = cacheService;
        this.csvZipProcessor = csvZipProcessor;
        this.kafkaTemplate = kafkaTemplate;
    }

    // #1: topology of the stream.
    // this is the entry point, listens to the input_zip_topic Kafka topic
    // , and for each message, calls processZip.
    @Bean
    public KStream<String, String> zipStream(StreamsBuilder builder) {
        ObjectMapper objectMapper = new ObjectMapper();

        KStream<String, String> stream = builder.stream("input_zip_topic", Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> processZip(value, objectMapper));

        return stream;
    }

    // #2: responsible for the extraction, enrichment, and processing of
    // metadata and CSV data from the zip file, and triggers sending messages to Kafka.
    private boolean processZip(String value, ObjectMapper objectMapper) {
        // make the code clear and not rely on extractZipPath method to throw exception because it won't be able
        // to handle the case where the zipPath is not found or does not match the expected pattern.
        if (value == null || value.trim().isEmpty()) {
            return false;
        }
        try {
            String zipPath = extractZipPath(value, objectMapper);
            if (zipPath == null) {
                return false;
            }
            Map<String, Object> metadata = extractAndEnrichMetadata(zipPath, objectMapper);
            if (metadata == null) {
                return false;
            }
            Integer resourceId = extractResourceId(metadata);
            if (resourceId == null) {
                return false;
            }
            List<String> csvJsons = csvZipProcessor.processZipCsv(zipPath, resourceId);
            sendCsvJsonsToKafka(csvJsons, metadata, objectMapper);
            return !metadata.isEmpty();
        } catch (Exception e) {
            System.out.println("Error processing zip or metadata: " + e.getMessage());
            return false;
        }
    }

    // #3: parses the incoming JSON to extract the zipPath and validates it against
    // the expected filename pattern.
    private String extractZipPath(String value, ObjectMapper objectMapper) throws Exception {
        // convert the incoming JSON string to a JsonNode
        JsonNode node = objectMapper.readTree(value);
        String zipPath = node.path("zipPath").asText();
        if (!zipPath.matches(".*111_csv_with_metadata_.*\\.zip$")) {
            return null;
        }
        return zipPath;
    }

    // #4: opens the zip file, reads metadata.json, enriches it with resource
    // description from the cache, and returns the enriched metadata map.
    private Map<String, Object> extractAndEnrichMetadata(String zipPath, ObjectMapper objectMapper) {
        try (ZipFile zipFile = new ZipFile(zipPath)) {
            ZipEntry metadataEntry = zipFile.getEntry("metadata.json");
            if (metadataEntry == null) {
                return null;
            }
            try (InputStream is = zipFile.getInputStream(metadataEntry)) {
                Map<String, Object> metadata = objectMapper.readValue(is, Map.class);
                Integer resourceId = extractResourceId(metadata);
                if (resourceId != null) {
                    Map<String, Object> resourceDesc = cacheService.getDescription(resourceId);
                    if (resourceDesc != null) {
                        metadata.putAll(resourceDesc);
                        System.out.println(GREEN + "Enriched Metadata: " + metadata + RESET);
                    } else {
                        System.out.println(YELLOW + "No resource description found for ID: " + resourceId + RESET);
                    }
                } else {
                    System.out.println(YELLOW + "RESOURCE_ID not found in metadata.json" + RESET);
                }
                return metadata;
            }
        } catch (Exception e) {
            System.out.println("Error extracting metadata: " + e.getMessage());
            return null;
        }
    }

    // #5: extracts the resourceId from the metadata map, handling any parsing errors.
    private Integer extractResourceId(Map<String, Object> metadata) {
        Object resourceIdObj = metadata.get("resourceId");
        if (resourceIdObj != null) {
            try {
                return Integer.parseInt(resourceIdObj.toString());
            } catch (NumberFormatException e) {
                System.out.println(RED + "Invalid RESOURCE_ID in metadata: " + resourceIdObj + RESET);
            }
        }
        return null;
    }

    // #11: sends the CSV JSONs to the output_results Kafka topic
    private void sendCsvJsonsToKafka(List<String> csvJsons, Map<String, Object> metadata, ObjectMapper objectMapper) throws Exception {
        String metadataJson = objectMapper.writeValueAsString(metadata);
        for (String csvJson : csvJsons) {
            Map<String, Object> csvMap = objectMapper.readValue(csvJson, Map.class);
            String concatenatedValues = new TreeMap<>(csvMap).values().stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining());
            String key = sha256Hex(concatenatedValues);
            String message = metadataJson + "\n" + csvJson;
            System.out.println(RED + "Sending message with key: " + key + RESET + BLUE + " and message:\n" + message + RESET);
            kafkaTemplate.send("output_results", key, message);
        }
    }

    // #12: generates a SHA-256 hash of the concatenated values from the CSV JSON
    private String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return Hex.encodeHexString(hash);
        } catch (Exception e) {
            throw new RuntimeException("Error hashing input", e);
        }
    }
}
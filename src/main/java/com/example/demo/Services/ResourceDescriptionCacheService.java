package com.example.demo.Services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ResourceDescriptionCacheService {
    private final JdbcTemplate jdbcTemplate;
    // it allows both high scalability and safe concurrent access(multiple threads can read from the map simultaneously, good in our case because it prevents refreshing
    // the cache and getting value from the data at the same time).
    private final Map<Integer, Map<String, Object>> cache = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(ResourceDescriptionCacheService.class);

    @Autowired
    public ResourceDescriptionCacheService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        try {
            refreshCache();
        } catch (Exception e) {
            logger.error("Failed to initialize ResourceDescriptionCacheService", e);
        }
    }

    // refreshing the cache keeps your data up-to-date, reduces database calls
    // , reduce load on the database, speeds up access to resource descriptions.
    @Scheduled(fixedRate = 5 * 60 * 1000)
    public void refreshCache() {
        Map<Integer, Map<String, Object>> newCache = new ConcurrentHashMap<>();
        jdbcTemplate.query("SELECT RESOURCE_ID, RESOURCE_VERSION, RESOURCE_NAME, FILE_TYPE FROM RESOURCE_DESCRIPTION",
                rs -> {
                    int id = rs.getInt("RESOURCE_ID");
                    Map<String, Object> desc = Map.of(
                            "RESOURCE_VERSION", rs.getString("RESOURCE_VERSION"),
                            "RESOURCE_NAME", rs.getString("RESOURCE_NAME"),
                            "FILE_TYPE", rs.getString("FILE_TYPE")
                    );
                    newCache.put(id, desc);
                });
        cache.clear();
        cache.putAll(newCache);
    }

    // #6: retrieves the description for a given resource ID from the cache.
    public Map<String, Object> getDescription(int resourceId) {
        return cache.get(resourceId);
    }
}
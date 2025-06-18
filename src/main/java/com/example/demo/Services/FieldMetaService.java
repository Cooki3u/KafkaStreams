package com.example.demo.Services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class FieldMetaService {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public FieldMetaService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public record FieldMeta(String type, String specialType) {
    }

    // #9: retrieves field metadata for a given resource ID from the database.
    public Map<String, FieldMeta> getFieldMeta(int resourceId) {
        String sql = "SELECT FIELD_NAME, FIELD_TYPE, FIELD_SPECIAL_TYPE FROM PROPS_DATA WHERE RESOURCE_ID = ?";
        Map<String, FieldMeta> map = new HashMap<>();
        jdbcTemplate.query(sql, new Object[]{resourceId}, rs -> {
            map.put(rs.getString("FIELD_NAME"),
                    new FieldMeta(rs.getString("FIELD_TYPE"), rs.getString("FIELD_SPECIAL_TYPE")));
        });
        return map;
    }

    // Returns the ordered list of field names for a given resourceId, excluding resourceId and resourceName
    public List<String> getPropsFieldNames(int resourceId) {
        String sql = "SELECT FIELD_NAME FROM PROPS_DATA WHERE RESOURCE_ID = ?";
        List<String> fieldNames = new ArrayList<>();
        jdbcTemplate.query(sql, new Object[]{resourceId}, rs -> {
            String fieldName = rs.getString("FIELD_NAME");
            if (!"resourceId".equals(fieldName) && !"resourceName".equals(fieldName)) {
                fieldNames.add(fieldName);
            }
        });
        return fieldNames;
    }
}

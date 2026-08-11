// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.lance;

import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.StringUtils;
import org.lance.Dataset;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;

/** Loads and normalizes logical index metadata from one latest Lance dataset snapshot. */
public final class LanceIndexMetadataLoader {
    private static final int MAX_LOGICAL_INDEXES = 256;
    private static final int MAX_COLUMNS_PER_INDEX = 64;
    private static final int MAX_COLUMN_NAMES_BYTES = 16 * 1024;
    private static final int MAX_EXTERNAL_STRING_BYTES = 1024;
    private static final int MAX_PROPERTIES_BYTES = 400;

    private static final Set<String> PROPERTY_ALLOWLIST = ImmutableSet.of(
            "metric_type",
            "target_partition_size",
            "compression_type",
            "num_bits",
            "num_sub_vectors",
            "hnsw_max_connections",
            "hnsw_construction_ef",
            "hnsw_max_level");

    private LanceIndexMetadataLoader() {
    }

    /** Loads logical indexes and schema fields from the same latest dataset snapshot. */
    public static List<LanceLogicalIndex> load(String datasetUri,
            Map<String, String> javaStorageOptions, BufferAllocator allocator) throws Exception {
        try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetUri)
                .readOptions(LanceReadOptions.build(javaStorageOptions, OptionalLong.empty())).build()) {
            Map<Integer, String> topLevelFieldNames = new HashMap<>();
            for (LanceField field : dataset.getLanceSchema().fields()) {
                topLevelFieldNames.put(field.getId(), field.getName());
            }
            return normalize(dataset.describeIndices(), topLevelFieldNames);
        }
    }

    /** Converts SDK descriptions into bounded immutable Java-only metadata. */
    static List<LanceLogicalIndex> normalize(List<IndexDescription> descriptions,
            Map<Integer, String> topLevelFieldNames) {
        if (descriptions == null) {
            throw new IllegalArgumentException("Lance index descriptions must not be null");
        }
        if (descriptions.size() > MAX_LOGICAL_INDEXES) {
            throw new IllegalArgumentException(
                    "Lance logical index count exceeds limit " + MAX_LOGICAL_INDEXES);
        }
        if (topLevelFieldNames == null) {
            throw new IllegalArgumentException("Lance top-level field names must not be null");
        }

        List<IndexedLogicalIndex> normalized = new ArrayList<>(descriptions.size());
        for (int position = 0; position < descriptions.size(); ++position) {
            IndexDescription description = descriptions.get(position);
            if (description == null) {
                throw new IllegalArgumentException("Lance logical index description must not be null");
            }

            String name = requireExternalString(
                    description.getName(), "Lance logical index name");
            String indexType = requireExternalString(
                    description.getIndexType(), "Lance logical index type");
            List<Integer> fieldIds = description.getFieldIds();
            if (fieldIds == null || fieldIds.isEmpty()) {
                throw new IllegalArgumentException(
                        "Lance logical index field IDs must not be null or empty");
            }
            if (fieldIds.size() > MAX_COLUMNS_PER_INDEX) {
                throw new IllegalArgumentException(
                        "Lance logical index column count exceeds limit " + MAX_COLUMNS_PER_INDEX);
            }

            List<String> columns = new ArrayList<>(fieldIds.size());
            Set<Integer> uniqueFieldIds = new HashSet<>();
            int columnNamesBytes = 0;
            for (Integer fieldId : fieldIds) {
                if (fieldId == null) {
                    throw new IllegalArgumentException(
                            "Lance logical index field ID must not be null");
                }
                if (!uniqueFieldIds.add(fieldId)) {
                    throw new IllegalArgumentException(
                            "Duplicate field id " + fieldId + " in Lance logical index metadata");
                }
                if (!topLevelFieldNames.containsKey(fieldId)) {
                    throw new IllegalArgumentException(
                            "Lance index metadata references unknown or nested field id " + fieldId);
                }
                String column = requireExternalString(
                        topLevelFieldNames.get(fieldId), "Lance logical index column name");
                columnNamesBytes += utf8Length(column);
                if (columnNamesBytes > MAX_COLUMN_NAMES_BYTES) {
                    throw new IllegalArgumentException(
                            "Lance logical index column names exceed limit "
                                    + MAX_COLUMN_NAMES_BYTES + " UTF-8 bytes");
                }
                columns.add(column);
            }

            String properties = normalizeProperties(name, description.getDetailsJson());
            LanceLogicalIndex index = new LanceLogicalIndex(name, columns, indexType, properties);
            normalized.add(new IndexedLogicalIndex(index, position));
        }

        Set<String> logicalIndexNames = new HashSet<>();
        for (IndexedLogicalIndex indexed : normalized) {
            String name = indexed.index.getName();
            if (!logicalIndexNames.add(name)) {
                throw new IllegalArgumentException(
                        "Duplicate Lance logical index name '" + name + "'");
            }
        }
        normalized.sort(Comparator.comparing(
                (IndexedLogicalIndex indexed) -> indexed.index.getName())
                .thenComparingInt(indexed -> indexed.position));
        List<LanceLogicalIndex> result = new ArrayList<>(normalized.size());
        for (IndexedLogicalIndex indexed : normalized) {
            result.add(indexed.index);
        }
        return Collections.unmodifiableList(result);
    }

    private static String normalizeProperties(String indexName, String detailsJson) {
        if (detailsJson == null) {
            return "{}";
        }
        if (utf8Length(detailsJson) > MAX_EXTERNAL_STRING_BYTES) {
            throw new IllegalArgumentException(
                    "Lance index details JSON exceeds limit "
                            + MAX_EXTERNAL_STRING_BYTES + " UTF-8 bytes");
        }
        if (StringUtils.isBlank(detailsJson)) {
            return "{}";
        }

        JsonElement parsed;
        try (JsonReader reader = new JsonReader(new StringReader(detailsJson))) {
            reader.setLenient(false);
            parsed = GsonUtils.GSON.getAdapter(JsonElement.class).read(reader);
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                throw invalidDetailsJson(indexName);
            }
        } catch (IOException | RuntimeException e) {
            throw invalidDetailsJson(indexName);
        }
        if (!parsed.isJsonObject()) {
            throw invalidDetailsJson(indexName);
        }

        TreeMap<String, JsonElement> allowedProperties = new TreeMap<>();
        JsonObject object = parsed.getAsJsonObject();
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            if (!PROPERTY_ALLOWLIST.contains(entry.getKey())) {
                continue;
            }
            JsonElement value = entry.getValue();
            if (value == null || value.isJsonNull()) {
                continue;
            }
            if (!value.isJsonPrimitive()) {
                throw invalidDetailsJson(indexName);
            }
            allowedProperties.put(entry.getKey(), value);
        }

        String properties = GsonUtils.GSON.toJson(allowedProperties);
        if (utf8Length(properties) > MAX_PROPERTIES_BYTES) {
            throw new IllegalArgumentException(
                    "Lance index properties exceed limit "
                            + MAX_PROPERTIES_BYTES + " UTF-8 bytes");
        }
        return properties;
    }

    private static IllegalArgumentException invalidDetailsJson(String indexName) {
        return new IllegalArgumentException(
                "Invalid Lance index details JSON for '" + indexName + "'");
    }

    private static String requireExternalString(String value, String valueType) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(valueType + " must not be null or empty");
        }
        if (utf8Length(value) > MAX_EXTERNAL_STRING_BYTES) {
            throw new IllegalArgumentException(valueType + " exceeds limit "
                    + MAX_EXTERNAL_STRING_BYTES + " UTF-8 bytes");
        }
        return value;
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static final class IndexedLogicalIndex {
        private final LanceLogicalIndex index;
        private final int position;

        private IndexedLogicalIndex(LanceLogicalIndex index, int position) {
            this.index = index;
            this.position = position;
        }
    }
}

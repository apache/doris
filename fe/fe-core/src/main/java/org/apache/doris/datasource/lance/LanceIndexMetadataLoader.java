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
import org.lance.index.Index;
import org.lance.index.IndexCriteria;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Loads and normalizes logical and physical index metadata from one latest Lance dataset snapshot. */
public final class LanceIndexMetadataLoader {
    private static final int MAX_LOGICAL_INDEXES = 256;
    private static final int MAX_COLUMNS_PER_INDEX = 64;
    // Post-JNI cap for physical entries, independent of the logical-name limit.
    private static final int MAX_PHYSICAL_INDEX_ENTRIES = 16 * 1024;
    private static final int MAX_COLUMN_NAMES_BYTES = 16 * 1024;
    private static final int MAX_SCHEMA_FIELDS = MAX_LOGICAL_INDEXES * MAX_COLUMNS_PER_INDEX;
    private static final int MAX_SCHEMA_DEPTH = 64;
    private static final int MAX_EXTERNAL_STRING_BYTES = 1024;
    private static final int MAX_PROPERTIES_BYTES = 400;

    // System entries exposed by the pinned Lance producers and SDK paths.
    private static final Set<String> SYSTEM_INDEX_NAMES = ImmutableSet.of(
            "__lance_frag_reuse",
            "__lance_mem_wal");
    private static final Set<String> TOP_LEVEL_PROPERTY_ALLOWLIST = ImmutableSet.of(
            "metric_type",
            "target_partition_size");
    private static final Set<String> HNSW_PROPERTY_ALLOWLIST = ImmutableSet.of(
            "construction_ef",
            "max_connections",
            "max_level");
    private static final Set<String> COMPRESSION_PROPERTY_ALLOWLIST = ImmutableSet.of(
            "type",
            "num_bits",
            "num_sub_vectors",
            "rotation_type");

    private LanceIndexMetadataLoader() {
    }

    /** Loads logical indexes and schema fields from the same latest dataset snapshot. */
    public static List<LanceLogicalIndex> load(String datasetUri,
            Map<String, String> javaStorageOptions, BufferAllocator allocator) throws Exception {
        try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetUri)
                .readOptions(LanceReadOptions.build(javaStorageOptions, OptionalLong.empty())).build()) {
            Map<Integer, String> fieldNames = buildFieldNamesById(dataset.getLanceSchema().fields());
            return normalize(describeUserIndexes(dataset), fieldNames);
        }
    }

    /**
     * Loads physical index entries from one latest dataset snapshot. This path only calls
     * {@link Dataset#getIndexes()}; it never describes indexes or reads row statistics.
     */
    public static List<LancePhysicalIndexEntry> loadPhysicalEntries(String datasetUri,
            Map<String, String> javaStorageOptions, BufferAllocator allocator) throws Exception {
        try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetUri)
                .readOptions(LanceReadOptions.build(javaStorageOptions, OptionalLong.empty())).build()) {
            return collectPhysicalEntries(dataset);
        }
    }

    /** Converts the snapshot's raw index list into sorted immutable physical entries. */
    static List<LancePhysicalIndexEntry> collectPhysicalEntries(Dataset dataset) {
        List<Index> indexes = dataset.getIndexes();
        if (indexes == null) {
            throw new IllegalArgumentException("Lance physical index entries must not be null");
        }
        // Bound the raw provider response before any filtering so a flood of system
        // entries still fails closed instead of consuming unbounded memory.
        if (indexes.size() > MAX_PHYSICAL_INDEX_ENTRIES) {
            throw new IllegalArgumentException(
                    "Lance physical index entry count exceeds limit "
                            + MAX_PHYSICAL_INDEX_ENTRIES);
        }
        List<LancePhysicalIndexEntry> entries = new ArrayList<>(indexes.size());
        Set<String> seenUuids = new HashSet<>();
        for (Index index : indexes) {
            if (index == null) {
                throw new IllegalArgumentException(
                        "Lance physical index entry must not be null");
            }
            String name = requireExternalString(
                    index.name(), "Lance physical index entry name");
            UUID uuid = index.uuid();
            if (uuid == null) {
                throw new IllegalArgumentException(
                        "Lance physical index entry uuid must not be null");
            }
            long datasetVersion = index.datasetVersion();
            if (datasetVersion <= 0) {
                throw new IllegalArgumentException(
                        "Lance physical index entry dataset version must be positive");
            }
            String uuidString = uuid.toString();
            if (!seenUuids.add(uuidString)) {
                throw new IllegalArgumentException(
                        "Duplicate Lance physical index entry uuid '" + uuidString + "'");
            }
            // Validate every raw entry before filtering so malformed system metadata or a UUID
            // collision between a system and user entry cannot be hidden from the all-or-error read.
            if (SYSTEM_INDEX_NAMES.contains(name)) {
                continue;
            }
            entries.add(new LancePhysicalIndexEntry(name, uuidString, datasetVersion));
        }
        entries.sort(Comparator.comparing(LancePhysicalIndexEntry::getName)
                .thenComparing(LancePhysicalIndexEntry::getUuid));
        return Collections.unmodifiableList(entries);
    }

    /**
     * Describes only user-created indexes. The Lance JNI bulk describe path also tries to
     * materialize details for internal indexes, whose details are not supported by the SDK.
     */
    static List<IndexDescription> describeUserIndexes(Dataset dataset) {
        List<String> listedNames = dataset.listIndexes();
        if (listedNames == null) {
            throw new IllegalArgumentException("Lance index names must not be null");
        }
        if (listedNames.size() > MAX_PHYSICAL_INDEX_ENTRIES) {
            throw new IllegalArgumentException(
                    "Lance physical index entry count exceeds limit "
                            + MAX_PHYSICAL_INDEX_ENTRIES);
        }

        Set<String> userIndexNames = new LinkedHashSet<>();
        for (String listedName : listedNames) {
            if (SYSTEM_INDEX_NAMES.contains(listedName)) {
                continue;
            }
            String name = requireExternalString(listedName, "Lance logical index name");
            // nativeListIndexes returns physical entries, so one logical name can repeat.
            userIndexNames.add(name);
            if (userIndexNames.size() > MAX_LOGICAL_INDEXES) {
                throw new IllegalArgumentException(
                        "Lance logical index count exceeds limit " + MAX_LOGICAL_INDEXES);
            }
        }

        List<IndexDescription> descriptions = new ArrayList<>(userIndexNames.size());
        for (String name : userIndexNames) {
            IndexCriteria criteria = new IndexCriteria.Builder().hasName(name).build();
            List<IndexDescription> matching = dataset.describeIndices(criteria);
            if (matching == null) {
                throw new IllegalArgumentException("Lance index descriptions must not be null");
            }
            // A criteria query is an exact lookup; any other cardinality is inconsistent metadata.
            if (matching.size() != 1) {
                throw new IllegalArgumentException(
                        "Lance index criteria must return exactly one description");
            }
            IndexDescription description = matching.get(0);
            if (description == null) {
                throw new IllegalArgumentException(
                        "Lance logical index description must not be null");
            }
            String describedName = requireExternalString(
                    description.getName(), "Lance logical index name");
            if (!name.equals(describedName)) {
                throw new IllegalArgumentException(
                        "Lance index description name does not match requested name");
            }
            descriptions.add(description);
        }
        return descriptions;
    }

    static Map<Integer, String> buildFieldNamesById(List<LanceField> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("Lance schema fields must not be null");
        }
        if (fields.size() > MAX_SCHEMA_FIELDS) {
            throw new IllegalArgumentException(
                    "Lance schema field count exceeds limit " + MAX_SCHEMA_FIELDS);
        }
        Map<Integer, String> fieldNames = new HashMap<>();
        SchemaTraversalState traversalState = new SchemaTraversalState();
        for (LanceField field : fields) {
            collectFieldNames(field, "", 1, fieldNames, traversalState);
        }
        return fieldNames;
    }

    private static void collectFieldNames(LanceField field, String parentPath, int depth,
            Map<Integer, String> fieldNames, SchemaTraversalState traversalState) {
        if (depth > MAX_SCHEMA_DEPTH) {
            throw new IllegalArgumentException(
                    "Lance schema depth exceeds limit " + MAX_SCHEMA_DEPTH);
        }
        if (field == null) {
            throw new IllegalArgumentException("Lance schema field must not be null");
        }
        ++traversalState.fieldCount;
        if (traversalState.fieldCount > MAX_SCHEMA_FIELDS) {
            throw new IllegalArgumentException(
                    "Lance schema field count exceeds limit " + MAX_SCHEMA_FIELDS);
        }
        String segment = formatFieldPathSegment(
                requireExternalString(field.getName(), "Lance schema field name"));
        String path = requireExternalString(
                parentPath.isEmpty() ? segment : parentPath + "." + segment,
                "Lance schema field path");
        if (fieldNames.put(field.getId(), path) != null) {
            throw new IllegalArgumentException("Duplicate Lance schema field id " + field.getId());
        }
        List<LanceField> children = field.getChildren();
        if (children == null) {
            throw new IllegalArgumentException("Lance schema field children must not be null");
        }
        for (LanceField child : children) {
            collectFieldNames(child, path, depth + 1, fieldNames, traversalState);
        }
    }

    private static String formatFieldPathSegment(String segment) {
        boolean requiresQuoting = segment.codePoints()
                .anyMatch(codePoint -> !Character.isLetterOrDigit(codePoint)
                        && codePoint != '_');
        if (requiresQuoting) {
            return "`" + segment.replace("`", "``") + "`";
        }
        return segment;
    }

    /** Converts SDK descriptions into bounded immutable Java-only metadata. */
    static List<LanceLogicalIndex> normalize(List<IndexDescription> descriptions,
            Map<Integer, String> fieldNames) {
        if (descriptions == null) {
            throw new IllegalArgumentException("Lance index descriptions must not be null");
        }
        if (descriptions.size() > MAX_LOGICAL_INDEXES) {
            throw new IllegalArgumentException(
                    "Lance logical index count exceeds limit " + MAX_LOGICAL_INDEXES);
        }
        if (fieldNames == null) {
            throw new IllegalArgumentException("Lance field names must not be null");
        }

        List<IndexedLogicalIndex> normalized = new ArrayList<>(descriptions.size());
        int aggregateColumnNamesBytes = 0;
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
            for (Integer fieldId : fieldIds) {
                if (fieldId == null) {
                    throw new IllegalArgumentException(
                            "Lance logical index field ID must not be null");
                }
                if (!uniqueFieldIds.add(fieldId)) {
                    throw new IllegalArgumentException(
                            "Duplicate field id " + fieldId + " in Lance logical index metadata");
                }
                if (!fieldNames.containsKey(fieldId)) {
                    throw new IllegalArgumentException(
                            "Lance index metadata references unknown field id " + fieldId);
                }
                String column = requireExternalString(
                        fieldNames.get(fieldId), "Lance logical index column name");
                aggregateColumnNamesBytes += utf8Length(column);
                if (aggregateColumnNamesBytes > MAX_COLUMN_NAMES_BYTES) {
                    throw new IllegalArgumentException(
                            "Lance logical index column names exceed aggregate limit "
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
        copyPrimitiveProperties(
                object, TOP_LEVEL_PROPERTY_ALLOWLIST, allowedProperties, indexName);
        copyNestedProperties(
                object, "compression", COMPRESSION_PROPERTY_ALLOWLIST,
                allowedProperties, indexName);
        copyNestedProperties(
                object, "hnsw", HNSW_PROPERTY_ALLOWLIST, allowedProperties, indexName);

        String properties = GsonUtils.GSON.toJson(allowedProperties);
        if (utf8Length(properties) > MAX_PROPERTIES_BYTES) {
            throw new IllegalArgumentException(
                    "Lance index properties exceed limit "
                            + MAX_PROPERTIES_BYTES + " UTF-8 bytes");
        }
        return properties;
    }

    private static void copyNestedProperties(JsonObject source, String propertyName,
            Set<String> allowlist, Map<String, JsonElement> target, String indexName) {
        JsonElement nested = source.get(propertyName);
        if (nested == null || nested.isJsonNull()) {
            return;
        }
        if (!nested.isJsonObject()) {
            throw invalidDetailsJson(indexName);
        }

        TreeMap<String, JsonElement> allowedNested = new TreeMap<>();
        copyPrimitiveProperties(nested.getAsJsonObject(), allowlist, allowedNested, indexName);
        if (allowedNested.isEmpty()) {
            return;
        }
        JsonObject normalizedNested = new JsonObject();
        for (Map.Entry<String, JsonElement> entry : allowedNested.entrySet()) {
            normalizedNested.add(entry.getKey(), entry.getValue());
        }
        target.put(propertyName, normalizedNested);
    }

    private static void copyPrimitiveProperties(JsonObject source, Set<String> allowlist,
            Map<String, JsonElement> target, String indexName) {
        for (Map.Entry<String, JsonElement> entry : source.entrySet()) {
            if (!allowlist.contains(entry.getKey())) {
                continue;
            }
            JsonElement value = entry.getValue();
            if (value == null || value.isJsonNull()) {
                continue;
            }
            if (!value.isJsonPrimitive()) {
                throw invalidDetailsJson(indexName);
            }
            target.put(entry.getKey(), value);
        }
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

    private static final class SchemaTraversalState {
        private int fieldCount;
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

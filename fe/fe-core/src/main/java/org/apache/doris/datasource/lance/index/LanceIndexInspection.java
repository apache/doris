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

package org.apache.doris.datasource.lance.index;

import org.apache.doris.datasource.lance.LanceIndexAdmissionSnapshot;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.apache.commons.lang3.StringUtils;
import org.lance.Dataset;
import org.lance.index.Index;
import org.lance.index.IndexDescription;
import org.lance.index.IndexType;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Builds display metadata for SHOW INDEX and lance_index_entries() from a caller-owned Dataset.
 * Query planning reads index segments through LanceDatasetIndexDiscovery instead.
 */
public final class LanceIndexInspection {
    public static final int MAX_LOGICAL_INDEXES = LanceDatasetIndexDiscovery.MAX_LOGICAL_INDEXES;
    public static final int MAX_PHYSICAL_INDEX_ENTRIES = LanceDatasetIndexDiscovery.MAX_PHYSICAL_INDEX_ENTRIES;
    public static final int MAX_EXTERNAL_STRING_BYTES = LanceDatasetIndexDiscovery.MAX_EXTERNAL_STRING_BYTES;
    private static final Set<String> SYSTEM_INDEX_NAMES = LanceDatasetIndexDiscovery.SYSTEM_INDEX_NAMES;
    private static final int MAX_COLUMNS_PER_INDEX = 64;
    private static final int MAX_COLUMN_NAMES_BYTES = 16 * 1024;
    public static final int MAX_SCHEMA_FIELDS = MAX_LOGICAL_INDEXES * MAX_COLUMNS_PER_INDEX;
    private static final int MAX_SCHEMA_DEPTH = 64;
    private static final int MAX_PROPERTIES_BYTES = 400;

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

    private LanceIndexInspection() {
    }

    /** Resolves logical indexes and their column names from the same fixed snapshot. */
    public static List<LanceShowIndexInfo> readIndexesForShow(Dataset dataset) {
        Map<Integer, String> fieldNames = buildFieldNamesById(dataset.getLanceSchema().fields());
        return normalize(LanceDatasetIndexDiscovery.describeUserIndexes(dataset), fieldNames);
    }

    /** Converts the snapshot's raw index list into sorted immutable physical entries. */
    public static List<LancePhysicalIndexEntry> readPhysicalEntries(Dataset dataset) {
        List<Index> indexes = dataset.getIndexes();
        if (indexes == null) {
            throw new IllegalArgumentException(
                    "Lance physical index entries must not be null: Dataset.getIndexes() returned null");
        }
        // Bound the raw provider response before any filtering so a flood of system
        // entries still fails closed instead of consuming unbounded memory.
        if (indexes.size() > MAX_PHYSICAL_INDEX_ENTRIES) {
            throw new IllegalArgumentException(
                    "Lance physical index entry count exceeds limit "
                            + MAX_PHYSICAL_INDEX_ENTRIES + " (actual=" + indexes.size() + ")");
        }
        List<LancePhysicalIndexEntry> entries = new ArrayList<>(indexes.size());
        Set<String> seenUuids = new HashSet<>();
        for (int position = 0; position < indexes.size(); ++position) {
            Index index = indexes.get(position);
            if (index == null) {
                throw new IllegalArgumentException(
                        "Lance physical index entry must not be null (entry_position=" + position
                                + ", total_entries=" + indexes.size() + ", positions are zero-based)");
            }
            String name = requireExternalString(
                    index.name(), "Lance physical index entry name at position " + position);
            UUID uuid = index.uuid();
            if (uuid == null) {
                throw new IllegalArgumentException(
                        "Lance physical index entry uuid must not be null for index '" + name + "'");
            }
            long datasetVersion = index.datasetVersion();
            if (datasetVersion <= 0) {
                throw new IllegalArgumentException(
                        "Lance physical index entry dataset version must be positive for index '"
                                + name + "' (uuid=" + uuid + ", actual=" + datasetVersion + ")");
            }
            String uuidString = uuid.toString();
            if (!seenUuids.add(uuidString)) {
                throw new IllegalArgumentException(
                        "Duplicate Lance physical index entry uuid '" + uuidString
                                + "' at index '" + name + "'; each physical segment must have a unique UUID");
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

    /** Reads admission metadata from one pinned snapshot without row or index statistics calls. */
    public static LanceIndexAdmissionSnapshot readAdmissionSnapshot(Dataset dataset, String datasetUri) {
        long datasetVersion = dataset.version();
        List<LanceField> topLevelFields = dataset.getLanceSchema().fields();
        List<LanceShowIndexInfo> logicalIndexes = normalize(
                LanceDatasetIndexDiscovery.describeUserIndexes(dataset), buildFieldNamesById(topLevelFields));
        List<LanceIndexAdmissionSnapshot.PhysicalIndexInfo> physicalIndexes = collectPhysicalIndexInfos(dataset);
        return new LanceIndexAdmissionSnapshot(
                datasetVersion, datasetUri, logicalIndexes, physicalIndexes, topLevelFields);
    }

    /**
     * Collects the physical entries of the opened snapshot, applying the same defenses as the
     * logical path: the raw list is bounded before per-entry validation, system entries are
     * validated then filtered out, and duplicate UUID ownership fails closed.
     */
    static List<LanceIndexAdmissionSnapshot.PhysicalIndexInfo> collectPhysicalIndexInfos(
            Dataset dataset) {
        List<Index> indexes = dataset.getIndexes();
        if (indexes == null) {
            throw new IllegalArgumentException("Lance physical index entries must not be null");
        }
        if (indexes.size() > MAX_PHYSICAL_INDEX_ENTRIES) {
            throw new IllegalArgumentException(
                    "Lance physical index entry count exceeds limit "
                            + MAX_PHYSICAL_INDEX_ENTRIES);
        }

        List<LanceIndexAdmissionSnapshot.PhysicalIndexInfo> entries = new ArrayList<>(indexes.size());
        Set<String> uuids = new HashSet<>();
        for (Index index : indexes) {
            if (index == null) {
                throw new IllegalArgumentException("Lance physical index entry must not be null");
            }
            String name = requireExternalString(index.name(), "Lance physical index name");
            if (index.uuid() == null) {
                throw new IllegalArgumentException("Lance physical index uuid must not be null");
            }
            String uuid = index.uuid().toString();
            long indexDatasetVersion = index.datasetVersion();
            if (indexDatasetVersion <= 0) {
                throw new IllegalArgumentException(
                        "Lance physical index dataset version must be positive");
            }
            IndexType indexType = index.indexType();
            if (indexType == null) {
                throw new IllegalArgumentException("Lance physical index type must not be null");
            }
            // UUID ownership is checked before the system-entry filter, so a UUID shared
            // between a system entry and a user entry cannot hide from the all-or-error read.
            if (!uuids.add(uuid)) {
                throw new IllegalArgumentException("Duplicate Lance physical index uuid");
            }
            if (SYSTEM_INDEX_NAMES.contains(name)) {
                continue;
            }
            entries.add(new LanceIndexAdmissionSnapshot.PhysicalIndexInfo(
                    name, uuid, indexDatasetVersion, indexType.name()));
        }
        entries.sort(Comparator.comparing(LanceIndexAdmissionSnapshot.PhysicalIndexInfo::getName)
                .thenComparing(LanceIndexAdmissionSnapshot.PhysicalIndexInfo::getUuid));
        return Collections.unmodifiableList(entries);
    }

    static Map<Integer, String> buildFieldNamesById(List<LanceField> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("Lance schema fields must not be null: "
                    + "expected the schema field list for resolving index column IDs");
        }
        if (fields.size() > MAX_SCHEMA_FIELDS) {
            throw new IllegalArgumentException(
                    "Lance schema field count exceeds limit " + MAX_SCHEMA_FIELDS
                            + " (actual=" + fields.size() + ")");
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
                    "Lance schema depth exceeds limit " + MAX_SCHEMA_DEPTH
                            + " (observed=" + depth + ", parent_path='" + parentPath + "')");
        }
        if (field == null) {
            throw new IllegalArgumentException("Lance schema field must not be null "
                    + "(parent_path='" + parentPath + "', depth=" + depth + ")");
        }
        ++traversalState.fieldCount;
        if (traversalState.fieldCount > MAX_SCHEMA_FIELDS) {
            throw new IllegalArgumentException(
                    "Lance schema field count exceeds limit " + MAX_SCHEMA_FIELDS
                            + " (observed=" + traversalState.fieldCount + ", parent_path='"
                            + parentPath + "'); the limit includes nested fields");
        }
        String segment = formatFieldPathSegment(
                requireExternalString(field.getName(), "Lance schema field name for field ID " + field.getId()));
        String path = requireExternalString(
                parentPath.isEmpty() ? segment : parentPath + "." + segment,
                "Lance schema field path for field ID " + field.getId());
        String previousPath = fieldNames.put(field.getId(), path);
        if (previousPath != null) {
            throw new IllegalArgumentException("Duplicate Lance schema field id " + field.getId()
                    + " (first_path='" + previousPath + "', duplicate_path='" + path + "')");
        }
        List<LanceField> children = field.getChildren();
        if (children == null) {
            throw new IllegalArgumentException("Lance schema field children must not be null for field '"
                    + path + "' (field_id=" + field.getId() + "); expected an empty list for a leaf field");
        }
        for (LanceField child : children) {
            collectFieldNames(child, path, depth + 1, fieldNames, traversalState);
        }
    }

    public static String formatFieldPathSegment(String segment) {
        boolean requiresQuoting = segment.codePoints()
                .anyMatch(codePoint -> !Character.isLetterOrDigit(codePoint)
                        && codePoint != '_');
        if (requiresQuoting) {
            return "`" + segment.replace("`", "``") + "`";
        }
        return segment;
    }

    /** Converts SDK descriptions into bounded immutable Java-only metadata. */
    static List<LanceShowIndexInfo> normalize(List<IndexDescription> descriptions,
            Map<Integer, String> fieldNames) {
        if (descriptions == null) {
            throw new IllegalArgumentException("Lance index descriptions must not be null: "
                    + "expected a list of logical index descriptions to normalize");
        }
        if (descriptions.size() > MAX_LOGICAL_INDEXES) {
            throw new IllegalArgumentException(
                    "Lance logical index count exceeds limit " + MAX_LOGICAL_INDEXES
                            + " (actual=" + descriptions.size() + ")");
        }
        if (fieldNames == null) {
            throw new IllegalArgumentException("Lance field names must not be null: "
                    + "expected a mapping from schema field IDs to column paths");
        }

        List<LanceShowIndexInfo> normalized = new ArrayList<>(descriptions.size());
        Set<String> logicalIndexNames = new HashSet<>();
        int aggregateColumnNamesBytes = 0;
        for (int position = 0; position < descriptions.size(); ++position) {
            IndexDescription description = descriptions.get(position);
            if (description == null) {
                throw new IllegalArgumentException("Lance logical index description must not be null "
                        + "(description_position=" + position + ", total_descriptions="
                        + descriptions.size() + ", positions are zero-based)");
            }

            String name = requireExternalString(
                    description.getName(), "Lance logical index name at description position " + position);
            String indexType = requireExternalString(
                    description.getIndexType(), "Lance logical index type for index '" + name + "'");
            List<Integer> fieldIds = description.getFieldIds();
            if (fieldIds == null || fieldIds.isEmpty()) {
                throw new IllegalArgumentException(
                        "Lance logical index field IDs must not be null or empty for index '" + name
                                + "' (actual=" + (fieldIds == null ? "null" : "empty list") + ")");
            }
            if (fieldIds.size() > MAX_COLUMNS_PER_INDEX) {
                throw new IllegalArgumentException(
                        "Lance logical index column count exceeds limit " + MAX_COLUMNS_PER_INDEX
                                + " (actual=" + fieldIds.size() + ", index='" + name + "')");
            }

            List<String> columns = new ArrayList<>(fieldIds.size());
            Set<Integer> uniqueFieldIds = new HashSet<>();
            for (Integer fieldId : fieldIds) {
                if (fieldId == null) {
                    throw new IllegalArgumentException(
                            "Lance logical index field ID must not be null for index '" + name + "'");
                }
                if (!uniqueFieldIds.add(fieldId)) {
                    throw new IllegalArgumentException(
                            "Duplicate field id " + fieldId + " in Lance logical index metadata for index '"
                                    + name + "'; each indexed column must appear only once");
                }
                if (!fieldNames.containsKey(fieldId)) {
                    throw new IllegalArgumentException(
                            "Lance index metadata references unknown field id " + fieldId
                                    + " for index '" + name + "'; this ID is absent from the dataset schema");
                }
                String column = requireExternalString(
                        fieldNames.get(fieldId), "Lance logical index column name for index '" + name
                                + "', field ID " + fieldId);
                aggregateColumnNamesBytes += utf8Length(column);
                if (aggregateColumnNamesBytes > MAX_COLUMN_NAMES_BYTES) {
                    throw new IllegalArgumentException(
                            "Lance logical index column names exceed aggregate limit "
                                    + MAX_COLUMN_NAMES_BYTES + " UTF-8 bytes (observed="
                                    + aggregateColumnNamesBytes + ", index='" + name + "', column='"
                                    + column + "'); the limit covers column names across all logical indexes");
                }
                columns.add(column);
            }

            String properties = normalizeProperties(name, description.getDetailsJson());
            LanceShowIndexInfo index = new LanceShowIndexInfo(name, columns, indexType, properties);

            if (!logicalIndexNames.add(name)) {
                throw new IllegalArgumentException(
                        "Duplicate Lance logical index name '" + name
                                + "'; same-name physical segments must belong to one logical description");
            }
            normalized.add(index);
        }
        normalized.sort(Comparator.comparing(LanceShowIndexInfo::getName));
        return Collections.unmodifiableList(normalized);
    }

    private static String normalizeProperties(String indexName, String detailsJson) {
        if (detailsJson == null) {
            return "{}";
        }
        if (utf8Length(detailsJson) > MAX_EXTERNAL_STRING_BYTES) {
            throw new IllegalArgumentException(
                    "Lance index details JSON exceeds limit "
                            + MAX_EXTERNAL_STRING_BYTES + " UTF-8 bytes (actual="
                            + utf8Length(detailsJson) + ", index='" + indexName + "')");
        }
        if (StringUtils.isBlank(detailsJson)) {
            return "{}";
        }

        JsonElement parsed;
        try (JsonReader reader = new JsonReader(new StringReader(detailsJson))) {
            reader.setLenient(false);
            parsed = GsonUtils.GSON.getAdapter(JsonElement.class).read(reader);
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                throw invalidDetailsJson(indexName, "expected one JSON object without trailing content");
            }
        } catch (IOException | RuntimeException e) {
            // Parser exceptions may contain raw JSON (including credentials). Report the
            // expected format without copying the parser message or retaining its cause.
            throw invalidDetailsJson(indexName, "expected one well-formed JSON object without trailing content");
        }
        if (!parsed.isJsonObject()) {
            throw invalidDetailsJson(indexName, "expected a JSON object at the root");
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
                            + MAX_PROPERTIES_BYTES + " UTF-8 bytes (actual="
                            + utf8Length(properties) + ", index='" + indexName + "')");
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
            throw invalidDetailsJson(indexName, "property '" + propertyName + "' must be a JSON object");
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
                throw invalidDetailsJson(indexName, "property '" + entry.getKey()
                        + "' must be a string, number or boolean");
            }
            target.put(entry.getKey(), value);
        }
    }

    private static IllegalArgumentException invalidDetailsJson(String indexName, String reason) {
        return new IllegalArgumentException(
                "Invalid Lance index details JSON for '" + indexName + "': " + reason);
    }

    private static String requireExternalString(String value, String valueType) {
        return LanceDatasetIndexDiscovery.requireExternalString(value, valueType);
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static final class SchemaTraversalState {
        private int fieldCount;
    }

}

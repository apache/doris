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

package org.apache.doris.connector.es;

import org.apache.doris.connector.spi.DorisConnectorException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * ES mapping parsing and field resolution utilities.
 * Combines logic from fe-core's EsUtil (mapping parsing) and MappingPhase (field resolution).
 * No fe-core dependencies.
 */
public final class EsMappingUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final Set<String> DEFAULT_DOCVALUE_DISABLED_FIELDS =
            new HashSet<>(Arrays.asList("text", "annotated_text", "match_only_text"));

    private EsMappingUtils() {
    }

    /**
     * Parse the top-level "mappings" node from an index mapping JSON response.
     */
    public static ObjectNode getMapping(String indexMapping) {
        try {
            ObjectNode jsonNodes = (ObjectNode) MAPPER.readTree(indexMapping);
            // Alias mapping response order is unspecified. A bare/typed empty backing can appear
            // first; select a populated schema without discarding its _meta array annotations.
            for (JsonNode index : jsonNodes) {
                ObjectNode mapping = (ObjectNode) index.get("mappings");
                ObjectNode root = getRootSchema(mapping.deepCopy(), null, new ArrayList<>(), true);
                if (root.path("properties").size() > 0) {
                    return mapping;
                }
            }
            // Preserve the existing diagnostic when no backing index provides a usable schema.
            return (ObjectNode) jsonNodes.iterator().next().get("mappings");
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to parse ES mapping: " + e.getMessage());
        }
    }

    /**
     * Extract the root schema from mapping node, handling ES 5.x/6.x/7.x/8.x differences.
     */
    public static ObjectNode getRootSchema(ObjectNode mappings, String mappingType,
            List<String> arrayFields) {
        return getRootSchema(mappings, mappingType, arrayFields, false);
    }

    private static ObjectNode getRootSchema(ObjectNode mappings, String mappingType,
            List<String> arrayFields, boolean allowEmpty) {
        if (mappingType == null) {
            checkNonPropertiesFields(mappings, arrayFields, allowEmpty);
            // Alias safety analysis must accept backing indices that have not acquired a mapping yet.
            if (mappings.isEmpty()) {
                return mappings;
            }
            String firstType = mappings.fieldNames().next();
            if (StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                    mappings.fieldNames(), Spliterator.ORDERED), false)
                    .anyMatch(s -> s.contains("properties"))) {
                return mappings;
            } else {
                ObjectNode firstData = (ObjectNode) mappings.get(firstType);
                checkNonPropertiesFields(firstData, arrayFields, allowEmpty);
                return firstData;
            }
        } else {
            if (mappings.has(mappingType)) {
                ObjectNode jsonData = (ObjectNode) mappings.get(mappingType);
                checkNonPropertiesFields(jsonData, arrayFields, allowEmpty);
                return jsonData;
            }
            return getRootSchema(mappings, null, arrayFields, allowEmpty);
        }
    }

    /**
     * Get mapping properties ObjectNode for a given index.
     */
    public static ObjectNode getMappingProps(String sourceIndex, String indexMapping,
            String mappingType) {
        ObjectNode mappings = getMapping(indexMapping);
        ObjectNode rootSchema = getRootSchema(mappings, mappingType, new ArrayList<>());
        ObjectNode properties = (ObjectNode) rootSchema.get("properties");
        if (properties == null) {
            throw new DorisConnectorException(
                    "index[" + sourceIndex + "] type[" + mappingType
                            + "] mapping not found for the ES Cluster");
        }
        return properties;
    }

    /**
     * Resolve field contexts (keyword sniff, doc_value, date compat) from mapping properties.
     *
     * @param columnNames column names to resolve
     * @param sourceIndex index name for error messages
     * @param indexMapping raw mapping JSON
     * @param mappingType mapping type (null for ES 7+)
     * @return populated EsFieldContext
     */
    public static EsFieldContext resolveFieldContext(List<String> columnNames,
            String sourceIndex, String indexMapping, String mappingType) {
        EsFieldContext fieldContext = new EsFieldContext();
        ObjectNode properties = getMappingProps(sourceIndex, indexMapping, mappingType);

        // When columnNames is empty, resolve all fields from the mapping.
        // This ensures column2typeMap, keyword sniff, docvalue, and date compat contexts
        // are populated even when specific column handles are not provided.
        Iterable<String> fieldsToResolve;
        if (columnNames == null || columnNames.isEmpty()) {
            List<String> allFields = new ArrayList<>();
            Iterator<String> iter = properties.fieldNames();
            while (iter.hasNext()) {
                allFields.add(iter.next());
            }
            fieldsToResolve = allFields;
        } else {
            fieldsToResolve = columnNames;
        }

        for (String colName : fieldsToResolve) {
            if ("_id".equals(colName)) {
                continue;
            }
            if (!properties.has(colName)) {
                throw new DorisConnectorException(
                        "index[" + sourceIndex + "] mapping[" + indexMapping
                                + "] not found column " + colName + " for the ES Cluster");
            }
            ObjectNode fieldObject = (ObjectNode) properties.get(colName);
            if (!fieldObject.has("type")) {
                fieldContext.getColumn2typeMap().put(colName, "object");
                continue;
            }
            String fieldType = fieldObject.get("type").asText();
            fieldContext.getColumn2typeMap().put(colName, fieldType);
            resolveDateFields(fieldContext, fieldObject, colName, fieldType);
            resolveDocValuesFields(fieldContext, fieldObject, colName, fieldType);
        }
        resolveSourceValueSafety(fieldContext, indexMapping, mappingType, properties);
        return fieldContext;
    }

    private static void resolveSourceValueSafety(EsFieldContext context, String indexMapping, String mappingType,
            ObjectNode schemaProperties) {
        try {
            // Mapping-induced source/index differences on any alias backing index must prevent
            // filtering or replacing source values through that path.
            List<ObjectNode> roots = new ArrayList<>();
            Set<String> columns = new HashSet<>(context.getColumn2typeMap().keySet());
            for (JsonNode index : MAPPER.readTree(indexMapping)) {
                ObjectNode root = getRootSchema((ObjectNode) index.get("mappings"), mappingType,
                        new ArrayList<>(), true);
                roots.add(root);
            }
            // Use the same populated schema as inference, including unprojected filter columns.
            schemaProperties.fieldNames().forEachRemaining(columns::add);
            // Filters can reference columns absent from the projection. Resolve their query paths
            // from the exposed schema too; the projection still controls docvalue read eligibility.
            for (String column : columns) {
                ObjectNode field = (ObjectNode) schemaProperties.get(column);
                resolveKeywordFields(context, field, column, field.path("type").asText());
            }
            Set<String> unsafeDocValues = new HashSet<>();
            for (ObjectNode root : roots) {
                JsonNode properties = root.path("properties");
                Set<String> copyTargets = new HashSet<>();
                collectCopyTargets(properties, copyTargets);
                for (String column : columns) {
                    JsonNode field = properties.path(column);
                    JsonNode queryField = fieldAtPath(field, column, context.getFetchFieldsContext().get(column));
                    JsonNode readField = fieldAtPath(field, column, context.getDocValueFieldsContext().get(column));
                    boolean sourceChanged = filtersSourceColumn(root.path("_source"), column)
                            || root.path("runtime").has(column) || copyTargets.contains(column);
                    // ES mappings do not distinguish arrays from scalars. Even a short equality can
                    // match one indexed array element while Doris reads the entire source JSON string.
                    if (sourceChanged || changesSourceValue(field) || changesSourceValue(queryField)
                            || "text".equals(queryField.path("type").asText()) || !isSearchable(queryField)) {
                        context.getSourceValueUnsafeFields().add(column);
                    }
                    // These mappings round indexed numbers while _source retains the original value.
                    // Keep docvalues eligible: only a final source read makes their predicates unsafe.
                    if (roundsNumericSourceValue(field) || roundsNumericSourceValue(queryField)) {
                        context.getSourceModeUnsafeFields().add(column);
                    }
                    // Query and docvalue paths may select different multi-fields. An unused sibling's
                    // normalizer must not invalidate either path; only the chosen read value matters.
                    if (sourceChanged || changesSourceValue(field) || changesSourceValue(readField)
                            || !readField.path("doc_values").asBoolean(true)
                            || DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(readField.path("type").asText())) {
                        // One backing without this docvalue path would return missing values for its hits.
                        unsafeDocValues.add(column);
                    }
                }
            }
            unsafeDocValues.forEach(context.getDocValueFieldsContext()::remove);
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to parse ES mapping: " + e.getMessage());
        }
    }

    private static JsonNode fieldAtPath(JsonNode field, String column, String path) {
        return path == null || column.equals(path) ? field
                : field.path("fields").path(path.substring(column.length() + 1));
    }

    private static boolean changesSourceValue(JsonNode field) {
        return field.isMissingNode() || field.has("ignore_above") || field.hasNonNull("normalizer")
                || field.hasNonNull("null_value") || field.path("ignore_malformed").asBoolean(false);
    }

    private static boolean roundsNumericSourceValue(JsonNode field) {
        String type = field.path("type").asText();
        return "scaled_float".equals(type) || "half_float".equals(type);
    }

    private static boolean isSearchable(JsonNode field) {
        // Disabling the inverted index does not change the value. ES can still search keyword
        // docvalues; only a field with neither representation needs a local predicate.
        return field.path("index").asBoolean(true)
                || ("keyword".equals(field.path("type").asText()) && field.path("doc_values").asBoolean(true));
    }

    private static boolean filtersSourceColumn(JsonNode source, String column) {
        JsonNode includes = source.path("includes");
        return !source.path("enabled").asBoolean(true)
                || (includes.size() > 0 && !matchesSourcePattern(includes, column))
                || matchesSourcePattern(source.path("excludes"), column);
    }

    private static boolean matchesSourcePattern(JsonNode patterns, String column) {
        for (JsonNode pattern : patterns) {
            // Source filters use '*' globs, not SQL LIKE or regular expressions. Treat every
            // other character literally and avoid regex backtracking on mapping-supplied patterns.
            String[] parts = pattern.asText().split("\\*", -1);
            if (parts.length == 1) {
                if (column.equals(parts[0])) {
                    return true;
                }
                continue;
            }
            if (!column.startsWith(parts[0])) {
                continue;
            }
            int offset = parts[0].length();
            for (int i = 1; i < parts.length - 1 && offset >= 0; i++) {
                int found = column.indexOf(parts[i], offset);
                offset = found < 0 ? -1 : found + parts[i].length();
            }
            String suffix = parts[parts.length - 1];
            if (offset >= 0 && offset + suffix.length() <= column.length() && column.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    private static void collectCopyTargets(JsonNode properties, Set<String> targets) {
        for (JsonNode field : properties) {
            JsonNode copyTo = field.path("copy_to");
            if (copyTo.isTextual()) {
                targets.add(copyTo.asText());
            } else if (copyTo.isArray()) {
                copyTo.forEach(target -> targets.add(target.asText()));
            }
            collectCopyTargets(field.path("properties"), targets);
            collectCopyTargets(field.path("fields"), targets);
        }
    }

    private static void resolveDateFields(EsFieldContext context, ObjectNode fieldObject,
            String colName, String fieldType) {
        if ("date".equals(fieldType)) {
            if (!fieldObject.has("format")
                    || "strict_date_optional_time".equals(fieldObject.get("format").asText())) {
                context.getNeedCompatDateFields().add(colName);
            }
        }
    }

    private static void resolveKeywordFields(EsFieldContext context, ObjectNode fieldObject,
            String colName, String fieldType) {
        if ("text".equals(fieldType)) {
            JsonNode fieldsObject = fieldObject.get("fields");
            if (fieldsObject != null) {
                Iterator<String> fieldNames = fieldsObject.fieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    ObjectNode innerTypeObject = (ObjectNode) fieldsObject.get(fieldName);
                    if ("keyword".equals(innerTypeObject.get("type").asText())) {
                        context.getFetchFieldsContext()
                                .put(colName, colName + "." + fieldName);
                    }
                }
            }
        }
    }

    private static void resolveDocValuesFields(EsFieldContext context, ObjectNode fieldObject,
            String colName, String fieldType) {
        String docValueField = null;
        if (DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(fieldType)) {
            JsonNode fieldsObject = fieldObject.get("fields");
            if (fieldsObject != null) {
                Iterator<String> fieldNames = fieldsObject.fieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    ObjectNode innerTypeObject = (ObjectNode) fieldsObject.get(fieldName);
                    if (DEFAULT_DOCVALUE_DISABLED_FIELDS
                            .contains(innerTypeObject.get("type").asText())) {
                        continue;
                    }
                    if (innerTypeObject.has("doc_values")) {
                        boolean docValue = innerTypeObject.get("doc_values").asBoolean();
                        if (docValue) {
                            // A multi-field has its own path; the parent text has no docvalues.
                            docValueField = colName + "." + fieldName;
                        }
                    } else if (innerTypeObject.has("ignore_above")) {
                        // keyword with ignore_above — cannot rely on doc_values
                    } else {
                        docValueField = colName + "." + fieldName;
                    }
                }
            }
        } else {
            if (fieldObject.has("doc_values")) {
                boolean docValue = fieldObject.get("doc_values").asBoolean();
                if (!docValue) {
                    return;
                }
            } else if (fieldType == null || "nested".equals(fieldType)) {
                return;
            } else if (fieldObject.has("ignore_above")) {
                return;
            }
            docValueField = colName;
        }
        if (docValueField != null && !docValueField.isEmpty()) {
            context.getDocValueFieldsContext().put(colName, docValueField);
        }
    }

    private static void checkNonPropertiesFields(ObjectNode mappings,
            List<String> arrayFields, boolean allowEmpty) {
        JsonNode metaNode = mappings.remove("_meta");
        if (metaNode != null) {
            JsonNode dorisMeta = metaNode.get("doris");
            if (dorisMeta != null) {
                JsonNode arrayNode = dorisMeta.get("array_fields");
                if (arrayNode != null) {
                    for (JsonNode jsonNode : arrayNode) {
                        arrayFields.add(jsonNode.asText());
                    }
                }
            }
        }
        mappings.remove("dynamic_templates");
        mappings.remove("dynamic");
        mappings.remove("_default_");
        if (!allowEmpty && mappings.isEmpty()) {
            throw new DorisConnectorException(
                    "Do not support index without explicit mapping.");
        }
    }
}

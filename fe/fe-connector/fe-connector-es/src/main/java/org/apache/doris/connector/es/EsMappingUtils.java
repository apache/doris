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

import org.apache.doris.connector.api.DorisConnectorException;

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
        if (mappingType == null) {
            checkNonPropertiesFields(mappings, arrayFields);
            String firstType = mappings.fieldNames().next();
            if (StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                    mappings.fieldNames(), Spliterator.ORDERED), false)
                    .anyMatch(s -> s.contains("properties"))) {
                return mappings;
            } else {
                ObjectNode firstData = (ObjectNode) mappings.get(firstType);
                checkNonPropertiesFields(firstData, arrayFields);
                return firstData;
            }
        } else {
            if (mappings.has(mappingType)) {
                ObjectNode jsonData = (ObjectNode) mappings.get(mappingType);
                checkNonPropertiesFields(jsonData, arrayFields);
                return jsonData;
            }
            return getRootSchema(mappings, null, arrayFields);
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
            resolveKeywordFields(fieldContext, fieldObject, colName, fieldType);
            resolveDocValuesFields(fieldContext, fieldObject, colName, fieldType);
        }
        return fieldContext;
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
                            docValueField = colName;
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
            List<String> arrayFields) {
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
        if (mappings.isEmpty()) {
            throw new DorisConnectorException(
                    "Do not support index without explicit mapping.");
        }
    }
}

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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Maps ES field types to ConnectorType.
 * Adapted from fe-core's EsUtil — uses ConnectorColumn/ConnectorType instead of Column/Type.
 */
public final class EsTypeMapping {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Set<String> DOCVALUE_DISABLED_FIELDS =
            new HashSet<>(Arrays.asList("text", "annotated_text", "match_only_text"));

    private static final List<String> ALLOW_DATE_FORMATS = Arrays.asList(
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "epoch_millis");

    private EsTypeMapping() {
    }

    /**
     * Parses ES index mapping JSON and produces a list of ConnectorColumns.
     */
    public static List<ConnectorColumn> parseMapping(String indexName, String mappingJson,
            boolean mappingEsId) {
        ObjectNode mappings = getMapping(mappingJson);
        List<String> arrayFields = new ArrayList<>();
        ObjectNode rootSchema = getRootSchema(mappings, null, arrayFields);
        return buildColumns(indexName, rootSchema, mappingEsId, arrayFields);
    }

    private static ObjectNode getMapping(String indexMapping) {
        try {
            ObjectNode jsonNodes = (ObjectNode) MAPPER.readTree(indexMapping);
            return (ObjectNode) jsonNodes.iterator().next().get("mappings");
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to parse ES mapping: " + e.getMessage());
        }
    }

    private static ObjectNode getRootSchema(ObjectNode mappings, String mappingType,
            List<String> arrayFields) {
        if (mappingType == null) {
            checkNonPropertiesFields(mappings, arrayFields);
            String firstType = mappings.fieldNames().next();
            boolean hasProperties = false;
            Iterator<String> fieldNames = mappings.fieldNames();
            while (fieldNames.hasNext()) {
                if (fieldNames.next().contains("properties")) {
                    hasProperties = true;
                    break;
                }
            }
            if (hasProperties) {
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

    private static List<ConnectorColumn> buildColumns(String indexName,
            ObjectNode rootSchema, boolean mappingEsId, List<String> arrayFields) {
        List<ConnectorColumn> columns = new ArrayList<>();
        if (mappingEsId) {
            columns.add(new ConnectorColumn("_id",
                    new ConnectorType("VARCHAR", 255, -1), null, true, null, true));
        }
        ObjectNode properties = (ObjectNode) rootSchema.get("properties");
        if (properties == null) {
            throw new DorisConnectorException(
                    "index[" + indexName + "] mapping not found for the ES Cluster");
        }
        Iterator<String> iterator = properties.fieldNames();
        while (iterator.hasNext()) {
            String fieldName = iterator.next();
            ObjectNode fieldValue = (ObjectNode) properties.get(fieldName);
            fieldValue = replaceFieldAlias(properties, fieldValue);
            ConnectorColumn column = parseField(fieldName, fieldValue, arrayFields);
            columns.add(column);
        }
        return columns;
    }

    private static ObjectNode replaceFieldAlias(ObjectNode mappingProps,
            ObjectNode fieldValue) {
        if (!fieldValue.has("type")) {
            return fieldValue;
        }
        String typeStr = fieldValue.get("type").asText();
        if ("alias".equals(typeStr)) {
            String path = fieldValue.get("path").asText();
            if ("_id".equals(path)) {
                fieldValue.put("type", "keyword");
            } else if (mappingProps.has(path)) {
                return (ObjectNode) mappingProps.get(path);
            }
        }
        return fieldValue;
    }

    private static ConnectorColumn parseField(String fieldName, ObjectNode fieldValue,
            List<String> arrayFields) {
        ConnectorType type;
        String comment = null;

        if (fieldValue.has("type")) {
            String typeStr = fieldValue.get("type").asText();
            comment = "Elasticsearch type is " + typeStr;
            type = mapEsType(typeStr, fieldValue);
        } else {
            comment = "Elasticsearch type is object";
            type = new ConnectorType("JSONB");
        }

        if (arrayFields.contains(fieldName)) {
            type = new ConnectorType("ARRAY", -1, -1,
                    Collections.singletonList(type));
        }
        return new ConnectorColumn(fieldName, type, comment, true, null, true);
    }

    private static ConnectorType mapEsType(String esType, ObjectNode field) {
        switch (esType) {
            case "null":
                return new ConnectorType("NULL");
            case "boolean":
                return new ConnectorType("BOOLEAN");
            case "byte":
                return new ConnectorType("TINYINT");
            case "short":
                return new ConnectorType("SMALLINT");
            case "integer":
                return new ConnectorType("INT");
            case "long":
                return new ConnectorType("BIGINT");
            case "unsigned_long":
                return new ConnectorType("LARGEINT");
            case "float":
            case "half_float":
                return new ConnectorType("FLOAT");
            case "double":
            case "scaled_float":
                return new ConnectorType("DOUBLE");
            case "date":
                return parseDateType(field);
            case "keyword":
            case "text":
            case "ip":
            case "wildcard":
            case "constant_keyword":
                return new ConnectorType("STRING");
            case "object":
            case "nested":
            case "flattened":
                return new ConnectorType("JSONB");
            default:
                return new ConnectorType("UNSUPPORTED");
        }
    }

    private static ConnectorType parseDateType(ObjectNode field) {
        if (!field.has("format")) {
            return new ConnectorType("DATETIMEV2", 0, -1);
        }
        String originFormat = field.get("format").asText();
        String[] formats = originFormat.split("\\|\\|");
        boolean dateTimeFlag = false;
        boolean dateFlag = false;
        boolean bigIntFlag = false;
        for (String format : formats) {
            String trimFormat = format.trim();
            if (!ALLOW_DATE_FORMATS.contains(trimFormat)) {
                return new ConnectorType("STRING");
            }
            switch (trimFormat) {
                case "yyyy-MM-dd HH:mm:ss":
                    dateTimeFlag = true;
                    break;
                case "yyyy-MM-dd":
                    dateFlag = true;
                    break;
                default:
                    bigIntFlag = true;
            }
        }
        if (dateTimeFlag) {
            return new ConnectorType("DATETIMEV2", 0, -1);
        }
        if (dateFlag) {
            return new ConnectorType("DATEV2");
        }
        if (bigIntFlag) {
            return new ConnectorType("BIGINT");
        }
        return new ConnectorType("STRING");
    }
}

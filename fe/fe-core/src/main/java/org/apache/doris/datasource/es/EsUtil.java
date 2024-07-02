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

package org.apache.doris.datasource.es;

import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.JsonUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * Util for ES, some static method.
 **/
public class EsUtil {

    private static final Logger LOG = LogManager.getLogger(EsUtil.class);

    /**
     * Analyze partition and distributionDesc.
     **/
    public static void analyzePartitionAndDistributionDesc(PartitionDesc partitionDesc,
            DistributionDesc distributionDesc) throws AnalysisException {
        if (partitionDesc == null && distributionDesc == null) {
            return;
        }

        if (partitionDesc != null) {
            if (!(partitionDesc instanceof RangePartitionDesc)) {
                throw new AnalysisException("Elasticsearch table only permit range partition");
            }

            RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
            analyzePartitionDesc(rangePartitionDesc);
        }

        if (distributionDesc != null) {
            throw new AnalysisException("could not support distribution clause");
        }
    }

    private static void analyzePartitionDesc(RangePartitionDesc partDesc) throws AnalysisException {
        if (partDesc.getPartitionColNames() == null || partDesc.getPartitionColNames().isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }

        if (partDesc.getPartitionColNames().size() > 1) {
            throw new AnalysisException("Elasticsearch table's partition column could only be a single column");
        }
    }

    /**
     * Get boolean throw DdlException when parse error
     **/
    public static boolean getBoolean(Map<String, String> properties, String name) throws DdlException {
        String property = properties.get(name).trim();
        try {
            return Boolean.parseBoolean(property);
        } catch (Exception e) {
            throw new DdlException(String.format("fail to parse %s, %s = %s, `%s` should be like 'true' or 'false', "
                    + "value should be double quotation marks", name, name, property, name));
        }
    }

    @VisibleForTesting
    public static ObjectNode getMapping(String indexMapping) {
        ObjectNode jsonNodes = JsonUtil.parseObject(indexMapping);
        // If the indexName use alias takes the first mapping
        return (ObjectNode) jsonNodes.iterator().next().get("mappings");
    }

    @VisibleForTesting
    public static ObjectNode getRootSchema(ObjectNode mappings, String mappingType, List<String> arrayFields) {
        // Type is null in the following three cases
        // 1. Equal 6.8.x and after
        // 2. Multi-catalog auto infer
        // 3. Equal 6.8.x and before user not passed
        if (mappingType == null) {
            // remove dynamic templates, for ES 7.x and 8.x
            checkNonPropertiesFields(mappings, arrayFields);
            String firstType = mappings.fieldNames().next();
            //The first parameter may not be properties, so we need to first determine whether it is 7.x or above.
            if (StreamSupport.stream(Spliterators
                            .spliteratorUnknownSize(mappings.fieldNames(), Spliterator.ORDERED), false)
                    .anyMatch(s -> s.contains("properties"))) {
                // Equal 7.x and after
                return mappings;
            } else {
                ObjectNode firstData = (ObjectNode) mappings.get(firstType);
                // check for ES 6.x and before
                checkNonPropertiesFields(firstData, arrayFields);
                return firstData;
            }
        } else {
            if (mappings.has(mappingType)) {
                ObjectNode jsonData = (ObjectNode) mappings.get(mappingType);
                // check for ES 6.x and before
                checkNonPropertiesFields(jsonData, arrayFields);
                return jsonData;
            }
            // Compatible type error
            return getRootSchema(mappings, null, arrayFields);
        }
    }

    /**
     * Check non properties fields
     *
     * @param mappings
     */
    private static void checkNonPropertiesFields(ObjectNode mappings, List<String> arrayFields) {
        // remove `_meta` field and parse array_fields
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
        // remove `dynamic_templates` field
        mappings.remove("dynamic_templates");
        // remove `dynamic` field
        mappings.remove("dynamic");
        // remove `_default` field, we do not parse `_default_` mapping, only explicit mapping.
        // `_default` _mapping type is deprecated in 7.0 and removed in 8.0
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.17/removal-of-types.html
        mappings.remove("_default_");
        // check explicit mapping
        if (mappings.isEmpty()) {
            throw new DorisEsException("Do not support index without explicit mapping.");
        }
    }

    /**
     * Get mapping properties transform to ObjectNode.
     **/
    public static ObjectNode getMappingProps(String sourceIndex, String indexMapping, String mappingType) {
        ObjectNode mappings = getMapping(indexMapping);
        ObjectNode rootSchema = getRootSchema(mappings, mappingType, new ArrayList<>());
        ObjectNode properties = (ObjectNode) rootSchema.get("properties");
        if (properties == null) {
            throw new DorisEsException(
                    "index[" + sourceIndex + "] type[" + mappingType + "] mapping not found for the ES Cluster");
        }
        return properties;
    }

    /**
     * Generate columns from ES Cluster.
     * Add mappingEsId config in es external catalog.
     **/
    public static List<Column> genColumnsFromEs(EsRestClient client, String indexName, String mappingType,
            boolean mappingEsId) {
        String mapping = client.getMapping(indexName);
        ObjectNode mappings = getMapping(mapping);
        // Get array_fields while removing _meta property.
        List<String> arrayFields = new ArrayList<>();
        ObjectNode rootSchema = getRootSchema(mappings, mappingType, arrayFields);
        return genColumnsFromEs(indexName, mappingType, rootSchema, mappingEsId, arrayFields);
    }

    @VisibleForTesting
    public static List<Column> genColumnsFromEs(String indexName, String mappingType, ObjectNode rootSchema,
            boolean mappingEsId, List<String> arrayFields) {
        List<Column> columns = new ArrayList<>();
        if (mappingEsId) {
            Column column = new Column();
            column.setName("_id");
            column.setIsKey(true);
            column.setType(ScalarType.createVarcharType(255));
            column.setIsAllowNull(true);
            column.setUniqueId(-1);
            columns.add(column);
        }
        ObjectNode mappingProps = (ObjectNode) rootSchema.get("properties");
        if (mappingProps == null) {
            throw new DorisEsException(
                    "index[" + indexName + "] type[" + mappingType + "] mapping not found for the ES Cluster");
        }
        Iterator<String> iterator = mappingProps.fieldNames();
        while (iterator.hasNext()) {
            String fieldName = iterator.next();
            ObjectNode fieldValue = (ObjectNode) mappingProps.get(fieldName);
            Column column = parseEsField(fieldName, replaceFieldAlias(mappingProps, fieldValue), arrayFields);
            columns.add(column);
        }
        return columns;
    }

    private static ObjectNode replaceFieldAlias(ObjectNode mappingProps, ObjectNode fieldValue) {
        if (!fieldValue.has("type")) {
            return fieldValue;
        }
        String typeStr = fieldValue.get("type").asText();
        if ("alias".equals(typeStr)) {
            String path = fieldValue.get("path").asText();
            if ("_id".equals(path)) {
                // _id is not in mappingProps, use keyword type.
                fieldValue.put("type", "keyword");
            } else {
                if (mappingProps.has(path)) {
                    return (ObjectNode) mappingProps.get(path);
                }
            }
        }
        return fieldValue;
    }

    private static Column parseEsField(String fieldName, ObjectNode fieldValue, List<String> arrayFields) {
        Column column = new Column();
        column.setName(fieldName);
        column.setIsKey(true);
        column.setIsAllowNull(true);
        column.setUniqueId(-1);
        Type type;
        // Complex types are treating as String types for now.
        if (fieldValue.has("type")) {
            String typeStr = fieldValue.get("type").asText();
            column.setComment("Elasticsearch type is " + typeStr);
            // reference https://www.elastic.co/guide/en/elasticsearch/reference/8.3/sql-data-types.html
            switch (typeStr) {
                case "null":
                    type = Type.NULL;
                    break;
                case "boolean":
                    type = Type.BOOLEAN;
                    break;
                case "byte":
                    type = Type.TINYINT;
                    break;
                case "short":
                    type = Type.SMALLINT;
                    break;
                case "integer":
                    type = Type.INT;
                    break;
                case "long":
                    type = Type.BIGINT;
                    break;
                case "unsigned_long":
                    type = Type.LARGEINT;
                    break;
                case "float":
                case "half_float":
                    type = Type.FLOAT;
                    break;
                case "double":
                case "scaled_float":
                    type = Type.DOUBLE;
                    break;
                case "date":
                    type = parseEsDateType(column, fieldValue);
                    break;
                case "keyword":
                case "text":
                case "ip":
                case "wildcard":
                case "constant_keyword":
                    type = ScalarType.createStringType();
                    break;
                case "nested":
                case "object":
                    type = Type.JSONB;
                    break;
                default:
                    type = Type.UNSUPPORTED;
            }
        } else {
            type = Type.JSONB;
            column.setComment("Elasticsearch no type");
        }
        if (arrayFields.contains(fieldName)) {
            column.setType(ArrayType.create(type, true));
        } else {
            column.setType(type);
        }
        return column;
    }

    private static final List<String> ALLOW_DATE_FORMATS = Lists.newArrayList("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd",
            "epoch_millis");

    /**
     * Parse es date to doris type by format
     **/
    private static Type parseEsDateType(Column column, ObjectNode field) {
        if (!field.has("format")) {
            // default format
            column.setComment("Elasticsearch type is date, no format");
            return ScalarType.createDatetimeV2Type(0);
        } else {
            String originFormat = field.get("format").asText();
            String[] formats = originFormat.split("\\|\\|");
            boolean dateTimeFlag = false;
            boolean dateFlag = false;
            boolean bigIntFlag = false;
            for (String format : formats) {
                // pre-check format
                String trimFormat = format.trim();
                if (!ALLOW_DATE_FORMATS.contains(trimFormat)) {
                    column.setComment(
                            "Elasticsearch type is date, format is " + trimFormat + " not support, use String type");
                    return ScalarType.createStringType();
                }
                switch (trimFormat) {
                    case "yyyy-MM-dd HH:mm:ss":
                        dateTimeFlag = true;
                        break;
                    case "yyyy-MM-dd":
                        dateFlag = true;
                        break;
                    case "epoch_millis":
                    default:
                        bigIntFlag = true;
                }
            }
            column.setComment("Elasticsearch type is date, format is " + originFormat);
            if (dateTimeFlag) {
                return ScalarType.createDatetimeV2Type(0);
            }
            if (dateFlag) {
                return ScalarType.createDateV2Type();
            }
            if (bigIntFlag) {
                return Type.BIGINT;
            }
            return ScalarType.createStringType();
        }
    }
}

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

package org.apache.doris.external.elasticsearch;

import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * Get the json object from specified jsonObject
     */
    public static JSONObject getJsonObject(JSONObject jsonObject, String key, int fromIndex) {
        int firstOccr = key.indexOf('.', fromIndex);
        if (firstOccr == -1) {
            String token = key.substring(key.lastIndexOf('.') + 1);
            if (jsonObject.containsKey(token)) {
                return (JSONObject) jsonObject.get(token);
            } else {
                return null;
            }
        }
        String fieldName = key.substring(fromIndex, firstOccr);
        if (jsonObject.containsKey(fieldName)) {
            return getJsonObject((JSONObject) jsonObject.get(fieldName), key, firstOccr + 1);
        } else {
            return null;
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

    /**
     * Get Array fields.
     **/
    public static List<String> getArrayFields(String indexMapping) {
        JSONObject mappings = getMapping(indexMapping);
        JSONObject meta;
        if (!mappings.containsKey("_meta")) {
            // For ES 6.x and 7.x
            String firstType = (String) mappings.keySet().iterator().next();
            if (!"properties".equals(firstType)) {
                // If type is not passed in takes the first type.
                JSONObject firstData = (JSONObject) mappings.get(firstType);
                if (!firstData.containsKey("_meta")) {
                    return new ArrayList<>();
                } else {
                    meta = (JSONObject) firstData.get("_meta");
                }
            } else {
                return new ArrayList<>();
            }
        } else {
            meta = (JSONObject) mappings.get("_meta");
        }
        if (!meta.containsKey("doris")) {
            return new ArrayList<>();
        }
        JSONObject dorisMeta = (JSONObject) meta.get("doris");
        return (List<String>) dorisMeta.get("array_fields");
    }

    private static JSONObject getMapping(String indexMapping) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(indexMapping);
        // If the indexName use alias takes the first mapping
        Iterator<String> keys = jsonObject.keySet().iterator();
        String docKey = keys.next();
        JSONObject docData = (JSONObject) jsonObject.get(docKey);
        return (JSONObject) docData.get("mappings");
    }

    private static JSONObject getRootSchema(JSONObject mappings, String mappingType) {
        // Type is null in the following three cases
        // 1. Equal 6.8.x and after
        // 2. Multi-catalog auto infer
        // 3. Equal 6.8.x and before user not passed
        if (mappingType == null) {
            // remove dynamic templates, for ES 7.x and 8.x
            checkNonPropertiesFields(mappings);
            String firstType = (String) mappings.keySet().iterator().next();
            if (!"properties".equals(firstType)) {
                // If type is not passed in takes the first type.
                JSONObject firstData = (JSONObject) mappings.get(firstType);
                // check for ES 6.x and before
                checkNonPropertiesFields(firstData);
                return firstData;
            }
            // Equal 7.x and after
            return mappings;
        } else {
            if (mappings.containsKey(mappingType)) {
                JSONObject jsonData = (JSONObject) mappings.get(mappingType);
                // check for ES 6.x and before
                checkNonPropertiesFields(jsonData);
                return jsonData;
            }
            // Compatible type error
            return getRootSchema(mappings, null);
        }
    }

    /**
     * Check non properties fields
     *
     * @param mappings
     */
    private static void checkNonPropertiesFields(JSONObject mappings) {
        // remove `_meta` field
        mappings.remove("_meta");
        // remove `dynamic_templates` field
        mappings.remove("dynamic_templates");
        // check explicit mapping
        if (mappings.isEmpty()) {
            throw new DorisEsException("Do not support index without explicit mapping.");
        }
    }

    /**
     * Get mapping properties JSONObject.
     **/
    public static JSONObject getMappingProps(String sourceIndex, String indexMapping, String mappingType) {
        JSONObject mappings = getMapping(indexMapping);
        JSONObject rootSchema = getRootSchema(mappings, mappingType);
        JSONObject properties = (JSONObject) rootSchema.get("properties");
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
        JSONObject mappingProps = getMappingProps(indexName, mapping, mappingType);
        List<String> arrayFields = getArrayFields(mapping);
        Set<String> keys = (Set<String>) mappingProps.keySet();
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
        for (String key : keys) {
            JSONObject field = (JSONObject) mappingProps.get(key);
            Type type;
            // Complex types are treating as String types for now.
            if (field.containsKey("type")) {
                type = toDorisType(field.get("type").toString());
            } else {
                type = Type.STRING;
            }
            Column column = new Column();
            column.setName(key);
            column.setIsKey(true);
            column.setIsAllowNull(true);
            column.setUniqueId(-1);
            if (arrayFields.contains(key)) {
                column.setType(ArrayType.create(type, true));
            } else {
                column.setType(type);
            }
            columns.add(column);
        }
        return columns;
    }

    /**
     * Transfer es type to doris type.
     **/
    public static Type toDorisType(String esType) {
        // reference https://www.elastic.co/guide/en/elasticsearch/reference/8.3/sql-data-types.html
        switch (esType) {
            case "null":
                return Type.NULL;
            case "boolean":
                return Type.BOOLEAN;
            case "byte":
                return Type.TINYINT;
            case "short":
                return Type.SMALLINT;
            case "integer":
                return Type.INT;
            case "long":
                return Type.BIGINT;
            case "unsigned_long":
                return Type.LARGEINT;
            case "float":
            case "half_float":
                return Type.FLOAT;
            case "double":
            case "scaled_float":
                return Type.DOUBLE;
            case "date":
                return ScalarType.createDateV2Type();
            case "keyword":
            case "text":
            case "ip":
            case "nested":
            case "object":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }

}


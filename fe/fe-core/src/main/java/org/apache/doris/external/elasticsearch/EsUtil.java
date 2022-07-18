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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Iterator;
import java.util.Map;

/**
 * Util for ES, some static method.
 **/
public class EsUtil {

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
     * Get mapping properties JSONObject.
     **/
    public static JSONObject getMappingProps(String sourceIndex, String indexMapping, String mappingType) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(indexMapping);
        // the indexName use alias takes the first mapping
        Iterator<String> keys = jsonObject.keySet().iterator();
        String docKey = keys.next();
        JSONObject docData = (JSONObject) jsonObject.get(docKey);
        JSONObject mappings = (JSONObject) docData.get("mappings");
        JSONObject rootSchema = (JSONObject) mappings.get(mappingType);
        JSONObject properties;
        // Elasticsearch 7.x, type was removed from ES mapping, default type is `_doc`
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.0/removal-of-types.html
        // Elasticsearch 8.x, include_type_name parameter is removed
        if (rootSchema == null) {
            properties = (JSONObject) mappings.get("properties");
        } else {
            properties = (JSONObject) rootSchema.get("properties");
        }
        if (properties == null) {
            throw new DorisEsException(
                    "index[" + sourceIndex + "] type[" + mappingType + "] mapping not found for the ES Cluster");
        }
        return properties;
    }

    /**
     * Parse the required field information from the json.
     *
     * @param searchContext the current associated column searchContext
     * @param indexMapping the return value of _mapping
     */
    public static void resolveFields(SearchContext searchContext, String indexMapping) throws DorisEsException {
        JSONObject properties = getMappingProps(searchContext.sourceIndex(), indexMapping, searchContext.type());
        for (Column col : searchContext.columns()) {
            String colName = col.getName();
            // if column exists in Doris Table but no found in ES's mapping, we choose to ignore this situation?
            if (!properties.containsKey(colName)) {
                throw new DorisEsException(
                        "index[" + searchContext.sourceIndex() + "] type[" + indexMapping + "] mapping not found column"
                                + colName + " for the ES Cluster");
            }
            JSONObject fieldObject = (JSONObject) properties.get(colName);
            resolveKeywordFields(searchContext, fieldObject, colName);
            resolveDocValuesFields(searchContext, fieldObject, colName);
        }
    }

    // get a field of keyword type in the fields
    private static void resolveKeywordFields(SearchContext searchContext, JSONObject fieldObject, String colName) {
        String fieldType = (String) fieldObject.get("type");
        // string-type field used keyword type to generate predicate
        // if text field type seen, we should use the `field` keyword type?
        if ("text".equals(fieldType)) {
            JSONObject fieldsObject = (JSONObject) fieldObject.get("fields");
            if (fieldsObject != null) {
                for (Object key : fieldsObject.keySet()) {
                    JSONObject innerTypeObject = (JSONObject) fieldsObject.get((String) key);
                    // just for text type
                    if ("keyword".equals((String) innerTypeObject.get("type"))) {
                        searchContext.fetchFieldsContext().put(colName, colName + "." + key);
                    }
                }
            }
        }
    }

    private static void resolveDocValuesFields(SearchContext searchContext, JSONObject fieldObject, String colName) {
        String fieldType = (String) fieldObject.get("type");
        String docValueField = null;
        if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(fieldType)) {
            JSONObject fieldsObject = (JSONObject) fieldObject.get("fields");
            if (fieldsObject != null) {
                for (Object key : fieldsObject.keySet()) {
                    JSONObject innerTypeObject = (JSONObject) fieldsObject.get((String) key);
                    if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS.contains((String) innerTypeObject.get("type"))) {
                        continue;
                    }
                    if (innerTypeObject.containsKey("doc_values")) {
                        boolean docValue = (Boolean) innerTypeObject.get("doc_values");
                        if (docValue) {
                            docValueField = colName;
                        }
                    } else {
                        // a : {c : {}} -> a -> a.c
                        docValueField = colName + "." + key;
                    }
                }
            }
        } else {
            // set doc_value = false manually
            if (fieldObject.containsKey("doc_values")) {
                Boolean docValue = (Boolean) fieldObject.get("doc_values");
                if (!docValue) {
                    return;
                }
            }
            docValueField = colName;
        }
        // docValueField Cannot be null
        if (StringUtils.isNotEmpty(docValueField)) {
            searchContext.docValueFieldsContext().put(colName, docValueField);
        }
    }
}

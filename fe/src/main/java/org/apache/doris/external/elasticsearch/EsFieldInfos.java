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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsTable;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * It is used to hold the field information obtained from es, currently including the fields and docValue,
 * it will eventually be added to the EsTable
 **/
public class EsFieldInfos {
    
    private static final Logger LOG = LogManager.getLogger(EsFieldInfos.class);

    // userId => userId.keyword
    private Map<String, String> fieldsContext;

    // city => city.raw
    private Map<String, String> docValueContext;
    
    public EsFieldInfos(Map<String, String> fieldsContext, Map<String, String> docValueContext) {
        this.fieldsContext = fieldsContext;
        this.docValueContext = docValueContext;
    }
    
    public Map<String, String> getFieldsContext() {
        return fieldsContext;
    }
    
    public Map<String, String> getDocValueContext() {
        return docValueContext;
    }
    
    /**
     * Parse the required field information from the json
     * @param colList table column
     * @param indexName indexName(alias or really name)
     * @param indexMapping the return value of _mapping
     * @param docType The docType used by the index
     * @return fieldsContext and docValueContext
     * @throws Exception
     */
    public static EsFieldInfos fromMapping(List<Column> colList, String indexName, String indexMapping, String docType) throws DorisEsException {
        JSONObject jsonObject = new JSONObject(indexMapping);
        // the indexName use alias takes the first mapping
        Iterator<String> keys = jsonObject.keys();
        String docKey = keys.next();
        JSONObject docData = jsonObject.optJSONObject(docKey);
        //{
        //  "mappings": {
        //    "doc": {
        //      "dynamic": "strict",
        //      "properties": {
        //        "time": {
        //          "type": "long"
        //        },
        //        "type": {
        //          "type": "keyword"
        //        },
        //        "userId": {
        //          "type": "text",
        //          "fields": {
        //            "keyword": {
        //              "type": "keyword"
        //            }
        //          }
        //        }
        //      }
        //    }
        //  }
        //}
        JSONObject mappings = docData.optJSONObject("mappings");
        JSONObject rootSchema = mappings.optJSONObject(docType);
        JSONObject properties;
        // no type in es7
        if (rootSchema == null) {
            properties = mappings.optJSONObject("properties");
        } else {
            properties = rootSchema.optJSONObject("properties");
        }
        if (properties == null) {
            throw new DorisEsException( "index[" + indexName + "] type[" + docType + "] mapping not found for the Elasticsearch Cluster");
        }
        return parseProperties(colList, properties);
    }

    // get fields information in properties
    private static EsFieldInfos parseProperties(List<Column> colList, JSONObject properties) {
        if (properties == null) {
            return null;
        }
        Map<String, String> fieldsMap = new HashMap<>();
        Map<String, String> docValueMap = new HashMap<>();
        for (Column col : colList) {
            String colName = col.getName();
            if (!properties.has(colName)) {
                continue;
            }
            JSONObject fieldObject = properties.optJSONObject(colName);
            String keywordField = getKeywordField(fieldObject, colName);
            if (StringUtils.isNotEmpty(keywordField)) {
                fieldsMap.put(colName, keywordField);
            }
            String docValueField = getDocValueField(fieldObject, colName);
            if (StringUtils.isNotEmpty(docValueField)) {
                docValueMap.put(colName, docValueField);
            }
        }
        return new EsFieldInfos(fieldsMap, docValueMap);
    }

    // get a field of keyword type in the fields
    private static String getKeywordField(JSONObject fieldObject, String colName) {
        String fieldType = fieldObject.optString("type");
        // string-type field used keyword type to generate predicate
        // if text field type seen, we should use the `field` keyword type?
        if ("text".equals(fieldType)) {
            JSONObject fieldsObject = fieldObject.optJSONObject("fields");
            if (fieldsObject != null) {
                for (String key : fieldsObject.keySet()) {
                    JSONObject innerTypeObject = fieldsObject.optJSONObject(key);
                    // just for text type
                    if ("keyword".equals(innerTypeObject.optString("type"))) {
                        return colName + "." + key;
                    }
                }
            }
        }
        return null;
    }
    
    private static String getDocValueField(JSONObject fieldObject, String colName) {
        String fieldType = fieldObject.optString("type");
        String docValueField = null;
        if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(fieldType)) {
            JSONObject fieldsObject = fieldObject.optJSONObject("fields");
            if (fieldsObject != null) {
                for (String key : fieldsObject.keySet()) {
                    JSONObject innerTypeObject = fieldsObject.optJSONObject(key);
                    if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(innerTypeObject.optString("type"))) {
                        continue;
                    }
                    if (innerTypeObject.has("doc_values")) {
                        boolean docValue = innerTypeObject.getBoolean("doc_values");
                        if (docValue) {
                            docValueField = colName;
                        }
                    } else {
                        // a : {c : {}} -> a -> a.c
                        docValueField = colName + "." + key;
                    }
                }
            }
            return docValueField;
        }
        // set doc_value = false manually
        if (fieldObject.has("doc_values")) {
            boolean docValue = fieldObject.optBoolean("doc_values");
            if (!docValue) {
                return docValueField;
            }
        }
        docValueField = colName;
        return docValueField;
    }
}

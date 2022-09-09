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
import org.json.simple.JSONObject;

/**
 * Get index mapping from remote ES Cluster, and resolved `keyword` and `doc_values` field
 * Later we can use it to parse all relevant indexes
 */
public class MappingPhase implements SearchPhase {

    private EsRestClient client;

    // json response for `{index}/_mapping` API
    private String jsonMapping;

    public MappingPhase(EsRestClient client) {
        this.client = client;
    }

    @Override
    public void execute(SearchContext context) throws DorisEsException {
        jsonMapping = client.getMapping(context.sourceIndex());
    }

    @Override
    public void postProcess(SearchContext context) {
        resolveFields(context, jsonMapping);
    }

    /**
     * Parse the required field information from the json.
     *
     * @param searchContext the current associated column searchContext
     * @param indexMapping the return value of _mapping
     */
    public static void resolveFields(SearchContext searchContext, String indexMapping) throws DorisEsException {
        JSONObject properties = EsUtil.getMappingProps(searchContext.sourceIndex(), indexMapping, searchContext.type());
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
            } else if (fieldType == null || "nested".equals(fieldType)) {
                // The object field has no type, and nested not support doc value.
                return;
            }
            docValueField = colName;
        }
        // docValueField Cannot be null
        if (StringUtils.isNotEmpty(docValueField)) {
            searchContext.docValueFieldsContext().put(colName, docValueField);
        }
    }

}

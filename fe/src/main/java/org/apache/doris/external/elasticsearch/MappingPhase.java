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

import org.json.JSONObject;

import java.util.Iterator;

/**
 * Get index mapping from remote ES Cluster, and resolved `keyword` and `doc_values` field
 * Later we can use it to parse all relevant indexes
 */
public class MappingPhase implements SearchPhase {

    private EsRestClient client;

    // json response for `{index}/_mapping` API
    private String jsonMapping;

    private boolean includeTypeName = false;

    public MappingPhase(EsRestClient client) {
        this.client = client;
    }

    @Override
    public void preProcess(SearchContext context) {
        if (context.version() != null && context.version().onOrAfter(EsMajorVersion.V_7_X)) {
            includeTypeName = true;
        }
    }

    @Override
    public void execute(SearchContext context) throws DorisEsException {
        jsonMapping = client.getMapping(context.sourceIndex(), includeTypeName);
    }

    @Override
    public void postProcess(SearchContext context) {
        resolveFields(context, jsonMapping);
    }


    /**
     * Parse the required field information from the json
     *
     * @param searchContext the current associated column searchContext
     * @param indexMapping  the return value of _mapping
     * @return fetchFieldsContext and docValueFieldsContext
     * @throws Exception
     */
    public void resolveFields(SearchContext searchContext, String indexMapping) throws DorisEsException {
        JSONObject jsonObject = new JSONObject(indexMapping);
        // the indexName use alias takes the first mapping
        Iterator<String> keys = jsonObject.keys();
        String docKey = keys.next();
        JSONObject docData = jsonObject.optJSONObject(docKey);
        JSONObject mappings = docData.optJSONObject("mappings");
        JSONObject rootSchema = mappings.optJSONObject(searchContext.type());
        JSONObject properties;
        // After (include) 7.x, type was removed from ES mapping, default type is `_doc`
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.0/removal-of-types.html
        if (rootSchema == null) {
            if (searchContext.type().equals("_doc") == false) {
                throw new DorisEsException("index[" + searchContext.sourceIndex() + "]'s type must be exists, "
                        + " and after ES7.x type must be `_doc`, but found ["
                        + searchContext.type() + "], for table ["
                        + searchContext.esTable().getName() + "]");
            }
            properties = mappings.optJSONObject("properties");
        } else {
            properties = rootSchema.optJSONObject("properties");
        }
        if (properties == null) {
            throw new DorisEsException("index[" + searchContext.sourceIndex() + "] type[" + searchContext.type() + "] mapping not found for the ES Cluster");
        }
        for (Column col : searchContext.columns()) {
            String colName = col.getName();
            // if column exists in Doris Table but no found in ES's mapping, we choose to ignore this situation?
            if (!properties.has(colName)) {
                continue;
            }
            JSONObject fieldObject = properties.optJSONObject(colName);

            resolveKeywordFields(searchContext, fieldObject, colName);
            resolveDocValuesFields(searchContext, fieldObject, colName);
        }
    }

    // get a field of keyword type in the fields
    private void resolveKeywordFields(SearchContext searchContext, JSONObject fieldObject, String colName) {
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
                        searchContext.fetchFieldsContext().put(colName, colName + "." + key);
                    }
                }
            }
        }
    }

    private void resolveDocValuesFields(SearchContext searchContext, JSONObject fieldObject, String colName) {
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
        } else {
            // set doc_value = false manually
            if (fieldObject.has("doc_values")) {
                boolean docValue = fieldObject.optBoolean("doc_values");
                if (!docValue) {
                    return;
                }
            }
            docValueField = colName;
        }
        searchContext.docValueFieldsContext().put(colName, docValueField);
    }
}

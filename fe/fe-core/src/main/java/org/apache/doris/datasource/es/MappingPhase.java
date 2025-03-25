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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsTable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;

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
        ObjectNode properties = EsUtil.getMappingProps(searchContext.sourceIndex(), indexMapping, searchContext.type());
        for (Column col : searchContext.columns()) {
            String colName = col.getName();
            // _id not exist mapping, but be can query it.
            if (!"_id".equals(colName)) {
                if (!properties.has(colName)) {
                    throw new DorisEsException(
                            "index[" + searchContext.sourceIndex() + "] mapping[" + indexMapping + "] not found "
                                    + "column " + colName + " for the ES Cluster");
                }
                ObjectNode fieldObject = (ObjectNode) properties.get(colName);
                if (!fieldObject.has("type")) {
                    continue;
                }
                String fieldType = fieldObject.get("type").asText();
                resolveDateFields(searchContext, fieldObject, colName, fieldType);
                resolveKeywordFields(searchContext, fieldObject, colName, fieldType);
                resolveDocValuesFields(searchContext, fieldObject, colName, fieldType);
            }
        }
    }

    private static void resolveDateFields(SearchContext searchContext, ObjectNode fieldObject, String colName,
            String fieldType) {
        // Compat use default/strict_date_optional_time format date type, need transform datetime to
        if ("date".equals(fieldType)) {
            if (!fieldObject.has("format") || "strict_date_optional_time".equals(fieldObject.get("format").asText())) {
                searchContext.needCompatDateFields().add(colName);
            }
        }
    }


    // get a field of keyword type in the fields
    private static void resolveKeywordFields(SearchContext searchContext, ObjectNode fieldObject, String colName,
            String fieldType) {
        // string-type field used keyword type to generate predicate
        // if text field type seen, we should use the `field` keyword type?
        if ("text".equals(fieldType)) {
            JsonNode fieldsObject = fieldObject.get("fields");
            if (fieldsObject != null) {
                Iterator<String> fieldNames = fieldsObject.fieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    ObjectNode innerTypeObject = (ObjectNode) fieldsObject.get(fieldName);
                    // just for text type
                    if ("keyword".equals(innerTypeObject.get("type").asText())) {
                        searchContext.fetchFieldsContext().put(colName, colName + "." + fieldName);
                    }
                }
            }
        }
    }

    private static void resolveDocValuesFields(SearchContext searchContext, ObjectNode fieldObject, String colName,
            String fieldType) {
        String docValueField = null;
        if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(fieldType)) {
            JsonNode fieldsObject = fieldObject.get("fields");
            if (fieldsObject != null) {
                Iterator<String> fieldNames = fieldsObject.fieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    ObjectNode innerTypeObject = (ObjectNode) fieldsObject.get(fieldName);
                    if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(innerTypeObject.get("type").asText())) {
                        continue;
                    }
                    if (innerTypeObject.has("doc_values")) {
                        boolean docValue = innerTypeObject.get("doc_values").asBoolean();
                        if (docValue) {
                            docValueField = colName;
                        }
                    } else if (innerTypeObject.has("ignore_above")) {
                        // reference:
                        // https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html#keyword-params
                        // > ignore_above
                        // > Do not index any string longer than this value. Defaults to 2147483647 so that all values
                        // > would be accepted. Please however note that default dynamic mapping rules create a sub
                        // > keyword field that overrides this default by setting ignore_above: 256.
                        // this field has `ignore_above` param
                        // Strings longer than the ignore_above setting will not be indexed or stored
                        // so we cannot rely on its doc_values
                    } else {
                        // a : {c : {}} -> a -> a.c
                        docValueField = colName + "." + fieldName;
                    }
                }
            }
        } else {
            // set doc_value = false manually
            if (fieldObject.has("doc_values")) {
                boolean docValue = fieldObject.get("doc_values").asBoolean();
                if (!docValue) {
                    return;
                }
            } else if (fieldType == null || "nested".equals(fieldType)) {
                // The object field has no type, and nested not support doc value.
                return;
            } else if (fieldObject.has("ignore_above")) {
                // reference:
                // https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html#keyword-params
                // > ignore_above
                // > Do not index any string longer than this value. Defaults to 2147483647 so that all values
                // > would be accepted. Please however note that default dynamic mapping rules create a sub
                // > keyword field that overrides this default by setting ignore_above: 256.
                // this field has `ignore_above` param
                // Strings longer than the ignore_above setting will not be indexed or stored
                // so we cannot rely on its doc_values
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

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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LikePredicate.Operator;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.external.elasticsearch.QueryBuilders.QueryBuilder;
import org.apache.doris.thrift.TExprOpcode;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    private static QueryBuilder toCompoundEsDsl(Expr expr) {
        CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
        switch (compoundPredicate.getOp()) {
            case AND: {
                QueryBuilder left = toEsDsl(compoundPredicate.getChild(0));
                QueryBuilder right = toEsDsl(compoundPredicate.getChild(1));
                if (left != null && right != null) {
                    return QueryBuilders.boolQuery().must(left).must(right);
                }
                return null;
            }
            case OR: {
                QueryBuilder left = toEsDsl(compoundPredicate.getChild(0));
                QueryBuilder right = toEsDsl(compoundPredicate.getChild(1));
                if (left != null && right != null) {
                    return QueryBuilders.boolQuery().should(left).should(right);
                }
                return null;
            }
            case NOT: {
                QueryBuilder child = toEsDsl(compoundPredicate.getChild(0));
                if (child != null) {
                    return QueryBuilders.boolQuery().mustNot(child);
                }
                return null;
            }
            default:
                return null;
        }
    }

    /**
     * Doris expr to es dsl.
     **/
    public static QueryBuilder toEsDsl(Expr expr) {
        if (expr == null) {
            return null;
        }
        // CompoundPredicate, `between` also converted to CompoundPredicate.
        if (expr instanceof CompoundPredicate) {
            return toCompoundEsDsl(expr);
        }
        TExprOpcode opCode = expr.getOpcode();
        String column = ((SlotRef) expr.getChild(0)).getColumnName();
        if (expr instanceof BinaryPredicate) {
            Object value = toDorisLiteral(expr.getChild(1));
            switch (opCode) {
                case EQ:
                case EQ_FOR_NULL:
                    return QueryBuilders.termQuery(column, value);
                case NE:
                    return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(column, value));
                case GE:
                    return QueryBuilders.rangeQuery(column).gte(value);
                case GT:
                    return QueryBuilders.rangeQuery(column).gt(value);
                case LE:
                    return QueryBuilders.rangeQuery(column).lte(value);
                case LT:
                    return QueryBuilders.rangeQuery(column).lt(value);
                default:
                    return null;
            }
        }
        if (expr instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
            if (isNullPredicate.isNotNull()) {
                return QueryBuilders.existsQuery(column);
            }
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(column));
        }
        if (expr instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) expr;
            if (likePredicate.getOp().equals(Operator.LIKE)) {
                char[] chars = likePredicate.getChild(1).getStringValue().toCharArray();
                // example of translation :
                //      abc_123  ===> abc?123
                //      abc%ykz  ===> abc*123
                //      %abc123  ===> *abc123
                //      _abc123  ===> ?abc123
                //      \\_abc1  ===> \\_abc1
                //      abc\\_123 ===> abc\\_123
                //      abc\\%123 ===> abc\\%123
                // NOTE. user must input sql like 'abc\\_123' or 'abc\\%ykz'
                for (int i = 0; i < chars.length; i++) {
                    if (chars[i] == '_' || chars[i] == '%') {
                        if (i == 0) {
                            chars[i] = (chars[i] == '_') ? '?' : '*';
                        } else if (chars[i - 1] != '\\') {
                            chars[i] = (chars[i] == '_') ? '?' : '*';
                        }
                    }
                }
                return QueryBuilders.wildcardQuery(column, new String(chars));
            } else {
                return QueryBuilders.wildcardQuery(column, likePredicate.getChild(1).getStringValue());
            }
        }
        if (expr instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) expr;
            List<Object> values = inPredicate.getListChildren().stream().map(EsUtil::toDorisLiteral)
                    .collect(Collectors.toList());
            if (inPredicate.isNotIn()) {
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(column, values));
            }
            return QueryBuilders.termsQuery(column, values);
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            if ("esquery".equals(functionCallExpr.getFnName().getFunction())) {
                String stringValue = functionCallExpr.getChild(1).getStringValue();
                return new QueryBuilders.EsQueryBuilder(stringValue);
            }
        }
        return null;
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
            case "unsigned_long":
                return Type.BIGINT;
            case "float":
            case "half_float":
                return Type.FLOAT;
            case "double":
            case "scaled_float":
                return Type.DOUBLE;
            case "keyword":
            case "text":
            case "ip":
            case "nested":
            case "object":
                return Type.STRING;
            case "date":
                return Type.DATE;
            default:
                return Type.INVALID;
        }
    }

    private static Object toDorisLiteral(Expr expr) {
        if (!expr.isLiteral()) {
            return null;
        }
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            return dateLiteral.getLongValue();
        } else if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            return decimalLiteral.getValue();
        } else if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            return floatLiteral.getValue();
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return intLiteral.getValue();
        } else if (expr instanceof LargeIntLiteral) {
            LargeIntLiteral largeIntLiteral = (LargeIntLiteral) expr;
            return largeIntLiteral.getLongValue();
        } else if (expr instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) expr;
            return stringLiteral.getStringValue();
        }
        return null;
    }

    /**
     * Generate url for be to query es.
     **/
    public static EsUrls genEsUrls(String index, String type, boolean docValueMode, long limit, long batchSize) {
        String filterPath = docValueMode ? "filter_path=_scroll_id,hits.total,hits.hits._score,hits.hits.fields"
                : "filter_path=_scroll_id,hits.hits._source,hits.total,hits.hits._id";
        if (limit <= 0) {
            StringBuilder initScrollUrl = new StringBuilder();
            StringBuilder nextScrollUrl = new StringBuilder();
            initScrollUrl.append("/").append(index);
            if (StringUtils.isNotBlank(type)) {
                initScrollUrl.append("/").append(type);
            }
            initScrollUrl.append("/_search?").append(filterPath).append("&terminate_after=")
                    .append(batchSize);
            nextScrollUrl.append("/_search/scroll?").append(filterPath);
            return new EsUrls(null, initScrollUrl.toString(), nextScrollUrl.toString());
        } else {
            StringBuilder searchUrl = new StringBuilder();
            searchUrl.append("/").append(index);
            if (StringUtils.isNotBlank(type)) {
                searchUrl.append("/").append(type);
            }
            searchUrl.append("/_search?terminate_after=").append(limit).append("&").append(filterPath);
            return new EsUrls(searchUrl.toString(), null, null);
        }
    }
}

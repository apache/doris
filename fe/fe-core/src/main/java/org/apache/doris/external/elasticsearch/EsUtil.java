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
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
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
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.external.elasticsearch.QueryBuilders.QueryBuilder;
import org.apache.doris.thrift.TExprOpcode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        if (!mappings.containsKey("_meta")) {
            return new ArrayList<>();
        }
        JSONObject meta = (JSONObject) mappings.get("_meta");
        if (!meta.containsKey("doris")) {
            return new ArrayList<>();
        }
        JSONObject dorisMeta = (JSONObject) meta.get("doris");
        return (List<String>) dorisMeta.get("array_field");
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
            String firstType = (String) mappings.keySet().iterator().next();
            if (!"properties".equals(firstType)) {
                // If type is not passed in takes the first type.
                return (JSONObject) mappings.get(firstType);
            }
            // Equal 7.x and after
            return mappings;
        } else {
            if (mappings.containsKey(mappingType)) {
                return (JSONObject) mappings.get(mappingType);
            }
            // Compatible type error
            return getRootSchema(mappings, null);
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

    private static QueryBuilder toCompoundEsDsl(Expr expr, List<Expr> notPushDownList,
            Map<String, String> fieldsContext) {
        CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
        switch (compoundPredicate.getOp()) {
            case AND: {
                QueryBuilder left = toEsDsl(compoundPredicate.getChild(0), notPushDownList, fieldsContext);
                QueryBuilder right = toEsDsl(compoundPredicate.getChild(1), notPushDownList, fieldsContext);
                if (left != null && right != null) {
                    return QueryBuilders.boolQuery().must(left).must(right);
                }
                return null;
            }
            case OR: {
                int beforeSize = notPushDownList.size();
                QueryBuilder left = toEsDsl(compoundPredicate.getChild(0), notPushDownList, fieldsContext);
                QueryBuilder right = toEsDsl(compoundPredicate.getChild(1), notPushDownList, fieldsContext);
                int afterSize = notPushDownList.size();
                if (left != null && right != null) {
                    return QueryBuilders.boolQuery().should(left).should(right);
                }
                // One 'or' association cannot be pushed down and the other cannot be pushed down
                if (afterSize > beforeSize) {
                    if (left != null) {
                        // add right if right don't pushdown
                        notPushDownList.add(compoundPredicate.getChild(0));
                    } else if (right != null) {
                        // add left if left don't pushdown
                        notPushDownList.add(compoundPredicate.getChild(1));
                    }
                }
                return null;
            }
            case NOT: {
                QueryBuilder child = toEsDsl(compoundPredicate.getChild(0), notPushDownList, fieldsContext);
                if (child != null) {
                    return QueryBuilders.boolQuery().mustNot(child);
                }
                return null;
            }
            default:
                return null;
        }
    }

    private static Expr exprWithoutCast(Expr expr) {
        if (expr instanceof CastExpr) {
            return exprWithoutCast(expr.getChild(0));
        }
        return expr;
    }

    public static QueryBuilder toEsDsl(Expr expr) {
        return toEsDsl(expr, new ArrayList<>(), new HashMap<>());
    }

    /**
     * Doris expr to es dsl.
     **/
    public static QueryBuilder toEsDsl(Expr expr, List<Expr> notPushDownList, Map<String, String> fieldsContext) {
        if (expr == null) {
            return null;
        }
        // CompoundPredicate, `between` also converted to CompoundPredicate.
        if (expr instanceof CompoundPredicate) {
            return toCompoundEsDsl(expr, notPushDownList, fieldsContext);
        }
        TExprOpcode opCode = expr.getOpcode();
        String column;
        Expr leftExpr = expr.getChild(0);
        // Type transformed cast can not pushdown
        if (leftExpr instanceof CastExpr) {
            Expr withoutCastExpr = exprWithoutCast(leftExpr);
            // pushdown col(float) >= 3
            if (withoutCastExpr.getType().equals(leftExpr.getType()) || (withoutCastExpr.getType().isFloatingPointType()
                    && leftExpr.getType().isFloatingPointType())) {
                column = ((SlotRef) withoutCastExpr).getColumnName();
            } else {
                notPushDownList.add(expr);
                return null;
            }
        } else if (leftExpr instanceof SlotRef) {
            column = ((SlotRef) leftExpr).getColumnName();
        } else {
            notPushDownList.add(expr);
            return null;
        }
        // Replace col with col.keyword if mapping exist.
        column = fieldsContext.getOrDefault(column, column);
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
     * Generate columns from ES Cluster.
     **/
    public static List<Column> genColumnsFromEs(EsRestClient client, String indexName, String mappingType) {
        String mapping = client.getMapping(indexName);
        JSONObject mappingProps = getMappingProps(indexName, mapping, mappingType);
        List<String> arrayFields = getArrayFields(mapping);
        Set<String> keys = (Set<String>) mappingProps.keySet();
        List<Column> columns = new ArrayList<>();
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
                return ScalarType.getDefaultDateType(Type.DATE);
            case "keyword":
            case "text":
            case "ip":
            case "nested":
            case "object":
            default:
                return ScalarType.createStringType();
        }
    }

    private static Object toDorisLiteral(Expr expr) {
        if (!expr.isLiteral()) {
            return null;
        }
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
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
        }
        return expr.getStringValue();
    }

}


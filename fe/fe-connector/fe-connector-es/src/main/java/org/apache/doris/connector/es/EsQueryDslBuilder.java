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

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLike;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Converts {@link ConnectorExpression} filter trees into Elasticsearch Query DSL JSON strings.
 *
 * <p>This is the connector-SPI counterpart of the legacy {@code QueryBuilders} in fe-core.
 * It works exclusively with {@code ConnectorExpression} types instead of fe-core's {@code Expr}
 * hierarchy, keeping the ES connector free of fe-core dependencies.</p>
 */
public final class EsQueryDslBuilder {

    private static final Logger LOG = LogManager.getLogger(EsQueryDslBuilder.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter ISO_DATETIME = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private EsQueryDslBuilder() {
        // utility class
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Build an ES Query DSL JSON string from a {@link ConnectorExpression} filter.
     *
     * @param filter              the filter expression tree (may be null)
     * @param fieldsContext        maps column names to their {@code .keyword} sub-field names
     * @param column2typeMap       maps column names to ES type strings
     * @param likePushDown         whether LIKE predicates should be pushed down
     * @param needCompatDateFields fields that need date format compatibility
     * @return JSON query DSL string, or {@code {"match_all":{}}} if nothing is pushable
     */
    public static String buildQueryDsl(ConnectorExpression filter,
            Map<String, String> fieldsContext,
            Map<String, String> column2typeMap,
            boolean likePushDown,
            List<String> needCompatDateFields) {
        return buildQueryDslWithResult(filter, fieldsContext, column2typeMap,
                likePushDown, needCompatDateFields).getQueryDsl();
    }

    /**
     * Build ES Query DSL and also return information about which top-level
     * AND conjuncts could not be pushed down.
     *
     * <p>When the filter is a {@link ConnectorAnd}, each conjunct is tried
     * individually. The returned {@link EsQueryDslResult} contains the indices
     * of conjuncts that could not be converted to ES DSL — these should be
     * retained for local evaluation in the scan node.</p>
     */
    public static EsQueryDslResult buildQueryDslWithResult(ConnectorExpression filter,
            Map<String, String> fieldsContext,
            Map<String, String> column2typeMap,
            boolean likePushDown,
            List<String> needCompatDateFields) {
        if (filter == null) {
            return new EsQueryDslResult(matchAllQuery(), Collections.emptyList());
        }

        // For AND expressions, try each conjunct independently to track which failed
        if (filter instanceof ConnectorAnd) {
            ConnectorAnd and = (ConnectorAnd) filter;
            List<ConnectorExpression> conjuncts = and.getConjuncts();
            List<String> parts = new ArrayList<>();
            List<Integer> notPushedIndices = new ArrayList<>();

            for (int i = 0; i < conjuncts.size(); i++) {
                List<ConnectorExpression> tempNotPushed = new ArrayList<>();
                String dsl = toEsDsl(conjuncts.get(i), tempNotPushed, fieldsContext,
                        likePushDown, needCompatDateFields, column2typeMap);
                if (dsl != null && tempNotPushed.isEmpty()) {
                    parts.add(dsl);
                } else {
                    notPushedIndices.add(i);
                }
            }

            String queryDsl;
            if (parts.isEmpty()) {
                queryDsl = matchAllQuery();
            } else if (parts.size() == 1) {
                queryDsl = parts.get(0);
            } else {
                ObjectNode root = MAPPER.createObjectNode();
                ObjectNode boolNode = root.putObject("bool");
                ArrayNode mustArray = boolNode.putArray("must");
                for (String part : parts) {
                    mustArray.add(parseJsonNode(part));
                }
                queryDsl = root.toString();
            }
            return new EsQueryDslResult(queryDsl, notPushedIndices);
        }

        // Single expression (not AND)
        List<ConnectorExpression> notPushDownList = new ArrayList<>();
        String dsl = toEsDsl(filter, notPushDownList, fieldsContext,
                likePushDown, needCompatDateFields, column2typeMap);
        if (dsl == null) {
            return new EsQueryDslResult(matchAllQuery(), Collections.singletonList(0));
        }
        return new EsQueryDslResult(dsl, Collections.emptyList());
    }

    /**
     * Returns the JSON string for a match-all query: {@code {"match_all":{}}}.
     */
    public static String matchAllQuery() {
        ObjectNode node = MAPPER.createObjectNode();
        node.putObject("match_all");
        return node.toString();
    }

    // -----------------------------------------------------------------------
    // Core conversion: ConnectorExpression -> JSON string (or null)
    // -----------------------------------------------------------------------

    /**
     * Convert a single {@link ConnectorExpression} to an ES Query DSL JSON string.
     *
     * @param expr                 expression to convert
     * @param notPushDownList      accumulates expressions that cannot be pushed down
     * @param fieldsContext        column → keyword sub-field mapping
     * @param likePushDown         whether LIKE predicates should be pushed down
     * @param needCompatDateFields fields requiring date format compatibility
     * @param column2typeMap       column → ES type mapping
     * @return JSON string for the query, or {@code null} if the expression cannot be pushed down
     */
    static String toEsDsl(ConnectorExpression expr,
            List<ConnectorExpression> notPushDownList,
            Map<String, String> fieldsContext,
            boolean likePushDown,
            List<String> needCompatDateFields,
            Map<String, String> column2typeMap) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof ConnectorAnd) {
            return andToDsl((ConnectorAnd) expr, notPushDownList, fieldsContext,
                    likePushDown, needCompatDateFields, column2typeMap);
        }
        if (expr instanceof ConnectorOr) {
            return orToDsl((ConnectorOr) expr, notPushDownList, fieldsContext,
                    likePushDown, needCompatDateFields, column2typeMap);
        }
        if (expr instanceof ConnectorNot) {
            return notToDsl((ConnectorNot) expr, notPushDownList, fieldsContext,
                    likePushDown, needCompatDateFields, column2typeMap);
        }
        if (expr instanceof ConnectorIsNull) {
            return isNullToDsl((ConnectorIsNull) expr);
        }
        if (expr instanceof ConnectorComparison) {
            return comparisonToDsl((ConnectorComparison) expr, notPushDownList,
                    fieldsContext, needCompatDateFields, column2typeMap);
        }
        if (expr instanceof ConnectorIn) {
            return inToDsl((ConnectorIn) expr, notPushDownList,
                    fieldsContext, needCompatDateFields, column2typeMap);
        }
        if (expr instanceof ConnectorLike) {
            return likeToDsl((ConnectorLike) expr, notPushDownList,
                    fieldsContext, likePushDown, column2typeMap);
        }
        if (expr instanceof ConnectorFunctionCall) {
            return functionCallToDsl((ConnectorFunctionCall) expr, notPushDownList);
        }
        // Unsupported expression type
        notPushDownList.add(expr);
        return null;
    }

    // -----------------------------------------------------------------------
    // AND / OR / NOT
    // -----------------------------------------------------------------------

    private static String andToDsl(ConnectorAnd and,
            List<ConnectorExpression> notPushDownList,
            Map<String, String> fieldsContext,
            boolean likePushDown,
            List<String> needCompatDateFields,
            Map<String, String> column2typeMap) {
        List<String> parts = new ArrayList<>();
        for (ConnectorExpression conjunct : and.getConjuncts()) {
            String dsl = toEsDsl(conjunct, notPushDownList, fieldsContext,
                    likePushDown, needCompatDateFields, column2typeMap);
            if (dsl != null) {
                parts.add(dsl);
            }
        }
        if (parts.isEmpty()) {
            return null;
        }
        if (parts.size() == 1) {
            return parts.get(0);
        }
        // {"bool":{"must":[...]}}
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode boolNode = root.putObject("bool");
        ArrayNode mustArray = boolNode.putArray("must");
        for (String part : parts) {
            mustArray.add(parseJsonNode(part));
        }
        return root.toString();
    }

    private static String orToDsl(ConnectorOr or,
            List<ConnectorExpression> notPushDownList,
            Map<String, String> fieldsContext,
            boolean likePushDown,
            List<String> needCompatDateFields,
            Map<String, String> column2typeMap) {
        int beforeSize = notPushDownList.size();
        List<String> parts = new ArrayList<>();
        boolean anyNull = false;
        for (ConnectorExpression disjunct : or.getDisjuncts()) {
            String dsl = toEsDsl(disjunct, notPushDownList, fieldsContext,
                    likePushDown, needCompatDateFields, column2typeMap);
            if (dsl != null) {
                parts.add(dsl);
            } else {
                anyNull = true;
            }
        }
        int afterSize = notPushDownList.size();
        if (anyNull) {
            // If any disjunct is not pushable, the whole OR is not pushable.
            // Remove sub-expressions that were added during failed conversion and add the whole OR.
            if (afterSize > beforeSize) {
                // Remove the items added by children; add the whole OR instead
                while (notPushDownList.size() > beforeSize) {
                    notPushDownList.remove(notPushDownList.size() - 1);
                }
            }
            notPushDownList.add(or);
            return null;
        }
        if (parts.size() == 1) {
            return parts.get(0);
        }
        // {"bool":{"should":[...]}}
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode boolNode = root.putObject("bool");
        ArrayNode shouldArray = boolNode.putArray("should");
        for (String part : parts) {
            shouldArray.add(parseJsonNode(part));
        }
        return root.toString();
    }

    private static String notToDsl(ConnectorNot not,
            List<ConnectorExpression> notPushDownList,
            Map<String, String> fieldsContext,
            boolean likePushDown,
            List<String> needCompatDateFields,
            Map<String, String> column2typeMap) {
        String child = toEsDsl(not.getOperand(), notPushDownList, fieldsContext,
                likePushDown, needCompatDateFields, column2typeMap);
        if (child == null) {
            return null;
        }
        // {"bool":{"must_not":<child>}}
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode boolNode = root.putObject("bool");
        boolNode.set("must_not", parseJsonNode(child));
        return root.toString();
    }

    // -----------------------------------------------------------------------
    // IS NULL / IS NOT NULL
    // -----------------------------------------------------------------------

    private static String isNullToDsl(ConnectorIsNull isNull) {
        ConnectorExpression operand = isNull.getOperand();
        if (!(operand instanceof ConnectorColumnRef)) {
            return null;
        }
        String column = ((ConnectorColumnRef) operand).getColumnName();
        if (isNull.isNegated()) {
            // IS NOT NULL -> {"exists":{"field":"col"}}
            return existsQuery(column);
        }
        // IS NULL -> {"bool":{"must_not":{"exists":{"field":"col"}}}}
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode boolNode = root.putObject("bool");
        boolNode.set("must_not", parseJsonNode(existsQuery(column)));
        return root.toString();
    }

    // -----------------------------------------------------------------------
    // Comparison (EQ / NE / LT / LE / GT / GE)
    // -----------------------------------------------------------------------

    private static String comparisonToDsl(ConnectorComparison comparison,
            List<ConnectorExpression> notPushDownList,
            Map<String, String> fieldsContext,
            List<String> needCompatDateFields,
            Map<String, String> column2typeMap) {
        ConnectorExpression leftExpr = comparison.getLeft();
        ConnectorExpression rightExpr = comparison.getRight();
        ConnectorComparison.Operator op = comparison.getOperator();
        boolean isFlip = false;

        // Determine column and value sides
        String column = extractColumnName(leftExpr);
        if (column == null) {
            column = extractColumnName(rightExpr);
            isFlip = true;
        }
        if (column == null) {
            notPushDownList.add(comparison);
            return null;
        }

        ConnectorExpression valueExpr = isFlip ? leftExpr : rightExpr;
        if (!(valueExpr instanceof ConnectorLiteral)) {
            notPushDownList.add(comparison);
            return null;
        }
        ConnectorLiteral literal = (ConnectorLiteral) valueExpr;

        if (isFlip) {
            op = flipOperator(op);
        }

        // Check date compatibility before keyword replacement
        boolean needDateCompat = needCompatDateFields != null && needCompatDateFields.contains(column);

        // Replace column with keyword sub-field if mapping exists
        column = fieldsContext.getOrDefault(column, column);

        Object value = extractLiteralValue(literal);
        if (needDateCompat) {
            value = compatDefaultDate(value);
        }

        return buildComparisonDsl(op, column, value, needDateCompat);
    }

    private static String buildComparisonDsl(ConnectorComparison.Operator op,
            String column, Object value, boolean needDateCompat) {
        switch (op) {
            case EQ:
                return termQuery(column, value);
            case EQ_FOR_NULL:
                // col <=> NULL means col IS NULL → field does not exist in ES
                if (value == null) {
                    ObjectNode eqNullRoot = MAPPER.createObjectNode();
                    ObjectNode eqNullBool = eqNullRoot.putObject("bool");
                    eqNullBool.set("must_not", parseJsonNode(existsQuery(column)));
                    return eqNullRoot.toString();
                }
                // col <=> non-null is equivalent to EQ (term queries exclude nulls)
                return termQuery(column, value);
            case NE:
                // col != '' means col.length() > 0, NULL should not appear in results
                if (value instanceof String && ((String) value).isEmpty()) {
                    ObjectNode root = MAPPER.createObjectNode();
                    ObjectNode boolNode = root.putObject("bool");
                    boolNode.set("must_not", parseJsonNode(termQuery(column, value)));
                    boolNode.set("must", parseJsonNode(existsQuery(column)));
                    return root.toString();
                }
                ObjectNode neRoot = MAPPER.createObjectNode();
                ObjectNode neBool = neRoot.putObject("bool");
                neBool.set("must_not", parseJsonNode(termQuery(column, value)));
                return neRoot.toString();
            case LT:
                return rangeQuery(column, "lt", value, needDateCompat);
            case LE:
                return rangeQuery(column, "lte", value, needDateCompat);
            case GT:
                return rangeQuery(column, "gt", value, needDateCompat);
            case GE:
                return rangeQuery(column, "gte", value, needDateCompat);
            default:
                return null;
        }
    }

    private static ConnectorComparison.Operator flipOperator(ConnectorComparison.Operator op) {
        switch (op) {
            case GE:
                return ConnectorComparison.Operator.LE;
            case GT:
                return ConnectorComparison.Operator.LT;
            case LE:
                return ConnectorComparison.Operator.GE;
            case LT:
                return ConnectorComparison.Operator.GT;
            default:
                return op;
        }
    }

    // -----------------------------------------------------------------------
    // IN / NOT IN
    // -----------------------------------------------------------------------

    private static String inToDsl(ConnectorIn inExpr,
            List<ConnectorExpression> notPushDownList,
            Map<String, String> fieldsContext,
            List<String> needCompatDateFields,
            Map<String, String> column2typeMap) {
        String column = extractColumnName(inExpr.getValue());
        if (column == null) {
            notPushDownList.add(inExpr);
            return null;
        }

        boolean needDateCompat = needCompatDateFields != null && needCompatDateFields.contains(column);

        // Replace column with keyword sub-field
        column = fieldsContext.getOrDefault(column, column);

        // All items must be literals
        List<Object> values = new ArrayList<>();
        for (ConnectorExpression item : inExpr.getInList()) {
            if (!(item instanceof ConnectorLiteral)) {
                notPushDownList.add(inExpr);
                return null;
            }
            Object val = extractLiteralValue((ConnectorLiteral) item);
            if (needDateCompat) {
                val = compatDefaultDate(val);
            }
            values.add(val);
        }

        String termsJson = termsQuery(column, values);
        if (inExpr.isNegated()) {
            // {"bool":{"must_not":{"terms":{...}}}}
            ObjectNode root = MAPPER.createObjectNode();
            ObjectNode boolNode = root.putObject("bool");
            boolNode.set("must_not", parseJsonNode(termsJson));
            return root.toString();
        }
        return termsJson;
    }

    // -----------------------------------------------------------------------
    // LIKE
    // -----------------------------------------------------------------------

    private static String likeToDsl(ConnectorLike like,
            List<ConnectorExpression> notPushDownList,
            Map<String, String> fieldsContext,
            boolean likePushDown,
            Map<String, String> column2typeMap) {
        if (!likePushDown) {
            notPushDownList.add(like);
            return null;
        }

        String column = extractColumnName(like.getValue());
        if (column == null) {
            notPushDownList.add(like);
            return null;
        }

        // REGEXP can be pushed down to any field type
        if (like.getOperator() == ConnectorLike.Operator.REGEXP) {
            // Replace column with keyword sub-field if available
            column = fieldsContext.getOrDefault(column, column);
            ConnectorExpression patternExpr = like.getPattern();
            if (!(patternExpr instanceof ConnectorLiteral)) {
                notPushDownList.add(like);
                return null;
            }
            String pattern = String.valueOf(((ConnectorLiteral) patternExpr).getValue());
            return regexpQuery(column, pattern);
        }

        // LIKE: only native keyword fields can safely apply wildcard queries.
        // Text fields with .keyword sub-fields may have ignore_above, which prevents
        // indexing values exceeding the limit — wildcard queries would miss those values.
        String type = column2typeMap != null ? column2typeMap.get(column) : null;
        if (!"keyword".equals(type)) {
            notPushDownList.add(like);
            return null;
        }
        column = fieldsContext.getOrDefault(column, column);

        ConnectorExpression patternExpr = like.getPattern();
        if (!(patternExpr instanceof ConnectorLiteral)) {
            notPushDownList.add(like);
            return null;
        }
        String pattern = String.valueOf(((ConnectorLiteral) patternExpr).getValue());

        // Convert SQL LIKE pattern to ES wildcard pattern
        String wildcardPattern = convertLikeToWildcard(pattern);
        return wildcardQuery(column, wildcardPattern);
    }

    /**
     * Convert SQL LIKE pattern to ES wildcard pattern.
     * <ul>
     *   <li>{@code %} → {@code *}</li>
     *   <li>{@code _} → {@code ?}</li>
     *   <li>Escaped {@code \_} and {@code \%} are left as-is</li>
     * </ul>
     */
    private static String convertLikeToWildcard(String pattern) {
        char[] chars = pattern.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == '_' || chars[i] == '%') {
                if (i == 0) {
                    chars[i] = (chars[i] == '_') ? '?' : '*';
                } else if (chars[i - 1] != '\\') {
                    chars[i] = (chars[i] == '_') ? '?' : '*';
                }
            }
        }
        return new String(chars);
    }

    // -----------------------------------------------------------------------
    // Function call (esquery)
    // -----------------------------------------------------------------------

    private static String functionCallToDsl(ConnectorFunctionCall funcCall,
            List<ConnectorExpression> notPushDownList) {
        if ("esquery".equals(funcCall.getFunctionName())
                && funcCall.getArguments().size() == 2) {
            ConnectorExpression secondArg = funcCall.getArguments().get(1);
            if (secondArg instanceof ConnectorLiteral) {
                Object val = ((ConnectorLiteral) secondArg).getValue();
                if (val instanceof String) {
                    String dslJson = (String) val;
                    // Validate and return the raw ES DSL JSON
                    try {
                        JsonNode parsed = MAPPER.readTree(dslJson);
                        return parsed.toString();
                    } catch (JsonProcessingException e) {
                        LOG.warn("Failed to parse esquery DSL JSON: {}", dslJson, e);
                        notPushDownList.add(funcCall);
                        return null;
                    }
                }
            }
        }
        notPushDownList.add(funcCall);
        return null;
    }

    // -----------------------------------------------------------------------
    // Elementary query builders (return JSON strings)
    // -----------------------------------------------------------------------

    /**
     * Build a term query: {@code {"term":{"field":"value"}}}.
     */
    private static String termQuery(String field, Object value) {
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode termNode = root.putObject("term");
        putValue(termNode, field, value);
        return root.toString();
    }

    /**
     * Build a terms query: {@code {"terms":{"field":["v1","v2"]}}}.
     */
    private static String termsQuery(String field, List<Object> values) {
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode termsNode = root.putObject("terms");
        ArrayNode arrayNode = termsNode.putArray(field);
        for (Object val : values) {
            addValueToArray(arrayNode, val);
        }
        return root.toString();
    }

    /**
     * Build a range query: {@code {"range":{"field":{"op":"value"}}}}.
     */
    private static String rangeQuery(String field, String op, Object value, boolean needDateCompat) {
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode rangeNode = root.putObject("range");
        ObjectNode fieldNode = rangeNode.putObject(field);
        putValue(fieldNode, op, value);
        // No explicit format needed — compatDefaultDate produces ISO 8601
        // which is natively compatible with strict_date_optional_time.
        return root.toString();
    }

    /**
     * Build an exists query: {@code {"exists":{"field":"col"}}}.
     */
    private static String existsQuery(String field) {
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode existsNode = root.putObject("exists");
        existsNode.put("field", field);
        return root.toString();
    }

    /**
     * Build a wildcard query: {@code {"wildcard":{"field":"pattern"}}}.
     */
    private static String wildcardQuery(String field, String pattern) {
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode wildcardNode = root.putObject("wildcard");
        wildcardNode.put(field, pattern);
        return root.toString();
    }

    /**
     * Build a regexp query: {@code {"regexp":{"field":"pattern"}}}.
     */
    private static String regexpQuery(String field, String pattern) {
        ObjectNode root = MAPPER.createObjectNode();
        ObjectNode regexpNode = root.putObject("regexp");
        regexpNode.put(field, pattern);
        return root.toString();
    }

    // -----------------------------------------------------------------------
    // Literal value extraction and helpers
    // -----------------------------------------------------------------------

    /**
     * Extract the column name from a {@link ConnectorColumnRef}.
     *
     * @return column name, or {@code null} if the expression is not a column reference
     */
    private static String extractColumnName(ConnectorExpression expr) {
        if (expr instanceof ConnectorColumnRef) {
            return ((ConnectorColumnRef) expr).getColumnName();
        }
        return null;
    }

    /**
     * Extract the Java value from a {@link ConnectorLiteral} for use in ES queries.
     *
     * <p>Returns:</p>
     * <ul>
     *   <li>{@code null} → null</li>
     *   <li>Boolean → Boolean</li>
     *   <li>Integer → Integer (numeric in JSON)</li>
     *   <li>Long → Long (numeric in JSON)</li>
     *   <li>Double → Double (numeric in JSON)</li>
     *   <li>BigDecimal → BigDecimal (uses {@code toPlainString()} for display)</li>
     *   <li>String → String</li>
     *   <li>LocalDate → ISO date string</li>
     *   <li>LocalDateTime → ISO date-time string</li>
     * </ul>
     */
    private static Object extractLiteralValue(ConnectorLiteral literal) {
        Object value = literal.getValue();
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean || value instanceof Integer
                || value instanceof Long || value instanceof Double
                || value instanceof String) {
            return value;
        }
        if (value instanceof BigDecimal) {
            return value;
        }
        if (value instanceof LocalDate) {
            return ((LocalDate) value).format(ISO_DATE);
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).format(ISO_DATETIME);
        }
        // Fallback: use toString()
        return value.toString();
    }

    /**
     * Apply Doris date compatibility transformation.
     * Converts date strings in "yyyy-MM-dd HH:mm:ss" format to ISO 8601 format
     * (e.g., "2024-01-15T10:30:00.000+08:00") compatible with ES's strict_date_optional_time.
     *
     * <p>Uses the system default timezone to match the old Joda-Time behavior where
     * {@code DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")} interprets values in the
     * JVM's default timezone.</p>
     */
    private static Object compatDefaultDate(Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof String) {
                String strVal = (String) value;
                // Try parsing as ISO datetime first (from extractLiteralValue),
                // then space-separated format, then date-only
                try {
                    LocalDateTime ldt = LocalDateTime.parse(strVal, ISO_DATETIME);
                    return ldt.atZone(ZoneId.systemDefault()).toOffsetDateTime()
                            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                } catch (Exception e0) {
                    try {
                        LocalDateTime ldt = LocalDateTime.parse(strVal,
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                        return ldt.atZone(ZoneId.systemDefault()).toOffsetDateTime()
                                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    } catch (Exception e1) {
                        try {
                            LocalDate ld = LocalDate.parse(strVal, ISO_DATE);
                            return ld.atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime()
                                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                        } catch (Exception e2) {
                            // Return as-is if parsing fails
                            return value;
                        }
                    }
                }
            }
            // For non-string date values, return as-is (already handled by extractLiteralValue)
            return value;
        } catch (Exception e) {
            LOG.warn("Failed to apply date compatibility for value: {}", value, e);
            return value;
        }
    }

    /**
     * Put a typed value into a Jackson ObjectNode field.
     */
    private static void putValue(ObjectNode node, String fieldName, Object value) {
        if (value == null) {
            node.putNull(fieldName);
        } else if (value instanceof Boolean) {
            node.put(fieldName, (Boolean) value);
        } else if (value instanceof Integer) {
            node.put(fieldName, (Integer) value);
        } else if (value instanceof Long) {
            node.put(fieldName, (Long) value);
        } else if (value instanceof Double) {
            node.put(fieldName, (Double) value);
        } else if (value instanceof BigDecimal) {
            node.put(fieldName, (BigDecimal) value);
        } else {
            node.put(fieldName, value.toString());
        }
    }

    /**
     * Add a typed value to a Jackson ArrayNode.
     */
    private static void addValueToArray(ArrayNode arrayNode, Object value) {
        if (value == null) {
            arrayNode.addNull();
        } else if (value instanceof Boolean) {
            arrayNode.add((Boolean) value);
        } else if (value instanceof Integer) {
            arrayNode.add((Integer) value);
        } else if (value instanceof Long) {
            arrayNode.add((Long) value);
        } else if (value instanceof Double) {
            arrayNode.add((Double) value);
        } else if (value instanceof BigDecimal) {
            arrayNode.add((BigDecimal) value);
        } else {
            arrayNode.add(value.toString());
        }
    }

    /**
     * Parse a JSON string into a Jackson {@link JsonNode}.
     * Falls back to a text node if parsing fails.
     */
    private static JsonNode parseJsonNode(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            LOG.warn("Failed to parse JSON: {}", json, e);
            return MAPPER.getNodeFactory().textNode(json);
        }
    }
}

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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LikePredicate.Operator;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.EsResource;
import org.apache.doris.thrift.TExprOpcode;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Utility class to generate elastic search queries.
 * Some query builders and static helper method have been copied from Elasticsearch
 */
public final class QueryBuilders {

    /**
     * Generate dsl from compound expr.
     **/
    private static QueryBuilder toCompoundEsDsl(Expr expr, List<Expr> notPushDownList,
            Map<String, String> fieldsContext, BuilderOptions builderOptions) {
        CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
        switch (compoundPredicate.getOp()) {
            case AND: {
                QueryBuilder left = toEsDsl(compoundPredicate.getChild(0), notPushDownList, fieldsContext,
                        builderOptions);
                QueryBuilder right = toEsDsl(compoundPredicate.getChild(1), notPushDownList, fieldsContext,
                        builderOptions);
                if (left != null && right != null) {
                    return QueryBuilders.boolQuery().must(left).must(right);
                }
                return null;
            }
            case OR: {
                int beforeSize = notPushDownList.size();
                QueryBuilder left = toEsDsl(compoundPredicate.getChild(0), notPushDownList, fieldsContext,
                        builderOptions);
                QueryBuilder right = toEsDsl(compoundPredicate.getChild(1), notPushDownList, fieldsContext,
                        builderOptions);
                int afterSize = notPushDownList.size();
                if (left != null && right != null) {
                    return QueryBuilders.boolQuery().should(left).should(right);
                }
                // One 'or' association cannot be pushed down and the other cannot be pushed down
                if (afterSize > beforeSize) {
                    notPushDownList.add(compoundPredicate);
                }
                return null;
            }
            case NOT: {
                QueryBuilder child = toEsDsl(compoundPredicate.getChild(0), notPushDownList, fieldsContext,
                        builderOptions);
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
     * Get the expr inside the cast.
     **/
    private static Expr exprWithoutCast(Expr expr) {
        if (expr instanceof CastExpr) {
            return exprWithoutCast(expr.getChild(0));
        }
        return expr;
    }

    public static QueryBuilder toEsDsl(Expr expr) {
        return toEsDsl(expr, new ArrayList<>(), new HashMap<>(),
                BuilderOptions.builder().likePushDown(Boolean.parseBoolean(EsResource.LIKE_PUSH_DOWN_DEFAULT_VALUE))
                        .build());
    }

    private static QueryBuilder parseBinaryPredicate(Expr expr, TExprOpcode opCode, String column,
            boolean needDateCompat) {
        Object value = toDorisLiteral(expr.getChild(1));
        if (needDateCompat) {
            value = compatDefaultDate(value);
        }
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

    private static QueryBuilder parseIsNullPredicate(Expr expr, String column) {
        IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
        if (isNullPredicate.isNotNull()) {
            return QueryBuilders.existsQuery(column);
        }
        return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(column));
    }

    private static QueryBuilder parseLikePredicate(Expr expr, String column) {
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

    private static QueryBuilder parseInPredicate(Expr expr, String column, boolean needDateCompat) {
        InPredicate inPredicate = (InPredicate) expr;
        List<Object> values = inPredicate.getListChildren().stream().map(v -> {
            if (needDateCompat) {
                return compatDefaultDate(v);
            }
            return toDorisLiteral(v);
        }).collect(Collectors.toList());
        if (inPredicate.isNotIn()) {
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(column, values));
        }
        return QueryBuilders.termsQuery(column, values);
    }

    private static QueryBuilder parseFunctionCallExpr(Expr expr) {
        // esquery(k1, '{
        //        "match_phrase": {
        //           "k1": "doris on es"
        //        }
        //    }');
        // The first child k1 compatible with expr syntax
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        String stringValue = functionCallExpr.getChild(1).getStringValue();
        return new QueryBuilders.EsQueryBuilder(stringValue);
    }

    /**
     * Doris expr to es dsl.
     **/
    public static QueryBuilder toEsDsl(Expr expr, List<Expr> notPushDownList, Map<String, String> fieldsContext,
            BuilderOptions builderOptions) {
        if (expr == null) {
            return null;
        }
        // esquery functionCallExpr will be rewritten to castExpr in where clause rewriter,
        // so we get the functionCallExpr here.
        if (expr instanceof CastExpr) {
            return toEsDsl(expr.getChild(0), notPushDownList, fieldsContext, builderOptions);
        }
        // CompoundPredicate, `between` also converted to CompoundPredicate.
        if (expr instanceof CompoundPredicate) {
            return toCompoundEsDsl(expr, notPushDownList, fieldsContext, builderOptions);
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
        // Check whether the date type need compat, it must before keyword replace.
        List<String> needCompatDateFields = builderOptions.getNeedCompatDateFields();
        boolean needDateCompat = needCompatDateFields != null && needCompatDateFields.contains(column);
        // Replace col with col.keyword if mapping exist.
        column = fieldsContext.getOrDefault(column, column);
        if (expr instanceof BinaryPredicate) {
            return parseBinaryPredicate(expr, opCode, column, needDateCompat);
        }
        if (expr instanceof IsNullPredicate) {
            return parseIsNullPredicate(expr, column);
        }
        if (expr instanceof LikePredicate) {
            if (!builderOptions.isLikePushDown()) {
                notPushDownList.add(expr);
                return null;
            } else {
                return parseLikePredicate(expr, column);
            }
        }
        if (expr instanceof InPredicate) {
            return parseInPredicate(expr, column, needDateCompat);
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            // current only esquery functionCallExpr can be push down to ES
            if (!"esquery".equals(functionCallExpr.getFnName().getFunction())) {
                notPushDownList.add(expr);
                return null;
            } else {
                return parseFunctionCallExpr(expr);
            }
        }
        return null;
    }

    private static final DateTimeFormatter dorisFmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter esFmt = ISODateTimeFormat.dateTime();

    private static Object compatDefaultDate(Object value) {
        if (value == null) {
            return null;
        }
        return dorisFmt.parseDateTime(value.toString()).toString(esFmt);
    }

    /**
     * Expr trans to doris literal.
     **/
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

    /**
     * A query that matches on all documents.
     */
    public static MatchAllQueryBuilder matchAllQuery() {
        return new MatchAllQueryBuilder();
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, String value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, int value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, long value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, float value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, double value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, boolean value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, Object value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * Implements the wildcard search query. Supported wildcards are {@code *}, which
     * matches any character sequence (including the empty one), and {@code ?},
     * which matches any single character. Note this query can be slow, as it
     * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
     * a Wildcard term should not start with one of the wildcards {@code *} or
     * {@code ?}.
     *
     * @param name The field name
     * @param query The wildcard query string
     */
    public static WildcardQueryBuilder wildcardQuery(String name, String query) {
        return new WildcardQueryBuilder(name, query);
    }

    /**
     * A Query that matches documents matching boolean combinations of other queries.
     */
    public static BoolQueryBuilder boolQuery() {
        return new BoolQueryBuilder();
    }


    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, Iterable<?> values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filter to filter only documents where a field exists in them.
     *
     * @param name The name of the field
     */
    public static ExistsQueryBuilder existsQuery(String name) {
        return new ExistsQueryBuilder(name);
    }

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param name The field name
     */
    public static RangeQueryBuilder rangeQuery(String name) {
        return new RangeQueryBuilder(name);
    }

    /**
     * Used to pass some parameters to generate the dsl
     **/
    @Builder
    @Data
    public static class BuilderOptions {

        private boolean likePushDown;

        private List<String> needCompatDateFields;
    }


    /**
     * Base class to build various ES queries
     */
    public abstract static class QueryBuilder {

        private static final Logger LOG = LogManager.getLogger(QueryBuilder.class);

        final ObjectMapper mapper = new ObjectMapper();

        /**
         * Convert query to JSON format
         *
         * @param out used to generate JSON elements
         * @throws IOException if IO error occurred
         */
        abstract void toJson(JsonGenerator out) throws IOException;

        /**
         * Convert query to JSON format and catch error.
         **/
        public String toJson() {
            StringWriter writer = new StringWriter();
            try {
                JsonGenerator gen = mapper.getFactory().createGenerator(writer);
                this.toJson(gen);
                gen.flush();
                gen.close();
            } catch (IOException e) {
                LOG.warn("QueryBuilder toJson error", e);
                return null;
            }
            return writer.toString();
        }
    }

    /**
     * Use for esquery, directly save value.
     **/
    public static class EsQueryBuilder extends QueryBuilder {

        private final String value;

        public EsQueryBuilder(String value) {
            this.value = value;
        }

        @Override
        void toJson(JsonGenerator out) throws IOException {
            JsonNode jsonNode = mapper.readTree(value);
            out.writeStartObject();
            Iterator<Entry<String, JsonNode>> values = jsonNode.fields();
            while (values.hasNext()) {
                Entry<String, JsonNode> value = values.next();
                out.writeFieldName(value.getKey());
                out.writeObject(value.getValue());
            }
            out.writeEndObject();
        }
    }

    /**
     * A Query that matches documents matching boolean combinations of other queries.
     */
    public static class BoolQueryBuilder extends QueryBuilder {

        private final List<QueryBuilder> mustClauses = new ArrayList<>();
        private final List<QueryBuilder> mustNotClauses = new ArrayList<>();
        private final List<QueryBuilder> filterClauses = new ArrayList<>();
        private final List<QueryBuilder> shouldClauses = new ArrayList<>();

        /**
         * Use for EsScanNode generate dsl.
         **/
        public BoolQueryBuilder must(QueryBuilder queryBuilder) {
            Objects.requireNonNull(queryBuilder);
            mustClauses.add(queryBuilder);
            return this;
        }

        BoolQueryBuilder filter(QueryBuilder queryBuilder) {
            Objects.requireNonNull(queryBuilder);
            filterClauses.add(queryBuilder);
            return this;
        }

        BoolQueryBuilder mustNot(QueryBuilder queryBuilder) {
            Objects.requireNonNull(queryBuilder);
            mustNotClauses.add(queryBuilder);
            return this;
        }

        BoolQueryBuilder should(QueryBuilder queryBuilder) {
            Objects.requireNonNull(queryBuilder);
            shouldClauses.add(queryBuilder);
            return this;
        }

        @Override
        protected void toJson(JsonGenerator out) throws IOException {
            out.writeStartObject();
            out.writeFieldName("bool");
            out.writeStartObject();
            writeJsonArray("must", mustClauses, out);
            writeJsonArray("filter", filterClauses, out);
            writeJsonArray("must_not", mustNotClauses, out);
            writeJsonArray("should", shouldClauses, out);
            out.writeEndObject();
            out.writeEndObject();
        }

        private void writeJsonArray(String field, List<QueryBuilder> clauses, JsonGenerator out) throws IOException {
            if (clauses.isEmpty()) {
                return;
            }

            if (clauses.size() == 1) {
                out.writeFieldName(field);
                clauses.get(0).toJson(out);
            } else {
                out.writeArrayFieldStart(field);
                for (QueryBuilder clause : clauses) {
                    clause.toJson(out);
                }
                out.writeEndArray();
            }
        }
    }

    /**
     * A Query that matches documents containing a term
     */
    static class TermQueryBuilder extends QueryBuilder {
        private final String fieldName;
        private final Object value;

        private TermQueryBuilder(final String fieldName, final Object value) {
            this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
            this.value = Objects.requireNonNull(value, "value");
        }

        @Override
        void toJson(final JsonGenerator out) throws IOException {
            out.writeStartObject();
            out.writeFieldName("term");
            out.writeStartObject();
            out.writeFieldName(fieldName);
            writeObject(out, value);
            out.writeEndObject();
            out.writeEndObject();
        }
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     */
    static class TermsQueryBuilder extends QueryBuilder {
        private final String fieldName;
        private final Iterable<?> values;

        private TermsQueryBuilder(final String fieldName, final Iterable<?> values) {
            this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
            this.values = Objects.requireNonNull(values, "values");
        }

        @Override
        void toJson(final JsonGenerator out) throws IOException {
            out.writeStartObject();
            out.writeFieldName("terms");
            out.writeStartObject();
            out.writeFieldName(fieldName);
            out.writeStartArray();
            for (Object value : values) {
                writeObject(out, value);
            }
            out.writeEndArray();
            out.writeEndObject();
            out.writeEndObject();
        }
    }

    /**
     * A Query that matches documents within an range of terms
     */
    static class RangeQueryBuilder extends QueryBuilder {

        private final String field;

        private Object lt;
        private boolean lte;
        private Object gt;
        private boolean gte;

        private String format;

        private RangeQueryBuilder(final String field) {
            this.field = Objects.requireNonNull(field, "fieldName");
        }

        private RangeQueryBuilder to(Object value, boolean lte) {
            this.lt = Objects.requireNonNull(value, "value");
            this.lte = lte;
            return this;
        }

        private RangeQueryBuilder from(Object value, boolean gte) {
            this.gt = Objects.requireNonNull(value, "value");
            this.gte = gte;
            return this;
        }

        RangeQueryBuilder lt(Object value) {
            return to(value, false);
        }

        RangeQueryBuilder lte(Object value) {
            return to(value, true);
        }

        RangeQueryBuilder gt(Object value) {
            return from(value, false);
        }

        RangeQueryBuilder gte(Object value) {
            return from(value, true);
        }

        RangeQueryBuilder format(String format) {
            this.format = format;
            return this;
        }

        @Override
        void toJson(final JsonGenerator out) throws IOException {
            if (lt == null && gt == null) {
                throw new IllegalStateException("Either lower or upper bound should be provided");
            }

            out.writeStartObject();
            out.writeFieldName("range");
            out.writeStartObject();
            out.writeFieldName(field);
            out.writeStartObject();

            if (gt != null) {
                final String op = gte ? "gte" : "gt";
                out.writeFieldName(op);
                writeObject(out, gt);
            }

            if (lt != null) {
                final String op = lte ? "lte" : "lt";
                out.writeFieldName(op);
                writeObject(out, lt);
            }

            if (format != null) {
                out.writeStringField("format", format);
            }

            out.writeEndObject();
            out.writeEndObject();
            out.writeEndObject();
        }
    }


    /**
     * Supported wildcards are {@code *}, which
     * matches any character sequence (including the empty one), and {@code ?},
     * which matches any single character
     */
    static class WildcardQueryBuilder extends QueryBuilder {

        private final String fieldName;
        private final String value;


        public WildcardQueryBuilder(String fieldName, String value) {
            this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
            this.value = Objects.requireNonNull(value, "value");
        }

        @Override
        void toJson(JsonGenerator out) throws IOException {
            out.writeStartObject();
            out.writeFieldName("wildcard");
            out.writeStartObject();
            out.writeFieldName(fieldName);
            out.writeString(value);
            out.writeEndObject();
            out.writeEndObject();
        }
    }

    /**
     * Query that only match on documents that the fieldName has a value in them
     */
    static class ExistsQueryBuilder extends QueryBuilder {

        private final String fieldName;

        ExistsQueryBuilder(final String fieldName) {
            this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
        }

        @Override
        void toJson(JsonGenerator out) throws IOException {
            out.writeStartObject();
            out.writeFieldName("exists");
            out.writeStartObject();
            out.writeStringField("field", fieldName);
            out.writeEndObject();
            out.writeEndObject();

        }
    }

    /**
     * A query that matches on all documents
     */
    static class MatchAllQueryBuilder extends QueryBuilder {

        private MatchAllQueryBuilder() {
        }

        @Override
        void toJson(final JsonGenerator out) throws IOException {
            out.writeStartObject();
            out.writeFieldName("match_all");
            out.writeStartObject();
            out.writeEndObject();
            out.writeEndObject();
        }
    }

    /**
     * Write (scalar) value (string, number, boolean or null) to json format
     *
     * @param out source target
     * @param value value to write
     * @throws IOException if error
     */
    private static void writeObject(JsonGenerator out, Object value) throws IOException {
        out.writeObject(value);
    }
}

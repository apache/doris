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

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Utility class to generate elastic search queries.
 * Some query builders and static helper method have been copied from Elasticsearch
 */
public final class QueryBuilders {

    /**
     * A query that matches on all documents.
     */
    public static MatchAllQueryBuilder matchAllQuery() {
        return new MatchAllQueryBuilder();
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, String value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, int value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, long value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, float value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, double value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, boolean value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
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
     * @param name  The field name
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
     * @param name   The field name
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
     * Base class to build various ES queries
     */
    abstract static class QueryBuilder {

        /**
         * Convert query to JSON format
         *
         * @param out used to generate JSON elements
         * @throws IOException if IO error occurred
         */
        abstract void toJson(JsonGenerator out) throws IOException;
    }

    /**
     * A Query that matches documents matching boolean combinations of other queries.
     */
    static class BoolQueryBuilder extends QueryBuilder {

        private final List<QueryBuilder> mustClauses = new ArrayList<>();
        private final List<QueryBuilder> mustNotClauses = new ArrayList<>();
        private final List<QueryBuilder> filterClauses = new ArrayList<>();
        private final List<QueryBuilder> shouldClauses = new ArrayList<>();

        BoolQueryBuilder must(QueryBuilder queryBuilder) {
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

        private void writeJsonArray(String field, List<QueryBuilder> clauses, JsonGenerator out)
                throws IOException {
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
     * @param out   source target
     * @param value value to write
     * @throws IOException if error
     */
    private static void writeObject(JsonGenerator out, Object value) throws IOException {
        out.writeObject(value);
    }
}

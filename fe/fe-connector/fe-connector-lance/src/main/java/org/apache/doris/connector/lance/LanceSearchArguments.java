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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.search.ConnectorSearchFunction;
import org.apache.doris.connector.spi.search.ConnectorSearchRequest;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFtsMatchOperator;
import org.apache.doris.thrift.TFtsQueryType;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TSearchFilter;
import org.apache.doris.thrift.TSearchFilterFormat;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The arguments of one {@code vector_search} or {@code full_text_search} over a Lance table, validated and turned
 * into the search request the BE executes, with the names, defaults, ranges and messages they have on branch-4.x.
 *
 * <p>{@link #parse} validates what the table's schema does not decide, before the table is read; {@link #bind}
 * resolves the column in the schema of the version the search reads.</p>
 *
 * <p>The request carries the logical {@code top_k} and {@code offset}: each range of the scan asks for
 * {@code top_k + offset} candidates of its own ({@link LanceSearchTableHandle#splitRequest()}) and the engine's TopN
 * keeps the best of them.</p>
 *
 * <p>The TVF {@code filter} is a Lance SQL string that lance-c evaluates, before the candidates are chosen with
 * {@code "prefilter" = "true"}, after it otherwise; the WHERE written on the function stays a Doris filter on the
 * candidates.
 * While a statement is prepared, {@code vector_search} may leave {@code query_vector}, {@code top_k},
 * {@code offset}, {@code filter} and the two distance bounds to parameters: none of them changes the columns.</p>
 */
final class LanceSearchArguments {
    static final String COLUMN = "column";
    static final String TOP_K = "top_k";
    static final String OFFSET = "offset";
    static final String FILTER = "filter";
    static final String PREFILTER = "prefilter";
    static final String QUERY_VECTOR = "query_vector";
    static final String METRIC = "metric";
    static final String QUERY_PARALLELISM = "query_parallelism";
    static final String NPROBES = "nprobes";
    static final String REFINE_FACTOR = "refine_factor";
    static final String EF = "ef";
    static final String USE_INDEX = "use_index";
    static final String DISTANCE_LOWER_BOUND = "distance_lower_bound";
    static final String DISTANCE_UPPER_BOUND = "distance_upper_bound";
    static final String QUERY = "query";
    static final String COVERAGE_MODE = "coverage_mode";
    static final String QUERY_TYPE = "query_type";
    static final String OPERATOR = "operator";
    static final String MAX_FUZZY_DISTANCE = "max_fuzzy_distance";
    static final String SLOP = "slop";

    /** lance-c takes {@code top_k + offset} as a uint32. */
    private static final long UINT32_MAX = 0xFFFF_FFFFL;
    private static final Set<String> VECTOR_ARGUMENTS = setOf(COLUMN, QUERY_VECTOR, TOP_K, OFFSET, METRIC, FILTER,
            PREFILTER, NPROBES, REFINE_FACTOR, EF, USE_INDEX, QUERY_PARALLELISM, DISTANCE_LOWER_BOUND,
            DISTANCE_UPPER_BOUND);
    private static final Set<String> FULL_TEXT_ARGUMENTS = setOf(COLUMN, QUERY, TOP_K, OFFSET, FILTER, PREFILTER,
            COVERAGE_MODE, QUERY_TYPE, OPERATOR, MAX_FUZZY_DISTANCE, SLOP);
    /** The vector_search arguments a prepared statement may leave to parameters; none changes the columns. */
    private static final Set<String> VECTOR_DEFERRABLE_ARGUMENTS = setOf(QUERY_VECTOR, TOP_K, OFFSET, FILTER,
            DISTANCE_LOWER_BOUND, DISTANCE_UPPER_BOUND);

    private final ConnectorSearchFunction function;
    /** Every argument but the column and the query vector, which only the schema can check. */
    private final TExternalSearchRequest request;
    private final String column;
    /** The query vector's JSON; null for a full-text search, or while a prepared statement defers it. */
    private final String queryVector;
    private final int refineFactor;
    private final boolean searchesIndex;
    private final long topK;
    private final long offset;

    private LanceSearchArguments(ConnectorSearchFunction function, TExternalSearchRequest request, String column,
            String queryVector, int refineFactor, boolean searchesIndex, long topK, long offset) {
        this.function = function;
        this.request = request;
        this.column = column;
        this.queryVector = queryVector;
        this.refineFactor = refineFactor;
        this.searchesIndex = searchesIndex;
        this.topK = topK;
        this.offset = offset;
    }

    /**
     * Validates every argument of {@code request} that the table's schema does not decide, so that an invalid one
     * fails before the table is read.
     *
     * @throws DorisConnectorException with a message for the user when an argument is missing, unknown or invalid
     */
    static LanceSearchArguments parse(ConnectorSearchRequest request) {
        return request.getFunction() == ConnectorSearchFunction.VECTOR_SEARCH
                ? parseVectorSearch(request) : parseFullTextSearch(request);
    }

    /**
     * Resolves the searched column in {@code schema}, the schema of the version the search reads, and completes the
     * request: a vector search's query vector must fit the column.
     *
     * @throws DorisConnectorException with a message for the user when the column or the query vector is invalid
     */
    Bound bind(Schema schema) {
        TExternalSearchRequest bound = request.deepCopy();
        if (function == ConnectorSearchFunction.FULL_TEXT_SEARCH) {
            Field field = findStringField(schema, column);
            bound.getSearchQuery().getFullTextSearch().setColumn(field.getName());
            return new Bound(bound, field);
        }
        TVectorSearchParams vectorParams = bound.getSearchQuery().getVectorSearch();
        Field vectorField = requireSearchColumn(schema, column, "vector");
        // Rejected while a statement is prepared too: lance-c only applies bounds to a single-vector search.
        if ((vectorParams.isSetDistanceLowerBound() || vectorParams.isSetDistanceUpperBound())
                && LanceVectorQuery.isMultiVector(vectorField)) {
            throw new DorisConnectorException("Distance bounds are not supported for Lance multi-vector search");
        }
        vectorParams.setColumn(vectorField.getName());
        if (queryVector == null) {
            LanceVectorQuery.checkSearchable(vectorField);
            return new Bound(bound, vectorField);
        }
        TSearchVector encoded = LanceVectorQuery.parseAndEncode(vectorField, queryVector);
        // The checks that need the vector itself wait for the execution that supplies it.
        LanceVectorQuery.validateMultiVectorBudget(encoded, topK, offset, refineFactor);
        if (encoded.isSetNumVectors() && vectorParams.getMetric() == TVectorMetric.HAMMING) {
            throw new DorisConnectorException("Lance multi-vector search supports l2, cosine, and dot metrics");
        }
        vectorParams.setQueryVector(encoded);
        return new Bound(bound, vectorField);
    }

    /**
     * Whether the search may read an index, so that planning needs the searched field's Lance field id: always for
     * a full-text search, unless {@code use_index} is false for a vector search.
     */
    boolean searchesIndex() {
        return searchesIndex;
    }

    long getTopK() {
        return topK;
    }

    long getOffset() {
        return offset;
    }

    /** A search whose column is resolved in the schema of the version it reads. */
    static final class Bound {
        private final TExternalSearchRequest request;
        private final Field searchField;

        private Bound(TExternalSearchRequest request, Field searchField) {
            this.request = request;
            this.searchField = searchField;
        }

        /** The logical search: the whole search's {@code top_k} and {@code offset}, never a range's. */
        TExternalSearchRequest getRequest() {
            return request;
        }

        /** The searched column, with its name as the Lance schema spells it, which the BE is sent. */
        Field getSearchField() {
            return searchField;
        }
    }

    private static LanceSearchArguments parseVectorSearch(ConnectorSearchRequest request) {
        String functionName = request.getFunction().getFunctionName();
        Map<String, String> arguments = request.getArguments();
        checkArgumentNames(request, VECTOR_ARGUMENTS);
        for (String deferred : request.getDeferredArguments()) {
            if (!VECTOR_DEFERRABLE_ARGUMENTS.contains(deferred)) {
                throw new DorisConnectorException(functionName + " property '" + deferred
                        + "' must be constant in a prepared statement");
            }
        }
        TVectorSearchParams vectorParams = parseDistanceBounds(arguments);
        boolean useIndex = !arguments.containsKey(USE_INDEX) || parseBoolean(arguments.get(USE_INDEX), USE_INDEX);
        long topK = parseTopK(request);
        long offset = parseOffset(request, topK);
        String column = required(arguments, COLUMN, functionName);
        String queryVector = request.getDeferredArguments().contains(QUERY_VECTOR) ? null
                : required(arguments, QUERY_VECTOR, functionName);
        vectorParams.setTopK(topK).setOffset(offset);
        // Pin the default on every range; Lance would otherwise take the metric of the index it searches.
        vectorParams.setMetric(arguments.containsKey(METRIC)
                ? LanceVectorQuery.parseMetric(arguments.get(METRIC)) : TVectorMetric.L2);
        TExternalSearchRequest searchRequest = new TExternalSearchRequest()
                .setSchemaVersion(1)
                .setSearchQuery(TExternalSearchQuery.vector_search(vectorParams));
        TVectorSearchOptions options = buildVectorSearchOptions(arguments, useIndex);
        if (options != null) {
            searchRequest.setVectorSearchOptions(options);
        }
        setSearchFilter(searchRequest, arguments);
        int refineFactor = options != null && options.isSetRefineFactor() ? options.getRefineFactor() : 1;
        return new LanceSearchArguments(request.getFunction(), searchRequest, column, queryVector, refineFactor,
                useIndex, topK, offset);
    }

    private static LanceSearchArguments parseFullTextSearch(ConnectorSearchRequest request) {
        String functionName = request.getFunction().getFunctionName();
        Map<String, String> arguments = request.getArguments();
        checkArgumentNames(request, FULL_TEXT_ARGUMENTS);
        if (!request.getDeferredArguments().isEmpty()) {
            throw new DorisConnectorException(functionName + " properties must be constant in a prepared statement");
        }
        long topK = parseTopK(request);
        long offset = parseOffset(request, topK);
        String column = required(arguments, COLUMN, functionName);
        String query = required(arguments, QUERY, functionName);
        if (query.indexOf('\0') >= 0) {
            throw new DorisConnectorException("'query' must not contain an embedded NUL byte");
        }
        TFtsQueryType queryType = parseQueryType(arguments.getOrDefault(QUERY_TYPE, "match"));
        TFullTextSearchParams fullTextParams = new TFullTextSearchParams()
                .setQuery(query)
                .setTopK(topK)
                .setOffset(offset)
                .setQueryType(queryType)
                .setCoverageMode(parseCoverageMode(arguments.getOrDefault(COVERAGE_MODE, "strict")));
        if (queryType == TFtsQueryType.MATCH) {
            if (arguments.containsKey(SLOP)) {
                throw new DorisConnectorException("'slop' is only valid for query_type=phrase");
            }
            int maxFuzzyDistance = (int) parseLong(arguments.getOrDefault(MAX_FUZZY_DISTANCE, "0"),
                    MAX_FUZZY_DISTANCE, 0, Integer.MAX_VALUE);
            if (maxFuzzyDistance != 0) {
                throw new DorisConnectorException("'max_fuzzy_distance' must be 0 because the bundled "
                        + "Lance-C does not yet support prepared fuzzy matching");
            }
            fullTextParams.setMatchOperator(parseMatchOperator(arguments.getOrDefault(OPERATOR, "or")))
                    .setMaxFuzzyDistance(maxFuzzyDistance);
        } else {
            if (arguments.containsKey(OPERATOR)) {
                throw new DorisConnectorException("'operator' is only valid for query_type=match");
            }
            if (arguments.containsKey(MAX_FUZZY_DISTANCE)) {
                throw new DorisConnectorException("'max_fuzzy_distance' is only valid for query_type=match");
            }
            fullTextParams.setPhraseSlop((int) parseLong(arguments.getOrDefault(SLOP, "0"), SLOP, 0,
                    Integer.MAX_VALUE));
        }
        TExternalSearchRequest searchRequest = new TExternalSearchRequest()
                .setSchemaVersion(1)
                .setSearchQuery(TExternalSearchQuery.full_text_search(fullTextParams));
        setSearchFilter(searchRequest, arguments);
        return new LanceSearchArguments(request.getFunction(), searchRequest, column, null, 1, true, topK, offset);
    }

    private static void checkArgumentNames(ConnectorSearchRequest request, Set<String> allowed) {
        for (String name : request.getArguments().keySet()) {
            checkArgumentName(name, allowed, request.getFunction());
        }
        for (String name : request.getDeferredArguments()) {
            checkArgumentName(name, allowed, request.getFunction());
        }
    }

    private static void checkArgumentName(String name, Set<String> allowed, ConnectorSearchFunction function) {
        if (!allowed.contains(name)) {
            throw new DorisConnectorException("'" + name + "' is an invalid property for "
                    + function.getFunctionName() + "()");
        }
    }

    /** {@code top_k}, 10 by default; 1 while a prepared statement leaves it to a parameter. */
    private static long parseTopK(ConnectorSearchRequest request) {
        return request.getDeferredArguments().contains(TOP_K) ? 1
                : parseLong(request.getArguments().getOrDefault(TOP_K, "10"), TOP_K, 1, Long.MAX_VALUE);
    }

    /** {@code offset}, 0 by default and while a prepared statement leaves it to a parameter. */
    private static long parseOffset(ConnectorSearchRequest request, long topK) {
        long offset = request.getDeferredArguments().contains(OFFSET) ? 0
                : parseLong(request.getArguments().getOrDefault(OFFSET, "0"), OFFSET, 0, Long.MAX_VALUE);
        if (offset > UINT32_MAX || topK > UINT32_MAX - offset) {
            throw new DorisConnectorException("'top_k + offset' must not exceed " + UINT32_MAX);
        }
        return offset;
    }

    /**
     * The TVF {@code filter}, and whether lance-c applies it before choosing the candidates ({@code prefilter},
     * false by default: after). A prepared statement's deferred filter is left out until its execution.
     */
    private static void setSearchFilter(TExternalSearchRequest searchRequest, Map<String, String> arguments) {
        if (arguments.containsKey(FILTER)) {
            searchRequest.setSearchFilter(new TSearchFilter()
                    .setFormat(TSearchFilterFormat.SQL)
                    .setPayload(encodeSqlFilter(arguments.get(FILTER))));
        }
        searchRequest.setPrefilter(arguments.containsKey(PREFILTER)
                && parseBoolean(arguments.get(PREFILTER), PREFILTER));
    }

    static TVectorSearchParams parseDistanceBounds(Map<String, String> arguments) {
        TVectorSearchParams vector = new TVectorSearchParams();
        if (arguments.containsKey(DISTANCE_LOWER_BOUND)) {
            vector.setDistanceLowerBound(parseDistanceBound(arguments.get(DISTANCE_LOWER_BOUND),
                    DISTANCE_LOWER_BOUND));
        }
        if (arguments.containsKey(DISTANCE_UPPER_BOUND)) {
            vector.setDistanceUpperBound(parseDistanceBound(arguments.get(DISTANCE_UPPER_BOUND),
                    DISTANCE_UPPER_BOUND));
        }
        // Compare after FLOAT rounding, matching the native API even for nearly equal inputs.
        if (vector.isSetDistanceLowerBound() && vector.isSetDistanceUpperBound()
                && vector.getDistanceLowerBound() >= vector.getDistanceUpperBound()) {
            throw new DorisConnectorException(
                    "distance_lower_bound must be less than distance_upper_bound at FLOAT precision");
        }
        return vector;
    }

    private static float parseDistanceBound(String value, String property) {
        try {
            float bound = Float.parseFloat(value);
            if (Float.isFinite(bound)) {
                return bound;
            }
        } catch (NumberFormatException ignored) {
            // Report the SQL property rather than a Java parsing exception.
        }
        throw new DorisConnectorException("'" + property + "' must be a finite FLOAT");
    }

    /** The vector search options the user set; null when none is set, so the request carries none. */
    static TVectorSearchOptions buildVectorSearchOptions(Map<String, String> arguments, boolean useIndex) {
        TVectorSearchOptions options = new TVectorSearchOptions();
        boolean configured = false;
        if (arguments.containsKey(QUERY_PARALLELISM)) {
            options.setQueryParallelism((int) parseLong(arguments.get(QUERY_PARALLELISM), QUERY_PARALLELISM, -1,
                    Integer.MAX_VALUE));
            configured = true;
        }
        if (arguments.containsKey(NPROBES)) {
            options.setNprobes(parsePositiveInt(arguments.get(NPROBES), NPROBES));
            configured = true;
        }
        if (arguments.containsKey(REFINE_FACTOR)) {
            options.setRefineFactor(parsePositiveInt(arguments.get(REFINE_FACTOR), REFINE_FACTOR));
            configured = true;
        }
        if (arguments.containsKey(EF)) {
            options.setEf(parsePositiveInt(arguments.get(EF), EF));
            configured = true;
        }
        if (arguments.containsKey(USE_INDEX)) {
            options.setUseIndex(useIndex);
            configured = true;
        }
        return configured ? options : null;
    }

    /** The column {@code column} names, matched case-insensitively; its Lance spelling goes to the BE. */
    static Field requireSearchColumn(Schema schema, String column, String searchDescription) {
        Field match = null;
        for (Field field : schema.getFields()) {
            if (field.getName().equalsIgnoreCase(column)) {
                if (match != null) {
                    throw new DorisConnectorException("Lance " + searchDescription + " column '" + column
                            + "' is ambiguous under case-insensitive matching");
                }
                match = field;
            }
        }
        if (match == null) {
            throw new DorisConnectorException("Lance " + searchDescription + " column '" + column
                    + "' does not exist");
        }
        return match;
    }

    static Field findStringField(Schema schema, String column) {
        Field match = requireSearchColumn(schema, column, "full-text");
        ArrowType.ArrowTypeID typeId = match.getType().getTypeID();
        if (typeId != ArrowType.ArrowTypeID.Utf8 && typeId != ArrowType.ArrowTypeID.LargeUtf8) {
            throw new DorisConnectorException("Lance full-text column '" + match.getName() + "' must be STRING");
        }
        return match;
    }

    static TFtsCoverageMode parseCoverageMode(String value) {
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "strict":
                return TFtsCoverageMode.STRICT;
            case "index_only":
            case "index-only":
                return TFtsCoverageMode.INDEX_ONLY;
            default:
                throw new DorisConnectorException("Unsupported FTS coverage_mode '" + value
                        + "': expected strict or index_only");
        }
    }

    static TFtsQueryType parseQueryType(String value) {
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "match":
                return TFtsQueryType.MATCH;
            case "phrase":
                return TFtsQueryType.PHRASE;
            default:
                throw new DorisConnectorException("Unsupported FTS query_type '" + value
                        + "': expected match or phrase");
        }
    }

    static TFtsMatchOperator parseMatchOperator(String value) {
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "or":
                return TFtsMatchOperator.OR;
            case "and":
                return TFtsMatchOperator.AND;
            default:
                throw new DorisConnectorException("Unsupported FTS operator '" + value
                        + "': expected 'or' or 'and'");
        }
    }

    /** The filter's UTF-8 bytes; lance-c reads it as a NUL-terminated string, so it cannot hold a NUL. */
    static byte[] encodeSqlFilter(String filter) {
        if (filter.trim().isEmpty()) {
            throw new DorisConnectorException("'filter' must not be empty");
        }
        if (filter.indexOf('\0') >= 0) {
            throw new DorisConnectorException("'filter' must not contain an embedded NUL byte");
        }
        return filter.getBytes(StandardCharsets.UTF_8);
    }

    private static String required(Map<String, String> arguments, String name, String functionName) {
        String value = arguments.get(name);
        if (value == null || value.trim().isEmpty()) {
            throw new DorisConnectorException("Missing required " + functionName + "() property '" + name + "'");
        }
        return value.trim();
    }

    static long parseLong(String value, String property, long min, long max) {
        long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new DorisConnectorException("'" + property + "' must be an integer", e);
        }
        if (parsed < min || parsed > max) {
            throw new DorisConnectorException("'" + property + "' must be between " + min + " and " + max);
        }
        return parsed;
    }

    private static int parsePositiveInt(String value, String property) {
        return (int) parseLong(value, property, 1, Integer.MAX_VALUE);
    }

    static boolean parseBoolean(String value, String property) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new DorisConnectorException("'" + property + "' must be 'true' or 'false'");
    }

    private static Set<String> setOf(String... names) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(names)));
    }
}

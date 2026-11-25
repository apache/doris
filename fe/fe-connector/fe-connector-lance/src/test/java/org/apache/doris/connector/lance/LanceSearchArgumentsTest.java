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
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFtsMatchOperator;
import org.apache.doris.thrift.TFtsQueryType;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TSearchFilterFormat;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The arguments of vector_search and full_text_search over a Lance table: their names, defaults, ranges and
 * messages, the request they make, and which of them a prepared statement may leave to parameters. The argument
 * cases of branch-4.2's VectorSearchTableValuedFunctionTest (with #68815's distance bounds) and
 * FullTextSearchTableValuedFunctionTest, and #68813's prefilter; the {@code table} argument is the engine's.
 */
class LanceSearchArgumentsTest {
    private static final Schema SCHEMA = new Schema(Arrays.asList(
            Field.nullable("id", new ArrowType.Int(64, true)),
            new Field("Embedding", FieldType.nullable(new ArrowType.FixedSizeList(2)), Collections.singletonList(
                    Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)))),
            new Field("multi", FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(
                    new Field("item", FieldType.notNullable(new ArrowType.FixedSizeList(2)),
                            Collections.singletonList(Field.nullable("item",
                                    new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)))))),
            Field.nullable("Body", ArrowType.Utf8.INSTANCE)));

    @Test
    void vectorSearchHasTheBranch4Defaults() {
        LanceSearchArguments arguments = LanceSearchArguments.parse(vector(map("column", "embedding",
                "query_vector", "[1, 2]")));
        LanceSearchArguments.Bound bound = arguments.bind(SCHEMA);

        TExternalSearchRequest request = bound.getRequest();
        TVectorSearchParams vector = request.getSearchQuery().getVectorSearch();
        // The column goes to the BE as Lance spells it.
        Assertions.assertEquals("Embedding", vector.getColumn());
        Assertions.assertEquals("Embedding", bound.getSearchField().getName());
        Assertions.assertEquals(10, vector.getTopK());
        Assertions.assertEquals(0, vector.getOffset());
        Assertions.assertEquals(TVectorMetric.L2, vector.getMetric());
        Assertions.assertEquals(2, vector.getQueryVector().getDimension());
        Assertions.assertFalse(vector.isSetDistanceLowerBound());
        Assertions.assertFalse(request.isSetVectorSearchOptions());
        Assertions.assertFalse(request.isSetSearchFilter());
        // Without "prefilter" the TVF filter applies after the candidates are chosen, as on branch-4.x since #68813.
        Assertions.assertTrue(request.isSetPrefilter());
        Assertions.assertFalse(request.isPrefilter());
        Assertions.assertEquals(1, request.getSchemaVersion());
        Assertions.assertTrue(arguments.searchesIndex());
        Assertions.assertEquals(10, arguments.getTopK());
        Assertions.assertEquals(0, arguments.getOffset());
    }

    @Test
    void vectorSearchCarriesItsOptionsFilterAndPrefilter() {
        TExternalSearchRequest request = LanceSearchArguments.parse(vector(map("column", "embedding",
                "query_vector", "[1, 2]", "top_k", "5", "offset", "2", "metric", "cosine", "nprobes", "4",
                "refine_factor", "2", "ef", "32", "use_index", "FALSE", "query_parallelism", "-1",
                "filter", "category = 'book'", "prefilter", "True"))).bind(SCHEMA).getRequest();

        TVectorSearchParams vector = request.getSearchQuery().getVectorSearch();
        Assertions.assertEquals(5, vector.getTopK());
        Assertions.assertEquals(2, vector.getOffset());
        Assertions.assertEquals(TVectorMetric.COSINE, vector.getMetric());
        TVectorSearchOptions options = request.getVectorSearchOptions();
        Assertions.assertEquals(4, options.getNprobes());
        Assertions.assertEquals(2, options.getRefineFactor());
        Assertions.assertEquals(32, options.getEf());
        Assertions.assertFalse(options.isUseIndex());
        Assertions.assertEquals(-1, options.getQueryParallelism());
        Assertions.assertEquals(TSearchFilterFormat.SQL, request.getSearchFilter().getFormat());
        Assertions.assertArrayEquals("category = 'book'".getBytes(StandardCharsets.UTF_8),
                request.getSearchFilter().getPayload());
        Assertions.assertTrue(request.isPrefilter());
        Assertions.assertFalse(LanceSearchArguments.parse(vector(map("column", "embedding", "query_vector", "[1, 2]",
                "use_index", "false"))).searchesIndex());
    }

    @Test
    void queryParallelismRoundTripsAndIsBounded() throws Exception {
        Assertions.assertNull(LanceSearchArguments.buildVectorSearchOptions(Collections.emptyMap(), true));
        Assertions.assertFalse(LanceSearchArguments.buildVectorSearchOptions(map("nprobes", "4"), true)
                .isSetQueryParallelism());
        for (String value : new String[] {"-1", "0", "1", "4", "2147483647"}) {
            TVectorSearchOptions options = LanceSearchArguments.buildVectorSearchOptions(
                    map("query_parallelism", value), true);
            TVectorSearchOptions decoded = new TVectorSearchOptions();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(options));
            Assertions.assertEquals(Integer.parseInt(value), decoded.getQueryParallelism());
            Assertions.assertFalse(decoded.isSetNprobes());
        }
        for (String value : new String[] {"-2", "2147483648", "1.5", "abc", ""}) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> LanceSearchArguments.buildVectorSearchOptions(map("query_parallelism", value), true));
            Assertions.assertTrue(error.getMessage().contains("query_parallelism"), error.getMessage());
        }
    }

    @Test
    void distanceBoundsRoundTripAsFloats() throws Exception {
        Assertions.assertFalse(LanceSearchArguments.parseDistanceBounds(Collections.emptyMap())
                .isSetDistanceLowerBound());
        for (Map<String, String> arguments : Arrays.asList(map("distance_lower_bound", "-0.5"),
                map("distance_upper_bound", "0"), map("distance_lower_bound", "0.1", "distance_upper_bound", "1.5"))) {
            TVectorSearchParams params = LanceSearchArguments.parseDistanceBounds(arguments);
            TVectorSearchParams decoded = new TVectorSearchParams();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(params));
            Assertions.assertEquals(params, decoded);
            Assertions.assertEquals(arguments.containsKey("distance_lower_bound"), decoded.isSetDistanceLowerBound());
            Assertions.assertEquals(arguments.containsKey("distance_upper_bound"), decoded.isSetDistanceUpperBound());
            if (decoded.isSetDistanceLowerBound()) {
                Assertions.assertEquals((double) Float.parseFloat(arguments.get("distance_lower_bound")),
                        decoded.getDistanceLowerBound(), 0.0);
            }
        }
    }

    @Test
    void rejectsDistanceBoundsThatAreNotFiniteOrNotAnEmptyRangeBeforeReadingTheTable() {
        for (String value : new String[] {"NaN", "Infinity", "-Infinity", "1e100", "invalid", ""}) {
            assertParseFails(vector(map("distance_upper_bound", value)), "finite FLOAT");
        }
        for (String upper : new String[] {"0", "-1", "0.00000000000000000000000000000000000000000000001"}) {
            assertParseFails(vector(map("distance_lower_bound", "0", "distance_upper_bound", upper)),
                    "must be less than");
        }
        // Equal at FLOAT precision.
        assertParseFails(vector(map("distance_lower_bound", "1", "distance_upper_bound", "1.00000001")),
                "must be less than");
    }

    @Test
    void multiVectorSearchTakesNoDistanceBoundsNorHamming() {
        assertBindFails(vector(map("column", "multi", "query_vector", "[[1, 2]]", "distance_upper_bound", "1")),
                "Distance bounds are not supported for Lance multi-vector search");
        assertBindFails(vector(map("column", "multi", "query_vector", "[[1, 2]]", "metric", "hamming")),
                "supports l2, cosine, and dot metrics");
        Assertions.assertEquals(1, LanceSearchArguments.parse(vector(map("column", "multi",
                "query_vector", "[[1, 2]]"))).bind(SCHEMA).getRequest().getSearchQuery().getVectorSearch()
                .getQueryVector().getNumVectors());
    }

    @Test
    void rejectsMissingUnknownAndInvalidArguments() {
        assertParseFails(vector(map("query_vector", "[1, 2]")), "Missing required vector_search() property 'column'");
        assertParseFails(vector(map("column", "embedding", "query_vector", "  ")),
                "Missing required vector_search() property 'query_vector'");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "topk", "5")),
                "'topk' is an invalid property for vector_search()");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "top_k", "0")),
                "'top_k' must be between 1 and 9223372036854775807");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "top_k", " 5")),
                "'top_k' must be an integer");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "offset", "-1")),
                "'offset' must be between 0");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "top_k", "4294967295",
                "offset", "1")), "'top_k + offset' must not exceed 4294967295");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "use_index", "yes")),
                "'use_index' must be 'true' or 'false'");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "prefilter", "maybe")),
                "'prefilter' must be 'true' or 'false'");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "nprobes", "0")),
                "'nprobes' must be between 1 and 2147483647");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "metric", "manhattan")),
                "Unsupported vector metric 'manhattan'");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "filter", "  ")),
                "'filter' must not be empty");
        assertParseFails(vector(map("column", "embedding", "query_vector", "[1, 2]", "filter", "a = 1\0 OR true")),
                "'filter' must not contain an embedded NUL byte");
        assertBindFails(vector(map("column", "missing", "query_vector", "[1, 2]")),
                "Lance vector column 'missing' does not exist");
        assertBindFails(vector(map("column", "embedding", "query_vector", "[1, 2, 3]")), "dimension");
    }

    @Test
    void preparedVectorSearchDefersOnlyArgumentsThatKeepTheColumns() {
        ConnectorSearchRequest prepared = new ConnectorSearchRequest(ConnectorSearchFunction.VECTOR_SEARCH,
                map("column", "embedding"), set("query_vector", "top_k", "offset", "filter",
                        "distance_lower_bound", "distance_upper_bound"));
        LanceSearchArguments arguments = LanceSearchArguments.parse(prepared);
        LanceSearchArguments.Bound bound = arguments.bind(SCHEMA);

        TVectorSearchParams vector = bound.getRequest().getSearchQuery().getVectorSearch();
        Assertions.assertFalse(vector.isSetQueryVector());
        Assertions.assertEquals(1, vector.getTopK());
        Assertions.assertEquals(0, vector.getOffset());
        Assertions.assertFalse(vector.isSetDistanceLowerBound());
        Assertions.assertFalse(bound.getRequest().isSetSearchFilter());
        Assertions.assertEquals(1, arguments.getTopK());
        // The column is still checked: it decides the relation's columns.
        assertBindFails(new ConnectorSearchRequest(ConnectorSearchFunction.VECTOR_SEARCH, map("column", "Body"),
                set("query_vector")), "Lance vector column 'Body' must be fixed_size_list");
        for (String constant : new String[] {"column", "metric", "use_index", "nprobes", "prefilter"}) {
            Map<String, String> given = map("column", "embedding", "query_vector", "[1, 2]");
            given.remove(constant);
            assertParseFails(new ConnectorSearchRequest(ConnectorSearchFunction.VECTOR_SEARCH, given,
                    set(constant)),
                    "vector_search property '" + constant + "' must be constant in a prepared statement");
        }
    }

    @Test
    void fullTextSearchHasTheBranch4DefaultsAndRules() {
        LanceSearchArguments arguments = LanceSearchArguments.parse(fullText(map("column", "body",
                "query", " lance ")));
        TExternalSearchRequest request = arguments.bind(SCHEMA).getRequest();

        TFullTextSearchParams fullText = request.getSearchQuery().getFullTextSearch();
        Assertions.assertEquals("Body", fullText.getColumn());
        Assertions.assertEquals("lance", fullText.getQuery());
        Assertions.assertEquals(10, fullText.getTopK());
        Assertions.assertEquals(TFtsQueryType.MATCH, fullText.getQueryType());
        Assertions.assertEquals(TFtsCoverageMode.STRICT, fullText.getCoverageMode());
        Assertions.assertEquals(TFtsMatchOperator.OR, fullText.getMatchOperator());
        Assertions.assertEquals(0, fullText.getMaxFuzzyDistance());
        Assertions.assertFalse(fullText.isSetPhraseSlop());
        Assertions.assertFalse(request.isPrefilter());
        Assertions.assertTrue(arguments.searchesIndex());

        TFullTextSearchParams phrase = LanceSearchArguments.parse(fullText(map("column", "body", "query", "a b",
                "query_type", "PHRASE", "slop", "2", "coverage_mode", "index-only", "prefilter", "true")))
                .bind(SCHEMA).getRequest().getSearchQuery().getFullTextSearch();
        Assertions.assertEquals(TFtsQueryType.PHRASE, phrase.getQueryType());
        Assertions.assertEquals(2, phrase.getPhraseSlop());
        Assertions.assertEquals(TFtsCoverageMode.INDEX_ONLY, phrase.getCoverageMode());
        Assertions.assertFalse(phrase.isSetMatchOperator());

        assertParseFails(fullText(map("column", "body", "query", "a", "slop", "1")),
                "'slop' is only valid for query_type=phrase");
        assertParseFails(fullText(map("column", "body", "query", "a", "max_fuzzy_distance", "1")),
                "'max_fuzzy_distance' must be 0");
        assertParseFails(fullText(map("column", "body", "query", "a", "query_type", "phrase", "operator", "and")),
                "'operator' is only valid for query_type=match");
        assertParseFails(fullText(map("column", "body", "query", "a", "query_type", "phrase",
                "max_fuzzy_distance", "0")), "'max_fuzzy_distance' is only valid for query_type=match");
        assertParseFails(fullText(map("column", "body", "query", "a\0b")),
                "'query' must not contain an embedded NUL byte");
        assertParseFails(fullText(map("column", "body", "query", "a", "metric", "l2")),
                "'metric' is an invalid property for full_text_search()");
        assertParseFails(new ConnectorSearchRequest(ConnectorSearchFunction.FULL_TEXT_SEARCH,
                map("column", "body"), set("query")),
                "full_text_search properties must be constant in a prepared statement");
    }

    @Test
    void parsesTheFullTextEnumsAndChecksTheColumn() {
        Assertions.assertEquals(TFtsCoverageMode.STRICT, LanceSearchArguments.parseCoverageMode(" Strict "));
        Assertions.assertEquals(TFtsCoverageMode.INDEX_ONLY, LanceSearchArguments.parseCoverageMode("index_only"));
        assertFails(() -> LanceSearchArguments.parseCoverageMode("partial"), "expected strict or index_only");
        Assertions.assertEquals(TFtsQueryType.PHRASE, LanceSearchArguments.parseQueryType("phrase"));
        assertFails(() -> LanceSearchArguments.parseQueryType("fuzzy"), "expected match or phrase");
        Assertions.assertEquals(TFtsMatchOperator.AND, LanceSearchArguments.parseMatchOperator("AND"));
        assertFails(() -> LanceSearchArguments.parseMatchOperator("xor"), "expected 'or' or 'and'");

        Assertions.assertEquals("Body", LanceSearchArguments.findStringField(SCHEMA, "BODY").getName());
        assertFails(() -> LanceSearchArguments.findStringField(SCHEMA, "id"),
                "Lance full-text column 'id' must be STRING");
        assertFails(() -> LanceSearchArguments.findStringField(SCHEMA, "title"),
                "Lance full-text column 'title' does not exist");
        Schema twoBodies = new Schema(Arrays.asList(Field.nullable("body", ArrowType.Utf8.INSTANCE),
                Field.nullable("BODY", ArrowType.LargeUtf8.INSTANCE)));
        assertFails(() -> LanceSearchArguments.findStringField(twoBodies, "Body"),
                "is ambiguous under case-insensitive matching");
    }

    private static void assertParseFails(ConnectorSearchRequest request, String message) {
        assertFails(() -> LanceSearchArguments.parse(request), message);
    }

    private static void assertBindFails(ConnectorSearchRequest request, String message) {
        LanceSearchArguments arguments = LanceSearchArguments.parse(request);
        assertFails(() -> arguments.bind(SCHEMA), message);
    }

    private static void assertFails(Executable executable, String message) {
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class, executable);
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    private static ConnectorSearchRequest vector(Map<String, String> arguments) {
        return new ConnectorSearchRequest(ConnectorSearchFunction.VECTOR_SEARCH, arguments, Collections.emptySet());
    }

    private static ConnectorSearchRequest fullText(Map<String, String> arguments) {
        return new ConnectorSearchRequest(ConnectorSearchFunction.FULL_TEXT_SEARCH, arguments,
                Collections.emptySet());
    }

    private static Map<String, String> map(String... keysAndValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }

    private static Set<String> set(String... names) {
        return new LinkedHashSet<>(Arrays.asList(names));
    }
}

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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.datasource.lance.LanceVectorQuery;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Relation TVF for a fixed-snapshot Lance vector search. */
public class VectorSearchTableValuedFunction extends LanceExternalSearchTableValuedFunction {
    public static final String NAME = "vector_search";
    public static final String DISTANCE_COLUMN = "_distance";

    private static final String QUERY_VECTOR = "query_vector";
    private static final String METRIC = "metric";
    private static final String NPROBES = "nprobes";
    private static final String REFINE_FACTOR = "refine_factor";
    private static final String EF = "ef";
    private static final String USE_INDEX = "use_index";
    private static final Set<String> PROPERTIES = ImmutableSet.of(
            TABLE, COLUMN, QUERY_VECTOR, TOP_K, OFFSET, METRIC, FILTER,
            NPROBES, REFINE_FACTOR, EF, USE_INDEX);

    public VectorSearchTableValuedFunction(Map<String, String> properties)
            throws AnalysisException {
        super(prepare(properties));
    }

    private static PreparedSearch prepare(Map<String, String> properties)
            throws AnalysisException {
        Map<String, String> params = normalizeProperties(properties, PROPERTIES, NAME);
        boolean useIndex = !params.containsKey(USE_INDEX)
                || parseBoolean(params.get(USE_INDEX), USE_INDEX);
        CommonSearch common = prepareCommon(params, NAME,
                "VectorSearchTableValuedFunction", "vector search", useIndex);

        Field vectorField = LanceVectorQuery.findVectorColumnField(
                common.metadata().getSchema(), required(params, COLUMN, NAME));
        int vectorFieldId = useIndex
                ? requireLanceFieldId(common.metadata(), vectorField) : -1;
        TSearchVector queryVector = LanceVectorQuery.parseAndEncodeQueryVector(
                vectorField, required(params, QUERY_VECTOR, NAME));

        TVectorSearchParams vectorParams = new TVectorSearchParams()
                .setColumn(vectorField.getName())
                .setQueryVector(queryVector)
                .setTopK(common.topK())
                .setOffset(common.offset());
        if (params.containsKey(METRIC)) {
            vectorParams.setMetric(parseMetric(params.get(METRIC)));
        }

        TExternalSearchRequest searchRequest = new TExternalSearchRequest()
                .setSchemaVersion(1)
                .setSearchQuery(TExternalSearchQuery.vector_search(vectorParams));
        TVectorSearchOptions vectorSearchOptions = buildVectorSearchOptions(params, useIndex);
        if (vectorSearchOptions != null) {
            searchRequest.setVectorSearchOptions(vectorSearchOptions);
        }
        return prepareSearch(
                common, vectorFieldId, searchRequest, DISTANCE_COLUMN, "vector search");
    }

    private static TVectorSearchOptions buildVectorSearchOptions(
            Map<String, String> params, boolean useIndex) throws AnalysisException {
        TVectorSearchOptions options = new TVectorSearchOptions();
        boolean configured = false;
        if (params.containsKey(NPROBES)) {
            options.setNprobes(parsePositiveInt(params.get(NPROBES), NPROBES));
            configured = true;
        }
        if (params.containsKey(REFINE_FACTOR)) {
            options.setRefineFactor(
                    parsePositiveInt(params.get(REFINE_FACTOR), REFINE_FACTOR));
            configured = true;
        }
        if (params.containsKey(EF)) {
            options.setEf(parsePositiveInt(params.get(EF), EF));
            configured = true;
        }
        if (params.containsKey(USE_INDEX)) {
            options.setUseIndex(useIndex);
            configured = true;
        }
        return configured ? options : null;
    }

    @VisibleForTesting
    static List<Column> buildOutputColumns(LanceTableMetadata metadata)
            throws AnalysisException {
        return buildOutputColumns(metadata, DISTANCE_COLUMN, "vector search");
    }

    @VisibleForTesting
    static int requireLanceFieldId(LanceTableMetadata metadata, Field field)
            throws AnalysisException {
        return requireLanceFieldId(metadata, field, "vector");
    }

    private static int parsePositiveInt(String value, String property)
            throws AnalysisException {
        long parsed = parseLong(value, property, 1, Integer.MAX_VALUE);
        return (int) parsed;
    }

    private static boolean parseBoolean(String value, String property)
            throws AnalysisException {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new AnalysisException("'" + property + "' must be 'true' or 'false'");
    }

    private static TVectorMetric parseMetric(String value) throws AnalysisException {
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "l2":
                return TVectorMetric.L2;
            case "cosine":
                return TVectorMetric.COSINE;
            case "dot":
            case "dot_product":
                return TVectorMetric.DOT_PRODUCT;
            case "hamming":
                return TVectorMetric.HAMMING;
            default:
                throw new AnalysisException("Unsupported vector metric '" + value
                        + "': expected l2, cosine, dot, or hamming");
        }
    }
}

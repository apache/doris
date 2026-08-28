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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFullTextSearchParams;

import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Relation TVF for a fixed-snapshot distributed Lance full-text search. */
public class FullTextSearchTableValuedFunction extends LanceExternalSearchTableValuedFunction {
    public static final String NAME = "full_text_search";
    public static final String SCORE_COLUMN = "_score";

    private static final String QUERY = "query";
    private static final String COVERAGE_MODE = "coverage_mode";
    private static final Set<String> PROPERTIES = ImmutableSet.of(
            TABLE, COLUMN, QUERY, TOP_K, OFFSET, FILTER, COVERAGE_MODE);

    public FullTextSearchTableValuedFunction(Map<String, String> properties)
            throws AnalysisException {
        super(prepare(properties));
    }

    private static PreparedSearch prepare(Map<String, String> properties)
            throws AnalysisException {
        Map<String, String> params = normalizeProperties(properties, PROPERTIES, NAME);
        CommonSearch common = prepareCommon(params, NAME,
                "FullTextSearchTableValuedFunction", "full-text search", true);

        Field field = findStringField(common.metadata(), required(params, COLUMN, NAME));
        int fieldId = requireLanceFieldId(common.metadata(), field, "full-text");
        String query = required(params, QUERY, NAME);
        if (query.indexOf('\0') >= 0) {
            throw new AnalysisException("'query' must not contain an embedded NUL byte");
        }

        TFullTextSearchParams fullTextParams = new TFullTextSearchParams()
                .setColumn(field.getName())
                .setQuery(query)
                .setTopK(common.topK())
                .setOffset(common.offset())
                .setCoverageMode(parseCoverageMode(
                        params.getOrDefault(COVERAGE_MODE, "strict")));
        TExternalSearchRequest searchRequest = new TExternalSearchRequest()
                .setSchemaVersion(1)
                .setSearchQuery(TExternalSearchQuery.full_text_search(fullTextParams));
        return prepareSearch(
                common, fieldId, searchRequest, SCORE_COLUMN, "full-text search");
    }

    private static Field findStringField(LanceTableMetadata metadata, String column)
            throws AnalysisException {
        Field match = null;
        for (Field field : metadata.getSchema().getFields()) {
            if (field.getName().equalsIgnoreCase(column)) {
                if (match != null) {
                    throw new AnalysisException("Lance full-text column '" + column
                            + "' is ambiguous under case-insensitive matching");
                }
                match = field;
            }
        }
        if (match == null) {
            throw new AnalysisException("Lance full-text column '" + column + "' does not exist");
        }
        ArrowType.ArrowTypeID typeId = match.getType().getTypeID();
        if (typeId != ArrowType.ArrowTypeID.Utf8
                && typeId != ArrowType.ArrowTypeID.LargeUtf8) {
            throw new AnalysisException("Lance full-text column '" + match.getName()
                    + "' must be STRING");
        }
        return match;
    }

    private static TFtsCoverageMode parseCoverageMode(String value) throws AnalysisException {
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "strict":
                return TFtsCoverageMode.STRICT;
            case "index_only":
            case "index-only":
                return TFtsCoverageMode.INDEX_ONLY;
            default:
                throw new AnalysisException("Unsupported FTS coverage_mode '" + value
                        + "': expected strict or index_only");
        }
    }
}

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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.scan.ConnectorVectorSearch;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TPaimonVectorSearchOptions;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchParams;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.index.pkvector.PkVectorBucketIndexState;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.VectorType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Paimon-specific admission and wire options for the connector-neutral vector search path. */
final class PaimonVectorSearch {
    static final String DISTANCE_COLUMN = "__paimon_search_score_to_dis";
    private static final Set<String> FILTER_TYPES = new HashSet<>(Arrays.asList(
            "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "STRING", "DATE", "DATEV2"));

    private PaimonVectorSearch() {
    }

    static Optional<ConnectorVectorSearch> prepare(FileStoreTable table, ConnectorVectorSearch search) {
        TExternalSearchRequest request = search.getRequest();
        TVectorSearchParams vector = request.getSearchQuery().getVectorSearch();
        CoreOptions options = table.coreOptions();
        String column = table.rowType().getFieldNames().stream()
                .filter(name -> name.equalsIgnoreCase(vector.getColumn())).findFirst().orElse(null);
        if (column == null || !options.primaryKeyVectorIndexEnabled()
                || !options.primaryKeyVectorIndexColumns().contains(column)) {
            return Optional.empty();
        }
        if (!PaimonRustReaderCapabilities.hasRustCompatibleTimestamps(table.schema().fields())) {
            return Optional.empty();
        }
        DataType type = table.rowType().getTypeAt(table.rowType().getFieldIndex(column));
        if (!(type instanceof VectorType)
                || ((VectorType) type).getElementType().getTypeRoot() != DataTypeRoot.FLOAT
                || ((VectorType) type).getLength() != vector.getQueryVector().getDimension()
                || (!search.isTableFunction() && type.isNullable())) {
            return Optional.empty();
        }
        String metric = options.primaryKeyVectorDistanceMetric(column);
        if (!(vector.getMetric() == TVectorMetric.L2 && "l2".equals(metric))
                && !(vector.getMetric() == TVectorMetric.DOT_PRODUCT && "inner_product".equals(metric))) {
            return Optional.empty();
        }
        for (String name : table.rowType().getFieldNames()) {
            if (DISTANCE_COLUMN.equalsIgnoreCase(name) || "__paimon_search_score".equalsIgnoreCase(name)
                    || "_PKEY_VECTOR_POSITION".equalsIgnoreCase(name)
                    || (search.isTableFunction() && "_distance".equalsIgnoreCase(name))) {
                return Optional.empty();
            }
        }
        // Arrow safe casts can silently replace out-of-range historical values with NULL. A bounded
        // search has no JNI fallback; decline schema evolution instead of changing the SQL result.
        for (Long id : table.schemaManager().listAllIds()) {
            TableSchema old = table.schemaManager().schema(id);
            for (DataField field : table.schema().fields()) {
                Optional<DataField> previous = old.fields().stream()
                        .filter(candidate -> candidate.id() == field.id()).findFirst();
                if (previous.isPresent() && !previous.get().type().equalsIgnoreNullable(field.type())) {
                    return Optional.empty();
                }
                if (!search.isTableFunction() && field.name().equals(column)
                        && (!previous.isPresent() || previous.get().type().isNullable())) {
                    return Optional.empty();
                }
            }
        }
        if (search.getFilter().isPresent()) {
            ConnectorExpression filter = search.getFilter().get();
            if (!supportsFilter(filter) || columnNames(filter).stream().anyMatch(name ->
                    table.rowType().getFieldNames().stream().noneMatch(name::equalsIgnoreCase))) {
                return Optional.empty();
            }
            boolean dataFilter = columnNames(filter).stream()
                    .anyMatch(name -> table.partitionKeys().stream().noneMatch(name::equalsIgnoreCase));
            if (dataFilter && (!options.deletionVectorsEnabled()
                    || "true".equalsIgnoreCase(table.options().get("deletion-vectors.merge-on-read")))) {
                return Optional.empty();
            }
        }
        Map<String, String> overrides = new HashMap<>();
        String algorithm = options.primaryKeyVectorIndexType(column);
        String fieldPrefix = "fields." + column + ".";
        options.primaryKeyVectorIndexOptions(column).toMap().forEach((key, value) -> {
            if ((key.startsWith(fieldPrefix) && !key.startsWith(fieldPrefix + "pk-vector."))
                    || key.startsWith(algorithm + ".")) {
                overrides.put(key, value);
            }
        });
        if (request.isSetVectorSearchOptions()) {
            if (request.getVectorSearchOptions().isSetNprobes()) {
                overrides.put("ivf.nprobe", Integer.toString(request.getVectorSearchOptions().getNprobes()));
            }
            if (request.getVectorSearchOptions().isSetRefineFactor()) {
                overrides.put("refine_factor", Integer.toString(request.getVectorSearchOptions().getRefineFactor()));
            }
            if (request.getVectorSearchOptions().isSetUseIndex()
                    && !request.getVectorSearchOptions().isUseIndex()) {
                overrides.put("vector-index.search-mode", "full");
            }
        }
        request.setPaimonOptions(new TPaimonVectorSearchOptions().setOptions(overrides)
                .setTvfDistance(search.isTableFunction()));
        vector.setColumn(column);
        return Optional.of(new ConnectorVectorSearch(request,
                search.isTableFunction() ? "_distance" : DISTANCE_COLUMN,
                search.isTableFunction(), search.getFilter()));
    }

    // FAST vector search omits unindexed sources, and even FULL ignores APPEND / level-zero
    // files. Ordinary SQL must retain its scalar scan unless every active source is covered.
    static boolean hasSqlCoverage(int fieldId, String indexType, List<BucketVectorSearchSplit> buckets) {
        for (BucketVectorSearchSplit bucket : buckets) {
            List<DataFileMeta> files = bucket.dataSplit().dataFiles();
            if (files.stream().anyMatch(file -> !PrimaryKeyIndexSourcePolicy.shouldRead(file))) {
                return false;
            }
            Set<String> covered = PkVectorBucketIndexState.fromActiveDataFiles(
                    fieldId, indexType, files, bucket.payloadFiles()).sourceFileToAnnSegment().keySet();
            if (files.stream().anyMatch(file -> !covered.contains(file.fileName()))) {
                return false;
            }
        }
        return true;
    }

    /** Must agree with the native converter: dropping even one conjunct after TopK loses rows. */
    static boolean supportsFilter(ConnectorExpression expression) {
        if (expression instanceof ConnectorAnd) {
            return expression.getChildren().stream().allMatch(PaimonVectorSearch::supportsFilter);
        }
        if (expression instanceof ConnectorComparison) {
            ConnectorComparison comparison = (ConnectorComparison) expression;
            return supportedColumn(comparison.getLeft()) && comparison.getRight() instanceof ConnectorLiteral
                    && !((ConnectorLiteral) comparison.getRight()).isNull();
        }
        if (expression instanceof ConnectorIn) {
            ConnectorIn in = (ConnectorIn) expression;
            return supportedColumn(in.getValue()) && !in.getInList().isEmpty()
                    && in.getInList().size() < 0xffff
                    && in.getInList().stream().allMatch(value -> value instanceof ConnectorLiteral
                            && !((ConnectorLiteral) value).isNull());
        }
        if (expression instanceof ConnectorNot
                && expression.getChildren().get(0) instanceof ConnectorIsNull) {
            return supportsFilter(expression.getChildren().get(0));
        }
        if (expression instanceof ConnectorIsNull) {
            return supportedColumn(((ConnectorIsNull) expression).getOperand());
        }
        return false;
    }

    private static Set<String> columnNames(ConnectorExpression expression) {
        Set<String> names = new HashSet<>();
        if (expression instanceof ConnectorColumnRef) {
            names.add(((ConnectorColumnRef) expression).getColumnName());
        }
        expression.getChildren().forEach(child -> names.addAll(columnNames(child)));
        return names;
    }

    private static boolean supportedColumn(ConnectorExpression expression) {
        return expression instanceof ConnectorColumnRef && FILTER_TYPES.contains(
                ((ConnectorColumnRef) expression).getType().getTypeName().toUpperCase(Locale.ROOT));
    }
}

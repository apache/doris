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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonMvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonScanParams;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.paimon.source.PaimonScanNode;
import org.apache.doris.datasource.paimon.source.PaimonVectorSearch;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.NormalizeBinaryPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.NullSafeEqualToEqual;
import org.apache.doris.nereids.rules.rewrite.PushDownVectorTopNIntoPaimonScan;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TPaimonVectorSearchOptions;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.VectorType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Paimon PK-vector implementation of the shared vector_search SQL interface. */
public class PaimonVectorSearchTableValuedFunction extends ExternalSearchTableValuedFunction {
    private static final String NAME = VectorSearchTableValuedFunction.NAME;
    private static final String DISTANCE_COLUMN = VectorSearchTableValuedFunction.DISTANCE_COLUMN;
    // The Rust search eagerly materializes each split before Doris can account for its batches.
    private static final int MAX_CANDIDATES_PER_SPLIT = 100_000;
    private static final Set<String> PROPERTIES = ImmutableSet.of(
            "table", "column", "query_vector", "top_k", "offset", "metric", "filter", "prefilter",
            "nprobes", "refine_factor", "use_index", "version", "timestamp", "tag", "branch");
    private static final ExpressionRewrite PREFILTER_NORMALIZER = new ExpressionRewrite(
            ExpressionRewrite.bottomUp(NormalizeBinaryPredicatesRule.INSTANCE, NullSafeEqualToEqual.INSTANCE));

    private final TableName tableName;
    private final PaimonExternalTable sourceTable;
    private final FileStoreTable table;
    private final List<Column> columns;
    private final TExternalSearchRequest request;
    private final long topK;
    private final long offset;
    private final boolean emptySnapshot;
    private final Optional<String> filter;
    private final boolean prefilter;
    private List<Expression> boundPrefilter = Collections.emptyList();

    public PaimonVectorSearchTableValuedFunction(Map<String, String> properties, boolean deferQueryVector)
            throws AnalysisException {
        Map<String, String> params = normalizeProperties(properties, PROPERTIES, NAME);
        tableName = LanceExternalSearchTableValuedFunction.parseTableName(required(params, "table", NAME));
        sourceTable = findPaimonTable(tableName);
        LanceRefSelector selector = LanceExternalSearchTableValuedFunction.parseSelector(params);
        if (selector.getBranch().isPresent() && selector.getSnapshot().isPresent()) {
            throw new AnalysisException("Paimon does not support combining branch with version or timestamp");
        }
        TableScanParams scanParams = selector.getBranch().map(name -> new TableScanParams(
                "branch", Collections.emptyMap(), Collections.singletonList(name))).orElse(null);
        if (selector.getTag().isPresent()) {
            scanParams = new TableScanParams("tag", Collections.emptyMap(),
                    Collections.singletonList(selector.getTag().get()));
        }
        // A TVF does not pass through ordinary relation binding. Load its source snapshot here
        // so selectors work and latest reads share the statement's existing snapshot fence.
        ConnectContext context = ConnectContext.get();
        Optional<MvccSnapshot> snapshot = context != null && context.getStatementContext() != null
                ? context.getStatementContext().loadSnapshots(sourceTable,
                        selector.getSnapshot(), Optional.ofNullable(scanParams))
                : Optional.of(sourceTable.loadSnapshot(selector.getSnapshot(), Optional.ofNullable(scanParams)));
        Table selected = sourceTable.getPaimonTable(snapshot);
        if (!(selected instanceof FileStoreTable)) {
            throw new AnalysisException("Paimon vector_search requires a file-store table");
        }
        FileStoreTable selectedTable = (FileStoreTable) selected;
        long snapshotId = ((PaimonMvccSnapshot) snapshot.get()).getSnapshotCacheValue()
                .getSnapshot().getSnapshotId();
        emptySnapshot = snapshotId == PaimonSnapshot.INVALID_SNAPSHOT_ID;
        // Preserve the bound schema, including columns added after the last data commit.
        // Retained tags can also outlive the snapshot file and must keep their read handle.
        table = emptySnapshot || selector.getTag().isPresent() ? selectedTable
                : selectedTable.copyWithoutTimeTravel(PaimonScanParams.isolateSnapshotRead(snapshotId));
        topK = parseLong(params.getOrDefault("top_k", "10"), "top_k", 1, MAX_CANDIDATES_PER_SPLIT);
        offset = parseLong(params.getOrDefault("offset", "0"), "offset", 0, MAX_CANDIDATES_PER_SPLIT - topK);
        filter = Optional.ofNullable(params.get("filter"));
        if (filter.isPresent()) {
            validateAndEncodeSqlFilter(filter.get());
        }
        prefilter = params.containsKey("prefilter") && parseBoolean(params.get("prefilter"), "prefilter");

        String column = required(params, "column", NAME);
        DataType vectorType = null;
        for (org.apache.paimon.types.DataField field : table.rowType().getFields()) {
            if (field.name().equalsIgnoreCase(column)) {
                column = field.name();
                vectorType = field.type();
                break;
            }
        }
        if (!(vectorType instanceof VectorType)
                || ((VectorType) vectorType).getElementType().getTypeRoot() != DataTypeRoot.FLOAT) {
            throw new AnalysisException("Paimon vector_search requires a VECTOR<FLOAT> column");
        }
        CoreOptions options = CoreOptions.fromMap(table.options());
        if (!options.primaryKeyVectorIndexEnabled()
                || !options.primaryKeyVectorIndexColumns().contains(column)) {
            throw new AnalysisException("Paimon vector_search requires a PK-vector index on '" + column + "'");
        }
        String metric = params.getOrDefault("metric", "l2").toLowerCase(Locale.ROOT);
        TVectorMetric vectorMetric;
        if (metric.equals("l2")) {
            vectorMetric = TVectorMetric.L2;
        } else if (metric.equals("dot") || metric.equals("dot_product") || metric.equals("inner_product")) {
            vectorMetric = TVectorMetric.DOT_PRODUCT;
        } else {
            throw new AnalysisException("Paimon vector_search supports only l2 and dot metrics");
        }
        String indexMetric = options.primaryKeyVectorDistanceMetric(column);
        if (!(vectorMetric == TVectorMetric.L2 ? "l2" : "inner_product").equals(indexMetric)) {
            throw new AnalysisException("Paimon vector_search metric does not match the PK-vector index");
        }
        TSearchVector query = null;
        if (!deferQueryVector) {
            Field field = new Field(column, FieldType.nullable(
                    new ArrowType.FixedSizeList(((VectorType) vectorType).getLength())),
                    Collections.singletonList(Field.nullable("item",
                            new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));
            query = VectorSearchTableValuedFunction.parseAndEncodeQueryVector(
                    field, required(params, "query_vector", NAME));
            ByteBuffer values = ByteBuffer.wrap(query.getValues()).order(ByteOrder.LITTLE_ENDIAN);
            while (values.hasRemaining()) {
                if (!Float.isFinite(values.getFloat())) {
                    throw new AnalysisException("Paimon query_vector coordinates must be finite FLOAT values");
                }
            }
        }
        Map<String, String> searchOptions = new HashMap<>();
        if (params.containsKey("nprobes")) {
            searchOptions.put("ivf.nprobe", String.valueOf(parseLong(
                    params.get("nprobes"), "nprobes", 1, Integer.MAX_VALUE)));
        }
        if (params.containsKey("refine_factor")) {
            // Refinement oversamples the bounded candidate set before returning the final Top-K.
            searchOptions.put("refine_factor", String.valueOf(parseLong(
                    params.get("refine_factor"), "refine_factor", 1, MAX_CANDIDATES_PER_SPLIT / (topK + offset))));
        }
        request = new TExternalSearchRequest().setSchemaVersion(1)
                .setSearchQuery(TExternalSearchQuery.vector_search(new TVectorSearchParams()
                        .setColumn(column).setQueryVector(query).setMetric(vectorMetric)
                        .setTopK(topK).setOffset(offset)))
                .setPaimonOptions(new TPaimonVectorSearchOptions().setOptions(searchOptions).setTvfDistance(true));
        if (params.containsKey("use_index")) {
            request.setVectorSearchOptions(new TVectorSearchOptions()
                    .setUseIndex(parseBoolean(params.get("use_index"), "use_index")));
        }
        List<Column> schema = new ArrayList<>(PaimonUtil.parseSchema(table,
                sourceTable.getCatalog().getEnableMappingVarbinary(),
                sourceTable.getCatalog().getEnableMappingTimestampTz()));
        for (Column field : schema) {
            if (field.getName().equalsIgnoreCase(DISTANCE_COLUMN)
                    || field.getName().equalsIgnoreCase(PaimonVectorSearch.SEARCH_DISTANCE_COLUMN)
                    || field.getName().equalsIgnoreCase("__paimon_search_score")
                    || field.getName().equalsIgnoreCase("_PKEY_VECTOR_POSITION")) {
                throw new AnalysisException("Paimon vector_search column name is reserved: " + field.getName());
            }
        }
        schema.add(new Column(DISTANCE_COLUMN, Type.FLOAT, false));
        columns = Collections.unmodifiableList(schema);
    }

    static PaimonExternalTable findPaimonTable(TableName name) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), name, PrivPredicate.SELECT)) {
            throw new AnalysisException("SELECT privilege is required on " + name);
        }
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(name.getCtl());
        TableIf table = catalog.getDbOrAnalysisException(name.getDb()).getTableOrAnalysisException(name.getTbl());
        if (!(table instanceof PaimonExternalTable)) {
            throw new AnalysisException("Paimon vector_search requires a Paimon data table");
        }
        return (PaimonExternalTable) table;
    }

    public Optional<String> getFilter() {
        return filter;
    }

    public void bindFilter(LogicalFilter<?> filterPlan, CascadesContext cascadesContext) {
        if (!prefilter) {
            return;
        }
        // This copy survives later plan rewrites. Normalize it in filter context before
        // retaining it: BE expects slot-on-left comparisons and <=> only for null checks.
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(filterPlan, cascadesContext);
        filterPlan = filterPlan.withConjuncts(filterPlan.getConjuncts().stream()
                .map(expression -> PREFILTER_NORMALIZER.rewrite(expression, rewriteContext))
                .collect(ImmutableSet.toImmutableSet()));
        if (filterPlan.getInputSlots().isEmpty()) {
            return;
        }
        if (!PushDownVectorTopNIntoPaimonScan.conjunctsRideIntoVectorSearch(filterPlan)) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "Paimon prefilter requires supported scalar column comparisons, IN, or null checks");
        }
        boolean dataFilter = filterPlan.getInputSlots().stream()
                .anyMatch(slot -> !table.partitionKeys().contains(slot.getName()));
        if (dataFilter && !PaimonScanNode.vectorSearchSupportsDataPredicate(table)) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "Paimon data prefilter requires deletion vectors without merge-on-read");
        }
        boundPrefilter = Collections.unmodifiableList(new ArrayList<>(filterPlan.getConjuncts()));
    }

    @Override
    public long getTopK() {
        return topK;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    public TExternalSearchRequest getSearchRequest() {
        return request.deepCopy();
    }

    @Override
    public String getTableName() {
        return "PaimonVectorSearch<" + tableName + ">";
    }

    @Override
    public List<Column> getTableColumns() {
        return columns;
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        TExternalSearchRequest scanRequest = request.deepCopy();
        PlanTranslatorContext translation = new PlanTranslatorContext();
        List<Expr> predicates = new ArrayList<>();
        List<Expr> partitionAndDataPredicates = new ArrayList<>();
        for (Expression expression : boundPrefilter) {
            for (Slot slot : expression.getInputSlots()) {
                SlotDescriptor descriptor = desc.getSlots().stream()
                        .filter(candidate -> candidate.getColumn().getName().equalsIgnoreCase(slot.getName()))
                        .findFirst().orElseThrow(() -> new IllegalStateException(
                                "Paimon prefilter column was pruned: " + slot.getName()));
                translation.addExprIdSlotRefPair(slot.getExprId(), new SlotRef(descriptor));
            }
            Expr translated = ExpressionTranslator.translate(expression, translation);
            partitionAndDataPredicates.add(translated);
            if (expression.getInputSlots().stream().anyMatch(
                    slot -> !table.partitionKeys().contains(slot.getName()))) {
                // FE bucket planning already enforces partition-only predicates.
                predicates.add(translated);
            }
        }
        if (!predicates.isEmpty()) {
            scanRequest.getPaimonOptions().setPrefilter(predicates.stream()
                    .map(Expr::treeToThrift).collect(java.util.stream.Collectors.toList()));
        }
        return PaimonScanNode.forExternalSearch(id, desc, sourceTable, table,
                scanRequest, partitionAndDataPredicates, emptySnapshot, sv);
    }
}

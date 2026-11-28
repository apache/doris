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
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorVectorSearch;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.connector.converter.VectorSearchPredicateConverter;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.scan.ConnectorVectorSearchPlanner;
import org.apache.doris.datasource.scan.PluginDrivenScanNode;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.NormalizeBinaryPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.NullSafeEqualToEqual;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TVectorElementType;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Connector-backed vector search with a snapshot-wide Doris merge TopN. */
public class VectorSearchTableValuedFunction extends ExternalSearchTableValuedFunction {
    public static final String NAME = "vector_search";
    public static final String DISTANCE_COLUMN = "_distance";
    private static final int MAX_CANDIDATES = 100_000;
    private static final Set<String> PROPERTIES = ImmutableSet.of(
            "table", "column", "query_vector", "top_k", "offset", "metric", "filter", "prefilter",
            "nprobes", "refine_factor", "use_index", "version", "timestamp", "tag", "branch");
    private static final ExpressionRewrite PREFILTER_NORMALIZER = new ExpressionRewrite(
            ExpressionRewrite.bottomUp(NormalizeBinaryPredicatesRule.INSTANCE, NullSafeEqualToEqual.INSTANCE));

    private final PluginDrivenExternalTable source;
    private final Optional<TableSnapshot> snapshot;
    private final Optional<TableScanParams> scanParams;
    private final long topK;
    private final long offset;
    private final Optional<String> filter;
    private final boolean prefilter;
    private final List<Column> columns;
    private final TExternalSearchRequest request;
    private List<Expression> boundPrefilter = Collections.emptyList();
    private Optional<ConnectorExpression> candidateFilter = Optional.empty();

    public VectorSearchTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        Map<String, String> params = normalizeProperties(properties, PROPERTIES, NAME);
        Expression target = new NereidsParser().parseExpression(required(params, "table", NAME));
        if (!(target instanceof UnboundSlot) || ((UnboundSlot) target).getNameParts().size() != 3) {
            throw new AnalysisException("vector_search requires a fully qualified catalog.database.table");
        }
        List<String> names = ((UnboundSlot) target).getNameParts();
        TableNameInfo name = new TableNameInfo(names.get(0), names.get(1), names.get(2));
        ConnectContext context = ConnectContext.get();
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(name.getCtl());
        TableIf table = catalog.getDbOrAnalysisException(name.getDb()).getTableOrAnalysisException(name.getTbl());
        if (!(table instanceof PluginDrivenExternalTable)) {
            throw new AnalysisException("vector_search requires a connector data table");
        }
        source = (PluginDrivenExternalTable) table;
        int selected = 0;
        for (String key : new String[] {"version", "timestamp", "tag", "branch"}) {
            if (params.containsKey(key)) {
                required(params, key, NAME);
                selected++;
            }
        }
        if (selected > 1) {
            throw new AnalysisException("version, timestamp, tag and branch are mutually exclusive");
        }
        snapshot = params.containsKey("version") ? Optional.of(TableSnapshot.versionOf(params.get("version")))
                : params.containsKey("timestamp") ? Optional.of(TableSnapshot.timeOf(params.get("timestamp")))
                : Optional.empty();
        String branchOrTag = params.containsKey("branch") ? "branch" : params.containsKey("tag") ? "tag" : null;
        scanParams = branchOrTag == null ? Optional.empty() : Optional.of(new TableScanParams(branchOrTag,
                Collections.emptyMap(), Collections.singletonList(params.get(branchOrTag))));
        context.getStatementContext().loadSnapshots(source, snapshot, scanParams);
        topK = parseLong(params.getOrDefault("top_k", "10"), "top_k", 1, MAX_CANDIDATES);
        offset = parseLong(params.getOrDefault("offset", "0"), "offset", 0, MAX_CANDIDATES - topK);
        filter = Optional.ofNullable(params.get("filter"));
        if (filter.isPresent()) {
            validateAndEncodeSqlFilter(filter.get());
        }
        prefilter = params.containsKey("prefilter") && parseBoolean(params.get("prefilter"), "prefilter");
        String metricName = params.getOrDefault("metric", "l2").toLowerCase(Locale.ROOT);
        TVectorMetric metric;
        if ("l2".equals(metricName)) {
            metric = TVectorMetric.L2;
        } else if ("dot".equals(metricName) || "dot_product".equals(metricName) || "inner_product".equals(metricName)) {
            metric = TVectorMetric.DOT_PRODUCT;
        } else {
            throw new AnalysisException("vector_search supports only l2 and dot metrics");
        }
        TVectorSearchParams vector = new TVectorSearchParams().setColumn(required(params, "column", NAME))
                .setQueryVector(encodeQuery(required(params, "query_vector", NAME)))
                .setMetric(metric).setTopK(topK + offset).setOffset(0);
        TVectorSearchOptions options = new TVectorSearchOptions();
        if (params.containsKey("nprobes")) {
            options.setNprobes((int) parseLong(params.get("nprobes"), "nprobes", 1, Integer.MAX_VALUE));
        }
        if (params.containsKey("refine_factor")) {
            options.setRefineFactor((int) parseLong(params.get("refine_factor"), "refine_factor", 1,
                    MAX_CANDIDATES / (topK + offset)));
        }
        if (params.containsKey("use_index")) {
            options.setUseIndex(parseBoolean(params.get("use_index"), "use_index"));
        }
        request = new TExternalSearchRequest().setSchemaVersion(1)
                .setSearchQuery(TExternalSearchQuery.vector_search(vector)).setVectorSearchOptions(options);
        List<Column> schema = new ArrayList<>(source.getFullSchema(
                MvccUtil.getSnapshotFromContext(source, snapshot, scanParams)));
        schema.removeIf(column -> !column.isVisible());
        if (schema.stream().anyMatch(column -> DISTANCE_COLUMN.equalsIgnoreCase(column.getName()))) {
            throw new AnalysisException("vector_search output column '_distance' is reserved");
        }
        schema.add(new Column(DISTANCE_COLUMN, Type.FLOAT, false));
        columns = Collections.unmodifiableList(schema);
        checkAuth(context);
        prepareSearch();
    }

    @Override
    public void checkAuth(ConnectContext context) {
        // Candidate generation and the TVF's public schema can read every source column.
        Set<String> sourceColumns = columns.stream().map(Column::getName)
                .filter(column -> !DISTANCE_COLUMN.equals(column)).collect(Collectors.toSet());
        try {
            UserAuthentication.checkPermission(source, context, sourceColumns);
        } catch (UserException e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(e.getMessage(), e);
        }
        rejectSourceDataPolicies(context.getEnv().getAccessManager(), context.getCurrentUserIdentity(),
                source.getCatalog().getName(), source.getDatabase().getFullName(), source.getName(), sourceColumns);
    }

    static void rejectSourceDataPolicies(AccessControllerManager access, UserIdentity user,
            String catalog, String database, String table, Set<String> sourceColumns) {
        if (user.isSystemUser()) {
            return;
        }
        // This TVF is not a source CatalogRelation, so CheckPolicy cannot inject its policies.
        // Applying a policy after TopK would still search excluded rows and change the answer.
        if (!access.evalRowFilterPolicies(user, catalog, database, table).isEmpty()
                || !access.evalDataMaskPolicies(user, catalog, database, table, sourceColumns).isEmpty()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "vector_search does not support source row filters or column masks");
        }
    }

    private static TSearchVector encodeQuery(String json) throws AnalysisException {
        try {
            JsonArray array = JsonParser.parseString(json).getAsJsonArray();
            if (array.size() == 0) {
                throw new AnalysisException("query_vector must not be empty");
            }
            ByteBuffer values = ByteBuffer.allocate(Math.multiplyExact(array.size(), Float.BYTES))
                    .order(ByteOrder.LITTLE_ENDIAN);
            for (JsonElement element : array) {
                if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()
                        || !Float.isFinite(element.getAsFloat())) {
                    throw new AnalysisException("query_vector requires finite, non-null FLOAT coordinates");
                }
                values.putFloat(element.getAsFloat());
            }
            return new TSearchVector().setElementType(TVectorElementType.FLOAT32)
                    .setDimension(array.size()).setValues(values.array());
        } catch (RuntimeException e) {
            throw new AnalysisException("Invalid query_vector JSON array", e);
        }
    }

    private ConnectorVectorSearch prepareSearch() {
        if (!ConnectContext.get().getSessionVariable().enableFileScannerV2) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("vector_search requires FileScannerV2");
        }
        return ConnectorVectorSearchPlanner.prepare(source, snapshot, scanParams,
                new ConnectorVectorSearch(request, DISTANCE_COLUMN, true, candidateFilter))
                .orElseThrow(() -> new org.apache.doris.nereids.exceptions.AnalysisException(
                        "vector_search is not supported for this table, metric, reader or prefilter"));
    }

    public boolean isPrefilter() {
        return prefilter;
    }

    public List<Slot> getPrefilterSlots() {
        return boundPrefilter.stream().flatMap(expression -> expression.getInputSlots().stream())
                .distinct().collect(Collectors.toList());
    }

    public Optional<String> getFilter() {
        return filter;
    }

    public void bindFilter(LogicalFilter<?> plan, CascadesContext context) {
        if (!prefilter || plan.getInputSlots().isEmpty()) {
            return;
        }
        List<Expression> normalized = new ArrayList<>();
        List<ConnectorExpression> predicates = new ArrayList<>();
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(plan, context);
        for (Expression expression : plan.getConjuncts()) {
            Expression rewritten = PREFILTER_NORMALIZER.rewrite(expression, rewriteContext);
            ConnectorExpression converted = VectorSearchPredicateConverter.convert(rewritten);
            if (converted == null) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        "vector_search prefilter requires supported scalar predicates");
            }
            normalized.add(rewritten);
            predicates.add(converted);
        }
        boundPrefilter = Collections.unmodifiableList(normalized);
        candidateFilter = Optional.of(predicates.size() == 1 ? predicates.get(0) : new ConnectorAnd(predicates));
        prepareSearch();
    }

    @Override
    public long getTopK() {
        return topK;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public String getTableName() {
        return "VectorSearch<" + source.getName() + ">";
    }

    @Override
    public List<Column> getTableColumns() {
        return columns;
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        desc.setTable(source);
        PluginDrivenScanNode node = PluginDrivenScanNode.create(id, desc, false, sv,
                ScanContext.builder().clusterName(sv.resolveCloudClusterName())
                        .beExecVersion(ConnectContext.get().getStatementContext().getBeExecVersion()).build(),
                (PluginDrivenExternalCatalog) source.getCatalog(), source);
        snapshot.ifPresent(node::setQueryTableSnapshot);
        scanParams.ifPresent(node::setScanParams);
        node.setVectorSearch(prepareSearch());
        PlanTranslatorContext translation = new PlanTranslatorContext();
        List<Expr> predicates = new ArrayList<>();
        for (Expression expression : boundPrefilter) {
            for (Slot slot : expression.getInputSlots()) {
                SlotDescriptor descriptor = desc.getSlots().stream()
                        .filter(candidate -> candidate.getColumn().getName().equalsIgnoreCase(slot.getName()))
                        .findFirst().orElseThrow(() -> new IllegalStateException(
                                "vector_search prefilter column was pruned: " + slot.getName()));
                translation.addExprIdSlotRefPair(slot.getExprId(), new SlotRef(descriptor));
            }
            predicates.add(ExpressionTranslator.translate(expression, translation));
        }
        node.setVectorPrefilter(predicates);
        return node;
    }
}

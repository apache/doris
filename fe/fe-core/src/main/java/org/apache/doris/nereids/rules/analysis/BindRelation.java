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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.TableFrom;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.QuantileUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalEsScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHudiScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTestScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Rule to bind relations in query plan.
 */
public class BindRelation extends OneAnalysisRuleFactory {

    public BindRelation() {}

    @Override
    public Rule build() {
        return unboundRelation().thenApply(ctx -> {
            Plan plan = doBindRelation(ctx);
            if (!(plan instanceof Unbound) && plan instanceof Relation) {
                // init output and allocate slot id immediately, so that the slot id increase
                // in the order in which the table appears.
                LogicalProperties logicalProperties = plan.getLogicalProperties();
                logicalProperties.getOutput();
            }
            return plan;
        }).toRule(RuleType.BINDING_RELATION);
    }

    private Plan doBindRelation(MatchingContext<UnboundRelation> ctx) {
        List<String> nameParts = ctx.root.getNameParts();
        switch (nameParts.size()) {
            case 1: { // table
                // Use current database name from catalog.
                return bindWithCurrentDb(ctx.cascadesContext, ctx.root);
            }
            case 2:
                // db.table
                // Use database name from table name parts.
            case 3: {
                // catalog.db.table
                // Use catalog and database name from name parts.
                return bind(ctx.cascadesContext, ctx.root);
            }
            default:
                throw new IllegalStateException("Table name [" + ctx.root.getTableName() + "] is invalid.");
        }
    }

    private LogicalPlan bindWithCurrentDb(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        String tableName = unboundRelation.getNameParts().get(0);
        // check if it is a CTE's name
        CTEContext cteContext = cascadesContext.getCteContext().findCTEContext(tableName).orElse(null);
        if (cteContext != null) {
            Optional<LogicalPlan> analyzedCte = cteContext.getAnalyzedCTEPlan(tableName);
            if (analyzedCte.isPresent()) {
                LogicalCTEConsumer consumer = new LogicalCTEConsumer(unboundRelation.getRelationId(),
                        cteContext.getCteId(), tableName, analyzedCte.get());
                if (cascadesContext.isLeadingJoin()) {
                    LeadingHint leading = (LeadingHint) cascadesContext.getHintMap().get("Leading");
                    leading.putRelationIdAndTableName(Pair.of(consumer.getRelationId(), tableName));
                    leading.getRelationIdToScanMap().put(consumer.getRelationId(), consumer);
                }
                return consumer;
            }
        }

        List<String> tableQualifier = RelationUtil.getQualifierName(
                cascadesContext.getConnectContext(), unboundRelation.getNameParts());
        TableIf table = cascadesContext.getStatementContext().getAndCacheTable(tableQualifier, TableFrom.QUERY);

        LogicalPlan scan = getLogicalPlan(table, unboundRelation, tableQualifier, cascadesContext);
        if (cascadesContext.isLeadingJoin()) {
            LeadingHint leading = (LeadingHint) cascadesContext.getHintMap().get("Leading");
            leading.putRelationIdAndTableName(Pair.of(unboundRelation.getRelationId(), tableName));
            leading.getRelationIdToScanMap().put(unboundRelation.getRelationId(), scan);
        }
        return scan;
    }

    private LogicalPlan bind(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                unboundRelation.getNameParts());
        TableIf table = cascadesContext.getStatementContext().getAndCacheTable(tableQualifier, TableFrom.QUERY);
        return getLogicalPlan(table, unboundRelation, tableQualifier, cascadesContext);
    }

    private LogicalPlan makeOlapScan(TableIf table, UnboundRelation unboundRelation, List<String> qualifier) {
        LogicalOlapScan scan;
        List<Long> partIds = getPartitionIds(table, unboundRelation, qualifier);
        List<Long> tabletIds = unboundRelation.getTabletIds();
        if (!CollectionUtils.isEmpty(partIds) && !unboundRelation.getIndexName().isPresent()) {
            scan = new LogicalOlapScan(unboundRelation.getRelationId(),
                    (OlapTable) table, qualifier, partIds,
                    tabletIds, unboundRelation.getHints(), unboundRelation.getTableSample());
        } else {
            Optional<String> indexName = unboundRelation.getIndexName();
            // For direct mv scan.
            if (indexName.isPresent()) {
                OlapTable olapTable = (OlapTable) table;
                Long indexId = olapTable.getIndexIdByName(indexName.get());
                if (indexId == null) {
                    throw new AnalysisException("Table " + olapTable.getName()
                        + " doesn't have materialized view " + indexName.get());
                }
                PreAggStatus preAggStatus = olapTable.isDupKeysOrMergeOnWrite() ? PreAggStatus.unset()
                        : PreAggStatus.off("For direct index scan on mor/agg.");

                scan = new LogicalOlapScan(unboundRelation.getRelationId(),
                    (OlapTable) table, qualifier, tabletIds,
                    CollectionUtils.isEmpty(partIds) ? ((OlapTable) table).getPartitionIds() : partIds, indexId,
                    preAggStatus, CollectionUtils.isEmpty(partIds) ? ImmutableList.of() : partIds,
                    unboundRelation.getHints(), unboundRelation.getTableSample());
            } else {
                scan = new LogicalOlapScan(unboundRelation.getRelationId(),
                    (OlapTable) table, qualifier, tabletIds, unboundRelation.getHints(),
                    unboundRelation.getTableSample());
            }
        }
        if (!tabletIds.isEmpty()) {
            // This tabletIds is set manually, so need to set specifiedTabletIds
            scan = scan.withManuallySpecifiedTabletIds(tabletIds);
        }
        if (needGenerateLogicalAggForRandomDistAggTable(scan)) {
            // it's a random distribution agg table
            // add agg on olap scan
            return preAggForRandomDistribution(scan);
        } else {
            // it's a duplicate, unique or hash distribution agg table
            // add delete sign filter on olap scan if needed
            return checkAndAddDeleteSignFilter(scan, ConnectContext.get(), (OlapTable) table);
        }
    }

    private boolean needGenerateLogicalAggForRandomDistAggTable(LogicalOlapScan olapScan) {
        if (ConnectContext.get() != null && ConnectContext.get().getState() != null
                && ConnectContext.get().getState().isQuery()) {
            // we only need to add an agg node for query, and should not do it for deleting
            // from random distributed table. see https://github.com/apache/doris/pull/37985 for more info
            OlapTable olapTable = olapScan.getTable();
            KeysType keysType = olapTable.getKeysType();
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            return keysType == KeysType.AGG_KEYS
                    && distributionInfo.getType() == DistributionInfo.DistributionInfoType.RANDOM;
        } else {
            return false;
        }
    }

    /**
     * add LogicalAggregate above olapScan for preAgg
     * @param olapScan olap scan plan
     * @return rewritten plan
     */
    private LogicalPlan preAggForRandomDistribution(LogicalOlapScan olapScan) {
        OlapTable olapTable = olapScan.getTable();
        List<Slot> childOutputSlots = olapScan.computeOutput();
        List<Expression> groupByExpressions = new ArrayList<>();
        List<NamedExpression> outputExpressions = new ArrayList<>();
        List<Column> columns = olapTable.getBaseSchema();

        for (Column col : columns) {
            // use exist slot in the plan
            SlotReference slot = SlotReference.fromColumn(olapTable, col, col.getName(), olapScan.qualified());
            ExprId exprId = slot.getExprId();
            for (Slot childSlot : childOutputSlots) {
                if (childSlot instanceof SlotReference && ((SlotReference) childSlot).getName().equals(col.getName())) {
                    exprId = childSlot.getExprId();
                    slot = slot.withExprId(exprId);
                    break;
                }
            }
            if (col.isKey()) {
                groupByExpressions.add(slot);
                outputExpressions.add(slot);
            } else {
                Expression function = generateAggFunction(slot, col);
                // DO NOT rewrite
                if (function == null) {
                    return olapScan;
                }
                Alias alias = new Alias(exprId, ImmutableList.of(function), col.getName(),
                        olapScan.qualified(), true);
                outputExpressions.add(alias);
            }
        }
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(groupByExpressions, outputExpressions,
                olapScan);
        return aggregate;
    }

    /**
     * generate aggregation function according to the aggType of column
     *
     * @param slot slot of column
     * @return aggFunction generated
     */
    private Expression generateAggFunction(SlotReference slot, Column column) {
        AggregateType aggregateType = column.getAggregationType();
        switch (aggregateType) {
            case SUM:
                return new Sum(slot);
            case MAX:
                return new Max(slot);
            case MIN:
                return new Min(slot);
            case HLL_UNION:
                return new HllUnion(slot);
            case BITMAP_UNION:
                return new BitmapUnion(slot);
            case QUANTILE_UNION:
                return new QuantileUnion(slot);
            case GENERIC:
                Type type = column.getType();
                if (!type.isAggStateType()) {
                    return null;
                }
                AggStateType aggState = (AggStateType) type;
                // use AGGREGATE_FUNCTION_UNION to aggregate multiple agg_state into one
                String funcName = aggState.getFunctionName() + AggCombinerFunctionBuilder.UNION_SUFFIX;
                FunctionRegistry functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
                FunctionBuilder builder = functionRegistry.findFunctionBuilder(funcName, slot);
                return builder.build(funcName, ImmutableList.of(slot)).first;
            default:
                return null;
        }
    }

    private Optional<LogicalPlan> handleMetaTable(TableIf table, UnboundRelation unboundRelation,
            List<String> qualifiedTableName) {
        Optional<TableValuedFunction> tvf = table.getDatabase().getCatalog().getMetaTableFunction(
                qualifiedTableName.get(1), qualifiedTableName.get(2));
        if (tvf.isPresent()) {
            return Optional.of(new LogicalTVFRelation(unboundRelation.getRelationId(), tvf.get()));
        }
        return Optional.empty();
    }

    /**
     * Add delete sign filter on olap scan if need.
     */
    public static LogicalPlan checkAndAddDeleteSignFilter(LogicalOlapScan scan, ConnectContext connectContext,
            OlapTable olapTable) {
        if (!Util.showHiddenColumns() && scan.getTable().hasDeleteSign()
                && !connectContext.getSessionVariable().skipDeleteSign()) {
            // table qualifier is catalog.db.table, we make db.table.column
            Slot deleteSlot = null;
            for (Slot slot : scan.getOutput()) {
                if (slot.getName().equals(Column.DELETE_SIGN)) {
                    deleteSlot = slot;
                    break;
                }
            }
            Preconditions.checkArgument(deleteSlot != null);
            Expression conjunct = new EqualTo(new TinyIntLiteral((byte) 0), deleteSlot);
            if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
                scan = scan.withPreAggStatus(PreAggStatus.off(
                        Column.DELETE_SIGN + " is used as conjuncts."));
            }
            return new LogicalFilter<>(Sets.newHashSet(conjunct), scan);
        }
        return scan;
    }

    private LogicalPlan getLogicalPlan(TableIf table, UnboundRelation unboundRelation,
                                       List<String> qualifiedTableName, CascadesContext cascadesContext) {
        // for create view stmt replace tableName to ctl.db.tableName
        unboundRelation.getIndexInSqlString().ifPresent(pair -> {
            StatementContext statementContext = cascadesContext.getStatementContext();
            statementContext.addIndexInSqlToString(pair,
                    Utils.qualifiedNameWithBackquote(qualifiedTableName));
        });

        // Handle meta table like "table_name$partitions"
        // qualifiedTableName should be like "ctl.db.tbl$partitions"
        Optional<LogicalPlan> logicalPlan = handleMetaTable(table, unboundRelation, qualifiedTableName);
        if (logicalPlan.isPresent()) {
            return logicalPlan.get();
        }

        List<String> qualifierWithoutTableName = Lists.newArrayList();
        qualifierWithoutTableName.addAll(qualifiedTableName.subList(0, qualifiedTableName.size() - 1));
        boolean isView = false;
        try {
            switch (table.getType()) {
                case OLAP:
                case MATERIALIZED_VIEW:
                    return makeOlapScan(table, unboundRelation, qualifierWithoutTableName);
                case VIEW:
                    View view = (View) table;
                    isView = true;
                    Plan viewBody = parseAndAnalyzeDorisView(view, qualifiedTableName, cascadesContext);
                    LogicalView<Plan> logicalView = new LogicalView<>(view, viewBody);
                    return new LogicalSubQueryAlias<>(qualifiedTableName, logicalView);
                case HMS_EXTERNAL_TABLE:
                    HMSExternalTable hmsTable = (HMSExternalTable) table;
                    if (Config.enable_query_hive_views && hmsTable.isView()) {
                        isView = true;
                        String hiveCatalog = hmsTable.getCatalog().getName();
                        String hiveDb = hmsTable.getDatabase().getFullName();
                        String ddlSql = hmsTable.getViewText();
                        Plan hiveViewPlan = parseAndAnalyzeHiveView(
                                hmsTable, hiveCatalog, hiveDb, ddlSql, cascadesContext);
                        return new LogicalSubQueryAlias<>(qualifiedTableName, hiveViewPlan);
                    }
                    if (hmsTable.getDlaType() == DLAType.HUDI) {
                        LogicalHudiScan hudiScan = new LogicalHudiScan(unboundRelation.getRelationId(), hmsTable,
                                qualifierWithoutTableName, unboundRelation.getTableSample(),
                                unboundRelation.getTableSnapshot());
                        hudiScan = hudiScan.withScanParams(hmsTable, unboundRelation.getScanParams());
                        return hudiScan;
                    } else {
                        return new LogicalFileScan(unboundRelation.getRelationId(), (HMSExternalTable) table,
                                qualifierWithoutTableName,
                                unboundRelation.getTableSample(),
                                unboundRelation.getTableSnapshot());
                    }
                case ICEBERG_EXTERNAL_TABLE:
                case PAIMON_EXTERNAL_TABLE:
                case MAX_COMPUTE_EXTERNAL_TABLE:
                    return new LogicalFileScan(unboundRelation.getRelationId(), (ExternalTable) table,
                            qualifierWithoutTableName, unboundRelation.getTableSample(),
                            unboundRelation.getTableSnapshot());
                case SCHEMA:
                    return new LogicalSchemaScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                case JDBC_EXTERNAL_TABLE:
                case JDBC:
                    return new LogicalJdbcScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                case ODBC:
                    return new LogicalOdbcScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                case ES_EXTERNAL_TABLE:
                case ELASTICSEARCH:
                    return new LogicalEsScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                case TEST_EXTERNAL_TABLE:
                    return new LogicalTestScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                default:
                    throw new AnalysisException("Unsupported tableType " + table.getType());
            }
        } finally {
            if (!isView) {
                Optional<SqlCacheContext> sqlCacheContext = cascadesContext.getStatementContext().getSqlCacheContext();
                if (sqlCacheContext.isPresent()) {
                    if (table instanceof OlapTable) {
                        sqlCacheContext.get().addUsedTable(table);
                    } else {
                        sqlCacheContext.get().setHasUnsupportedTables(true);
                    }
                }
            }
        }
    }

    private Plan parseAndAnalyzeHiveView(
            HMSExternalTable table, String hiveCatalog, String hiveDb, String ddlSql, CascadesContext cascadesContext) {
        ConnectContext ctx = cascadesContext.getConnectContext();
        String previousCatalog = ctx.getCurrentCatalog().getName();
        String previousDb = ctx.getDatabase();
        // change catalog and db to hive catalog and db, so that we can parse and analyze the view sql in hive context.
        ctx.changeDefaultCatalog(hiveCatalog);
        ctx.setDatabase(hiveDb);
        try {
            return parseAndAnalyzeView(table, ddlSql, cascadesContext);
        } finally {
            // restore catalog and db in connect context
            ctx.changeDefaultCatalog(previousCatalog);
            ctx.setDatabase(previousDb);
        }
    }

    private Plan parseAndAnalyzeDorisView(View view, List<String> tableQualifier, CascadesContext parentContext) {
        String viewDef = parentContext.getStatementContext().getAndCacheViewInfo(tableQualifier, view);
        return parseAndAnalyzeView(view, viewDef, parentContext);
    }

    private Plan parseAndAnalyzeView(TableIf view, String ddlSql, CascadesContext parentContext) {
        parentContext.getStatementContext().addViewDdlSql(ddlSql);
        Optional<SqlCacheContext> sqlCacheContext = parentContext.getStatementContext().getSqlCacheContext();
        if (sqlCacheContext.isPresent()) {
            sqlCacheContext.get().addUsedView(view, ddlSql);
        }
        LogicalPlan parsedViewPlan = new NereidsParser().parseSingle(ddlSql);
        // TODO: use a good to do this, such as eliminate UnboundResultSink
        if (parsedViewPlan instanceof UnboundResultSink) {
            parsedViewPlan = (LogicalPlan) ((UnboundResultSink<?>) parsedViewPlan).child();
        }
        CascadesContext viewContext = CascadesContext.initContext(
                parentContext.getStatementContext(), parsedViewPlan, PhysicalProperties.ANY);
        viewContext.keepOrShowPlanProcess(parentContext.showPlanProcess(), () -> {
            viewContext.newAnalyzer().analyze();
        });
        parentContext.addPlanProcesses(viewContext.getPlanProcesses());
        // we should remove all group expression of the plan which in other memo, so the groupId would not conflict
        return viewContext.getRewritePlan();
    }

    private List<Long> getPartitionIds(TableIf t, UnboundRelation unboundRelation, List<String> qualifier) {
        List<String> parts = unboundRelation.getPartNames();
        if (CollectionUtils.isEmpty(parts)) {
            return ImmutableList.of();
        }
        if (!t.isManagedTable()) {
            throw new AnalysisException(String.format(
                    "Only OLAP table is support select by partition for now,"
                            + "Table: %s is not OLAP table", t.getName()));
        }
        return parts.stream().map(name -> {
            Partition part = ((OlapTable) t).getPartition(name, unboundRelation.isTempPart());
            if (part == null) {
                List<String> qualified;
                if (!CollectionUtils.isEmpty(qualifier)) {
                    qualified = qualifier;
                } else {
                    qualified = Lists.newArrayList();
                }
                qualified.add(unboundRelation.getTableName());
                throw new AnalysisException(String.format("Partition: %s is not exists on table %s",
                        name, String.join(".", qualified)));
            }
            return part.getId();
        }).collect(ImmutableList.toImmutableList());
    }
}

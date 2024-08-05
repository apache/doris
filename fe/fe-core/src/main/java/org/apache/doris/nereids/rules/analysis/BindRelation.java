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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.es.EsExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
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

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Rule to bind relations in query plan.
 */
public class BindRelation extends OneAnalysisRuleFactory {

    private final Optional<CustomTableResolver> customTableResolver;

    public BindRelation() {
        this(Optional.empty());
    }

    public BindRelation(Optional<CustomTableResolver> customTableResolver) {
        this.customTableResolver = customTableResolver;
    }

    // TODO: cte will be copied to a sub-query with different names but the id of the unbound relation in them
    //  are the same, so we use new relation id when binding relation, and will fix this bug later.
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
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                unboundRelation.getNameParts());
        TableIf table = null;
        if (customTableResolver.isPresent()) {
            table = customTableResolver.get().apply(tableQualifier);
        }
        // In some cases even if we have already called the "cascadesContext.getTableByName",
        // it also gets the null. So, we just check it in the catalog again for safety.
        if (table == null) {
            table = RelationUtil.getTable(tableQualifier, cascadesContext.getConnectContext().getEnv());
        }

        // TODO: should generate different Scan sub class according to table's type
        LogicalPlan scan = getLogicalPlan(table, unboundRelation, tableQualifier, cascadesContext);
        if (cascadesContext.isLeadingJoin()) {
            LeadingHint leading = (LeadingHint) cascadesContext.getHintMap().get("Leading");
            leading.putRelationIdAndTableName(Pair.of(unboundRelation.getRelationId(), tableName));
            leading.getRelationIdToScanMap().put(unboundRelation.getRelationId(), scan);
        }
        return scan;
    }

    private LogicalPlan bind(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        List<String> qualifiedTablName = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                unboundRelation.getNameParts());
        TableIf table = null;
        if (customTableResolver.isPresent()) {
            table = customTableResolver.get().apply(qualifiedTablName);
        }
        // In some cases even if we have already called the "cascadesContext.getTableByName",
        // it also gets the null. So, we just check it in the catalog again for safety.
        if (table == null) {
            table = RelationUtil.getTable(qualifiedTablName, cascadesContext.getConnectContext().getEnv());
        }
        return getLogicalPlan(table, unboundRelation, qualifiedTablName, cascadesContext);
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
        if (!Util.showHiddenColumns() && scan.getTable().hasDeleteSign()
                && !ConnectContext.get().getSessionVariable().skipDeleteSign()) {
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
            if (!((OlapTable) table).getEnableUniqueKeyMergeOnWrite()) {
                scan = scan.withPreAggStatus(PreAggStatus.off(
                        Column.DELETE_SIGN + " is used as conjuncts."));
            }
            return new LogicalFilter<>(Sets.newHashSet(conjunct), scan);
        }
        return scan;
    }

    private LogicalPlan getLogicalPlan(TableIf table, UnboundRelation unboundRelation,
                                               List<String> qualifiedTableName, CascadesContext cascadesContext) {
        // for create view stmt replace tablNname to ctl.db.tableName
        unboundRelation.getIndexInSqlString().ifPresent(pair -> {
            StatementContext statementContext = cascadesContext.getStatementContext();
            statementContext.addIndexInSqlToString(pair,
                    Utils.qualifiedNameWithBackquote(qualifiedTableName));
        });
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
                    String inlineViewDef = view.getInlineViewDef();
                    Plan viewBody = parseAndAnalyzeView(view, inlineViewDef, cascadesContext);
                    LogicalView<Plan> logicalView = new LogicalView<>(view, viewBody);
                    return new LogicalSubQueryAlias<>(qualifiedTableName, logicalView);
                case HMS_EXTERNAL_TABLE:
                    HMSExternalTable hmsTable = (HMSExternalTable) table;
                    if (Config.enable_query_hive_views && hmsTable.isView()) {
                        isView = true;
                        String hiveCatalog = hmsTable.getCatalog().getName();
                        String ddlSql = hmsTable.getViewText();
                        Plan hiveViewPlan = parseAndAnalyzeHiveView(hmsTable, hiveCatalog, ddlSql, cascadesContext);
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
                                qualifierWithoutTableName, unboundRelation.getTableSample(),
                                unboundRelation.getTableSnapshot());
                    }
                case ICEBERG_EXTERNAL_TABLE:
                case PAIMON_EXTERNAL_TABLE:
                case MAX_COMPUTE_EXTERNAL_TABLE:
                case TRINO_CONNECTOR_EXTERNAL_TABLE:
                case LAKESOUl_EXTERNAL_TABLE:
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
                    return new LogicalEsScan(unboundRelation.getRelationId(), (EsExternalTable) table,
                            qualifierWithoutTableName);
                case TEST_EXTERNAL_TABLE:
                    return new LogicalTestScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                default:
                    try {
                        // TODO: support other type table, such as ELASTICSEARCH
                        cascadesContext.getConnectContext().getSessionVariable().enableFallbackToOriginalPlannerOnce();
                    } catch (Exception e) {
                        // ignore
                    }
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
            HMSExternalTable table, String hiveCatalog, String ddlSql, CascadesContext cascadesContext) {
        ConnectContext ctx = cascadesContext.getConnectContext();
        String previousCatalog = ctx.getCurrentCatalog().getName();
        String previousDb = ctx.getDatabase();
        ctx.changeDefaultCatalog(hiveCatalog);
        Plan hiveViewPlan = parseAndAnalyzeView(table, ddlSql, cascadesContext);
        ctx.changeDefaultCatalog(previousCatalog);
        ctx.setDatabase(previousDb);
        return hiveViewPlan;
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
            viewContext.newAnalyzer(customTableResolver).analyze();
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

    /** CustomTableResolver */
    public interface CustomTableResolver extends Function<List<String>, TableIf> {}
}

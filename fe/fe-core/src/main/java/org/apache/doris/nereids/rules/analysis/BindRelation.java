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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.external.EsExternalTable;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.catalog.external.JdbcExternalTable;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.CTEContext;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalEsScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.RelationUtil;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Optional;

/**
 * Rule to bind relations in query plan.
 */
public class BindRelation extends OneAnalysisRuleFactory {

    // TODO: cte will be copied to a sub-query with different names but the id of the unbound relation in them
    //  are the same, so we use new relation id when binding relation, and will fix this bug later.
    @Override
    public Rule build() {
        return unboundRelation().thenApply(ctx -> {
            Plan plan = doBindRelation(ctx);
            if (!(plan instanceof Unbound)) {
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
            case 2: { // db.table
                // Use database name from table name parts.
                return bindWithDbNameFromNamePart(ctx.cascadesContext, ctx.root);
            }
            case 3: { // catalog.db.table
                // Use catalog and database name from name parts.
                return bindWithCatalogNameFromNamePart(ctx.cascadesContext, ctx.root);
            }
            default:
                throw new IllegalStateException("Table name [" + ctx.root.getTableName() + "] is invalid.");
        }
    }

    private TableIf getTable(String catalogName, String dbName, String tableName, Env env) {
        CatalogIf catalog = env.getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new RuntimeException(String.format("Catalog %s does not exist.", catalogName));
        }
        DatabaseIf<TableIf> db = null;
        try {
            db = (DatabaseIf<TableIf>) catalog.getDb(dbName)
                    .orElseThrow(() -> new RuntimeException("Database [" + dbName + "] does not exist."));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return db.getTable(tableName).orElseThrow(() -> new RuntimeException(
            "Table [" + tableName + "] does not exist in database [" + dbName + "]."));
    }

    private LogicalPlan bindWithCurrentDb(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        String tableName = unboundRelation.getNameParts().get(0);
        // check if it is a CTE's name
        CTEContext cteContext = cascadesContext.getCteContext();
        Optional<LogicalPlan> analyzedCte = cteContext.getAnalyzedCTE(tableName);
        if (analyzedCte.isPresent()) {
            LogicalPlan ctePlan = analyzedCte.get();
            if (ctePlan instanceof LogicalSubQueryAlias
                    && ((LogicalSubQueryAlias<?>) ctePlan).getAlias().equals(tableName)) {
                return ctePlan;
            }
            return new LogicalSubQueryAlias<>(unboundRelation.getNameParts(), ctePlan);
        }
        String catalogName = cascadesContext.getConnectContext().getCurrentCatalog().getName();
        String dbName = cascadesContext.getConnectContext().getDatabase();
        TableIf table = getTable(catalogName, dbName, tableName, cascadesContext.getConnectContext().getEnv());
        // TODO: should generate different Scan sub class according to table's type
        List<String> tableQualifier = Lists.newArrayList(catalogName, dbName, tableName);
        return getLogicalPlan(table, unboundRelation, tableQualifier, cascadesContext);
    }

    private LogicalPlan bindWithDbNameFromNamePart(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        List<String> nameParts = unboundRelation.getNameParts();
        ConnectContext connectContext = cascadesContext.getConnectContext();
        String catalogName = cascadesContext.getConnectContext().getCurrentCatalog().getName();
        // if the relation is view, nameParts.get(0) is dbName.
        String dbName = nameParts.get(0);
        if (!dbName.equals(connectContext.getDatabase())) {
            dbName = connectContext.getClusterName() + ":" + dbName;
        }
        String tableName = nameParts.get(1);
        TableIf table = getTable(catalogName, dbName, tableName, connectContext.getEnv());
        List<String> tableQualifier = Lists.newArrayList(catalogName, dbName, tableName);
        return getLogicalPlan(table, unboundRelation, tableQualifier, cascadesContext);
    }

    private LogicalPlan bindWithCatalogNameFromNamePart(CascadesContext cascadesContext,
                                                        UnboundRelation unboundRelation) {
        List<String> nameParts = unboundRelation.getNameParts();
        ConnectContext connectContext = cascadesContext.getConnectContext();
        String catalogName = nameParts.get(0);
        String dbName = nameParts.get(1);
        if (!dbName.equals(connectContext.getDatabase())) {
            dbName = connectContext.getClusterName() + ":" + dbName;
        }
        String tableName = nameParts.get(2);
        TableIf table = getTable(catalogName, dbName, tableName, connectContext.getEnv());
        List<String> tableQualifier = Lists.newArrayList(catalogName, dbName, tableName);
        return getLogicalPlan(table, unboundRelation, tableQualifier, cascadesContext);
    }

    private LogicalPlan makeOlapScan(TableIf table, UnboundRelation unboundRelation, List<String> tableQualifier) {
        LogicalOlapScan scan;
        List<Long> partIds = getPartitionIds(table, unboundRelation);
        if (!CollectionUtils.isEmpty(partIds)) {
            scan = new LogicalOlapScan(RelationUtil.newRelationId(),
                    (OlapTable) table, ImmutableList.of(tableQualifier.get(1)), partIds, unboundRelation.getHints());
        } else {
            scan = new LogicalOlapScan(RelationUtil.newRelationId(),
                    (OlapTable) table, ImmutableList.of(tableQualifier.get(1)), unboundRelation.getHints());
        }
        if (!Util.showHiddenColumns() && scan.getTable().hasDeleteSign()
                && !ConnectContext.get().getSessionVariable()
                .skipDeleteSign()) {
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
            return new LogicalFilter(Sets.newHashSet(conjunct), scan);
        }
        return scan;
    }

    private LogicalPlan getLogicalPlan(TableIf table, UnboundRelation unboundRelation, List<String> tableQualifier,
                                       CascadesContext cascadesContext) {
        String dbName = tableQualifier.get(1); //[catalogName, dbName, tableName]
        switch (table.getType()) {
            case OLAP:
                return makeOlapScan(table, unboundRelation, tableQualifier);
            case VIEW:
                Plan viewPlan = parseAndAnalyzeView(((View) table).getDdlSql(), cascadesContext);
                return new LogicalSubQueryAlias<>(tableQualifier, viewPlan);
            case HMS_EXTERNAL_TABLE:
                return new LogicalFileScan(RelationUtil.newRelationId(),
                    (HMSExternalTable) table, ImmutableList.of(dbName));
            case SCHEMA:
                return new LogicalSchemaScan(RelationUtil.newRelationId(), table, ImmutableList.of(dbName));
            case JDBC_EXTERNAL_TABLE:
                return new LogicalJdbcScan(RelationUtil.newRelationId(),
                    (JdbcExternalTable) table, ImmutableList.of(dbName));
            case ES_EXTERNAL_TABLE:
                return new LogicalEsScan(RelationUtil.newRelationId(),
                    (EsExternalTable) table, ImmutableList.of(dbName));
            default:
                throw new AnalysisException("Unsupported tableType:" + table.getType());
        }
    }

    private Plan parseAndAnalyzeView(String viewSql, CascadesContext parentContext) {
        LogicalPlan parsedViewPlan = new NereidsParser().parseSingle(viewSql);
        CascadesContext viewContext = CascadesContext.newRewriteContext(
                parentContext.getStatementContext(), parsedViewPlan, PhysicalProperties.ANY);
        viewContext.newAnalyzer().analyze();

        // we should remove all group expression of the plan which in other memo, so the groupId would not conflict
        return viewContext.getRewritePlan();
    }

    private List<Long> getPartitionIds(TableIf t, UnboundRelation unboundRelation) {
        List<String> parts = unboundRelation.getPartNames();
        if (CollectionUtils.isEmpty(parts)) {
            return ImmutableList.of();
        }
        if (!t.getType().equals(TableIf.TableType.OLAP)) {
            throw new IllegalStateException(String.format(
                    "Only OLAP table is support select by partition for now,"
                            + "Table: %s is not OLAP table", t.getName()));
        }
        return parts.stream().map(name -> {
            Partition part = ((OlapTable) t).getPartition(name, unboundRelation.isTempPart());
            if (part == null) {
                throw new IllegalStateException(String.format("Partition: %s is not exists", name));
            }
            return part.getId();
        }).collect(ImmutableList.toImmutableList());
    }
}

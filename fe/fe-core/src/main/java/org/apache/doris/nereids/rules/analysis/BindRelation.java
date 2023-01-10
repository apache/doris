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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.RelationUtil;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Rule to bind relations in query plan.
 */
public class BindRelation extends OneAnalysisRuleFactory {

    // TODO: cte will be copied to a sub-query with different names but the id of the unbound relation in them
    //  are the same, so we use new relation id when binding relation, and will fix this bug later.
    @Override
    public Rule build() {
        return unboundRelation().thenApply(ctx -> {
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
        }).toRule(RuleType.BINDING_RELATION);
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
            return new LogicalSubQueryAlias<>(tableName, ctePlan);
        }
        String catalogName = cascadesContext.getConnectContext().getCurrentCatalog().getName();
        String dbName = cascadesContext.getConnectContext().getDatabase();
        TableIf table = getTable(catalogName, dbName, tableName, cascadesContext.getConnectContext().getEnv());
        // TODO: should generate different Scan sub class according to table's type
        return getLogicalPlan(table, unboundRelation, dbName, cascadesContext);
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
        return getLogicalPlan(table, unboundRelation, dbName, cascadesContext);
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
        return getLogicalPlan(table, unboundRelation, dbName, cascadesContext);
    }

    private LogicalPlan getLogicalPlan(TableIf table, UnboundRelation unboundRelation, String dbName,
                                       CascadesContext cascadesContext) {
        switch (table.getType()) {
            case OLAP:
                List<Long> partIds = getPartitionIds(table, unboundRelation);
                if (!CollectionUtils.isEmpty(partIds)) {
                    return new LogicalOlapScan(RelationUtil.newRelationId(),
                        (OlapTable) table, ImmutableList.of(dbName), partIds);
                } else {
                    return new LogicalOlapScan(RelationUtil.newRelationId(),
                        (OlapTable) table, ImmutableList.of(dbName));
                }
            case VIEW:
                Plan viewPlan = parseAndAnalyzeView(((View) table).getDdlSql(), cascadesContext);
                return new LogicalSubQueryAlias<>(table.getName(), viewPlan);
            case HMS_EXTERNAL_TABLE:
                return new LogicalFileScan(cascadesContext.getStatementContext().getNextRelationId(),
                    (HMSExternalTable) table, ImmutableList.of(dbName));
            case SCHEMA:
                return new LogicalSchemaScan(RelationUtil.newRelationId(), (Table) table, ImmutableList.of(dbName));
            default:
                throw new AnalysisException("Unsupported tableType:" + table.getType());
        }
    }

    private Plan parseAndAnalyzeView(String viewSql, CascadesContext parentContext) {
        LogicalPlan parsedViewPlan = new NereidsParser().parseSingle(viewSql);
        CascadesContext viewContext = new Memo(parsedViewPlan)
                .newCascadesContext(parentContext.getStatementContext());
        viewContext.newAnalyzer().analyze();

        // we should remove all group expression of the plan which in other memo, so the groupId would not conflict
        return viewContext.getMemo().copyOut(false);
    }

    private List<Long> getPartitionIds(TableIf t, UnboundRelation unboundRelation) {
        List<String> parts = unboundRelation.getPartNames();
        if (CollectionUtils.isEmpty(parts)) {
            return Collections.emptyList();
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
        }).collect(Collectors.toList());
    }
}

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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
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
                default:
                    throw new IllegalStateException("Table name [" + ctx.root.getTableName() + "] is invalid.");
            }
        }).toRule(RuleType.BINDING_RELATION);
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

        String dbName = cascadesContext.getConnectContext().getDatabase();
        Table table = cascadesContext.getTable(dbName, tableName, cascadesContext.getConnectContext().getEnv());
        // TODO: should generate different Scan sub class according to table's type
        List<Long> partIds = getPartitionIds(table, unboundRelation);
        if (table.getType() == TableType.OLAP) {
            if (!CollectionUtils.isEmpty(partIds)) {
                return new LogicalOlapScan(RelationUtil.newRelationId(),
                        (OlapTable) table, ImmutableList.of(dbName), partIds);
            } else {
                return new LogicalOlapScan(RelationUtil.newRelationId(),
                        (OlapTable) table, ImmutableList.of(dbName));
            }
        } else if (table.getType() == TableType.VIEW) {
            Plan viewPlan = parseAndAnalyzeView(table.getDdlSql(), cascadesContext);
            return new LogicalSubQueryAlias<>(table.getName(), viewPlan);
        } else if (table.getType() == TableType.SCHEMA) {
            return new LogicalSchemaScan(RelationUtil.newRelationId(), table, ImmutableList.of(dbName));
        }
        throw new AnalysisException("Unsupported tableType:" + table.getType());
    }

    private LogicalPlan bindWithDbNameFromNamePart(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        List<String> nameParts = unboundRelation.getNameParts();
        ConnectContext connectContext = cascadesContext.getConnectContext();
        // if the relation is view, nameParts.get(0) is dbName.
        String dbName = nameParts.get(0);
        if (!dbName.equals(connectContext.getDatabase())) {
            dbName = connectContext.getClusterName() + ":" + dbName;
        }
        Table table = cascadesContext.getTable(dbName, nameParts.get(1), connectContext.getEnv());
        List<Long> partIds = getPartitionIds(table, unboundRelation);
        if (table.getType() == TableType.OLAP) {
            if (!CollectionUtils.isEmpty(partIds)) {
                return new LogicalOlapScan(RelationUtil.newRelationId(), (OlapTable) table,
                        ImmutableList.of(dbName), partIds);
            } else {
                return new LogicalOlapScan(RelationUtil.newRelationId(), (OlapTable) table, ImmutableList.of(dbName));
            }
        } else if (table.getType() == TableType.VIEW) {
            Plan viewPlan = parseAndAnalyzeView(table.getDdlSql(), cascadesContext);
            return new LogicalSubQueryAlias<>(table.getName(), viewPlan);
        } else if (table.getType() == TableType.SCHEMA) {
            return new LogicalSchemaScan(RelationUtil.newRelationId(), table, ImmutableList.of(dbName));
        }
        throw new AnalysisException("Unsupported tableType:" + table.getType());
    }

    private Plan parseAndAnalyzeView(String viewSql, CascadesContext parentContext) {
        LogicalPlan parsedViewPlan = new NereidsParser().parseSingle(viewSql);
        CascadesContext viewContext = new Memo(parsedViewPlan)
                .newCascadesContext(parentContext.getStatementContext());
        viewContext.newAnalyzer().analyze();

        // we should remove all group expression of the plan which in other memo, so the groupId would not conflict
        return viewContext.getMemo().copyOut(false);
    }

    private List<Long> getPartitionIds(Table t, UnboundRelation unboundRelation) {
        List<String> parts = unboundRelation.getPartNames();
        if (CollectionUtils.isEmpty(parts)) {
            return Collections.emptyList();
        }
        if (!t.getType().equals(TableType.OLAP)) {
            throw new IllegalStateException(String.format(
                    "Only OLAP table is support select by partition for now,"
                            + "Table: %s is not OLAP table", t.getName()));
        }
        return parts.stream().map(name -> {
            Partition part = ((OlapTable) t).getPartition(name);
            if (part == null) {
                throw new IllegalStateException(String.format("Partition: %s is not exists", name));
            }
            return part.getId();
        }).collect(Collectors.toList());
    }
}

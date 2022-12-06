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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Rule to bind relations in query plan.
 */
public class BindRelation extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return unboundRelation().thenApply(ctx -> {
            List<String> nameParts = ctx.root.getNameParts();
            switch (nameParts.size()) {
                case 1: { // table
                    // Use current database name from catalog.
                    return bindWithCurrentDb(ctx.cascadesContext, nameParts.get(0));
                }
                case 2: { // db.table
                    // Use database name from table name parts.
                    return bindWithDbNameFromNamePart(ctx.cascadesContext, nameParts);
                }
                default:
                    throw new IllegalStateException("Table name [" + ctx.root.getTableName() + "] is invalid.");
            }
        }).toRule(RuleType.BINDING_RELATION);
    }

    private Table getTable(String dbName, String tableName, Env env) {
        Database db = env.getInternalCatalog().getDb(dbName)
                .orElseThrow(() -> new RuntimeException("Database [" + dbName + "] does not exist."));
        db.readLock();
        try {
            return db.getTable(tableName).orElseThrow(() -> new RuntimeException(
                    "Table [" + tableName + "] does not exist in database [" + dbName + "]."));
        } finally {
            db.readUnlock();
        }
    }

    private LogicalPlan bindWithCurrentDb(CascadesContext cascadesContext, String tableName) {
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
        Table table = getTable(dbName, tableName, cascadesContext.getConnectContext().getEnv());
        // TODO: should generate different Scan sub class according to table's type
        if (table.getType() == TableType.OLAP) {
            return new LogicalOlapScan(cascadesContext.getStatementContext().getNextRelationId(),
                    (OlapTable) table, ImmutableList.of(dbName));
        } else if (table.getType() == TableType.VIEW) {
            Plan viewPlan = parseAndAnalyzeView(table.getDdlSql(), cascadesContext);
            return new LogicalSubQueryAlias<>(table.getName(), viewPlan);
        }
        throw new AnalysisException("Unsupported tableType:" + table.getType());
    }

    private LogicalPlan bindWithDbNameFromNamePart(CascadesContext cascadesContext, List<String> nameParts) {
        ConnectContext connectContext = cascadesContext.getConnectContext();
        // if the relation is view, nameParts.get(0) is dbName.
        String dbName = nameParts.get(0);
        if (!dbName.equals(connectContext.getDatabase())) {
            dbName = connectContext.getClusterName() + ":" + dbName;
        }
        Table table = getTable(dbName, nameParts.get(1), connectContext.getEnv());
        if (table.getType() == TableType.OLAP) {
            return new LogicalOlapScan(cascadesContext.getStatementContext().getNextRelationId(),
                    (OlapTable) table, ImmutableList.of(dbName));
        } else if (table.getType() == TableType.VIEW) {
            Plan viewPlan = parseAndAnalyzeView(table.getDdlSql(), cascadesContext);
            return new LogicalSubQueryAlias<>(table.getName(), viewPlan);
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
}

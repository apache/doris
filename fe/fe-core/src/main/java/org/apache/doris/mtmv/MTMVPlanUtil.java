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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class MTMVPlanUtil {

    public static ConnectContext createMTMVContext(MTMV mtmv) {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setQualifiedUser(Auth.ADMIN_USER);
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().enableFallbackToOriginalPlanner = false;
        ctx.getSessionVariable().enableNereidsDML = true;
        // Debug session variable should be disabled when refreshed
        ctx.getSessionVariable().skipDeletePredicate = false;
        ctx.getSessionVariable().skipDeleteBitmap = false;
        ctx.getSessionVariable().skipDeleteSign = false;
        ctx.getSessionVariable().skipStorageEngineMerge = false;
        ctx.getSessionVariable().showHiddenColumns = false;
        ctx.getSessionVariable().allowModifyMaterializedViewData = true;
        // Disable add default limit rule to avoid refresh data wrong
        ctx.getSessionVariable().setDisableNereidsRules(
                String.join(",", ImmutableSet.of(RuleType.ADD_DEFAULT_LIMIT.name())));
        Optional<String> workloadGroup = mtmv.getWorkloadGroup();
        if (workloadGroup.isPresent()) {
            ctx.getSessionVariable().setWorkloadGroup(workloadGroup.get());
        }
        ctx.setStartTime();
        // Set db&catalog to be used when creating materialized views to avoid SQL statements not writing the full path
        // After https://github.com/apache/doris/pull/36543,
        // After 1, this logic is no longer needed. This is to be compatible with older versions
        setCatalogAndDb(ctx, mtmv);
        return ctx;
    }

    private static void setCatalogAndDb(ConnectContext ctx, MTMV mtmv) {
        EnvInfo envInfo = mtmv.getEnvInfo();
        if (envInfo == null) {
            return;
        }
        // switch catalog;
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(envInfo.getCtlId());
        // if catalog not exist, it may not have any impact, so there is no error and it will be returned directly
        if (catalog == null) {
            return;
        }
        ctx.changeDefaultCatalog(catalog.getName());
        // use db
        Optional<? extends DatabaseIf<? extends TableIf>> databaseIf = catalog.getDb(envInfo.getDbId());
        // if db not exist, it may not have any impact, so there is no error and it will be returned directly
        if (!databaseIf.isPresent()) {
            return;
        }
        ctx.setDatabase(databaseIf.get().getFullName());
    }

    public static MTMVRelation generateMTMVRelation(Set<TableIf> tablesInPlan, ConnectContext ctx) {
        Set<BaseTableInfo> oneLevelTables = Sets.newHashSet();
        Set<BaseTableInfo> allLevelTables = Sets.newHashSet();
        Set<BaseTableInfo> oneLevelViews = Sets.newHashSet();
        for (TableIf table : tablesInPlan) {
            BaseTableInfo baseTableInfo = new BaseTableInfo(table);
            if (table.getType() == TableType.VIEW) {
                // TODO reopen it after we support mv on view
                // oneLevelViews.add(baseTableInfo);
            } else {
                oneLevelTables.add(baseTableInfo);
                allLevelTables.add(baseTableInfo);
                if (table instanceof MTMV) {
                    allLevelTables.addAll(((MTMV) table).getRelation().getBaseTables());
                }
            }
        }
        return new MTMVRelation(allLevelTables, oneLevelTables, oneLevelViews);
    }

    public static Set<TableIf> getBaseTableFromQuery(String querySql, ConnectContext ctx) {
        List<StatementBase> statements;
        try {
            statements = new NereidsParser().parseSQL(querySql);
        } catch (Exception e) {
            throw new ParseException("Nereids parse failed. " + e.getMessage());
        }
        StatementBase parsedStmt = statements.get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        StatementContext original = ctx.getStatementContext();
        try (StatementContext tempCtx = new StatementContext()) {
            ctx.setStatementContext(tempCtx);
            try {
                NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
                planner.planWithLock(logicalPlan, PhysicalProperties.ANY, ExplainLevel.ANALYZED_PLAN);
                return Sets.newHashSet(ctx.getStatementContext().getTables().values());
            } finally {
                ctx.setStatementContext(original);
            }
        }
    }
}

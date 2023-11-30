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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector.TableCollectorContext;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * when do some operation, do something about cache
 */
public class MTMVCacheManager implements MTMVHookService {
    private static final Logger LOG = LogManager.getLogger(MTMVCacheManager.class);
    private Map<BaseTableInfo, Set<BaseTableInfo>> tableMTMVs = Maps.newConcurrentMap();

    public Set<BaseTableInfo> getMtmvsByBaseTable(BaseTableInfo table) {
        return tableMTMVs.get(table);
    }

    public boolean isAvailableMTMV(MTMV mtmv, ConnectContext ctx) throws AnalysisException, DdlException {
        // check session variable if enable rewrite
        if (!ctx.getSessionVariable().isEnableMvRewrite()) {
            return false;
        }
        MTMVRelation mtmvRelation = mtmv.getRelation();
        if (mtmvRelation == null) {
            return false;
        }
        // chaek mv is normal
        if (!(mtmv.getStatus().getState() == MTMVState.NORMAL
                && mtmv.getStatus().getRefreshState() == MTMVRefreshState.SUCCESS)) {
            return false;
        }
        // check external table
        boolean containsExternalTable = containsExternalTable(mtmvRelation.getBaseTables());
        if (containsExternalTable) {
            return ctx.getSessionVariable().isEnableExternalMvRewrite();
        }
        // check gracePeriod
        Long gracePeriod = mtmv.getGracePeriod();
        // do not care data is delayed
        if (gracePeriod < 0) {
            return true;
        }
        // compare with base table
        Long mtmvLastTime = getTableLastVisibleVersionTime(mtmv);
        Long maxAvailableTime = mtmvLastTime + gracePeriod;
        for (BaseTableInfo baseTableInfo : mtmvRelation.getBaseTables()) {
            long tableLastVisibleVersionTime = getTableLastVisibleVersionTime(baseTableInfo);
            if (tableLastVisibleVersionTime > maxAvailableTime) {
                return false;
            }
        }
        return true;
    }

    private long getTableLastVisibleVersionTime(BaseTableInfo baseTableInfo) throws AnalysisException, DdlException {
        Table table = Env.getCurrentEnv().getInternalCatalog()
                .getDbOrAnalysisException(baseTableInfo.getDbId())
                .getTableOrDdlException(baseTableInfo.getTableId(), TableType.OLAP);
        return getTableLastVisibleVersionTime((OlapTable) table);
    }

    private long getTableLastVisibleVersionTime(OlapTable table) {
        long result = 0L;
        long visibleVersionTime;
        for (Partition partition : table.getAllPartitions()) {
            visibleVersionTime = partition.getVisibleVersionTime();
            if (visibleVersionTime > result) {
                result = visibleVersionTime;
            }
        }
        return result;
    }

    private boolean containsExternalTable(Set<BaseTableInfo> baseTableInfos) {
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            if (InternalCatalog.INTERNAL_CATALOG_ID != baseTableInfo.getCtlId()) {
                return true;
            }
        }
        return false;
    }

    public static MTMVRelation generateMTMVRelation(MTMV mtmv, ConnectContext ctx) {
        Plan plan = getPlanBySql(mtmv.getQuerySql(), ctx);
        return new MTMVRelation(getBaseTables(plan), getBaseViews(plan));
    }

    private static Set<BaseTableInfo> getBaseTables(Plan plan) {
        TableCollectorContext collectorContext =
                new TableCollector.TableCollectorContext(
                        Sets.newHashSet(TableType.MATERIALIZED_VIEW, TableType.OLAP));
        plan.accept(TableCollector.INSTANCE, collectorContext);
        List<TableIf> collectedTables = collectorContext.getCollectedTables();
        return transferTableIfToInfo(collectedTables);
    }

    private static Set<BaseTableInfo> getBaseViews(Plan plan) {
        TableCollectorContext collectorContext =
                new TableCollector.TableCollectorContext(
                        Sets.newHashSet(TableType.VIEW));
        plan.accept(TableCollector.INSTANCE, collectorContext);
        List<TableIf> collectedTables = collectorContext.getCollectedTables();
        return transferTableIfToInfo(collectedTables);
    }

    private static Set<BaseTableInfo> transferTableIfToInfo(List<TableIf> tables) {
        Set<BaseTableInfo> result = Sets.newHashSet();
        for (TableIf table : tables) {
            result.add(new BaseTableInfo(table));
        }
        return result;
    }

    private static Plan getPlanBySql(String querySql, ConnectContext ctx) {
        List<StatementBase> statements;
        try {
            statements = new NereidsParser().parseSQL(querySql);
        } catch (Exception e) {
            throw new ParseException("Nereids parse failed. " + e.getMessage());
        }
        StatementBase parsedStmt = statements.get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        return planner.plan(logicalPlan, PhysicalProperties.ANY, ExplainLevel.NONE);
    }

    private Set<BaseTableInfo> getOrCreateMTMVs(BaseTableInfo baseTableInfo) {
        if (!tableMTMVs.containsKey(baseTableInfo)) {
            tableMTMVs.put(baseTableInfo, Sets.newConcurrentHashSet());
        }
        return tableMTMVs.get(baseTableInfo);
    }

    private void refreshMTMVCache(MTMVRelation relation, BaseTableInfo mtmvInfo) {
        LOG.info("refreshMTMVCache,relation: {}, mtmvInfo: {}", relation, mtmvInfo);
        removeMTMV(mtmvInfo);
        addMTMV(relation, mtmvInfo);
    }

    private void addMTMV(MTMVRelation relation, BaseTableInfo mtmvInfo) {
        if (relation == null) {
            return;
        }
        addMTMVTables(relation.getBaseTables(), mtmvInfo);
        addMTMVTables(relation.getBaseViews(), mtmvInfo);
    }

    private void addMTMVTables(Set<BaseTableInfo> baseTables, BaseTableInfo mtmvInfo) {
        if (CollectionUtils.isEmpty(baseTables)) {
            return;
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            getOrCreateMTMVs(baseTableInfo).add(mtmvInfo);
        }
    }

    private void removeMTMV(BaseTableInfo mtmvInfo) {
        for (Set<BaseTableInfo> sets : tableMTMVs.values()) {
            sets.remove(mtmvInfo);
        }
    }

    @Override
    public void createMTMV(MTMV mtmv) throws DdlException {

    }

    @Override
    public void dropMTMV(MTMV mtmv) throws DdlException {

    }

    /**
     * modify `tableMTMVs` by MTMVRelation
     * @param mtmv
     * @param dbId
     */
    @Override
    public void registerMTMV(MTMV mtmv, Long dbId) {
        refreshMTMVCache(mtmv.getRelation(), new BaseTableInfo(mtmv.getId(), dbId));
    }

    /**
     * remove cache of mtmv
     * @param mtmv
     */
    @Override
    public void deregisterMTMV(MTMV mtmv) {
        removeMTMV(new BaseTableInfo(mtmv));
    }

    @Override
    public void alterMTMV(MTMV mtmv, AlterMTMV alterMTMV) throws DdlException {

    }

    @Override
    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException {

    }

    /**
     * modify `tableMTMVs` by MTMVRelation
     * @param mtmv
     * @param relation
     * @param task
     */
    @Override
    public void refreshComplete(MTMV mtmv, MTMVRelation relation, MTMVTask task) {
        if (task.getStatus() == TaskStatus.SUCCESS) {
            Objects.requireNonNull(relation);
            refreshMTMVCache(relation, new BaseTableInfo(mtmv));
        }
    }

    /**
     * update mtmv status to `SCHEMA_CHANGE`
     * @param table
     */
    @Override
    public void dropTable(Table table) {
        processBaseTableChange(table, "The base table has been deleted:");
    }

    /**
     * update mtmv status to `SCHEMA_CHANGE`
     * @param table
     */
    @Override
    public void alterTable(Table table) {
        processBaseTableChange(table, "The base table has been updated:");
    }

    private void processBaseTableChange(Table table, String msgPrefix) {
        BaseTableInfo baseTableInfo = new BaseTableInfo(table);
        Set<BaseTableInfo> mtmvsByBaseTable = getMtmvsByBaseTable(baseTableInfo);
        if (CollectionUtils.isEmpty(mtmvsByBaseTable)) {
            return;
        }
        for (BaseTableInfo mtmvInfo : mtmvsByBaseTable) {
            Table mtmv = null;
            try {
                mtmv = Env.getCurrentEnv().getInternalCatalog()
                        .getDbOrAnalysisException(mtmvInfo.getDbId())
                        .getTableOrAnalysisException(mtmvInfo.getTableId());
            } catch (AnalysisException e) {
                LOG.warn(e);
                continue;
            }
            TableNameInfo tableNameInfo = new TableNameInfo(mtmv.getQualifiedDbName(),
                    mtmv.getName());
            MTMVStatus status = new MTMVStatus(MTMVState.SCHEMA_CHANGE,
                    msgPrefix + baseTableInfo);
            Env.getCurrentEnv().alterMTMVStatus(tableNameInfo, status);
        }
    }
}

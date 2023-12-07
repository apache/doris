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

import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector.TableCollectorContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.esotericsoftware.minlog.Log;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

public class MTMVUtil {

    public static long getTableLastVisibleVersionTime(BaseTableInfo baseTableInfo) throws DdlException {
        // current,we not check external table
        if (InternalCatalog.INTERNAL_CATALOG_ID == baseTableInfo.getCtlId()) {
            return 0L;
        }
        Table table = Env.getCurrentEnv().getInternalCatalog()
                .getDbOrDdlException(baseTableInfo.getDbId())
                .getTableOrDdlException(baseTableInfo.getTableId(), TableType.OLAP);
        return getTableLastVisibleVersionTime((OlapTable) table);
    }

    public static TableIf getTable(BaseTableInfo baseTableInfo) throws AnalysisException {
        TableIf table = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(baseTableInfo.getCtlId())
                .getDbOrAnalysisException(baseTableInfo.getDbId())
                .getTableOrAnalysisException(baseTableInfo.getTableId());
        return table;
    }

    public static long getTableLastVisibleVersionTime(OlapTable table) {
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

    public static boolean isMTMVFresh(MTMV mtmv, Set<BaseTableInfo> baseTables,
            Set<String> excludedTriggerTables) {
        Long mtmvLastTime = getTableLastVisibleVersionTime(mtmv);
        Long maxAvailableTime = mtmvLastTime;
        for (BaseTableInfo baseTableInfo : baseTables) {
            TableIf table = null;
            try {
                table = getTable(baseTableInfo);
            } catch (AnalysisException e) {
                return false;
            }
            if (excludedTriggerTables.contains(table.getName())) {
                continue;
            }
            if (!(table instanceof OlapTable)) {
                continue;
            }
            long tableLastVisibleVersionTime = getTableLastVisibleVersionTime((OlapTable) table);
            if (tableLastVisibleVersionTime > maxAvailableTime) {
                return false;
            }
        }
        return true;
    }

    private static long getExistPartitionId(PartitionItem target, Map<Long, PartitionItem> sources) {
        for (Entry<Long, PartitionItem> entry : sources.entrySet()) {
            if (target.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return -1L;
    }

    public static void dropPartition(MTMV mtmv, Long partitionId) {
        if (!mtmv.writeLockIfExist()) {
            return;
        }
        try {
            Partition partition = mtmv.getPartition(partitionId);
            DropPartitionClause dropPartitionClause = new DropPartitionClause(false, partition.getName(), false, false);
            Env.getCurrentEnv().dropPartition((Database) mtmv.getDatabase(), mtmv, dropPartitionClause);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mtmv.writeUnlock();
        }
    }

    public static void addPartition(MTMV mtmv, OlapTable relatedTable, Long partitionId)
            throws AnalysisException, DdlException {
        PartitionDesc partitionDesc = relatedTable.getPartitionInfo().toPartitionDesc(relatedTable);
        Partition partition = relatedTable.getPartition(partitionId);
        SinglePartitionDesc oldPartitionDesc = partitionDesc.getSinglePartitionDescByName(partition.getName());

        Map<String, String> partitionProperties = Maps.newHashMap();
        SinglePartitionDesc singleRangePartitionDesc = new SinglePartitionDesc(true,
                "p_" + UUID.randomUUID().toString().replace("-", "_"),
                oldPartitionDesc.getPartitionKeyDesc(), partitionProperties);

        AddPartitionClause addPartitionClause = new AddPartitionClause(singleRangePartitionDesc,
                mtmv.getDefaultDistributionInfo().toDistributionDesc(), partitionProperties, false);
        Env.getCurrentEnv().addPartition((Database) mtmv.getDatabase(), mtmv.getName(), addPartitionClause);
    }

    //partition p1_city values in (('1', 'Beijing'), ('2', 'Shanghai')),
    //partition p2_city values in (('3',"tj"))

    // will has two PartitionItem
    // first PartitionItem will has 2 PartitionKey (('1', 'Beijing') and ('2', 'Shanghai'))
    // second PartitionItem will has 1 PartitionKey(('3',"tj"))
    public static void dealMvPartition(MTMV mtmv, OlapTable relatedTable)
            throws DdlException, AnalysisException {
        Map<Long, PartitionItem> relatedTableItems = relatedTable.getPartitionInfo().getIdToItem(false);
        Map<Long, PartitionItem> mtmvItems = mtmv.getPartitionInfo().getIdToItem(false);
        // drop partition of mtmv
        for (Entry<Long, PartitionItem> entry : mtmvItems.entrySet()) {
            long partitionId = getExistPartitionId(entry.getValue(), relatedTableItems);
            if (partitionId == -1L) {
                dropPartition(mtmv, entry.getKey());
            }
        }

        // add partition for mtmv
        for (Entry<Long, PartitionItem> entry : relatedTableItems.entrySet()) {
            long partitionId = getExistPartitionId(entry.getValue(), mtmvItems);
            if (partitionId == -1L) {
                addPartition(mtmv, relatedTable, entry.getKey());
            }
        }

    }

    public static Map<Long, Set<Long>> getMvToBasePartitions(MTMV mtmv, OlapTable relatedTable) {
        HashMap<Long, Set<Long>> res = Maps.newHashMap();
        Map<Long, PartitionItem> relatedTableItems = relatedTable.getPartitionInfo().getIdToItem(false);
        Map<Long, PartitionItem> mtmvItems = mtmv.getPartitionInfo().getIdToItem(false);
        for (Entry<Long, PartitionItem> entry : mtmvItems.entrySet()) {
            long partitionId = getExistPartitionId(entry.getValue(), relatedTableItems);
            res.put(entry.getKey(), Sets.newHashSet(partitionId));
        }
        return res;
    }

    public static Set<Long> getMTMVStalePartitions(MTMV mtmv, OlapTable relatedTable) {
        Set<Long> ids = Sets.newHashSet();
        Map<Long, Set<Long>> mvToBasePartitions = getMvToBasePartitions(mtmv, relatedTable);
        for (Entry<Long, Set<Long>> entry : mvToBasePartitions.entrySet()) {
            long mvVersionTime = mtmv.getPartition(entry.getKey()).getVisibleVersionTime();
            for (Long partitionId : entry.getValue()) {
                long visibleVersionTime = relatedTable.getPartition(partitionId).getVisibleVersionTime();
                if (visibleVersionTime > mvVersionTime) {
                    ids.add(entry.getKey());
                    break;
                }
            }
        }
        return ids;
    }

    public static Set<PartitionItem> getPartitionItemsByIds(MTMV mtmv, Set<Long> ids) {
        Set<PartitionItem> res = Sets.newHashSet();
        for (Long partitionId : ids) {
            res.add(mtmv.getPartitionInfo().getItem(partitionId));
        }
        return res;
    }

    public static List<String> getPartitionNamesByIds(MTMV mtmv, Set<Long> ids) {
        List<String> res = Lists.newArrayList();
        for (Long partitionId : ids) {
            res.add(mtmv.getPartition(partitionId).getName());
        }
        return res;
    }

    public static ConnectContext createMTMVContext(MTMV mtmv) throws AnalysisException {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setQualifiedUser(Auth.ADMIN_USER);
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(mtmv.getEnvInfo().getCtlId());
        ctx.changeDefaultCatalog(catalog.getName());
        ctx.setDatabase(catalog.getDbOrAnalysisException(mtmv.getEnvInfo().getDbId()).getFullName());
        ctx.getSessionVariable().enableFallbackToOriginalPlanner = false;
        return ctx;
    }

    public static MTMVRelation generateMTMVRelation(MTMV mtmv, ConnectContext ctx) {
        Plan plan = getPlanBySql(mtmv.getQuerySql(), ctx);
        return new MTMVRelation(getBaseTables(plan), getBaseViews(plan));
    }

    private static Set<BaseTableInfo> getBaseTables(Plan plan) {
        TableCollectorContext collectorContext =
                new TableCollector.TableCollectorContext(
                        com.google.common.collect.Sets.newHashSet(TableType.MATERIALIZED_VIEW, TableType.OLAP));
        plan.accept(TableCollector.INSTANCE, collectorContext);
        List<TableIf> collectedTables = collectorContext.getCollectedTables();
        return transferTableIfToInfo(collectedTables);
    }

    private static Set<BaseTableInfo> getBaseViews(Plan plan) {
        TableCollectorContext collectorContext =
                new TableCollector.TableCollectorContext(
                        com.google.common.collect.Sets.newHashSet(TableType.VIEW));
        plan.accept(TableCollector.INSTANCE, collectorContext);
        List<TableIf> collectedTables = collectorContext.getCollectedTables();
        return transferTableIfToInfo(collectedTables);
    }

    private static Set<BaseTableInfo> transferTableIfToInfo(List<TableIf> tables) {
        Set<BaseTableInfo> result = com.google.common.collect.Sets.newHashSet();
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

    public static boolean isSyncWithOlapTables(MTMV mtmv) {
        MTMVRelation mtmvRelation = mtmv.getRelation();
        if (mtmvRelation == null) {
            return false;
        }
        return isMTMVFresh(mtmv, mtmv.getRelation().getBaseTables(), Sets.newHashSet());
    }

    public static boolean isSyncWithOlapTables(MTMV mtmv, Long partitionId) {
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return isSyncWithOlapTables(mtmv);
        }
        try {
            OlapTable relatedTable = (OlapTable) MTMVUtil.getTable(mtmv.getMvPartitionInfo().getRelatedTable());
            if (mtmv.getRelation() == null || CollectionUtils.isEmpty(mtmv.getRelation().getBaseTables())) {
                return false;
            }
            boolean mtmvFresh = isMTMVFresh(mtmv, mtmv.getRelation().getBaseTables(),
                    Sets.newHashSet(relatedTable.getName()));
            if (!mtmvFresh) {
                return false;
            }
            PartitionItem item = mtmv.getPartitionInfo().getItem(partitionId);
            long relatedPartitionId = getExistPartitionId(item, relatedTable.getPartitionInfo().getIdToItem(false));
            if (partitionId == -1L) {
                return false;
            }
            return mtmv.getPartition(partitionId).getVisibleVersionTime() > relatedTable
                    .getPartition(relatedPartitionId).getVisibleVersionTime();
        } catch (AnalysisException e) {
            Log.warn(e.getMessage());
            return false;
        }
    }

    public static boolean isAvailableMTMV(MTMV mtmv, ConnectContext ctx) throws AnalysisException, DdlException {
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
        Long mtmvLastTime = MTMVUtil.getTableLastVisibleVersionTime(mtmv);
        Long maxAvailableTime = mtmvLastTime + gracePeriod;
        for (BaseTableInfo baseTableInfo : mtmvRelation.getBaseTables()) {
            long tableLastVisibleVersionTime = MTMVUtil.getTableLastVisibleVersionTime(baseTableInfo);
            if (tableLastVisibleVersionTime > maxAvailableTime) {
                return false;
            }
        }
        return true;
    }

    private static boolean containsExternalTable(Set<BaseTableInfo> baseTableInfos) {
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            if (InternalCatalog.INTERNAL_CATALOG_ID != baseTableInfo.getCtlId()) {
                return true;
            }
        }
        return false;
    }
}

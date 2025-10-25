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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.nereids.rules.exploration.mv.PartitionCompensator;
import org.apache.doris.nereids.trees.plans.commands.info.CancelMTMVTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PauseMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ResumeMTMVInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * when do some operation, do something about cache
 */
public class MTMVRelationManager implements MTMVHookService {
    private static final Logger LOG = LogManager.getLogger(MTMVRelationManager.class);
    // when
    // create v1 as select * from table1
    // create v2 as select * from v1
    // create mv1 as select * from v1;
    // create mv2 as select * from mv1;
    // `tableMTMVs` will have 3 pair: table1 ==> mv1, mv1==>mv2, table1 ==> mv2
    // `tableMTMVsOneLevelAndFromView` will have 2 pair: table1 ==> mv1, mv1==>mv2
    // `viewMTMVs` will have 2 pair: v1 ==> mv1, v2 ==> mv1
    private final Map<BaseTableInfo, Set<BaseTableInfo>> tableMTMVs = Maps.newConcurrentMap();
    private final Map<BaseTableInfo, Set<BaseTableInfo>> tableMTMVsOneLevelAndFromView = Maps.newConcurrentMap();
    // view => mtmv
    private final Map<BaseTableInfo, Set<BaseTableInfo>> viewMTMVs = Maps.newConcurrentMap();

    public Set<BaseTableInfo> getMtmvsByBaseTable(BaseTableInfo table) {
        return tableMTMVs.getOrDefault(table, ImmutableSet.of());
    }

    public Set<BaseTableInfo> getMtmvsByBaseView(BaseTableInfo table) {
        return viewMTMVs.getOrDefault(table, ImmutableSet.of());
    }

    public Set<BaseTableInfo> getMtmvsByBaseTableOneLevelAndFromView(BaseTableInfo table) {
        return tableMTMVsOneLevelAndFromView.getOrDefault(table, ImmutableSet.of());
    }

    /**
     * if At least one partition is available, return this mtmv
     *
     * @param candidateMTMVs
     * @param ctx
     * @return
     */
    public Set<MTMV> getAvailableMTMVs(Set<MTMV> candidateMTMVs, ConnectContext ctx,
            boolean forceConsistent, BiPredicate<ConnectContext, MTMV> predicate) {
        Set<MTMV> res = Sets.newLinkedHashSet();
        Map<List<String>, Set<String>> queryUsedPartitions = PartitionCompensator.getQueryUsedPartitions(
                ctx.getStatementContext(), new BitSet());
        for (MTMV mtmv : candidateMTMVs) {
            if (predicate.test(ctx, mtmv)) {
                continue;
            }
            if (!mtmv.isUseForRewrite()) {
                continue;
            }
            if (isMVPartitionValid(mtmv, ctx, forceConsistent, queryUsedPartitions)) {
                res.add(mtmv);
            }
        }
        return res;
    }

    /**
     * get candidate mtmv related to tableInfos.
     */
    public Set<MTMV> getCandidateMTMVs(List<BaseTableInfo> tableInfos) {
        Set<MTMV> mtmvs = Sets.newLinkedHashSet();
        Set<BaseTableInfo> mvInfos = getMTMVInfos(tableInfos);
        for (BaseTableInfo tableInfo : mvInfos) {
            try {
                MTMV mtmv = (MTMV) MTMVUtil.getTable(tableInfo);
                if (mtmv.canBeCandidate()) {
                    mtmvs.add(mtmv);
                }
            } catch (Exception e) {
                // not throw exception to client, just ignore it
                LOG.warn("getTable failed: {}", tableInfo.toString(), e);
            }
        }
        return mtmvs;
    }

    @VisibleForTesting
    public boolean isMVPartitionValid(MTMV mtmv, ConnectContext ctx, boolean forceConsistent,
            Map<List<String>, Set<String>> queryUsedPartitions) {
        long currentTimeMillis = System.currentTimeMillis();
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil.getMTMVCanRewritePartitions(
                mtmv, ctx, currentTimeMillis, forceConsistent, queryUsedPartitions);
        // MTMVRewriteUtil.getMTMVCanRewritePartitions is time-consuming behavior, So record for used later
        ctx.getStatementContext().getMvCanRewritePartitionsMap().putIfAbsent(
                new BaseTableInfo(mtmv), mtmvCanRewritePartitions);
        return !CollectionUtils.isEmpty(mtmvCanRewritePartitions);
    }

    private Set<BaseTableInfo> getMTMVInfos(List<BaseTableInfo> tableInfos) {
        Set<BaseTableInfo> mvInfos = Sets.newLinkedHashSet();
        for (BaseTableInfo tableInfo : tableInfos) {
            mvInfos.addAll(getMtmvsByBaseTable(tableInfo));
        }
        return mvInfos;
    }

    private Set<BaseTableInfo> getOrCreateMTMVs(BaseTableInfo baseTableInfo) {
        if (!tableMTMVs.containsKey(baseTableInfo)) {
            tableMTMVs.put(baseTableInfo, Sets.newConcurrentHashSet());
        }
        return tableMTMVs.get(baseTableInfo);
    }

    private Set<BaseTableInfo> getOrCreateMTMVsView(BaseTableInfo baseTableInfo) {
        if (!viewMTMVs.containsKey(baseTableInfo)) {
            viewMTMVs.put(baseTableInfo, Sets.newConcurrentHashSet());
        }
        return viewMTMVs.get(baseTableInfo);
    }

    private Set<BaseTableInfo> getOrCreateMTMVsOneLevelAndFromView(BaseTableInfo baseTableInfo) {
        if (!tableMTMVsOneLevelAndFromView.containsKey(baseTableInfo)) {
            tableMTMVsOneLevelAndFromView.put(baseTableInfo, Sets.newConcurrentHashSet());
        }
        return tableMTMVsOneLevelAndFromView.get(baseTableInfo);
    }

    public void refreshMTMVCache(MTMVRelation relation, BaseTableInfo mtmvInfo) {
        LOG.info("refreshMTMVCache,relation: {}, mtmvInfo: {}", relation, mtmvInfo);
        removeMTMV(mtmvInfo);
        addMTMV(relation, mtmvInfo);
    }

    private void addMTMV(MTMVRelation relation, BaseTableInfo mtmvInfo) {
        if (relation == null) {
            return;
        }
        addMTMVTables(relation.getBaseTables(), mtmvInfo);
        addMTMVViews(relation.getBaseViews(), mtmvInfo);
        addMTMVTablesOneLevelAndFromView(relation.getBaseTablesOneLevelAndFromView(), mtmvInfo);
    }

    private void addMTMVTables(Set<BaseTableInfo> baseTables, BaseTableInfo mtmvInfo) {
        if (CollectionUtils.isEmpty(baseTables)) {
            return;
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            getOrCreateMTMVs(baseTableInfo).add(mtmvInfo);
        }
    }

    private void addMTMVViews(Set<BaseTableInfo> baseTables, BaseTableInfo mtmvInfo) {
        if (CollectionUtils.isEmpty(baseTables)) {
            return;
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            getOrCreateMTMVsView(baseTableInfo).add(mtmvInfo);
        }
    }

    private void addMTMVTablesOneLevelAndFromView(Set<BaseTableInfo> baseTables, BaseTableInfo mtmvInfo) {
        if (CollectionUtils.isEmpty(baseTables)) {
            return;
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            getOrCreateMTMVsOneLevelAndFromView(baseTableInfo).add(mtmvInfo);
        }
    }

    private void removeMTMV(BaseTableInfo mtmvInfo) {
        for (Set<BaseTableInfo> sets : tableMTMVs.values()) {
            sets.remove(mtmvInfo);
        }
        for (Set<BaseTableInfo> sets : viewMTMVs.values()) {
            sets.remove(mtmvInfo);
        }
        for (Set<BaseTableInfo> sets : tableMTMVsOneLevelAndFromView.values()) {
            sets.remove(mtmvInfo);
        }
    }

    /**
     * modify `tableMTMVs` by MTMVRelation
     *
     * @param mtmv
     * @param dbId
     */
    @Override
    public void registerMTMV(MTMV mtmv, Long dbId) {
        refreshMTMVCache(mtmv.getRelation(), new BaseTableInfo(mtmv, dbId));
    }

    /**
     * remove cache of mtmv
     *
     * @param mtmv
     */
    @Override
    public void unregisterMTMV(MTMV mtmv) {
        removeMTMV(new BaseTableInfo(mtmv));
    }

    @Override
    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException {

    }

    /**
     * modify `tableMTMVs` by MTMVRelation
     *
     * @param mtmv
     * @param relation
     * @param task
     */
    @Override
    public void refreshComplete(MTMV mtmv, MTMVRelation relation, MTMVTask task) {
        if (task.getStatus() == TaskStatus.SUCCESS) {
            Objects.requireNonNull(relation);
            if (mtmv.isDropped) {
                return;
            }
            refreshMTMVCache(relation, new BaseTableInfo(mtmv));
        }
    }

    /**
     * update mtmv status to `SCHEMA_CHANGE`
     *
     * @param table
     */
    @Override
    public void dropTable(Table table) {
        processBaseTableChange(new BaseTableInfo(table), "The base table has been deleted:");
    }

    /**
     * update mtmv status to `SCHEMA_CHANGE`
     *
     * @param isReplace
     */
    @Override
    public void alterTable(BaseTableInfo oldTableInfo, Optional<BaseTableInfo> newTableInfo, boolean isReplace) {
        // when replace, need deal two table
        if (isReplace) {
            processBaseTableChange(newTableInfo.get(), "The base table has been updated:");
        }
        processBaseTableChange(oldTableInfo, "The base table has been updated:");
    }

    @Override
    public void pauseMTMV(PauseMTMVInfo info) throws MetaNotFoundException, DdlException, JobException {

    }

    @Override
    public void resumeMTMV(ResumeMTMVInfo info) throws MetaNotFoundException, DdlException, JobException {

    }

    @Override
    public void postCreateMTMV(MTMV mtmv) {

    }

    @Override
    public void cancelMTMVTask(CancelMTMVTaskInfo info) {

    }

    /**
     * update mtmv status to `SCHEMA_CHANGE` and drop snapshot
     *
     * @param baseViewInfo
     */
    @Override
    public void alterView(BaseTableInfo baseViewInfo) {
        processBaseViewChange(baseViewInfo, "The base view has been updated:");
    }

    /**
     * update mtmv status to `SCHEMA_CHANGE` and drop snapshot
     *
     * @param baseViewInfo
     */
    @Override
    public void dropView(BaseTableInfo baseViewInfo) {
        processBaseViewChange(baseViewInfo, "The base view has been dropped:");
    }

    private void processBaseViewChange(BaseTableInfo baseViewInfo, String msgPrefix) {
        Set<BaseTableInfo> mtmvsByBaseView = getMtmvsByBaseView(baseViewInfo);
        LOG.info("processBaseViewChange, baseViewInfo: {}, mtmvsByBaseView: {}", baseViewInfo, mtmvsByBaseView);
        if (CollectionUtils.isEmpty(mtmvsByBaseView)) {
            return;
        }
        for (BaseTableInfo mtmvInfo : mtmvsByBaseView) {
            MTMV mtmv = null;
            try {
                mtmv = MTMVUtil.getMTMV(mtmvInfo);
            } catch (AnalysisException e) {
                LOG.warn(e);
                continue;
            }
            String schemaChangeDetail = msgPrefix + baseViewInfo;
            mtmv.processBaseViewChange(schemaChangeDetail);
        }
    }

    private void processBaseTableChange(BaseTableInfo baseTableInfo, String msgPrefix) {
        Set<BaseTableInfo> mtmvsByBaseTable = getMtmvsByBaseTableOneLevelAndFromView(baseTableInfo);
        if (CollectionUtils.isEmpty(mtmvsByBaseTable)) {
            return;
        }
        for (BaseTableInfo mtmvInfo : mtmvsByBaseTable) {
            Table mtmv = null;
            try {
                mtmv = (Table) MTMVUtil.getTable(mtmvInfo);
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

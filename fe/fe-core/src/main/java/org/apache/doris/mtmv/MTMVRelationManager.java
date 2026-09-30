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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.nereids.lineage.LineageInfoExtractor;
import org.apache.doris.nereids.rules.exploration.mv.PartitionCompensator;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.CancelMTMVTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PauseMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ResumeMTMVInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
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

    public void markIvmBaselineRebuild(BaseTableInfo baseTableInfo, String reason) {
        markIvmBaselineRebuild(baseTableInfo, true, Collections.emptyMap(), reason);
    }

    public void markIvmBaselineRebuildForPartitionChange(BaseTableInfo baseTableInfo,
            Map<String, Long> changedPartitions, String reason) {
        Preconditions.checkArgument(!changedPartitions.isEmpty(), "changed partitions can not be empty");
        markIvmBaselineRebuild(baseTableInfo, false, changedPartitions, reason);
    }

    private void markIvmBaselineRebuild(BaseTableInfo baseTableInfo, boolean allPartitionsChanged,
            Map<String, Long> changedPartitions, String reason) {
        TableNameInfo baseTableName = new TableNameInfo(baseTableInfo.getCtlName(),
                baseTableInfo.getDbName(), baseTableInfo.getTableName());
        for (BaseTableInfo mtmvInfo : getMtmvsByBaseTableOneLevelAndFromView(baseTableInfo)) {
            MTMV mtmv;
            try {
                mtmv = MTMVUtil.getMTMV(mtmvInfo);
            } catch (AnalysisException e) {
                LOG.warn("Skip IVM baseline barrier because dependent MTMV is missing, "
                        + "baseTable={}, mtmv={}, reason={}", baseTableInfo, mtmvInfo, reason, e);
                continue;
            }
            if (!mtmv.isIvm()) {
                continue;
            }
            // Excluded tables have no IVM stream, so their changes cannot break the incremental baseline.
            if (MTMVPartitionUtil.isTableExcluded(mtmv.getExcludedTriggerTables(), baseTableName)) {
                continue;
            }
            boolean invalidated;
            if (allPartitionsChanged) {
                // Awaited here, where no MV lock is held: the DDL does not return until the invalidation is
                // durable, which is what it was before the record was handed back to the caller.
                mtmv.invalidateWholeMv(reason).await();
                invalidated = true;
            } else {
                invalidated = mtmv.invalidateIvmBaseline(baseTableInfo, changedPartitions, reason);
            }
            // A partition change that no MV partition reads leaves nothing to rebuild, and saying that it
            // invalidated the baseline would claim a persisted barrier that does not exist.
            if (invalidated) {
                LOG.info("Invalidated IVM baseline, baseTable={}, mtmv={}, reason={}",
                        baseTableInfo, mtmvInfo, reason);
            } else {
                LOG.info("No MV partition reads the changed base partitions, nothing to invalidate. "
                        + "baseTable={}, mtmv={}, reason={}", baseTableInfo, mtmvInfo, reason);
            }
        }
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
        if (relation == null) {
            removeMTMV(mtmvInfo);
            return;
        }
        // Publish new dependencies before pruning stale ones. A concurrent base-table DDL can then
        // find the MV through either relation and cannot miss invalidating its IVM baseline.
        addMTMV(relation, mtmvInfo);
        removeMTMVFromStaleRelations(tableMTMVs, relation.getBaseTables(), mtmvInfo);
        removeMTMVFromStaleRelations(viewMTMVs, relation.getBaseViews(), mtmvInfo);
        removeMTMVFromStaleRelations(tableMTMVsOneLevelAndFromView,
                relation.getBaseTablesOneLevelAndFromView(), mtmvInfo);
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

    private void removeMTMVFromStaleRelations(Map<BaseTableInfo, Set<BaseTableInfo>> relationMap,
            Set<BaseTableInfo> currentBaseTables, BaseTableInfo mtmvInfo) {
        for (Map.Entry<BaseTableInfo, Set<BaseTableInfo>> entry : relationMap.entrySet()) {
            if (CollectionUtils.isEmpty(currentBaseTables) || !currentBaseTables.contains(entry.getKey())) {
                entry.getValue().remove(mtmvInfo);
            }
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
        // The message below names the table and what became of it, which is what an MV that reads it has to
        // know; the query check would replace that with the weaker "the query is no longer analyzable",
        // because a dropped table is the one change whose query is gone beyond doubt. What the two record
        // is the same state either way. Unlike a rename it stays an invalidation: the table is gone for
        // good, so the state is not something a later alter can make obsolete.
        processBaseTableChange(new BaseTableInfo(table), "The base table has been deleted:", null);
    }

    /**
     * update mtmv status to `SCHEMA_CHANGE`.
     *
     * @param isReplace
     * @param queryJudgedColumns the names the alter gives the table or takes away from it, which leave the
     *                           judgement about each MV's state to that MV's own query, or null when the
     *                           alter is not one a query decides. The names are carried rather than judged
     *                           before the call because the judgement is about them; see
     *                           {@code AlterOp#queryJudgedColumnNames} for which operations name one, and
     *                           {@link #invalidateMvUnlessQueryHolds} for what is asked about it. A rename
     *                           of the base table names no column: it is left to the record below, which
     *                           says what the MV that keeps spelling the old name needs to hear
     */
    @Override
    public void alterTable(BaseTableInfo oldTableInfo, Optional<BaseTableInfo> newTableInfo, boolean isReplace,
            QueryJudgedChange queryJudgedChange) {
        // when replace, need deal two table
        if (isReplace) {
            // REPLACE TABLE already invalidates the IVM baseline explicitly, see Alter#processReplaceTable
            processBaseTableChange(newTableInfo.get(), "The base table has been updated:", null);
        }
        processBaseTableChange(oldTableInfo, "The base table has been updated:", queryJudgedChange);
    }


    /**
     * Whether the query, as it is analysed now, reads a column of any of these names, and reads it where
     * the change can reach it.
     *
     * <p>There are two such places, and they are the two ways a name is the change's to answer for. One is
     * a column of the table the change is about: that is the column this view's rows were computed from, and
     * the names are matched case-insensitively because a name is what moves. The other is a column the query
     * reaches across a scope boundary -- the plan records those on the Apply that stands for the subquery,
     * whose correlation slots are the outer columns its right side reads -- because such a name is the
     * scopes' to answer for rather than the query's: the nearest column to the reference answers for it, so
     * a column the change takes away from a scope inside leaves the name to one outside, and a column it
     * gives to a scope inside takes the name over, while the query goes on producing the columns it always
     * produced out of rows from somewhere else. A name reached with the qualifier of another table inside
     * the query's own scope is neither: no later change can move it, so one to a column it does not name is
     * one this view's rows do not depend on.
     */
    private static boolean reachesAnyColumnOf(Plan plan, BaseTableInfo baseTableInfo, Set<String> columnNames) {
        if (plan == null) {
            // A query whose plan was not kept is one this cannot be answered about, and "it does" is the
            // answer that keeps the view safe.
            return true;
        }
        Set<String> names = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        names.addAll(columnNames);
        LineageInfo lineage = LineageInfoExtractor.extractLineageInfo(plan);
        for (SetMultimap<?, Expression> byType : lineage.getDirectLineageMap().values()) {
            if (reachesAnyColumn(byType.values(), names, baseTableInfo)) {
                return true;
            }
        }
        // The dataset predicates once, not once per output column: the per-output copy of them the lineage
        // also offers holds the same expressions for every column the query produces, and scanning it would
        // visit each of them once per column.
        if (reachesAnyColumn(lineage.getDatasetIndirectLineageMap().values(), names, baseTableInfo)) {
            return true;
        }
        return reachesAnyColumnAcrossScopes(plan, lineage, names, baseTableInfo);
    }

    /** Whether any of these expressions reads a column of one of these names from this table. */
    private static boolean reachesAnyColumn(Collection<Expression> expressions, Set<String> names,
            BaseTableInfo baseTableInfo) {
        for (Expression expression : expressions) {
            for (Slot slot : expression.getInputSlots()) {
                if (names.contains(slot.getName()) && isColumnOf(slot, baseTableInfo)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Whether this slot is a column of this table, through whatever views stand between the two. */
    private static boolean isColumnOf(Slot slot, BaseTableInfo baseTableInfo) {
        if (!(slot instanceof SlotReference)) {
            return false;
        }
        return ((SlotReference) slot).getOriginalTable()
                .map(table -> new BaseTableInfo(table).equals(baseTableInfo))
                .orElse(false);
    }

    /**
     * Whether the query resolves a column of one of these names across a scope boundary, which is a name
     * the change can move whatever the query writes it against.
     */
    private static boolean reachesAnyColumnAcrossScopes(Plan plan, LineageInfo lineage, Set<String> names,
            BaseTableInfo baseTableInfo) {
        // A name is only one the change can move if the table it is about is one the query reads at all.
        boolean isOneOfItsTables = lineage.getTableLineageSet().stream()
                .anyMatch(table -> new BaseTableInfo(table).equals(baseTableInfo));
        if (!isOneOfItsTables) {
            return false;
        }
        return plan.anyMatch(node -> node instanceof LogicalApply
                && ((LogicalApply<?, ?>) node).getCorrelationSlot().stream()
                        .anyMatch(slot -> names.contains(slot.getName())));
    }

    /**
     * An MV's query is only as good as the base table schema it was analyzed against, and a query that
     * analyses is not enough on its own: a name it reaches a column by can move to another column, and the
     * table can move on between the analysis and the answer. Re-analyzing the MV query here (right after
     * the alter was applied) is what detects a changed column identity:
     * dropping or renaming a column the MV uses makes the query unanalyzable, and a column re-added with
     * the same name is a different column, so pre-existing rows read its default value instead.
     *
     * <p>Such a change is metadata-only for light schema changes and emits no binlog, so an
     * incremental refresh would consume an empty delta and report SUCCESS while silently keeping the
     * rows computed under the old column epoch. Invalidating the MV is what keeps that from being
     * reported as current.
     *
     * <p>The check is the criterion, not just the reason for the record: a column the query does not name
     * is one this change leaves the MV's rows alone for, so nothing is invalidated for it. It is a whole
     * query that is analysed, not a column that is looked up: what the MV can no longer be computed from
     * is what the analysis refuses, wherever in the query it stood.
     *
     * <p>Every MV is checked, not only an IVM one: whether the query still analyzes is a property of
     * the MV and of the base table it reads, not of how the MV refreshes, and the invalidation is the
     * same one a change to that table records. What an IVM MV has on top of it is a per-partition
     * requirement, and that is decided elsewhere, from a query that analyzed.
     *
     * @return whether the MV was invalidated. False is the answer for a change that reaches neither the
     *         query nor the rows it computed, and it is the whole record for that change: there is nothing to
     *         write, and writing the generic "the base table has been updated" anyway would stand for a
     *         rebuild the MV does not owe.
     */
    private boolean invalidateMvUnlessQueryHolds(BaseTableInfo baseTableInfo, Table mvTable,
            QueryJudgedChange queryJudgedChange) {
        if (!(mvTable instanceof MTMV)) {
            return false;
        }
        MTMV mtmv = (MTMV) mvTable;
        // Analyse in a context owned by this check, never the session that issued the alter: the check
        // must not disturb the running statement, and it has to work on threads that have no session.
        // Setting a thread local is how a context is made current, so restore the previous one.
        MTMVAnalyzeQueryInfo analyzedQueryInfo;
        ConnectContext previousCtx = ConnectContext.get();
        try {
            analyzedQueryInfo = MTMVPlanUtil.ensureMTMVQueryUsable(mtmv,
                    MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK));
        } catch (Exception e) {
            LOG.info("Invalidate MV, the MV query is no longer usable. baseTable={}, mtmv={}, reason={}",
                    baseTableInfo, mtmv.getName(), e.getMessage());
            mtmv.invalidateWholeMv("The MV query is no longer analyzable: " + baseTableInfo).await();
            return true;
        } finally {
            if (previousCtx != null) {
                previousCtx.setThreadLocalInfo();
            } else {
                ConnectContext.remove();
            }
        }
        // A query that still analyses has not necessarily kept its meaning: a name can move. The column a
        // query reaches a name by is the nearest one to it in the query's scopes, so a column the change
        // takes away leaves the name to whatever else answers to it -- an unqualified name inside a subquery
        // falls back to a correlated outer one, or to one of a table joined there -- and a column the change
        // adds can answer for the name from then on. Either way the query produces the columns it always
        // produced while their rows come from elsewhere, and the view's rows are no longer the ones the
        // query computes. What the query reads is read from the analysed query's lineage, which names the
        // columns the query really reaches -- through its projections, filters, joins and aggregation, and
        // through whatever views stand between them -- so an alias or a string that happens to read the same
        // is not one of them, and one reached inside a view is.
        if (reachesAnyColumnOf(analyzedQueryInfo.getAnalyzedPlan(), baseTableInfo, queryJudgedChange.columns())) {
            LOG.info("Invalidate MV, the MV query reads a column the change is about. "
                            + "baseTable={}, columns={}, mtmv={}", baseTableInfo, queryJudgedChange.columns(),
                    mtmv.getName());
            mtmv.invalidateWholeMv("The MV query reads a column the change is about: " + baseTableInfo)
                    .await();
            return true;
        }
        // And the answer has to be about the table the change left. The analysis reads the table as it is
        // now, and a light change is applied before this hook runs, so a table that has moved on since --
        // a column taken away and added back under the same name, say -- would have been analysed in that
        // later state: the same shapes, a different column. Asked of the change itself rather than of the
        // columns, because what the table should hold is what that change asked for, and the answer is the
        // one the analysis was given about only while it still holds.
        if (!queryJudgedChange.hasReachedTheTable()) {
            LOG.info("Invalidate MV, the table is no longer the one the change was applied to. "
                            + "baseTable={}, columns={}, mtmv={}", baseTableInfo, queryJudgedChange.columns(),
                    mtmv.getName());
            mtmv.invalidateWholeMv("The table is no longer the one the change was applied to: "
                    + baseTableInfo).await();
            return true;
        }
        return false;
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

    /**
     * Puts every MV that reads this base table into {@code SCHEMA_CHANGE} -- or, for the changes that ask
     * for it, every MV that change does not leave alone.
     *
     * @param queryJudgedChange the change, left to each MV's own query, or null when the alter is not one a
     *                          query decides; see {@link #invalidateMvUnlessQueryHolds}
     */
    private void processBaseTableChange(BaseTableInfo baseTableInfo, String msgPrefix,
            QueryJudgedChange queryJudgedChange) {
        Set<BaseTableInfo> mtmvsByBaseTable = getMtmvsByBaseTableOneLevelAndFromView(baseTableInfo);
        if (CollectionUtils.isEmpty(mtmvsByBaseTable)) {
            return;
        }
        for (BaseTableInfo mtmvInfo : mtmvsByBaseTable) {
            Table mvTable = null;
            try {
                mvTable = (Table) MTMVUtil.getTable(mtmvInfo);
            } catch (AnalysisException e) {
                LOG.warn(e);
                continue;
            }
            if (queryJudgedChange != null) {
                // The change is left to each view's own query, which is asked and whose answer is held to
                // the two things that can move under it -- see invalidateMvUnlessQueryHolds. A view the
                // change leaves alone is not invalidated: that would discard the result of a refresh
                // running against it on the strength of a change that never reached it. Nothing else is
                // recorded here either -- the invalidation carries its reason, and a second record would
                // land on the same state with the blunter "the base table has been updated", having bumped
                // the version and dropped the snapshot a second time for one change.
                if (invalidateMvUnlessQueryHolds(baseTableInfo, mvTable, queryJudgedChange)) {
                    LOG.info("Invalidated MV, baseTable={}, mv={}", baseTableInfo, mvTable.getName());
                } else {
                    LOG.info("The change leaves the MV alone, nothing to invalidate. baseTable={}, mv={}",
                            baseTableInfo, mvTable.getName());
                }
                continue;
            }
            if (!(mvTable instanceof MTMV)) {
                continue;
            }
            // Applied and enqueued in one MV-lock critical section, like the invalidation above: they are
            // one change, and a task result enqueued between them would be replayed on a follower after
            // this record rather than before it -- leaving the follower in SCHEMA_CHANGE where this FE
            // ended NORMAL, which is a whole-MV rebuild the next refresh does not need.
            ((MTMV) mvTable).invalidateWholeMv(msgPrefix + baseTableInfo).await();
        }
    }
}

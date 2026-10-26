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
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Set;

public class MTMVRelationManagerTest {
    private BaseTableInfo mv1 = Mockito.mock(BaseTableInfo.class);
    private BaseTableInfo mv2 = Mockito.mock(BaseTableInfo.class);
    private BaseTableInfo t3 = Mockito.mock(BaseTableInfo.class);
    private BaseTableInfo t4 = Mockito.mock(BaseTableInfo.class);

    @BeforeEach
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        Mockito.when(mv1.getCtlName()).thenReturn("ctl1");
        Mockito.when(mv1.getDbName()).thenReturn("db1");
        Mockito.when(mv1.getTableName()).thenReturn("mv1");

        Mockito.when(mv2.getCtlName()).thenReturn("ctl1");
        Mockito.when(mv2.getDbName()).thenReturn("db1");
        Mockito.when(mv2.getTableName()).thenReturn("mv2");

        Mockito.when(t3.getCtlName()).thenReturn("ctl1");
        Mockito.when(t3.getDbName()).thenReturn("db1");
        Mockito.when(t3.getTableName()).thenReturn("t3");

        Mockito.when(t4.getCtlName()).thenReturn("ctl1");
        Mockito.when(t4.getDbName()).thenReturn("db1");
        Mockito.when(t4.getTableName()).thenReturn("t4");
    }

    @Test
    public void noEligibleCandidateDoesNotCollectQueryPartitions() {
        MTMVRelationManager manager = new MTMVRelationManager();
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Assertions.assertTrue(manager.getAvailableMTMVs(Set.of(), context, false, (ctx, mv) -> false).isEmpty());
        MTMV disabled = Mockito.mock(MTMV.class);
        MTMV excluded = Mockito.mock(MTMV.class);
        Assertions.assertTrue(manager.getAvailableMTMVs(Set.of(disabled, excluded), context, false,
                (ctx, mv) -> mv == excluded).isEmpty());
        Mockito.verifyNoInteractions(context);
    }

    @Test
    public void eligibleCandidatesShareOneQueryPartitionCollection() {
        MTMVRelationManager manager = Mockito.spy(new MTMVRelationManager());
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StatementContext statement = new StatementContext();
        Mockito.when(context.getStatementContext()).thenReturn(statement);
        MTMV first = Mockito.mock(MTMV.class);
        MTMV second = Mockito.mock(MTMV.class);
        Mockito.when(first.isUseForRewrite()).thenReturn(true);
        Mockito.when(second.isUseForRewrite()).thenReturn(true);
        Mockito.doReturn(true).when(manager).isMVPartitionValid(
                Mockito.any(), Mockito.eq(context), Mockito.eq(false), Mockito.anyMap());
        Assertions.assertEquals(Set.of(first, second), manager.getAvailableMTMVs(
                Set.of(first, second), context, false, (ctx, mv) -> false));
        Mockito.verify(context, Mockito.times(1)).getStatementContext();
        Mockito.verify(manager, Mockito.times(2)).isMVPartitionValid(
                Mockito.any(), Mockito.eq(context), Mockito.eq(false), Mockito.anyMap());
    }

    @Test
    public void testGetMtmvsByBaseTableOneLevelAndFromView() {
        // mock mv2==>mv1,t3; mv1==>t4
        MTMVRelationManager manager = new MTMVRelationManager();
        MTMVRelation mv2Relation = new MTMVRelation(Sets.newHashSet(mv1, t3, t4), Sets.newHashSet(mv1, t3),
                Sets.newHashSet(mv1, t3),
                Sets.newHashSet(), Sets.newHashSet());
        MTMVRelation mv1Relation = new MTMVRelation(Sets.newHashSet(t4), Sets.newHashSet(t4), Sets.newHashSet(t4),
                Sets.newHashSet(), Sets.newHashSet());
        manager.refreshMTMVCache(mv2Relation, mv2);
        manager.refreshMTMVCache(mv1Relation, mv1);
        // should return mv2
        Set<BaseTableInfo> mv1OneLevel = manager.getMtmvsByBaseTableOneLevelAndFromView(mv1);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), mv1OneLevel));
        // should return mv2
        Set<BaseTableInfo> t3OneLevel = manager.getMtmvsByBaseTableOneLevelAndFromView(t3);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), t3OneLevel));
        // should return mv1
        Set<BaseTableInfo> t4OneLevel = manager.getMtmvsByBaseTableOneLevelAndFromView(t4);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv1), t4OneLevel));

        // update mv2 only use t3,remove mv1
        mv2Relation = new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3), Sets.newHashSet(t3),
                Sets.newHashSet(), Sets.newHashSet());
        manager.refreshMTMVCache(mv2Relation, mv2);
        // should return empty
        mv1OneLevel = manager.getMtmvsByBaseTableOneLevelAndFromView(mv1);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(), mv1OneLevel));
        // should return mv2
        t3OneLevel = manager.getMtmvsByBaseTableOneLevelAndFromView(t3);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), t3OneLevel));
        // should return mv1
        t4OneLevel = manager.getMtmvsByBaseTableOneLevelAndFromView(t4);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv1), t4OneLevel));
    }

    @Test
    public void testGetMtmvsByBaseTable() {
        // mock mv2==>mv1,t3; mv1==>t4
        MTMVRelationManager manager = new MTMVRelationManager();
        MTMVRelation mv2Relation = new MTMVRelation(Sets.newHashSet(mv1, t3, t4), Sets.newHashSet(mv1, t3),
                Sets.newHashSet(mv1, t3),
                Sets.newHashSet(), Sets.newHashSet());
        MTMVRelation mv1Relation = new MTMVRelation(Sets.newHashSet(t4), Sets.newHashSet(t4), Sets.newHashSet(t4),
                Sets.newHashSet(), Sets.newHashSet());
        manager.refreshMTMVCache(mv2Relation, mv2);
        manager.refreshMTMVCache(mv1Relation, mv1);
        // should return mv2
        Set<BaseTableInfo> mv1All = manager.getMtmvsByBaseTable(mv1);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), mv1All));
        // should return mv2
        Set<BaseTableInfo> t3All = manager.getMtmvsByBaseTable(t3);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), t3All));
        // should return mv1
        Set<BaseTableInfo> t4All = manager.getMtmvsByBaseTable(t4);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv1, mv2), t4All));

        // update mv2 only use t3,remove mv1
        mv2Relation = new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3), Sets.newHashSet(t3),
                Sets.newHashSet(), Sets.newHashSet());
        manager.refreshMTMVCache(mv2Relation, mv2);
        // should return empty
        mv1All = manager.getMtmvsByBaseTableOneLevelAndFromView(mv1);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(), mv1All));
        // should return mv2
        t3All = manager.getMtmvsByBaseTableOneLevelAndFromView(t3);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), t3All));
        // should return mv1
        t4All = manager.getMtmvsByBaseTableOneLevelAndFromView(t4);
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv1), t4All));
    }

    @Test
    public void testRefreshMtmvCacheReplacesRelation() {
        MTMVRelationManager manager = new MTMVRelationManager();
        MTMVRelation oldRelation = new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3),
                Sets.newHashSet(t3), Sets.newHashSet(), Sets.newHashSet());
        MTMVRelation newRelation = new MTMVRelation(Sets.newHashSet(t4), Sets.newHashSet(t4),
                Sets.newHashSet(t4), Sets.newHashSet(), Sets.newHashSet());

        manager.refreshMTMVCache(oldRelation, mv1);
        manager.refreshMTMVCache(newRelation, mv1);

        Assertions.assertTrue(manager.getMtmvsByBaseTable(t3).isEmpty());
        Assertions.assertTrue(manager.getMtmvsByBaseTableOneLevelAndFromView(t3).isEmpty());
        Assertions.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv1),
                manager.getMtmvsByBaseTable(t4)));
    }

    @Test
    public void testBaselineBarrierOnlyInvalidatesIvm() {
        MTMVRelationManager manager = new MTMVRelationManager();
        manager.refreshMTMVCache(new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3),
                Sets.newHashSet(t3), Sets.newHashSet(), Sets.newHashSet()), mv1);
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        try (MockedStatic<MTMVUtil> util = Mockito.mockStatic(MTMVUtil.class)) {
            util.when(() -> MTMVUtil.getMTMV(mv1)).thenReturn(mtmv);

            manager.markIvmBaselineRebuild(t3, "test");
        }

        Mockito.verify(mtmv, Mockito.never()).invalidateWholeMv(Mockito.anyString());
    }

    @Test
    public void testBaselineBarrierSkipsExcludedTable() {
        MTMVRelationManager manager = new MTMVRelationManager();
        manager.refreshMTMVCache(new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3),
                Sets.newHashSet(t3), Sets.newHashSet(), Sets.newHashSet()), mv1);
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Sets.newHashSet(new TableNameInfo("t3")));
        try (MockedStatic<MTMVUtil> util = Mockito.mockStatic(MTMVUtil.class)) {
            util.when(() -> MTMVUtil.getMTMV(mv1)).thenReturn(mtmv);

            manager.markIvmBaselineRebuild(t3, "test");
        }

        Mockito.verify(mtmv, Mockito.never()).invalidateWholeMv(Mockito.anyString());
    }

    /**
     * The generic base-table change is an invalidation, and it is recorded the way the invalidation above
     * is: applied and enqueued in one MV-lock critical section, awaited where no MV lock is held. A record
     * enqueued while the state is still being applied would be replayed on a follower on the other side of
     * a concurrent task result, leaving the follower in SCHEMA_CHANGE where this FE ended NORMAL.
     *
     * <p>A rename is the shortest way into this path: it skips the query check, so what the hook does is
     * exactly the record under test. The rename is passed with the check asked for, which it cannot be
     * judged by -- the MV query keeps spelling the old name -- and the flag does not reach it.
     */
    @Test
    public void testABaseTableRenameIsRecordedThroughTheInvalidation() {
        MTMVRelationManager manager = new MTMVRelationManager();
        manager.refreshMTMVCache(new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3),
                Sets.newHashSet(t3), Sets.newHashSet(), Sets.newHashSet()), mv1);
        MTMV mtmv = Mockito.mock(MTMV.class);
        EditLogItem editLogItem = Mockito.mock(EditLogItem.class);
        Mockito.when(mtmv.invalidateWholeMv(Mockito.anyString())).thenReturn(editLogItem);
        try (MockedStatic<MTMVUtil> util = Mockito.mockStatic(MTMVUtil.class)) {
            util.when(() -> MTMVUtil.getTable(Mockito.any(BaseTableInfo.class))).thenReturn(mtmv);

            // A rename of the table names no column, so the alter does not hand the judgement to a query.
            manager.alterTable(t3, Optional.of(t4), false, null);
        }

        ArgumentCaptor<String> detail = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mtmv).invalidateWholeMv(detail.capture());
        Assertions.assertTrue(detail.getValue().startsWith("The base table has been updated:"),
                detail.getValue());
        Mockito.verify(editLogItem).await();
    }

    /**
     * A change the alter does not hand to the queries is invalidated as a base table change was before the
     * queries were asked at all. The alter says so by naming no column: it does that where every clause of
     * it is not one a query decides, and where the schema change has not reached the table yet -- asking
     * then would answer for the table from before the change.
     */
    @Test
    public void testAChangeTheQueryCannotBeAskedAboutInvalidates() {
        MTMVRelationManager manager = new MTMVRelationManager();
        manager.refreshMTMVCache(new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3),
                Sets.newHashSet(t3), Sets.newHashSet(), Sets.newHashSet()), mv1);
        MTMV mtmv = Mockito.mock(MTMV.class);
        EditLogItem editLogItem = Mockito.mock(EditLogItem.class);
        Mockito.when(mtmv.invalidateWholeMv(Mockito.anyString())).thenReturn(editLogItem);
        try (MockedStatic<MTMVUtil> util = Mockito.mockStatic(MTMVUtil.class)) {
            util.when(() -> MTMVUtil.getTable(Mockito.any(BaseTableInfo.class))).thenReturn(mtmv);

            // The same table on both sides: this is not a rename, so what the alter names alone decides.
            manager.alterTable(t3, Optional.of(t3), false, null);
        }

        Mockito.verify(mtmv).invalidateWholeMv(Mockito.anyString());
        Mockito.verify(editLogItem).await();
    }

    @Test
    public void testUnregisterMTMVClearsAllForwardMaps() {
        MTMVRelationManager manager = new MTMVRelationManager();
        MTMVRelation mv1Relation = new MTMVRelation(
                Sets.newHashSet(t3, t4),  // baseTables
                Sets.newHashSet(t3),       // baseTablesOneLevel
                Sets.newHashSet(t3),       // baseTablesOneLevelAndFromView
                Sets.newHashSet(t4),       // baseViews
                Sets.newHashSet(t4));      // baseViewsOneLevel
        manager.refreshMTMVCache(mv1Relation, mv1);

        Assertions.assertTrue(manager.getMtmvsByBaseTable(t3).contains(mv1));
        Assertions.assertTrue(manager.getMtmvsByBaseTable(t4).contains(mv1));
        Assertions.assertTrue(manager.getMtmvsByBaseView(t4).contains(mv1));
        Assertions.assertTrue(manager.getMtmvsByBaseTableOneLevelAndFromView(t3).contains(mv1));

        // Directly exercise removeMTMV through a second refresh with an empty relation.
        MTMVRelation empty = new MTMVRelation(Sets.newHashSet(), Sets.newHashSet(), Sets.newHashSet(),
                Sets.newHashSet(), Sets.newHashSet());
        manager.refreshMTMVCache(empty, mv1);

        Assertions.assertFalse(manager.getMtmvsByBaseTable(t3).contains(mv1));
        Assertions.assertFalse(manager.getMtmvsByBaseTable(t4).contains(mv1));
        Assertions.assertFalse(manager.getMtmvsByBaseView(t4).contains(mv1));
        Assertions.assertFalse(manager.getMtmvsByBaseTableOneLevelAndFromView(t3).contains(mv1));
    }

    @Test
    public void testDoubleRegisterSameMtmvIsIdempotent() {
        MTMVRelationManager manager = new MTMVRelationManager();
        MTMVRelation relation = new MTMVRelation(
                Sets.newHashSet(t3, t4), Sets.newHashSet(t3, t4), Sets.newHashSet(t3, t4),
                Sets.newHashSet(), Sets.newHashSet());
        manager.refreshMTMVCache(relation, mv1);
        Set<BaseTableInfo> firstRoundT3 = Sets.newHashSet(manager.getMtmvsByBaseTable(t3));
        Set<BaseTableInfo> firstRoundT4 = Sets.newHashSet(manager.getMtmvsByBaseTable(t4));

        // Double register with the same relation, mimicking Database.registerTable then
        // MTMVUtil.compatibleMTMV both calling registerMTMV for the same mv.
        manager.refreshMTMVCache(relation, mv1);
        Assertions.assertEquals(firstRoundT3, manager.getMtmvsByBaseTable(t3));
        Assertions.assertEquals(firstRoundT4, manager.getMtmvsByBaseTable(t4));
        Assertions.assertEquals(Sets.newHashSet(mv1), manager.getMtmvsByBaseTable(t3));
        Assertions.assertEquals(Sets.newHashSet(mv1), manager.getMtmvsByBaseTable(t4));
    }

    @Test
    public void testStaleRelationsAreDroppedWhileSharedRemain() {
        MTMVRelationManager manager = new MTMVRelationManager();
        // mv1 initially depends on t3 and t4 via all three relation maps.
        manager.refreshMTMVCache(
                new MTMVRelation(Sets.newHashSet(t3, t4), Sets.newHashSet(t3, t4), Sets.newHashSet(t3, t4),
                        Sets.newHashSet(t3, t4), Sets.newHashSet(t3, t4)),
                mv1);
        // mv2 depends only on t3 so we can verify the stale prune leaves other mtmvs alone.
        manager.refreshMTMVCache(
                new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3), Sets.newHashSet(t3),
                        Sets.newHashSet(t3), Sets.newHashSet(t3)),
                mv2);

        // mv1 now switches to depend on t3 only.
        manager.refreshMTMVCache(
                new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3), Sets.newHashSet(t3),
                        Sets.newHashSet(t3), Sets.newHashSet(t3)),
                mv1);

        // t4 dropped from mv1's relation, so mv1 should no longer appear under t4 anywhere.
        Assertions.assertFalse(manager.getMtmvsByBaseTable(t4).contains(mv1));
        Assertions.assertFalse(manager.getMtmvsByBaseView(t4).contains(mv1));
        Assertions.assertFalse(manager.getMtmvsByBaseTableOneLevelAndFromView(t4).contains(mv1));

        // t3 is still in mv1's new relation, so mv1 must remain reachable under t3. mv2 must
        // still be there too — the stale prune must not touch other mtmvs.
        Assertions.assertTrue(manager.getMtmvsByBaseTable(t3).contains(mv1));
        Assertions.assertTrue(manager.getMtmvsByBaseTable(t3).contains(mv2));
        Assertions.assertTrue(manager.getMtmvsByBaseView(t3).contains(mv1));
        Assertions.assertTrue(manager.getMtmvsByBaseView(t3).contains(mv2));
        Assertions.assertTrue(manager.getMtmvsByBaseTableOneLevelAndFromView(t3).contains(mv1));
        Assertions.assertTrue(manager.getMtmvsByBaseTableOneLevelAndFromView(t3).contains(mv2));
    }

    @Test
    public void testEmptyCategoryTransitionClearsReverseEntry() {
        MTMVRelationManager manager = new MTMVRelationManager();
        manager.refreshMTMVCache(
                new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3), Sets.newHashSet(t3),
                        Sets.newHashSet(t4), Sets.newHashSet(t4)),
                mv1);
        Assertions.assertTrue(manager.getMtmvsByBaseView(t4).contains(mv1));

        manager.refreshMTMVCache(
                new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3), Sets.newHashSet(t3),
                        Sets.newHashSet(), Sets.newHashSet()),
                mv1);

        Assertions.assertFalse(manager.getMtmvsByBaseView(t4).contains(mv1));

        Assertions.assertTrue(manager.getMtmvsByBaseTable(t3).contains(mv1));

        manager.refreshMTMVCache(
                new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3), Sets.newHashSet(t3),
                        Sets.newHashSet(t3), Sets.newHashSet(t3)),
                mv1);
        Assertions.assertFalse(manager.getMtmvsByBaseView(t4).contains(mv1));
        Assertions.assertTrue(manager.getMtmvsByBaseView(t3).contains(mv1));
    }
}

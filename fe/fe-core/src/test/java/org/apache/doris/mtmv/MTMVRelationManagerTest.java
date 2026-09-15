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

import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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

        Mockito.verify(mtmv, Mockito.never()).invalidateIvmBaseline();
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

        Mockito.verify(mtmv, Mockito.never()).invalidateIvmBaseline();
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
}

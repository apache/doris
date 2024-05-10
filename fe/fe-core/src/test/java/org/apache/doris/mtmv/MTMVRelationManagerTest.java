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

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class MTMVRelationManagerTest {
    @Test
    public void testGetMtmvsByBaseTableOneLevel() {
        // mock mv2==>mv1,t3; mv1==>t4
        MTMVRelationManager manager = new MTMVRelationManager();
        BaseTableInfo mv1 = new BaseTableInfo(0L, 1L);
        BaseTableInfo mv2 = new BaseTableInfo(0L, 2L);
        BaseTableInfo t3 = new BaseTableInfo(0L, 3L);
        BaseTableInfo t4 = new BaseTableInfo(0L, 4L);
        MTMVRelation mv2Relation = new MTMVRelation(Sets.newHashSet(mv1, t3, t4), Sets.newHashSet(mv1, t3),
                Sets.newHashSet());
        MTMVRelation mv1Relation = new MTMVRelation(Sets.newHashSet(t4), Sets.newHashSet(t4),
                Sets.newHashSet());
        manager.refreshMTMVCache(mv2Relation, mv2);
        manager.refreshMTMVCache(mv1Relation, mv1);
        // should return mv2
        Set<BaseTableInfo> mv1OneLevel = manager.getMtmvsByBaseTableOneLevel(mv1);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), mv1OneLevel));
        // should return mv2
        Set<BaseTableInfo> t3OneLevel = manager.getMtmvsByBaseTableOneLevel(t3);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), t3OneLevel));
        // should return mv1
        Set<BaseTableInfo> t4OneLevel = manager.getMtmvsByBaseTableOneLevel(t4);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv1), t4OneLevel));

        // update mv2 only use t3,remove mv1
        mv2Relation = new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3),
                Sets.newHashSet());
        manager.refreshMTMVCache(mv2Relation, mv2);
        // should return empty
        mv1OneLevel = manager.getMtmvsByBaseTableOneLevel(mv1);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(), mv1OneLevel));
        // should return mv2
        t3OneLevel = manager.getMtmvsByBaseTableOneLevel(t3);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), t3OneLevel));
        // should return mv1
        t4OneLevel = manager.getMtmvsByBaseTableOneLevel(t4);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv1), t4OneLevel));
    }

    @Test
    public void testGetMtmvsByBaseTable() {
        // mock mv2==>mv1,t3; mv1==>t4
        MTMVRelationManager manager = new MTMVRelationManager();
        BaseTableInfo mv1 = new BaseTableInfo(0L, 1L);
        BaseTableInfo mv2 = new BaseTableInfo(0L, 2L);
        BaseTableInfo t3 = new BaseTableInfo(0L, 3L);
        BaseTableInfo t4 = new BaseTableInfo(0L, 4L);
        MTMVRelation mv2Relation = new MTMVRelation(Sets.newHashSet(mv1, t3, t4), Sets.newHashSet(mv1, t3),
                Sets.newHashSet());
        MTMVRelation mv1Relation = new MTMVRelation(Sets.newHashSet(t4), Sets.newHashSet(t4),
                Sets.newHashSet());
        manager.refreshMTMVCache(mv2Relation, mv2);
        manager.refreshMTMVCache(mv1Relation, mv1);
        // should return mv2
        Set<BaseTableInfo> mv1All = manager.getMtmvsByBaseTable(mv1);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), mv1All));
        // should return mv2
        Set<BaseTableInfo> t3All = manager.getMtmvsByBaseTable(t3);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), t3All));
        // should return mv1
        Set<BaseTableInfo> t4All = manager.getMtmvsByBaseTable(t4);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv1, mv2), t4All));

        // update mv2 only use t3,remove mv1
        mv2Relation = new MTMVRelation(Sets.newHashSet(t3), Sets.newHashSet(t3),
                Sets.newHashSet());
        manager.refreshMTMVCache(mv2Relation, mv2);
        // should return empty
        mv1All = manager.getMtmvsByBaseTableOneLevel(mv1);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(), mv1All));
        // should return mv2
        t3All = manager.getMtmvsByBaseTableOneLevel(t3);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv2), t3All));
        // should return mv1
        t4All = manager.getMtmvsByBaseTableOneLevel(t4);
        Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newHashSet(mv1), t4All));
    }
}

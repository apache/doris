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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

public class MTMVPartitionUtilTest {
    @Mocked
    private MTMV mtmv;
    @Mocked
    private Partition p1;
    @Mocked
    private MTMVRelation relation;
    @Mocked
    private BaseTableInfo baseTableInfo;
    @Mocked
    private MTMVPartitionInfo mtmvPartitionInfo;
    @Mocked
    private OlapTable baseOlapTable;
    @Mocked
    private MTMVSnapshotIf baseSnapshotIf;
    @Mocked
    private MTMVRefreshSnapshot refreshSnapshot;
    @Mocked
    private MTMVUtil mtmvUtil;

    private Set<BaseTableInfo> baseTables = Sets.newHashSet();

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        baseTables.add(baseTableInfo);
        new Expectations() {
            {
                mtmv.getRelation();
                minTimes = 0;
                result = relation;

                mtmv.getPartitions();
                minTimes = 0;
                result = Lists.newArrayList(p1);

                p1.getId();
                minTimes = 0;
                result = 1L;

                mtmv.getMvPartitionInfo();
                minTimes = 0;
                result = mtmvPartitionInfo;

                // TODO: 2024/2/2 follow base table
                mtmvPartitionInfo.getPartitionType();
                minTimes = 0;
                result = MTMVPartitionType.SELF_MANAGE;

                mtmvUtil.getTable(baseTableInfo);
                minTimes = 0;
                result = baseOlapTable;

                baseOlapTable.needAutoRefresh();
                minTimes = 0;
                result = true;

                baseOlapTable.getTableSnapshot();
                minTimes = 0;
                result = baseSnapshotIf;

                mtmv.getPartitionName(anyLong);
                minTimes = 0;
                result = "p1";

                mtmv.getRefreshSnapshot();
                minTimes = 0;
                result = refreshSnapshot;

                refreshSnapshot.equalsWithBaseTable(anyString, anyLong, (MTMVSnapshotIf) any);
                minTimes = 0;
                result = true;

                relation.getBaseTables();
                minTimes = 0;
                result = baseTables;
            }
        };
    }

    @Test
    public void testIsMTMVSyncNormal() {
        boolean mtmvSync = MTMVPartitionUtil.isMTMVSync(mtmv);
        Assert.assertTrue(mtmvSync);
    }

    @Test
    public void testIsMTMVSyncSnapshotNotEqual() {
        new Expectations() {
            {
                refreshSnapshot.equalsWithBaseTable(anyString, anyLong, (MTMVSnapshotIf) any);
                minTimes = 0;
                result = false;
            }
        };
        boolean mtmvSync = MTMVPartitionUtil.isMTMVSync(mtmv);
        Assert.assertFalse(mtmvSync);
    }
}

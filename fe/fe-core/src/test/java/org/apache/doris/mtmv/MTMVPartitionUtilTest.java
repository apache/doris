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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
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

import java.util.List;
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

                mtmv.getPartitionNames();
                minTimes = 0;
                result = Sets.newHashSet("name1");

                p1.getName();
                minTimes = 0;
                result = "name1";

                mtmv.getMvPartitionInfo();
                minTimes = 0;
                result = mtmvPartitionInfo;

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
                result = "name1";

                mtmv.getRefreshSnapshot();
                minTimes = 0;
                result = refreshSnapshot;

                refreshSnapshot.equalsWithBaseTable(anyString, anyLong, (MTMVSnapshotIf) any);
                minTimes = 0;
                result = true;

                relation.getBaseTables();
                minTimes = 0;
                result = baseTables;

                baseOlapTable.needAutoRefresh();
                minTimes = 0;
                result = true;

                baseOlapTable.getPartitionSnapshot(anyString);
                minTimes = 0;
                result = baseSnapshotIf;

                baseOlapTable.getPartitionName(anyLong);
                minTimes = 0;
                result = "name1";

                refreshSnapshot.equalsWithRelatedPartition(anyString, anyString, (MTMVSnapshotIf) any);
                minTimes = 0;
                result = true;

                refreshSnapshot.getSnapshotPartitions(anyString);
                minTimes = 0;
                result = Sets.newHashSet("name2");
            }
        };
    }

    @Test
    public void testIsMTMVSyncNormal() {
        boolean mtmvSync = MTMVPartitionUtil.isMTMVSync(mtmv);
        Assert.assertTrue(mtmvSync);
    }

    @Test
    public void testIsMTMVSyncNotSync() {
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

    @Test
    public void testIsSyncWithPartition() throws AnalysisException {
        boolean isSyncWithPartition = MTMVPartitionUtil
                .isSyncWithPartitions(mtmv, "name1", baseOlapTable, Sets.newHashSet("name2"));
        Assert.assertTrue(isSyncWithPartition);
    }

    @Test
    public void testIsSyncWithPartitionNotEqual() throws AnalysisException {
        new Expectations() {
            {
                refreshSnapshot.getSnapshotPartitions(anyString);
                minTimes = 0;
                result = Sets.newHashSet("name2", "name3");
            }
        };
        boolean isSyncWithPartition = MTMVPartitionUtil
                .isSyncWithPartitions(mtmv, "name1", baseOlapTable, Sets.newHashSet("name2"));
        Assert.assertFalse(isSyncWithPartition);
    }

    @Test
    public void testIsSyncWithPartitionNotSync() throws AnalysisException {
        new Expectations() {
            {
                refreshSnapshot.equalsWithRelatedPartition(anyString, anyString, (MTMVSnapshotIf) any);
                minTimes = 0;
                result = false;
            }
        };
        boolean isSyncWithPartition = MTMVPartitionUtil
                .isSyncWithPartitions(mtmv, "name1", baseOlapTable, Sets.newHashSet("name2"));
        Assert.assertFalse(isSyncWithPartition);
    }

    @Test
    public void testGeneratePartitionName() {
        List<List<PartitionValue>> inValues = Lists.newArrayList();
        inValues.add(Lists.newArrayList(new PartitionValue("20201010 01:01:01"), new PartitionValue("value12")));
        inValues.add(Lists.newArrayList(new PartitionValue("value21"), new PartitionValue("value22")));
        PartitionKeyDesc inDesc = PartitionKeyDesc.createIn(inValues);
        String inName = MTMVPartitionUtil.generatePartitionName(inDesc);
        Assert.assertEquals("p_20201010010101_value12_value21_value22", inName);

        PartitionKeyDesc rangeDesc = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue(1L)),
                Lists.newArrayList(new PartitionValue(2L))
        );
        String rangeName = MTMVPartitionUtil.generatePartitionName(rangeDesc);
        Assert.assertEquals("p_1_2", rangeName);
    }
}

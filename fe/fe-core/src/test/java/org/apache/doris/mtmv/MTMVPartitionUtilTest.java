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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
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

    @Test
    public void testIfPartitionItemInRange() throws AnalysisException {
        // test range partition
        PartitionItem item = getRangePartitionItem("2020-01-01", "2020-02-01");
        MTMVRefreshPartitionRange refreshRange = getTestRange("2020-01-01", "2020-02-01");
        // equals
        Assert.assertTrue(MTMVPartitionUtil.ifPartitionItemInRange(item, refreshRange));
        refreshRange = getTestRange("2020-01-01", "2020-01-02");
        // only contain lower value
        Assert.assertTrue(MTMVPartitionUtil.ifPartitionItemInRange(item, refreshRange));
        refreshRange = getTestRange("2020-02-01", "2020-02-02");
        // upper is open
        Assert.assertFalse(MTMVPartitionUtil.ifPartitionItemInRange(item, refreshRange));
        refreshRange = getTestRange("2020-01-15", "2020-02-15");
        // intersect
        Assert.assertTrue(MTMVPartitionUtil.ifPartitionItemInRange(item, refreshRange));

        // test list partition
        item = getListPartitionItem("2020-01-02", "2020-01-04");
        refreshRange = getTestRange("2020-01-01", "2020-01-10");
        // contain all value
        Assert.assertTrue(MTMVPartitionUtil.ifPartitionItemInRange(item, refreshRange));
        refreshRange = getTestRange("2020-01-04", "2020-01-10");
        // contain one value
        Assert.assertTrue(MTMVPartitionUtil.ifPartitionItemInRange(item, refreshRange));
        refreshRange = getTestRange("2020-01-01", "2020-01-02");
        // upper is open
        Assert.assertFalse(MTMVPartitionUtil.ifPartitionItemInRange(item, refreshRange));
    }

    private MTMVRefreshPartitionRange getTestRange(String lower, String upper) {
        MTMVRefreshPartitionRange refreshPartitionRange = new MTMVRefreshPartitionRange(
                Lists.newArrayList(Literal.of(lower)), Lists.newArrayList(Literal.of(upper)));
        refreshPartitionRange.analyze();
        return refreshPartitionRange;
    }

    private PartitionItem getRangePartitionItem(String lower, String upper) throws AnalysisException {
        Column k1 = new Column("k1", ScalarType.createType(PrimitiveType.DATEV2), true, null, "", "key1");
        PartitionKey rangeP1Lower = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(lower)),
                Lists.newArrayList(k1));
        PartitionKey rangeP1Upper = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(upper)),
                Lists.newArrayList(k1));
        Range<PartitionKey> rangeP1 = Range.closedOpen(rangeP1Lower, rangeP1Upper);
        return new RangePartitionItem(rangeP1);
    }

    private PartitionItem getListPartitionItem(String value1, String value2) throws AnalysisException {
        Column k1 = new Column("k1", ScalarType.createType(PrimitiveType.VARCHAR), true, null, "", "key1");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(value1)),
                Lists.newArrayList(k1));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(value2)),
                Lists.newArrayList(k1));
        return new ListPartitionItem(Lists.newArrayList(partitionKey1, partitionKey2));
    }
}

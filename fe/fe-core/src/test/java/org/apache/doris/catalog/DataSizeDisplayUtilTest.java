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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataSizeDisplayUtilTest {
    private String originDeployMode;
    private String originCloudUniqueId;

    @Before
    public void setUp() {
        originDeployMode = Config.deploy_mode;
        originCloudUniqueId = Config.cloud_unique_id;
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originDeployMode;
        Config.cloud_unique_id = originCloudUniqueId;
    }

    @Test
    public void testPartitionDisplaySizeFallbackToReplicaIndexAndSegmentSize() {
        MaterializedIndex baseIndex = new MaterializedIndex(10L, IndexState.NORMAL);
        CloudTablet tablet = new CloudTablet(20L);
        CloudReplica replica = new CloudReplica(30L, 1L, Replica.ReplicaState.NORMAL, 2L, 0,
                100L, 200L, 300L, 10L, 0L);
        replica.setDataSize(0L);
        replica.setLocalInvertedIndexSize(111L);
        replica.setLocalSegmentSize(222L);
        tablet.addReplica(replica, true);
        baseIndex.addTablet(tablet, null, true);

        Partition partition = new Partition(300L, "p1", baseIndex, new RandomDistributionInfo(1));

        Pair<Long, Long> displayDataSize = DataSizeDisplayUtil.getDisplayDataSize(partition);
        Assert.assertEquals(0L, displayDataSize.first.longValue());
        Assert.assertEquals(333L, displayDataSize.second.longValue());
    }

    @Test
    public void testPartitionDisplaySizeMapsCloudDataSizeToRemoteSize() {
        MaterializedIndex baseIndex = new MaterializedIndex(10L, IndexState.NORMAL);
        CloudTablet tablet = new CloudTablet(20L);
        CloudReplica replica = new CloudReplica(30L, 1L, Replica.ReplicaState.NORMAL, 2L, 0,
                100L, 200L, 300L, 10L, 0L);
        replica.setDataSize(123L);
        tablet.addReplica(replica, true);
        baseIndex.addTablet(tablet, null, true);

        Partition partition = new Partition(300L, "p1", baseIndex, new RandomDistributionInfo(1));

        Pair<Long, Long> displayDataSize = DataSizeDisplayUtil.getDisplayDataSize(partition);
        Assert.assertEquals(0L, displayDataSize.first.longValue());
        Assert.assertEquals(123L, displayDataSize.second.longValue());
    }

    @Test
    public void testPartitionDisplaySizeAggregatesMixedReplicaDisplaySize() {
        MaterializedIndex baseIndex = new MaterializedIndex(10L, IndexState.NORMAL);

        CloudTablet tabletWithDataSize = new CloudTablet(20L);
        CloudReplica replicaWithDataSize = new CloudReplica(30L, 1L, Replica.ReplicaState.NORMAL, 2L, 0,
                100L, 200L, 300L, 10L, 0L);
        replicaWithDataSize.setDataSize(123L);
        tabletWithDataSize.addReplica(replicaWithDataSize, true);
        baseIndex.addTablet(tabletWithDataSize, null, true);

        CloudTablet tabletWithFallbackSize = new CloudTablet(21L);
        CloudReplica replicaWithFallbackSize = new CloudReplica(31L, 1L, Replica.ReplicaState.NORMAL, 2L, 0,
                100L, 200L, 300L, 10L, 1L);
        replicaWithFallbackSize.setDataSize(0L);
        replicaWithFallbackSize.setLocalInvertedIndexSize(111L);
        replicaWithFallbackSize.setLocalSegmentSize(222L);
        tabletWithFallbackSize.addReplica(replicaWithFallbackSize, true);
        baseIndex.addTablet(tabletWithFallbackSize, null, true);

        Partition partition = new Partition(300L, "p1", baseIndex, new RandomDistributionInfo(2));

        Pair<Long, Long> displayDataSize = DataSizeDisplayUtil.getDisplayDataSize(partition);
        Assert.assertEquals(0L, displayDataSize.first.longValue());
        Assert.assertEquals(456L, displayDataSize.second.longValue());
    }

    @Test
    public void testReplicaDisplaySizeFallbackToReplicaIndexAndSegmentSize() {
        CloudReplica replica = new CloudReplica(30L, 1L, Replica.ReplicaState.NORMAL, 2L, 0,
                100L, 200L, 300L, 10L, 0L);
        replica.setDataSize(0L);
        replica.setLocalInvertedIndexSize(111L);
        replica.setLocalSegmentSize(222L);

        Pair<Long, Long> displayDataSize = DataSizeDisplayUtil.getDisplayDataSize(replica);
        Assert.assertEquals(0L, displayDataSize.first.longValue());
        Assert.assertEquals(333L, displayDataSize.second.longValue());
    }
}

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

package org.apache.doris.planner;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.planner.OlapTableSink.AdaptiveBucketAssignment;
import org.apache.doris.planner.OlapTableSink.AdaptiveIndexBucketAssignment;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TOlapTableIndexTablets;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.thrift.TTabletLocation;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class OlapTableSinkTest {
    @Test
    public void testCreateLocationFiltersLoadUnavailableBackendsInSharedNothingMode() throws Exception {
        String originalDeployMode = Config.deploy_mode;
        String originalCloudUniqueId = Config.cloud_unique_id;
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.checkBackendLoadAvailable(1L)).thenReturn(true);
        Mockito.when(systemInfoService.checkBackendLoadAvailable(2L)).thenReturn(false);
        Mockito.when(systemInfoService.checkBackendLoadAvailable(3L)).thenReturn(true);
        Mockito.when(systemInfoService.checkExceedDiskCapacityLimit(Mockito.any(), Mockito.eq(true)))
                .thenReturn(Status.OK);

        Tablet tablet = Mockito.mock(Tablet.class, Mockito.CALLS_REAL_METHODS);
        Multimap<Long, Long> backendPaths = HashMultimap.create();
        backendPaths.put(1L, 101L);
        backendPaths.put(2L, 102L);
        backendPaths.put(3L, 103L);
        Mockito.doReturn(backendPaths).when(tablet).getNormalReplicaBackendPathMap();
        OlapTable table = mockTableWithSingleTablet(tablet, 2);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            Config.deploy_mode = "";
            Config.cloud_unique_id = "";

            TOlapTableSink thriftSink = new TOlapTableSink();
            thriftSink.setDbId(100L);
            OlapTableSink sink = new OlapTableSink(table, null, Collections.singletonList(10L));
            TOlapTableLocationParam location = sink.initLocationParam(thriftSink);

            Assertions.assertEquals(Sets.newHashSet(1L, 3L),
                    Sets.newHashSet(location.getTablets().get(0).getNodeIds()));
        } finally {
            Config.deploy_mode = originalDeployMode;
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testCreateLocationPreservesResolvedBackendInCloudMode() throws Exception {
        String originalDeployMode = Config.deploy_mode;
        String originalCloudUniqueId = Config.cloud_unique_id;
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(systemInfoService.getCurrentClusterId()).thenReturn("cluster-id");
        Mockito.when(systemInfoService.checkExceedDiskCapacityLimit(Mockito.any(), Mockito.eq(true)))
                .thenReturn(Status.OK);

        CloudTablet tablet = Mockito.mock(CloudTablet.class, Mockito.CALLS_REAL_METHODS);
        Multimap<Long, Long> backendPaths = HashMultimap.create();
        backendPaths.put(2L, 102L);
        Mockito.doReturn(backendPaths).when(tablet).getNormalReplicaBackendPathMapByClusterId("cluster-id");
        OlapTable table = mockTableWithSingleTablet(tablet, 1);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            Config.deploy_mode = "cloud";
            Config.cloud_unique_id = "";

            TOlapTableSink thriftSink = new TOlapTableSink();
            thriftSink.setDbId(100L);
            OlapTableSink sink = new OlapTableSink(table, null, Collections.singletonList(10L));
            TOlapTableLocationParam location = sink.initLocationParam(thriftSink);

            Assertions.assertEquals(Collections.singletonList(2L),
                    location.getTablets().get(0).getNodeIds());
            Mockito.verify(systemInfoService, Mockito.never()).checkBackendLoadAvailable(Mockito.anyLong());
        } finally {
            Config.deploy_mode = originalDeployMode;
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    private OlapTable mockTableWithSingleTablet(Tablet tablet, int loadRequiredReplicaNum) {
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(partitionInfo.enableAutomaticPartition()).thenReturn(false);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(table.getPartition(10L)).thenReturn(partition);
        Mockito.when(table.getLoadRequiredReplicaNum(10L)).thenReturn(loadRequiredReplicaNum);
        Mockito.when(partition.getMaterializedIndices(IndexExtState.ALL, true))
                .thenReturn(Collections.singletonList(index));
        Mockito.when(index.getTablets()).thenReturn(Collections.singletonList(tablet));
        return table;
    }

    @Test
    public void testCreateDummyLocationUsesLoadAvailableBackendInCurrentComputeGroup() throws Exception {
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend currentComputeGroupBackend = Mockito.mock(Backend.class);
        Backend loadDisabledBackend = Mockito.mock(Backend.class);
        OlapTable table = Mockito.mock(OlapTable.class);

        Mockito.when(currentComputeGroupBackend.getId()).thenReturn(1L);
        Mockito.when(currentComputeGroupBackend.isLoadAvailable()).thenReturn(true);
        Mockito.when(loadDisabledBackend.getId()).thenReturn(2L);
        Mockito.when(loadDisabledBackend.isLoadAvailable()).thenReturn(false);
        Mockito.when(systemInfoService.getBackendsByCurrentCluster())
                .thenReturn(ImmutableMap.of(1L, currentComputeGroupBackend, 2L, loadDisabledBackend));
        Mockito.when(systemInfoService.getAllBackendIds(true)).thenReturn(Collections.singletonList(3L));
        Mockito.when(table.getIndexNumber()).thenReturn(1);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            OlapTableSink sink = new OlapTableSink(table, null, Collections.emptyList());
            TOlapTableLocationParam location = sink.createDummyLocation(table);

            Assertions.assertEquals(Collections.singletonList(1L),
                    location.getTablets().get(0).getNodeIds());
            Mockito.verify(systemInfoService, Mockito.never()).getAllBackendIds(true);
            Mockito.verify(systemInfoService).getBackendsByCurrentCluster();
        }
    }

    @Test
    public void testCreateDummyLocationDoesNotShareBackendCandidatesAcrossIndexes() throws Exception {
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend currentComputeGroupBackend = Mockito.mock(Backend.class);
        OlapTable table = Mockito.mock(OlapTable.class);

        Mockito.when(currentComputeGroupBackend.getId()).thenReturn(1L);
        Mockito.when(currentComputeGroupBackend.isLoadAvailable()).thenReturn(true);
        Mockito.when(systemInfoService.getBackendsByCurrentCluster())
                .thenReturn(ImmutableMap.of(1L, currentComputeGroupBackend));
        Mockito.when(table.getIndexNumber()).thenReturn(2);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            OlapTableSink sink = new OlapTableSink(table, null, Collections.emptyList());
            TOlapTableLocationParam location = sink.createDummyLocation(table);

            Assertions.assertEquals(2, location.getTabletsSize());
            Assertions.assertEquals(Collections.singletonList(1L),
                    location.getTablets().get(0).getNodeIds());
            Assertions.assertEquals(Collections.singletonList(1L),
                    location.getTablets().get(1).getNodeIds());
        }
    }

    @Test
    public void testAdaptiveRandomBucketAssignmentIsPerIndex() {
        TOlapTablePartition partition = new TOlapTablePartition();
        partition.setId(1000L);
        partition.setNumBuckets(2);
        partition.setLoadTabletIdx(0);
        partition.addToIndexes(new TOlapTableIndexTablets(1L, Arrays.asList(100L, 101L)));
        partition.addToIndexes(new TOlapTableIndexTablets(2L, Arrays.asList(200L, 201L)));

        List<TTabletLocation> locations = Arrays.asList(
                new TTabletLocation(100L, Arrays.asList(10L)),
                new TTabletLocation(101L, Arrays.asList(20L)),
                new TTabletLocation(200L, Arrays.asList(20L)),
                new TTabletLocation(201L, Arrays.asList(10L)));

        Map<Long, Map<Long, AdaptiveBucketAssignment>> assignments =
                OlapTableSink.computeAdaptiveRandomBucketAssignments(
                        Arrays.asList(10L, 20L), Arrays.asList(partition), locations, 2);

        AdaptiveBucketAssignment be10Assignment = assignments.get(10L).get(1000L);
        Assertions.assertEquals(0, be10Assignment.getLoadTabletIdx());
        Assertions.assertEquals(10L, be10Assignment.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0), be10Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be10Assignment, 1L, 10L, Arrays.asList(0));
        assertIndexAssignment(be10Assignment, 2L, 20L, Arrays.asList(0));

        AdaptiveBucketAssignment be20Assignment = assignments.get(20L).get(1000L);
        Assertions.assertEquals(1, be20Assignment.getLoadTabletIdx());
        Assertions.assertEquals(20L, be20Assignment.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(1), be20Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be20Assignment, 1L, 20L, Arrays.asList(1));
        assertIndexAssignment(be20Assignment, 2L, 10L, Arrays.asList(1));

        OlapTableSink.applyAdaptiveRandomBucketAssignments(Arrays.asList(partition), assignments.get(10L));
        Assertions.assertEquals(10L, partition.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0), partition.getLocalBucketSeqs());
        Assertions.assertEquals(10L, partition.getIndexes().get(0).getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0), partition.getIndexes().get(0).getLocalBucketSeqs());
        Assertions.assertEquals(20L, partition.getIndexes().get(1).getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0), partition.getIndexes().get(1).getLocalBucketSeqs());
    }

    @Test
    public void testAdaptiveRandomBucketAssignmentIsSharedByReceiverPartition() {
        TOlapTablePartition partition = new TOlapTablePartition();
        partition.setId(1001L);
        partition.setNumBuckets(4);
        partition.setLoadTabletIdx(0);
        partition.addToIndexes(new TOlapTableIndexTablets(1L, Arrays.asList(100L, 101L, 102L, 103L)));
        partition.addToIndexes(new TOlapTableIndexTablets(2L, Arrays.asList(200L, 201L, 202L, 203L)));

        List<TTabletLocation> locations = Arrays.asList(
                new TTabletLocation(100L, Arrays.asList(10L)),
                new TTabletLocation(101L, Arrays.asList(10L)),
                new TTabletLocation(102L, Arrays.asList(20L)),
                new TTabletLocation(103L, Arrays.asList(20L)),
                new TTabletLocation(200L, Arrays.asList(30L)),
                new TTabletLocation(201L, Arrays.asList(30L)),
                new TTabletLocation(202L, Arrays.asList(30L)),
                new TTabletLocation(203L, Arrays.asList(30L)));

        Map<Long, Map<Long, AdaptiveBucketAssignment>> assignments =
                OlapTableSink.computeAdaptiveRandomBucketAssignments(
                        Arrays.asList(10L, 20L, 30L, 40L), Arrays.asList(partition), locations, 4);

        AdaptiveBucketAssignment be10Assignment = assignments.get(10L).get(1001L);
        Assertions.assertEquals(0, be10Assignment.getLoadTabletIdx());
        Assertions.assertEquals(10L, be10Assignment.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0, 1), be10Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be10Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));

        AdaptiveBucketAssignment be20Assignment = assignments.get(20L).get(1001L);
        Assertions.assertEquals(2, be20Assignment.getLoadTabletIdx());
        Assertions.assertEquals(20L, be20Assignment.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(2, 3), be20Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be20Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));

        AdaptiveBucketAssignment be30Assignment = assignments.get(30L).get(1001L);
        Assertions.assertEquals(be10Assignment.getLoadTabletIdx(), be30Assignment.getLoadTabletIdx());
        Assertions.assertEquals(be10Assignment.getBucketBeId(), be30Assignment.getBucketBeId());
        Assertions.assertEquals(be10Assignment.getLocalBucketSeqs(), be30Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be30Assignment, 1L, 10L, Arrays.asList(0, 1));
        assertIndexAssignment(be30Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));

        AdaptiveBucketAssignment be40Assignment = assignments.get(40L).get(1001L);
        Assertions.assertEquals(be20Assignment.getLoadTabletIdx(), be40Assignment.getLoadTabletIdx());
        Assertions.assertEquals(be20Assignment.getBucketBeId(), be40Assignment.getBucketBeId());
        Assertions.assertEquals(be20Assignment.getLocalBucketSeqs(), be40Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be40Assignment, 1L, 20L, Arrays.asList(2, 3));
        assertIndexAssignment(be40Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));
    }

    private void assertIndexAssignment(AdaptiveBucketAssignment assignment, long indexId, long bucketBeId,
            List<Integer> localBucketSeqs) {
        AdaptiveIndexBucketAssignment indexAssignment = assignment.getIndexAssignments().get(indexId);
        Assertions.assertNotNull(indexAssignment);
        Assertions.assertEquals(indexId, indexAssignment.getIndexId());
        Assertions.assertEquals(bucketBeId, indexAssignment.getBucketBeId());
        Assertions.assertEquals(localBucketSeqs, indexAssignment.getLocalBucketSeqs());
    }
}

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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TQueryCacheParam;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UnassignedScanSingleOlapTableJobTest {
    @Test
    public void testQueryCacheAssignByPartition() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setQueryId(new TUniqueId(1, 1));
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select * from t", 0));
        connectContext.setStatementContext(statementContext);

        long partitionOne = 100L;
        long partitionTwo = 200L;
        long selectedIndexId = 10L;
        Map<Long, Long> tabletToPartition = ImmutableMap.of(
                1L, partitionOne,
                2L, partitionOne,
                3L, partitionOne,
                4L, partitionTwo,
                5L, partitionTwo,
                6L, partitionTwo
        );

        OlapScanNode olapScanNode = Mockito.mock(OlapScanNode.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapScanNode.getSelectedPartitionIds())
                .thenReturn(Arrays.asList(partitionOne, partitionTwo));
        Mockito.when(olapScanNode.getSelectedIndexId()).thenReturn(selectedIndexId);
        Mockito.when(olapScanNode.getOlapTable()).thenReturn(olapTable);
        Mockito.when(olapScanNode.getScanContext()).thenReturn(ScanContext.EMPTY);
        Mockito.when(olapScanNode.getScanTabletIds())
                .thenReturn(new ArrayList<>(tabletToPartition.keySet()));

        Partition firstPartition = Mockito.mock(Partition.class);
        MaterializedIndex firstIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(olapTable.getPartition(partitionOne)).thenReturn(firstPartition);
        Mockito.when(firstPartition.getIndex(selectedIndexId)).thenReturn(firstIndex);
        Mockito.when(firstIndex.getTablets()).thenReturn(ImmutableList.of(
                tablet(1L), tablet(2L), tablet(3L)
        ));

        Partition secondPartition = Mockito.mock(Partition.class);
        MaterializedIndex secondIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(olapTable.getPartition(partitionTwo)).thenReturn(secondPartition);
        Mockito.when(secondPartition.getIndex(selectedIndexId)).thenReturn(secondIndex);
        Mockito.when(secondIndex.getTablets()).thenReturn(ImmutableList.of(
                tablet(4L), tablet(5L), tablet(6L)
        ));

        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.RANDOM);
        fragment.queryCacheParam = new TQueryCacheParam();

        DistributedPlanWorker worker1 = new TestWorker(1L, "be1");
        DistributedPlanWorker worker2 = new TestWorker(2L, "be2");
        Map<DistributedPlanWorker, UninstancedScanSource> workerToScanSources
                = new LinkedHashMap<>();
        // Same partition tablets on one BE should be grouped into one instance.
        workerToScanSources.put(worker1, new UninstancedScanSource(new DefaultScanSource(
                ImmutableMap.of(olapScanNode, scanRanges(1L, 2L, 4L)))));
        workerToScanSources.put(worker2, new UninstancedScanSource(new DefaultScanSource(
                ImmutableMap.of(olapScanNode, scanRanges(3L, 5L, 6L)))));

        ScanWorkerSelector scanWorkerSelector = Mockito.mock(ScanWorkerSelector.class);
        Mockito.when(scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(
                Mockito.eq(olapScanNode), Mockito.eq(connectContext)
        )).thenReturn(workerToScanSources);

        UnassignedScanSingleOlapTableJob unassignedJob = new UnassignedScanSingleOlapTableJob(
                statementContext,
                fragment,
                olapScanNode,
                ArrayListMultimap.create(),
                scanWorkerSelector
        );
        DistributeContext distributeContext = new DistributeContext(
                Mockito.mock(DistributedPlanWorkerManager.class),
                true
        );

        List<AssignedJob> assignedJobs = unassignedJob.computeAssignedJobs(
                distributeContext, ArrayListMultimap.create());

        Assertions.assertEquals(4, assignedJobs.size());

        Map<Long, Set<Set<Long>>> workerToInstanceTablets = new HashMap<>();
        for (AssignedJob assignedJob : assignedJobs) {
            DefaultScanSource defaultScanSource = (DefaultScanSource) assignedJob.getScanSource();
            ScanRanges ranges = defaultScanSource.scanNodeToScanRanges.get(olapScanNode);
            Set<Long> tabletIds = ranges.params.stream()
                    .map(param -> param.getScanRange().getPaloScanRange().getTabletId())
                    .collect(Collectors.toCollection(HashSet::new));
            Set<Long> partitionIds = tabletIds.stream()
                    .map(tabletToPartition::get)
                    .collect(Collectors.toSet());

            // Every instance must only contain tablets from one partition.
            Assertions.assertEquals(1, partitionIds.size());

            workerToInstanceTablets.computeIfAbsent(
                    assignedJob.getAssignedWorker().id(), k -> new HashSet<>()
            ).add(tabletIds);
        }

        Map<Long, Set<Set<Long>>> expected = new HashMap<>();
        expected.put(1L, new HashSet<>(Arrays.asList(
                new HashSet<>(Arrays.asList(1L, 2L)),
                new HashSet<>(Arrays.asList(4L))
        )));
        expected.put(2L, new HashSet<>(Arrays.asList(
                new HashSet<>(Arrays.asList(3L)),
                new HashSet<>(Arrays.asList(5L, 6L))
        )));

        // Different partitions are split into different instances on each BE.
        Assertions.assertEquals(expected, workerToInstanceTablets);
    }

    @Test
    public void testQueryCacheFallbackToDefaultWhenPartitionMappingIncomplete() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setQueryId(new TUniqueId(2, 2));
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select * from t", 0));
        connectContext.setStatementContext(statementContext);

        long partitionOne = 100L;
        long selectedIndexId = 10L;

        OlapScanNode olapScanNode = Mockito.mock(OlapScanNode.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        // Intentionally miss partitionTwo to trigger fallback.
        Mockito.when(olapScanNode.getSelectedPartitionIds())
                .thenReturn(ImmutableList.of(partitionOne));
        Mockito.when(olapScanNode.getSelectedIndexId()).thenReturn(selectedIndexId);
        Mockito.when(olapScanNode.getOlapTable()).thenReturn(olapTable);
        Mockito.when(olapScanNode.getScanContext()).thenReturn(ScanContext.EMPTY);
        Mockito.when(olapScanNode.getScanTabletIds())
                .thenReturn(new ArrayList<>(ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L)));

        Partition firstPartition = Mockito.mock(Partition.class);
        MaterializedIndex firstIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(olapTable.getPartition(partitionOne)).thenReturn(firstPartition);
        Mockito.when(firstPartition.getIndex(selectedIndexId)).thenReturn(firstIndex);
        Mockito.when(firstIndex.getTablets())
                .thenReturn(ImmutableList.of(tablet(1L), tablet(2L), tablet(3L)));

        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.RANDOM);
        fragment.queryCacheParam = new TQueryCacheParam();

        DistributedPlanWorker worker1 = new TestWorker(1L, "be1");
        DistributedPlanWorker worker2 = new TestWorker(2L, "be2");
        Map<DistributedPlanWorker, UninstancedScanSource> workerToScanSources
                = new LinkedHashMap<>();
        workerToScanSources.put(worker1, new UninstancedScanSource(new DefaultScanSource(
                ImmutableMap.of(olapScanNode, scanRanges(1L, 2L, 4L)))));
        workerToScanSources.put(worker2, new UninstancedScanSource(new DefaultScanSource(
                ImmutableMap.of(olapScanNode, scanRanges(3L, 5L, 6L)))));

        ScanWorkerSelector scanWorkerSelector = Mockito.mock(ScanWorkerSelector.class);
        Mockito.when(scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(
                Mockito.eq(olapScanNode), Mockito.eq(connectContext)
        )).thenReturn(workerToScanSources);

        UnassignedScanSingleOlapTableJob unassignedJob = new UnassignedScanSingleOlapTableJob(
                statementContext,
                fragment,
                olapScanNode,
                ArrayListMultimap.create(),
                scanWorkerSelector
        );

        List<AssignedJob> assignedJobs = unassignedJob.computeAssignedJobs(
                new DistributeContext(Mockito.mock(DistributedPlanWorkerManager.class), true),
                ArrayListMultimap.create());

        // query cache default planning uses one instance per tablet.
        Assertions.assertEquals(6, assignedJobs.size());
    }

    @Test
    public void testNonQueryCacheUseDefaultPlanning() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setQueryId(new TUniqueId(3, 3));
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select * from t", 0));
        connectContext.setStatementContext(statementContext);

        long partitionOne = 100L;
        long partitionTwo = 200L;
        long selectedIndexId = 10L;

        OlapScanNode olapScanNode = Mockito.mock(OlapScanNode.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapScanNode.getSelectedPartitionIds())
                .thenReturn(Arrays.asList(partitionOne, partitionTwo));
        Mockito.when(olapScanNode.getSelectedIndexId()).thenReturn(selectedIndexId);
        Mockito.when(olapScanNode.getOlapTable()).thenReturn(olapTable);
        Mockito.when(olapScanNode.getScanContext()).thenReturn(ScanContext.EMPTY);
        Mockito.when(olapScanNode.getScanTabletIds())
                .thenReturn(new ArrayList<>(ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L)));

        Partition firstPartition = Mockito.mock(Partition.class);
        MaterializedIndex firstIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(olapTable.getPartition(partitionOne)).thenReturn(firstPartition);
        Mockito.when(firstPartition.getIndex(selectedIndexId)).thenReturn(firstIndex);
        Mockito.when(firstIndex.getTablets())
                .thenReturn(ImmutableList.of(tablet(1L), tablet(2L), tablet(3L)));

        Partition secondPartition = Mockito.mock(Partition.class);
        MaterializedIndex secondIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(olapTable.getPartition(partitionTwo)).thenReturn(secondPartition);
        Mockito.when(secondPartition.getIndex(selectedIndexId)).thenReturn(secondIndex);
        Mockito.when(secondIndex.getTablets())
                .thenReturn(ImmutableList.of(tablet(4L), tablet(5L), tablet(6L)));

        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.RANDOM);
        // No query cache param, must use default planning.
        fragment.setParallelExecNum(10);

        DistributedPlanWorker worker1 = new TestWorker(1L, "be1");
        DistributedPlanWorker worker2 = new TestWorker(2L, "be2");
        Map<DistributedPlanWorker, UninstancedScanSource> workerToScanSources
                = new LinkedHashMap<>();
        workerToScanSources.put(worker1, new UninstancedScanSource(new DefaultScanSource(
                ImmutableMap.of(olapScanNode, scanRanges(1L, 2L, 4L)))));
        workerToScanSources.put(worker2, new UninstancedScanSource(new DefaultScanSource(
                ImmutableMap.of(olapScanNode, scanRanges(3L, 5L, 6L)))));

        ScanWorkerSelector scanWorkerSelector = Mockito.mock(ScanWorkerSelector.class);
        Mockito.when(scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(
                Mockito.eq(olapScanNode), Mockito.eq(connectContext)
        )).thenReturn(workerToScanSources);

        UnassignedScanSingleOlapTableJob unassignedJob = new UnassignedScanSingleOlapTableJob(
                statementContext,
                fragment,
                olapScanNode,
                ArrayListMultimap.create(),
                scanWorkerSelector
        );

        List<AssignedJob> assignedJobs = unassignedJob.computeAssignedJobs(
                new DistributeContext(Mockito.mock(DistributedPlanWorkerManager.class), true),
                ArrayListMultimap.create());

        // default planning splits by tablet count when parallelExecNum is large enough.
        Assertions.assertEquals(6, assignedJobs.size());
    }

    private static Tablet tablet(long tabletId) {
        return new LocalTablet(tabletId);
    }

    private static ScanRanges scanRanges(long... tabletIds) {
        ScanRanges scanRanges = new ScanRanges();
        for (long tabletId : tabletIds) {
            TPaloScanRange paloScanRange = new TPaloScanRange();
            paloScanRange.setTabletId(tabletId);
            TScanRange scanRange = new TScanRange();
            scanRange.setPaloScanRange(paloScanRange);
            TScanRangeParams scanRangeParams = new TScanRangeParams();
            scanRangeParams.setScanRange(scanRange);
            scanRanges.addScanRange(scanRangeParams, 1L);
        }
        return scanRanges;
    }

    private static class TestWorker implements DistributedPlanWorker {
        private final long id;
        private final String address;

        private TestWorker(long id, String address) {
            this.id = id;
            this.address = address;
        }

        @Override
        public long getCatalogId() {
            return 0;
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public String address() {
            return address;
        }

        @Override
        public String host() {
            return address;
        }

        @Override
        public int port() {
            return 0;
        }

        @Override
        public String brpcAddress() {
            return address;
        }

        @Override
        public int brpcPort() {
            return 0;
        }

        @Override
        public boolean available() {
            return true;
        }
    }
}

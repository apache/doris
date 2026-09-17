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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTableWrapper;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

class PruneEmptyPartitionTest implements MemoPatternMatchSupported {

    @Test
    void testIncrementalReadGetsFreshVersionsBeforePruning() throws Exception {
        long stalePartitionId = 100L;
        long nonEmptyPartitionId = 101L;
        List<Long> partitionIds = ImmutableList.of(stalePartitionId, nonEmptyPartitionId);

        CloudPartition stalePartition = Mockito.mock(CloudPartition.class);
        Mockito.when(stalePartition.getId()).thenReturn(stalePartitionId);
        CloudPartition nonEmptyPartition = Mockito.mock(CloudPartition.class);
        Mockito.when(nonEmptyPartition.getId()).thenReturn(nonEmptyPartitionId);

        OlapTable table = Mockito.spy(PlanConstructor.newOlapTable(10L, "incr_tbl", 0));
        Mockito.doReturn(partitionIds).when(table).getPartitionIds();
        Mockito.doReturn(stalePartition).when(table).getPartition(stalePartitionId);
        Mockito.doReturn(nonEmptyPartition).when(table).getPartition(nonEmptyPartitionId);
        Mockito.doReturn(ImmutableList.of(nonEmptyPartitionId)).when(table)
                .selectNonEmptyPartitionIds(Mockito.anyCollection(), Mockito.any());

        TableScanParams scanParams = new TableScanParams(
                TableScanParams.INCREMENTAL_READ, Collections.emptyMap(), Collections.emptyList());
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), table, ImmutableList.of("incr_tbl"),
                ImmutableList.of(), ImmutableList.of(), Optional.empty(), ImmutableList.of(),
                Optional.of(scanParams));
        Assertions.assertEquals(partitionIds, scan.getSelectedPartitionIds());
        ConnectContext connectContext = MemoTestUtils.createConnectContext();

        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class);
                MockedStatic<CloudPartition> mockedPartition = Mockito.mockStatic(CloudPartition.class)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(true);
            mockedPartition.when(() -> CloudPartition.getSnapshotVisibleVersionFromMs(
                    Mockito.anyList(), Mockito.eq(false))).thenReturn(ImmutableList.of(2L, 2L));

            LogicalOlapScan rewritten = (LogicalOlapScan) PlanChecker.from(connectContext, scan)
                    .applyTopDown(new PruneEmptyPartition())
                    .getPlan();

            Assertions.assertEquals(partitionIds, rewritten.getSelectedPartitionIds());
            mockedPartition.verify(() -> CloudPartition.getSnapshotVisibleVersionFromMs(
                    Mockito.anyList(), Mockito.eq(false)));
            Mockito.verify(table, Mockito.never()).selectNonEmptyPartitionIds(
                    Mockito.anyCollection(), Mockito.any());
        }
    }

    @Test
    void testNormalReadUsesCachedPartitionVersions() {
        long emptyPartitionId = 100L;
        long nonEmptyPartitionId = 101L;
        List<Long> partitionIds = ImmutableList.of(emptyPartitionId, nonEmptyPartitionId);

        CloudPartition emptyPartition = Mockito.mock(CloudPartition.class);
        Mockito.when(emptyPartition.getId()).thenReturn(emptyPartitionId);
        CloudPartition nonEmptyPartition = Mockito.mock(CloudPartition.class);
        Mockito.when(nonEmptyPartition.getId()).thenReturn(nonEmptyPartitionId);

        OlapTable table = Mockito.spy(PlanConstructor.newOlapTable(10L, "normal_tbl", 0));
        Mockito.doReturn(partitionIds).when(table).getPartitionIds();
        Mockito.doReturn(emptyPartition).when(table).getPartition(emptyPartitionId);
        Mockito.doReturn(nonEmptyPartition).when(table).getPartition(nonEmptyPartitionId);
        Mockito.doReturn(ImmutableList.of(nonEmptyPartitionId)).when(table)
                .selectNonEmptyPartitionIds(Mockito.anyCollection(), Mockito.any());

        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), table, ImmutableList.of("normal_tbl"));
        ConnectContext connectContext = MemoTestUtils.createConnectContext();

        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class);
                MockedStatic<CloudPartition> mockedPartition = Mockito.mockStatic(CloudPartition.class)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(true);

            LogicalOlapScan rewritten = (LogicalOlapScan) PlanChecker.from(connectContext, scan)
                    .applyTopDown(new PruneEmptyPartition())
                    .getPlan();

            Assertions.assertEquals(ImmutableList.of(nonEmptyPartitionId), rewritten.getSelectedPartitionIds());
            Mockito.verify(table).selectNonEmptyPartitionIds(partitionIds, Optional.empty());
            mockedPartition.verifyNoInteractions();
        }
    }

    @Test
    void testIncrementalReadWithFixedVersionsKeepsSnapshot() {
        long emptyPartitionId = 100L;
        long nonEmptyPartitionId = 101L;
        List<Long> partitionIds = ImmutableList.of(emptyPartitionId, nonEmptyPartitionId);

        CloudPartition emptyPartition = Mockito.mock(CloudPartition.class);
        CloudPartition nonEmptyPartition = Mockito.mock(CloudPartition.class);
        OlapTable originTable = Mockito.spy(PlanConstructor.newOlapTable(10L, "stream_tbl", 0));
        Mockito.doReturn(partitionIds).when(originTable).getPartitionIds();
        Mockito.doReturn(emptyPartition).when(originTable).getPartition(emptyPartitionId);
        Mockito.doReturn(nonEmptyPartition).when(originTable).getPartition(nonEmptyPartitionId);

        OlapTableWrapper table = Mockito.spy(new OlapTableWrapper(originTable, Collections.emptyMap(),
                ImmutableMap.of(emptyPartitionId, 1L, nonEmptyPartitionId, 2L)));
        Mockito.doReturn(ImmutableList.of(nonEmptyPartitionId)).when(table)
                .selectNonEmptyPartitionIds(Mockito.anyCollection(), Mockito.any());

        TableScanParams scanParams = new TableScanParams(
                TableScanParams.INCREMENTAL_READ, Collections.emptyMap(), Collections.emptyList());
        LogicalOlapScan scan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), table, ImmutableList.of("stream_tbl"),
                ImmutableList.of(), ImmutableList.of(), Optional.empty(), ImmutableList.of(),
                Optional.of(scanParams));
        ConnectContext connectContext = MemoTestUtils.createConnectContext();

        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class);
                MockedStatic<CloudPartition> mockedPartition = Mockito.mockStatic(CloudPartition.class)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(true);

            LogicalOlapScan rewritten = (LogicalOlapScan) PlanChecker.from(connectContext, scan)
                    .applyTopDown(new PruneEmptyPartition())
                    .getPlan();

            Assertions.assertEquals(ImmutableList.of(nonEmptyPartitionId), rewritten.getSelectedPartitionIds());
            Mockito.verify(table).selectNonEmptyPartitionIds(partitionIds, Optional.empty());
            mockedPartition.verifyNoInteractions();
        }
    }
}

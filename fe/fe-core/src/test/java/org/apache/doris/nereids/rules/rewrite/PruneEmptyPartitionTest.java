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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class PruneEmptyPartitionTest implements MemoPatternMatchSupported {

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

        try (MockedStatic<CloudPartition> mockedPartition = Mockito.mockStatic(CloudPartition.class)) {
            LogicalOlapScan rewritten = (LogicalOlapScan) PlanChecker.from(connectContext, scan)
                    .applyTopDown(new PruneEmptyPartition())
                    .getPlan();

            Assertions.assertEquals(ImmutableList.of(nonEmptyPartitionId), rewritten.getSelectedPartitionIds());
            Mockito.verify(table).selectNonEmptyPartitionIds(partitionIds, Optional.empty());
            mockedPartition.verifyNoInteractions();
        }
    }
}

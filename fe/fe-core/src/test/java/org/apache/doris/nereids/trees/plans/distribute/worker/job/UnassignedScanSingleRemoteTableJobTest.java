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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class UnassignedScanSingleRemoteTableJobTest {

    @Test
    public void testOneAssignedRangeCountPushdownUsesOneInstance() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.COUNT);

        Assertions.assertEquals(1, job.degreeOfParallelism(1, true));
    }

    @Test
    public void testCountWithMultipleAssignedRangesKeepsLocalShuffleParallelism() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.COUNT);

        Assertions.assertEquals(8, job.degreeOfParallelism(2, true));
    }

    @Test
    public void testNonCountKeepsLocalShuffleParallelism() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.NONE);

        Assertions.assertEquals(8, job.degreeOfParallelism(1, true));
    }

    private static UnassignedScanSingleRemoteTableJob createJob(TPushAggOp aggOp) {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setQueryId(new TUniqueId(10, 10));
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select count(*) from files", 0));
        connectContext.setStatementContext(statementContext);

        ScanNode scanNode = Mockito.mock(ScanNode.class);
        Mockito.when(scanNode.getPushDownAggNoGroupingOp()).thenReturn(aggOp);
        // The global scan range count can differ from the ranges assigned to this worker.
        Mockito.when(scanNode.getScanRangeNum()).thenReturn(1);

        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getDataPartition()).thenReturn(DataPartition.RANDOM);
        Mockito.when(fragment.getParallelExecNum()).thenReturn(8);

        return new UnassignedScanSingleRemoteTableJob(
                statementContext,
                fragment,
                scanNode,
                ArrayListMultimap.create(),
                Mockito.mock(ScanWorkerSelector.class));
    }
}

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

package org.apache.doris.qe;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class OldCoordinatorTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.tbl(id int, value int) distributed by hash(id) buckets 1 properties('replication_num'='1')");
        createTable("create table test.tbl2(id int, value int) distributed by hash(id) buckets 1 properties('replication_num'='1')");
        createTable("create table test.tbl3(id int, value int) distributed by hash(id) buckets 10 properties('replication_num'='1')");
    }

    @Test
    public void test() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 2;
        connectContext.getSessionVariable().setEnableLocalShuffle(false);
        StmtExecutor stmtExecutor = getSqlStmtExecutor(
                "select *\n"
                        + "from (\n"   // left most has exchange trigger shuffle instance logic
                        + "  select a.value\n"
                        + "  from (select value from test.tbl group by value)a\n"
                        + "  join[broadcast] test.tbl2\n"
                        + "  on a.value=tbl2.id\n"
                        + ")b\n"
                        + "join[shuffle] test.tbl3 c\n"   // 2 instances, should use as the shuffle instances
                        + "on b.value = c.id\n"
                        + "join[shuffle] (\n"            // 1 instance, skip
                        + "  select a.value\n"
                        + "  from (select value from test.tbl group by value)a\n"
                        + "  join[broadcast] test.tbl2\n"
                        + "  on a.value=tbl2.id\n"
                        + ")d\n"
                        + "on c.id = d.value\n"
                        + "join[shuffle] test.tbl e\n"   // 1 instance, skip
                        + "on d.value=e.id");

        AtomicBoolean shuffleFragmentHasMultiInstances = new AtomicBoolean(false);
        new Coordinator(connectContext, stmtExecutor.planner()) {
            public void test() throws Exception {
                super.processFragmentAssignmentAndParams();

                Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = getFragmentExecParamsMap();
                PlanFragmentId scanTbl3FragmentId = null;
                for (FragmentExecParams fragmentExecParams : fragmentExecParamsMap.values()) {
                    PlanNode planRoot = fragmentExecParams.fragment.getPlanRoot();
                    if (planRoot instanceof OlapScanNode && ((OlapScanNode) planRoot).getOlapTable().getName()
                            .equals("tbl3")) {
                        scanTbl3FragmentId = fragmentExecParams.fragment.getId();
                        break;
                    }
                }

                for (FragmentExecParams fragmentExecParams : fragmentExecParamsMap.values()) {
                    List<FInstanceExecParam> instances = fragmentExecParams.instanceExecParams;
                    boolean childScanTbl3 = false;
                    for (PlanFragment child : fragmentExecParams.fragment.getChildren()) {
                        if (child.getFragmentId().equals(scanTbl3FragmentId)) {
                            childScanTbl3 = true;
                            break;
                        }
                    }
                    if (childScanTbl3 && instances.size() >= 2) {
                        shuffleFragmentHasMultiInstances.set(true);
                    }
                }
            }
        }.test();
        Assertions.assertTrue(shuffleFragmentHasMultiInstances.get());
    }

    @ParameterizedTest
    @EnumSource(value = TStatusCode.class, names = {"CANCELLED", "TIMEOUT"})
    public void testTerminateBetweenExecutionAdmissionAndFragmentDispatch(TStatusCode statusCode) {
        Status cancelReason = new Status(statusCode, "terminate before fragment dispatch");
        Coordinator coordinator = new Coordinator(0L, new TUniqueId(1L, 1L),
                new DescriptorTable(), Collections.emptyList(), Collections.emptyList(), "UTC", false, false) {
            @Override
            protected void execInternal() throws Exception {
                cancel(cancelReason);
                sendPipelineCtx();
            }
        };

        UserException exception = Assertions.assertThrows(UserException.class, coordinator::exec);
        Assertions.assertTrue(exception.getMessage().contains("terminate before fragment dispatch"));
    }

    @Test
    public void testCancelPublishesStatusBeforeScanCleanupFailure() {
        Status cancelReason = new Status(TStatusCode.TIMEOUT, "timeout before scan cleanup");
        ScanNode failingScan = Mockito.mock(ScanNode.class);
        Mockito.doThrow(new RuntimeException("scan cleanup failed")).when(failingScan).stop();
        ScanNode remainingScan = Mockito.mock(ScanNode.class);
        AtomicBoolean cancelInternalCalled = new AtomicBoolean(false);
        Coordinator coordinator = new Coordinator(0L, new TUniqueId(1L, 1L),
                new DescriptorTable(), Collections.emptyList(), Arrays.asList(failingScan, remainingScan),
                "UTC", false, false) {
            @Override
            protected void cancelInternal(Status status) {
                cancelInternalCalled.set(true);
            }
        };

        // A fallible scan cleanup must neither escape to the caller (which would skip the owner's close and
        // mask the retained reason) nor skip the remaining scans. The terminal status is published first.
        Assertions.assertDoesNotThrow(() -> coordinator.cancel(cancelReason));

        Assertions.assertEquals(TStatusCode.TIMEOUT, coordinator.getExecStatus().getErrorCode());
        Assertions.assertEquals("timeout before scan cleanup", coordinator.getExecStatus().getErrorMsg());
        Assertions.assertTrue(cancelInternalCalled.get());
        Mockito.verify(remainingScan).stop();
    }

    @Test
    public void testQueueCancellationPrefersRetainedTerminalReason() {
        Coordinator coordinator = new Coordinator(0L, new TUniqueId(1L, 1L),
                new DescriptorTable(), Collections.emptyList(), Collections.emptyList(),
                "UTC", false, false) {
            @Override
            protected void cancelInternal(Status status) {
            }
        };

        // Without a retained status the queue token's own message stays.
        Assertions.assertTrue(coordinator.preferTerminalReason(new UserException("query is cancelled"))
                .getMessage().contains("query is cancelled"));

        coordinator.cancel(new Status(TStatusCode.TIMEOUT, "retained queue timeout"));
        // A TIMEOUT/KILL that unblocked the queue wait must win over the token's generic message.
        Assertions.assertTrue(coordinator.preferTerminalReason(new UserException("query is cancelled"))
                .getMessage().contains("retained queue timeout"));
    }
}

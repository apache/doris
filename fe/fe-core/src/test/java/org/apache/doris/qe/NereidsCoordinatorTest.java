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

import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.LocalExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class NereidsCoordinatorTest extends TestWithFeService {
    @BeforeAll
    public void init() throws Exception {
        FeConstants.runningUnitTest = true;

        createDatabase("test");
        useDatabase("test");

        createTable("create table tbl(id int) distributed by hash(id) buckets 10 properties('replication_num' = '1');");
        createTable("create table tbl2(id int) distributed by hash(id) buckets 7 properties('replication_num' = '1');");
    }

    @Test
    public void testNereidsCoordinatorScanRangeNum() throws IOException {
        NereidsPlanner planner = plan("select * from test.tbl");
        NereidsCoordinator coordinator = (NereidsCoordinator) EnvFactory.getInstance()
                .createCoordinator(connectContext, planner, null);
        int scanRangeNum = coordinator.getScanRangeNum();
        Assertions.assertEquals(10, scanRangeNum);
    }

    @Test
    public void testNereidsCoordinatorScanRangeNum2() throws IOException {
        NereidsPlanner planner = plan("select * from information_schema.columns");
        NereidsCoordinator coordinator = (NereidsCoordinator) EnvFactory.getInstance()
                .createCoordinator(connectContext, planner, null);
        int scanRangeNum = coordinator.getScanRangeNum();
        Assertions.assertEquals(0, scanRangeNum);
    }

    @Test
    public void testSimpleQueryUseOneInstance() throws IOException {
        ConnectContext connectContext = createDefaultCtx();
        connectContext.getSessionVariable().parallelPipelineTaskNum = 10;
        NereidsPlanner planner = plan("select * from test.tbl", connectContext);
        for (PlanFragment fragment : planner.getFragments()) {
            Assertions.assertEquals(1, fragment.getParallelExecNum());
        }

        planner = plan("select * from test.tbl where id=1", connectContext);
        for (PlanFragment fragment : planner.getFragments()) {
            Assertions.assertEquals(1, fragment.getParallelExecNum());
        }

        planner = plan("select id, id + 1 from test.tbl where id = 2 limit 1", connectContext);
        for (PlanFragment fragment : planner.getFragments()) {
            Assertions.assertEquals(1, fragment.getParallelExecNum());
        }
    }

    @Test
    public void testLegacyBeUsesNativeLocalShufflePlanning() throws Exception {
        int originalBeExecVersion = Config.be_exec_version;
        try {
            Config.be_exec_version = LocalExchangeNode.SUPPORT_UNCONDITIONAL_PASS_TO_ONE_VERSION - 1;
            ConnectContext legacyContext = createDefaultCtx();
            setupLocalShuffleWithPrivateBroadcastBuild(legacyContext);

            NereidsPlanner planner = plan("select sum(distinct a.id) from test.tbl a "
                    + "left join [shuffle] test.tbl2 b on a.id = b.id", legacyContext);
            // A query keeps the version used to build its plan even if the cluster-wide
            // compatibility version changes before the coordinator is constructed.
            Config.be_exec_version = LocalExchangeNode.SUPPORT_UNCONDITIONAL_PASS_TO_ONE_VERSION;
            NereidsCoordinator coordinator = (NereidsCoordinator) EnvFactory.getInstance()
                    .createCoordinator(legacyContext, planner, null);

            Assertions.assertEquals(
                    LocalExchangeNode.SUPPORT_UNCONDITIONAL_PASS_TO_ONE_VERSION - 1,
                    coordinator.getQueryOptions().getBeExecVersion());
            Assertions.assertFalse(coordinator.getQueryOptions().isEnableLocalShufflePlanner());
            Assertions.assertFalse(coordinator.getQueryOptions().isEnableShareHashTableForBroadcastJoin());
            Assertions.assertFalse(planner.getFragments().stream()
                    .anyMatch(fragment -> containsLocalExchange(fragment.getPlanRoot())));

            assertLegacyBucketExchangeFunnel(planner, legacyContext);
        } finally {
            Config.be_exec_version = originalBeExecVersion;
        }
    }

    @Test
    public void testCurrentBeKeepsPrivateBroadcastBuild() throws Exception {
        int originalBeExecVersion = Config.be_exec_version;
        try {
            Config.be_exec_version = LocalExchangeNode.SUPPORT_UNCONDITIONAL_PASS_TO_ONE_VERSION;
            ConnectContext currentContext = createDefaultCtx();
            setupLocalShuffleWithPrivateBroadcastBuild(currentContext);

            NereidsPlanner planner = plan("select sum(distinct a.id) from test.tbl a "
                    + "left join [broadcast] test.tbl b on a.id = b.id", currentContext);
            NereidsCoordinator coordinator = (NereidsCoordinator) EnvFactory.getInstance()
                    .createCoordinator(currentContext, planner, null);

            Assertions.assertEquals(
                    LocalExchangeNode.SUPPORT_UNCONDITIONAL_PASS_TO_ONE_VERSION,
                    coordinator.getQueryOptions().getBeExecVersion());
            Assertions.assertTrue(coordinator.getQueryOptions().isEnableLocalShufflePlanner());
            Assertions.assertFalse(
                    coordinator.getQueryOptions().isEnableShareHashTableForBroadcastJoin());
        } finally {
            Config.be_exec_version = originalBeExecVersion;
        }
    }

    private boolean containsLocalExchange(PlanNode node) {
        if (node instanceof LocalExchangeNode) {
            return true;
        }
        return node.getChildren().stream().anyMatch(this::containsLocalExchange);
    }

    private void assertLegacyBucketExchangeFunnel(NereidsPlanner planner, ConnectContext legacyContext) {
        int matchedExchangeCount = 0;
        for (DistributedPlan distributedPlan : planner.getDistributedPlans().values()) {
            PipelineDistributedPlan receiverPlan = (PipelineDistributedPlan) distributedPlan;
            for (Map.Entry<ExchangeNode, DistributedPlan> input : receiverPlan.getInputs().entries()) {
                ExchangeNode exchange = input.getKey();
                if (exchange.getPartitionType() != TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED
                        || !exchange.isSerialOperatorOnBe(legacyContext)) {
                    continue;
                }
                matchedExchangeCount++;
                Assertions.assertEquals(3, receiverPlan.getInstanceJobs().size());
                Assertions.assertTrue(receiverPlan.getInstanceJobs().stream()
                        .allMatch(LocalShuffleAssignedJob.class::isInstance));

                Map<DistributedPlanWorker, AssignedJob> firstInstancePerWorker = new LinkedHashMap<>();
                for (AssignedJob instance : receiverPlan.getInstanceJobs()) {
                    firstInstancePerWorker.putIfAbsent(instance.getAssignedWorker(), instance);
                }
                Assertions.assertEquals(1, firstInstancePerWorker.size());

                PipelineDistributedPlan senderPlan = (PipelineDistributedPlan) input.getValue();
                Assertions.assertEquals(1, senderPlan.getDestinations().size());
                List<AssignedJob> destinations = senderPlan.getDestinations().values().iterator().next();
                Assertions.assertEquals(Set.copyOf(firstInstancePerWorker.values()),
                        destinations.stream().collect(Collectors.toSet()));
                for (AssignedJob destination : destinations) {
                    Assertions.assertSame(firstInstancePerWorker.get(destination.getAssignedWorker()), destination);
                }

                TPlanNode thriftNode = exchange.getFragment().toThrift().getPlan().getNodes().stream()
                        .filter(node -> node.getNodeId() == exchange.getId().asInt())
                        .findFirst()
                        .orElseThrow();
                Assertions.assertTrue(thriftNode.isIsSerialOperator());
            }
        }
        Assertions.assertEquals(1, matchedExchangeCount);
    }

    private void setupLocalShuffleWithPrivateBroadcastBuild(ConnectContext context) throws Exception {
        SessionVariable sessionVariable = context.getSessionVariable();
        sessionVariable.setEnableLocalShufflePlanner(true);
        sessionVariable.setEnableLocalShuffle(true);
        sessionVariable.setEnableNereidsDistributePlanner(true);
        sessionVariable.setIgnoreStorageDataDistribution(true);
        sessionVariable.setForceToLocalShuffle(true);
        sessionVariable.disableColocatePlan = true;
        sessionVariable.enableBucketShuffleJoin = true;
        sessionVariable.enableShareHashTableForBroadcastJoin = false;
        sessionVariable.setPipelineTaskNum("3");
    }

    private NereidsPlanner plan(String sql) throws IOException {
        return plan(sql, connectContext);
    }

    private NereidsPlanner plan(String sql, ConnectContext connectContext) throws IOException {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION,OLAP_SCAN_TABLET_PRUNE");
        connectContext.setThreadLocalInfo();

        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        NereidsPlanner planner = PlanChecker.from(connectContext).plan(sql);
        return planner;
    }
}

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

package org.apache.doris.nereids.distribute;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedLocalShuffleUnionJob;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.MultiCastDataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class LocalShuffleUnionTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("create table test.tbl(id int) properties('replication_num' = '1')");
    }

    @Test
    public void testLocalShuffleUnion() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        StmtExecutor stmtExecutor = executeNereidsSql(
                "explain distributed plan select * from test.tbl union all select * from test.tbl");
        List<PlanFragment> fragments = stmtExecutor.planner().getFragments();
        assertHasLocalShuffleUnion(fragments);
    }

    @Test
    public void testLocalShuffleUnionWithCte() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        StmtExecutor stmtExecutor = executeNereidsSql(
                "explain distributed plan with a as (select * from test.tbl) select * from a union all select * from a");
        List<PlanFragment> fragments = stmtExecutor.planner().getFragments();
        assertHasLocalShuffleUnion(fragments);
    }

    @Test
    public void testLocalShuffleUnionWithJoin() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        StmtExecutor stmtExecutor = executeNereidsSql(
                "explain distributed plan select * from (select * from test.tbl union all select * from test.tbl)a left join[broadcast] (select * from test.tbl)b on a.id=b.id");
        List<PlanFragment> fragments = stmtExecutor.planner().getFragments();
        assertHasLocalShuffleUnion(fragments);

        FragmentIdMapping<DistributedPlan> distributedPlans
                = ((NereidsPlanner) stmtExecutor.planner()).getDistributedPlans();
        for (DistributedPlan plan : distributedPlans.values()) {
            PipelineDistributedPlan pipelineDistributedPlan = (PipelineDistributedPlan) plan;
            if (pipelineDistributedPlan.getFragmentJob() instanceof UnassignedLocalShuffleUnionJob) {
                List<AssignedJob> sourcesInstances = pipelineDistributedPlan.getInputs()
                        .values()
                        .stream()
                        .flatMap(source -> ((PipelineDistributedPlan) source).getInstanceJobs().stream())
                        .collect(Collectors.toList());

                List<AssignedJob> broadSourceInstances = pipelineDistributedPlan.getInputs()
                        .entries()
                        .stream()
                        .filter(kv -> kv.getKey().getPartitionType() != TPartitionType.RANDOM_LOCAL_SHUFFLE)
                        .flatMap(kv -> ((PipelineDistributedPlan) kv.getValue()).getInstanceJobs().stream())
                        .collect(Collectors.toList());

                Assertions.assertTrue(
                        pipelineDistributedPlan.getInstanceJobs().size() < sourcesInstances.size()
                );

                Assertions.assertEquals(
                        (sourcesInstances.size() - broadSourceInstances.size()),
                        pipelineDistributedPlan.getInstanceJobs().size()
                );
            }
        }
    }

    private void assertHasLocalShuffleUnion(List<PlanFragment> fragments) {
        boolean hasLocalShuffleUnion = false;
        for (PlanFragment fragment : fragments) {
            List<PlanNode> unions = fragment.getPlanRoot().collectInCurrentFragment(UnionNode.class::isInstance);
            for (PlanNode planNode : unions) {
                UnionNode union = (UnionNode) planNode;
                assertUnionIsInplace(union, fragment);
                hasLocalShuffleUnion = true;
            }
        }
        Assertions.assertTrue(hasLocalShuffleUnion);
    }

    private void assertUnionIsInplace(UnionNode unionNode, PlanFragment unionFragment) {
        Assertions.assertTrue(unionNode.isLocalShuffleUnion());
        for (PlanNode child : unionNode.getChildren()) {
            if (child instanceof ExchangeNode) {
                ExchangeNode exchangeNode = (ExchangeNode) child;
                Assertions.assertEquals(TPartitionType.RANDOM_LOCAL_SHUFFLE, exchangeNode.getPartitionType());
                for (PlanFragment childFragment : unionFragment.getChildren()) {
                    DataSink sink = childFragment.getSink();
                    if (sink instanceof DataStreamSink && sink.getExchNodeId().asInt() == exchangeNode.getId().asInt()) {
                        Assertions.assertEquals(TPartitionType.RANDOM_LOCAL_SHUFFLE, sink.getOutputPartition().getType());
                    } else if (sink instanceof MultiCastDataSink) {
                        for (DataStreamSink dataStreamSink : ((MultiCastDataSink) sink).getDataStreamSinks()) {
                            if (dataStreamSink.getExchNodeId().asInt() == exchangeNode.getId().asInt()) {
                                Assertions.assertEquals(TPartitionType.RANDOM_LOCAL_SHUFFLE, dataStreamSink.getOutputPartition().getType());
                            }
                        }
                    }
                }
            }
        }
    }
}

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

public class InplaceUnionTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("create table test.tbl(id int) properties('replication_num' = '1')");
    }

    @Test
    public void testInplaceUnion() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        StmtExecutor stmtExecutor = executeNereidsSql(
                "explain distributed plan select * from test.tbl union all select * from test.tbl");
        List<PlanFragment> fragments = stmtExecutor.planner().getFragments();
        assertHasInplaceUnion(fragments);
    }

    @Test
    public void testInplaceUnionWithCte() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        StmtExecutor stmtExecutor = executeNereidsSql(
                "explain distributed plan with a as (select * from test.tbl) select * from a union all select * from a");
        List<PlanFragment> fragments = stmtExecutor.planner().getFragments();
        assertHasInplaceUnion(fragments);
    }

    private void assertHasInplaceUnion(List<PlanFragment> fragments) {
        boolean hasInplaceUnion = false;
        for (PlanFragment fragment : fragments) {
            List<PlanNode> unions = fragment.getPlanRoot().collectInCurrentFragment(UnionNode.class::isInstance);
            for (PlanNode planNode : unions) {
                UnionNode union = (UnionNode) planNode;
                assertUnionIsInplace(union, fragment);
                hasInplaceUnion = true;
            }
        }
        Assertions.assertTrue(hasInplaceUnion);
    }

    private void assertUnionIsInplace(UnionNode unionNode, PlanFragment unionFragment) {
        Assertions.assertTrue(unionNode.isInplaceUnion());
        for (PlanNode child : unionNode.getChildren()) {
            if (child instanceof ExchangeNode) {
                ExchangeNode exchangeNode = (ExchangeNode) child;
                Assertions.assertEquals(TPartitionType.LOCAL_RANDOM, exchangeNode.getPartitionType());
                for (PlanFragment childFragment : unionFragment.getChildren()) {
                    DataSink sink = childFragment.getSink();
                    if (sink instanceof DataStreamSink && sink.getExchNodeId().asInt() == exchangeNode.getId().asInt()) {
                        Assertions.assertEquals(TPartitionType.LOCAL_RANDOM, sink.getOutputPartition().getType());
                    } else if (sink instanceof MultiCastDataSink) {
                        for (DataStreamSink dataStreamSink : ((MultiCastDataSink) sink).getDataStreamSinks()) {
                            if (dataStreamSink.getExchNodeId().asInt() == exchangeNode.getId().asInt()) {
                                Assertions.assertEquals(TPartitionType.LOCAL_RANDOM, dataStreamSink.getOutputPartition().getType());
                            }
                        }
                    }
                }
            }
        }
    }
}

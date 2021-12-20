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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import mockit.Expectations;
import mockit.Mocked;

public class JoinCostEvaluationTest {

    @Mocked
    private ConnectContext ctx;

    @Mocked
    private PlanNode node;

    @Mocked
    private TableRef ref;

    @Mocked
    private PlanFragmentId fragmentId;

    @Mocked
    private PlanNodeId nodeId;

    @Mocked
    private DataPartition partition;

    @Mocked
    private BinaryPredicate expr;

    @Before
    public void setUp() {
        new Expectations() {
            {
                node.getTupleIds();
                result = Lists.newArrayList();
                node.getTblRefIds();
                result = Lists.newArrayList();
                node.getChildren();
                result = Lists.newArrayList();
            }
        };
    }

    private PlanFragment createPlanFragment(long cardinality, float avgRowSize, int numNodes) {
        HashJoinNode root
                = new HashJoinNode(nodeId, node, node, ref, Lists.newArrayList(expr), Lists.newArrayList(expr));
        root.cardinality = cardinality;
        root.avgRowSize = avgRowSize;
        root.numNodes = numNodes;
        return new PlanFragment(fragmentId, root, partition);
    }

    private boolean callIsBroadcastCostSmaller(long rhsTreeCardinality, float rhsTreeAvgRowSize,
                                               long lhsTreeCardinality, float lhsTreeAvgRowSize, int lhsTreeNumNodes) {
        PlanFragment rightChildFragment = createPlanFragment(rhsTreeCardinality, rhsTreeAvgRowSize, 0);
        PlanFragment leftChildFragment = createPlanFragment(lhsTreeCardinality, lhsTreeAvgRowSize, lhsTreeNumNodes);
        JoinCostEvaluation joinCostEvaluation = new JoinCostEvaluation(node, rightChildFragment, leftChildFragment);
        return joinCostEvaluation.isBroadcastCostSmaller();
    }

    @Test
    public void testIsBroadcastCostSmaller() {
        new Expectations() {
            {
                ConnectContext.get();
                result = ctx;
                ConnectContext.get().getSessionVariable().getPreferJoinMethod();
                result = "broadcast";
            }
        };
        Assert.assertTrue(callIsBroadcastCostSmaller(1, 1, 1, 1, 1));
        Assert.assertTrue(callIsBroadcastCostSmaller(1, 1, 1, 1, 2));
        Assert.assertFalse(callIsBroadcastCostSmaller(1, 1, 1, 1, 3));
        Assert.assertTrue(callIsBroadcastCostSmaller(-1, 1, 1, 1, -1));
        Assert.assertTrue(callIsBroadcastCostSmaller(-1, 1, -1, 1, 1));
        Assert.assertFalse(callIsBroadcastCostSmaller(1, 1, -1, 1, 1));
        Assert.assertTrue(callIsBroadcastCostSmaller(20, 10, 5000, 2, 10));
        Assert.assertFalse(callIsBroadcastCostSmaller(20, 10, 5, 2, 10));
    }

    @Test
    public void testConstructHashTableSpace() {
        long rhsTreeCardinality = 4097;
        float rhsTreeAvgRowSize = 16;
        int rhsNodeTupleIdNum = 1;
        int rhsTreeTupleIdNum = rhsNodeTupleIdNum * 2;
        double nodeArrayLen = 6144;
        new Expectations() {
            {
                node.getTupleIds();
                result = new ArrayList<>(Collections.nCopies(rhsNodeTupleIdNum, 0));
            }
        };
        PlanFragment rightChildFragment = createPlanFragment(rhsTreeCardinality, rhsTreeAvgRowSize, 0);
        PlanFragment leftChildFragment = createPlanFragment(0, 0, 0);
        JoinCostEvaluation joinCostEvaluation = new JoinCostEvaluation(node, rightChildFragment, leftChildFragment);
        long hashTableSpace = Math.round((((rhsTreeCardinality / 0.75) * 8)
                + ((double) rhsTreeCardinality * rhsTreeAvgRowSize) + (nodeArrayLen * 16)
                + (nodeArrayLen * rhsTreeTupleIdNum * 8)) * PlannerContext.HASH_TBL_SPACE_OVERHEAD);

        Assert.assertEquals(hashTableSpace, joinCostEvaluation.constructHashTableSpace());
    }
}

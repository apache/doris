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
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 * this class is used to local shuffle between the same backend, to save network io.
 *
 * for example: we have A/B/C three backend, and every backend process 3 instances before the Union,
 *              then the Union will generate same instances for the source backend, and every source
 *              instances will random local shuffle to the self backend's three target instances, like this:
 *
 * UnionNode(9 target instances, [A4, B4, C4, A5, B5, C5, A6, B6, C6]) -- say there has 3 backends: A/B/C
 * |
 * +- ExchangeNode(3 source instances, [A1, B1, C1]) -- A1 random local shuffle to A4/A5/A6,
 * |                                                    B1 random local shuffle to B4/B5/B6,
 * |                                                    C1 random local shuffle to C4/C5/C6
 * |
 * +- ExchangeNode(3 source instances, [A2, B2, C2]) -- A2 random local shuffle to A4/A5/A6,
 * |                                                    B2 random local shuffle to B4/B5/B6,
 * |                                                    C2 random local shuffle to C4/C5/C6
 * |
 * +- ExchangeNode(3 source instances, [A3, B3, C3]) -- A3 random local shuffle to A4/A5/A6,
 *                                                      B3 random local shuffle to B4/B5/B6,
 *                                                      C3 random local shuffle to C4/C5/C6
 */
public class UnassignedLocalShuffleUnionJob extends AbstractUnassignedJob {

    public UnassignedLocalShuffleUnionJob(StatementContext statementContext, PlanFragment fragment,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(statementContext, fragment, ImmutableList.of(), exchangeToChildJob);
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            DistributeContext context, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        ConnectContext connectContext = statementContext.getConnectContext();
        DefaultScanSource noScanSource = DefaultScanSource.empty();
        List<AssignedJob> unionInstances = Lists.newArrayListWithCapacity(inputJobs.size());

        List<UnionNode> unionNodes = fragment.getPlanRoot().collectInCurrentFragment(UnionNode.class::isInstance);
        Set<Integer> exchangeIdToUnion = Sets.newLinkedHashSet();
        for (UnionNode unionNode : unionNodes) {
            for (PlanNode child : unionNode.getChildren()) {
                if (child instanceof ExchangeNode) {
                    exchangeIdToUnion.add(child.getId().asInt());
                }
            }
        }

        int id = 0;
        for (Entry<ExchangeNode, Collection<AssignedJob>> exchangeNodeToSources : inputJobs.asMap().entrySet()) {
            ExchangeNode exchangeNode = exchangeNodeToSources.getKey();
            if (!exchangeIdToUnion.contains(exchangeNode.getId().asInt())) {
                continue;
            }
            for (AssignedJob inputInstance : exchangeNodeToSources.getValue()) {
                StaticAssignedJob unionInstance = new StaticAssignedJob(
                        id++, connectContext.nextInstanceId(), this,
                        inputInstance.getAssignedWorker(), noScanSource
                );
                unionInstances.add(unionInstance);
            }
        }
        return unionInstances;
    }
}

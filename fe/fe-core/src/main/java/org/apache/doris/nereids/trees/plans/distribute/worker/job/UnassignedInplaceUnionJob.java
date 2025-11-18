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
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

/** UnassignedInplaceUnionJob */
public class UnassignedInplaceUnionJob extends AbstractUnassignedJob {

    public UnassignedInplaceUnionJob(StatementContext statementContext, PlanFragment fragment,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(statementContext, fragment, ImmutableList.of(), exchangeToChildJob);
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            DistributeContext context, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        ConnectContext connectContext = statementContext.getConnectContext();
        DefaultScanSource noScanSource = DefaultScanSource.empty();
        List<AssignedJob> unionInstances = Lists.newArrayListWithCapacity(inputJobs.size());
        int id = 0;
        for (Entry<ExchangeNode, Collection<AssignedJob>> exchangeNodeToSources : inputJobs.asMap().entrySet()) {
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

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

import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/*
 * MultiCast plan fragment.
 */
public class MultiCastPlanFragment extends PlanFragment {
    private final List<ExchangeNode> destNodeList = Lists.newArrayList();

    public MultiCastPlanFragment(PlanFragment planFragment) {
        super(planFragment.getFragmentId(), planFragment.getPlanRoot(), planFragment.getDataPartition(),
                planFragment.getBuilderRuntimeFilterIds(), planFragment.getTargetRuntimeFilterIds());
        this.hasColocatePlanNode = planFragment.hasColocatePlanNode;
        this.outputPartition = DataPartition.RANDOM;
        this.children.addAll(planFragment.getChildren());
    }

    public void addToDest(ExchangeNode exchangeNode) {
        destNodeList.add(exchangeNode);
    }


    public List<PlanFragment> getDestFragmentList() {
        return destNodeList.stream().map(PlanNode::getFragment).collect(Collectors.toList());
    }
}

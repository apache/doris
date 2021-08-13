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

package org.apache.doris.common.profile;

import org.apache.doris.common.UserException;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.thrift.TExplainLevel;

import com.clearspring.analytics.util.Lists;

import java.util.List;

public class PlanTreeBuilder {

    private List<PlanFragment> fragments;
    private PlanTreeNode treeRoot;
    private List<PlanTreeNode> sinkNodes = Lists.newArrayList();
    private List<PlanTreeNode> exchangeNodes = Lists.newArrayList();

    public PlanTreeBuilder(List<PlanFragment> fragments) {
        this.fragments = fragments;
    }

    public PlanTreeNode getTreeRoot() {
        return treeRoot;
    }

    public void build() throws UserException {
        buildFragmentPlans();
        assembleFragmentPlans();
    }

    private void buildFragmentPlans() {
        int i = 0;
        for (PlanFragment fragment : fragments) {
            DataSink sink = fragment.getSink();
            PlanTreeNode sinkNode = null;
            if (sink != null) {
                StringBuilder sb = new StringBuilder();
                sb.append("[").append(sink.getExchNodeId().asInt()).append(": ").append(sink.getClass().getSimpleName()).append("]");
                sb.append("\n[Fragment: ").append(fragment.getId().asInt()).append("]");
                sb.append("\n").append(sink.getExplainString("", TExplainLevel.BRIEF));
                sinkNode = new PlanTreeNode(sink.getExchNodeId(), sb.toString());
                if (i == 0) {
                    // sink of first fragment, set it as tree root
                    treeRoot = sinkNode;
                } else {
                    sinkNodes.add(sinkNode);
                }
            }

            PlanNode planRoot = fragment.getPlanRoot();
            if (planRoot != null) {
                buildForPlanNode(planRoot, sinkNode);
            }
            i++;
        }
    }

    private void assembleFragmentPlans() throws UserException {
        for (PlanTreeNode sender : sinkNodes) {
            if (sender == treeRoot) {
                // This is the result sink, skip it
                continue;
            }
            PlanNodeId senderId = sender.getId();
            PlanTreeNode exchangeNode = findExchangeNode(senderId);
            if (exchangeNode == null) {
                throw new UserException("Failed to find exchange node for sender id: " + senderId.asInt());
            }

            exchangeNode.addChild(sender);
        }
    }

    private PlanTreeNode findExchangeNode(PlanNodeId senderId) {
        for (PlanTreeNode exchangeNode : exchangeNodes) {
            if (exchangeNode.getId().equals(senderId)) {
                return exchangeNode;
            }
        }
        return null;
    }

    private void buildForPlanNode(PlanNode planNode, PlanTreeNode parent) {
        PlanTreeNode node = new PlanTreeNode(planNode.getId(), planNode.getPlanTreeExplanStr());

        if (parent != null) {
            parent.addChild(node);
        }

        if (planNode.getPlanNodeName().equals(ExchangeNode.EXCHANGE_NODE)
                || planNode.getPlanNodeName().equals(ExchangeNode.MERGING_EXCHANGE_NODE)) {
            exchangeNodes.add(node);
        } else {
            // Do not traverse children of exchange node,
            // They will be visited in other fragments.
            for (PlanNode child : planNode.getChildren()) {
                buildForPlanNode(child, node);
            }
        }
    }
}

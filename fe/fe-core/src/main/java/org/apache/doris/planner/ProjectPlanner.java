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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.common.NotImplementedException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class ProjectPlanner {
    private static final Logger LOG = LogManager.getLogger(PlanNode.class);

    private final Analyzer analyzer;

    public ProjectPlanner(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public void projectSingleNodePlan(List<Expr> resultExprs, PlanNode root) {
        Set<SlotId> resultSlotIds = getSlotIds(resultExprs, root);
        projectPlanNode(resultSlotIds, root);
    }

    public void projectPlanNode(Set<SlotId> outputSlotIds, PlanNode planNode) {
        try {
            planNode.initOutputSlotIds(outputSlotIds, analyzer);
            planNode.projectOutputTuple();
        } catch (NotImplementedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
        }
        if (planNode.getChildren().size() == 0) {
            return;
        }
        Set<SlotId> inputSlotIds = null;
        try {
            inputSlotIds = planNode.computeInputSlotIds(analyzer);
        } catch (NotImplementedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(e);
            }
        }
        for (PlanNode child : planNode.getChildren()) {
            projectPlanNode(inputSlotIds, child);
        }
    }

    private Set<SlotId> getSlotIds(List<Expr> resultExprs, PlanNode root) {
        List<Expr> resExprs = Expr.substituteList(resultExprs,
                root.getOutputSmap(), analyzer, false);
        Set<SlotId> result = Sets.newHashSet();
        for (Expr expr : resExprs) {
            List<SlotId> slotIdList = Lists.newArrayList();
            expr.getIds(null, slotIdList);
            result.addAll(slotIdList);
        }
        return result;
    }
}

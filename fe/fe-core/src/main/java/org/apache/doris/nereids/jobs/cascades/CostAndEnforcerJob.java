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

package org.apache.doris.nereids.jobs.cascades;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.ChildPropertyDeriver;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Job to compute cost and add enforcer.
 */
public class CostAndEnforcerJob extends Job<Plan> {
    // GroupExpression to optimize
    private final GroupExpression groupExpression;

    List<List<PhysicalProperties>> properties;
    // Current total cost
    private double curTotalCost;
    // Current stage of enumeration through child groups
    private int curChildIndex = -1;
    // Indicator of last child group that we waited for optimization
    private int prevChildIndex = -1;
    // Current stage of enumeration through outputInputProperties
    private int curPropertyPairIndex = 0;

    public CostAndEnforcerJob(GroupExpression groupExpression, PlannerContext context) {
        super(JobType.OPTIMIZE_CHILDREN, context);
        this.groupExpression = groupExpression;
    }

    @Override
    public void execute() {
        // Init logic: only run once per task
        if (curChildIndex != -1) {
            curTotalCost = 0;
            // TODO(wenjie): pruning

            // Property derive
            ChildPropertyDeriver childPropertyDeriver = new ChildPropertyDeriver(context, groupExpression);
            properties = childPropertyDeriver.getProperties();

            curChildIndex = 0;
        }

        for (; curPropertyPairIndex < properties.size(); curPropertyPairIndex++) {
            List<PhysicalProperties> requiredProperties = properties.get(curPropertyPairIndex);

            // Calculate local cost and update total cost
            if (curChildIndex == 0 && prevChildIndex == -1) {
                localCost = CostModel.calculateCost(groupExpression);
                curTotalCost += context.getOptimizerContext();
            }


            // Reset child idx and total cost
            prevChildIndex = -1;
            curChildIndex = 0;
            curTotalCost = 0;
        }
    }

    private List<List<PhysicalProperties>> getRequiredProps(GroupExpression groupExpression) {
        properties = Lists.newArrayList();
        groupExpression.getOperator().accept(this, new ExpressionContext(groupExpression));
        return requiredProperties;
    }
}

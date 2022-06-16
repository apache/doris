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
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.ChildPropertyDeriver;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * Job to compute cost and add enforcer.
 */
public class CostAndEnforcerJob extends Job<Plan> {
    // GroupExpression to optimize
    private final GroupExpression groupExpression;

    List<List<PhysicalProperties>> propertiesList;
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
            // TODO: pruning

            // Property derive
            ChildPropertyDeriver childPropertyDeriver = new ChildPropertyDeriver(context, groupExpression);
            propertiesList = childPropertyDeriver.getProperties();

            curChildIndex = 0;
        }

        for (; curPropertyPairIndex < propertiesList.size(); curPropertyPairIndex++) {
            List<PhysicalProperties> properties = propertiesList.get(curPropertyPairIndex);

            // Calculate local cost and update total cost
            if (curChildIndex == 0 && prevChildIndex == -1) {
                // curTotalCost += context.getOptimizerContext().getCostModel.calculateCost(groupExpression);
            }

            for (; curChildIndex < groupExpression.arity(); curChildIndex++) {
                PhysicalProperties property = properties.get(curChildIndex);
                Group childGroup = groupExpression.child(curChildIndex);

                Optional<Pair<Double, GroupExpression>> lowestCostPlanOpt = childGroup.getLowestCostPlan(property);

                if (!lowestCostPlanOpt.isPresent()) {
                    if (prevChildIndex >= curChildIndex) {
                        break;
                    }
                    prevChildIndex = curChildIndex;
                    // TODO: pushTask EnforceAndCostTask
                    double newCostUpperBound = context.getCostUpperBound() - curTotalCost;
                    PlannerContext plannerContext = new PlannerContext(context.getOptimizerContext(),
                            context.getConnectContext(), property, newCostUpperBound, context.getNeededAttributes());
                    pushTask(new OptimizeGroupJob(childGroup, plannerContext));
                    return;
                }

                GroupExpression lowestCostExpr = lowestCostPlanOpt.get().second;
                // TODO: check table
                curTotalCost += lowestCostExpr.getLowestCostTable().get(property).first;
                if (curTotalCost > context.getUpperBoundCost()) {
                    break;
                }
            }

            // Check whether we successfully optimize all child group
            if (curChildIndex == groupExpression.arity()) {

            }

            // Reset child idx and total cost
            prevChildIndex = -1;
            curChildIndex = 0;
            curTotalCost = 0;
        }
    }

}

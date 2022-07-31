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
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.ChildOutputPropertyDeriver;
import org.apache.doris.nereids.properties.EnforceMissingPropertiesHelper;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestPropertyDeriver;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * Job to compute cost and add enforcer.
 * Inspired by NoisePage and ORCA-Paper.
 */
public class CostAndEnforcerJob extends Job implements Cloneable {
    // GroupExpression to optimize
    private final GroupExpression groupExpression;
    // Current total cost
    private double curTotalCost;

    // Children properties from parent plan node.
    // Example: Physical Hash Join
    // [ [Properties {"", ANY}, Properties {"", BROADCAST}],
    //   [Properties {"", SHUFFLE_JOIN}, Properties {"", SHUFFLE_JOIN}]]
    private List<List<PhysicalProperties>> propertiesListList;

    private List<GroupExpression> childrenBestGroupExprList;
    private List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList();

    // Current stage of enumeration through child groups
    private int curChildIndex = -1;
    // Indicator of last child group that we waited for optimization
    private int prevChildIndex = -1;
    // Current stage of enumeration through outputInputProperties
    private int curPropertyPairIndex = 0;

    public CostAndEnforcerJob(GroupExpression groupExpression, JobContext context) {
        super(JobType.OPTIMIZE_CHILDREN, context);
        this.groupExpression = groupExpression;
    }

    @Override
    public void execute() {
        for (Group childGroup : groupExpression.children()) {
            if (!childGroup.isHasCost()) {
                // TODO: interim solution
                pushTask(new CostAndEnforcerJob(this.groupExpression, context));
                pushTask(new OptimizeGroupJob(childGroup, context));
                childGroup.setHasCost(true);
                return;
            }
        }
    }

    /*-
     * Please read the ORCA paper
     * - 4.1.4 Optimization.
     * - Figure 7
     *
     *                currentJobSubPlanRoot
     *             / ▲                     ▲ \
     *   requested/ /childOutput childOutput\ \requested
     * Properties/ /Properties     Properties\ \Properties
     *          ▼ /                           \ ▼
     *        child                           child
     *
     *
     *         requestPropertyFromParent          parentPlanNode
     *    ──►              │               ──►          ▲
     *                     ▼                            │
     *         requestPropertyToChildren        ChildOutputProperty
     *
     *         requestPropertyFromParent
     *                     ┼
     *    ──►             gap              ──►  add enforcer to fill the gap
     *                     ┼
     *            ChildOutputProperty
     */

    /**
     * execute.
     */
    //    @Override
    public void execute1() {
        // Do init logic of root plan/groupExpr of `subplan`, only run once per task.
        if (curChildIndex == -1) {
            curTotalCost = 0;

            // Get property from groupExpression plan (it's root of subplan).
            RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(context);
            propertiesListList = requestPropertyDeriver.getRequiredPropertyListList(groupExpression);

            curChildIndex = 0;
        }

        for (; curPropertyPairIndex < propertiesListList.size(); curPropertyPairIndex++) {
            // children input properties
            List<PhysicalProperties> childrenInputProperties = propertiesListList.get(curPropertyPairIndex);

            // Calculate cost of groupExpression and update total cost
            if (curChildIndex == 0 && prevChildIndex == -1) {
                curTotalCost += CostCalculator.calculateCost(groupExpression);
            }

            for (; curChildIndex < groupExpression.arity(); curChildIndex++) {
                PhysicalProperties childInputProperties = childrenInputProperties.get(curChildIndex);
                Group childGroup = groupExpression.child(curChildIndex);

                // Whether the child group was optimized for this childInputProperties according to
                // the result of returning.
                Optional<Pair<Double, GroupExpression>> lowestCostPlanOpt = childGroup.getLowestCostPlan(
                        childInputProperties);

                if (!lowestCostPlanOpt.isPresent()) {
                    // The child should be pruned due to cost prune.
                    if (prevChildIndex >= curChildIndex) {
                        break;
                    }

                    // This child isn't optimized, create new tasks to optimize it.
                    // Meaning that optimize recursively by derive tasks.
                    prevChildIndex = curChildIndex;
                    pushTask((CostAndEnforcerJob) clone());
                    double newCostUpperBound = context.getCostUpperBound() - curTotalCost;
                    JobContext jobContext = new JobContext(context.getPlannerContext(), childInputProperties,
                            newCostUpperBound);
                    pushTask(new OptimizeGroupJob(childGroup, jobContext));
                    return;
                }

                GroupExpression lowestCostExpr = lowestCostPlanOpt.get().second;

                PhysicalProperties childOutputProperty = lowestCostExpr.getPropertyFromMap(childInputProperties);
                // TODO: maybe need to record children lowestCostExpr
                childrenInputProperties.set(curChildIndex, childOutputProperty);

                // todo: check whether split agg broadcast row count limit.
                curTotalCost += lowestCostExpr.getLowestCostTable().get(childInputProperties).first;
                if (curTotalCost > context.getCostUpperBound()) {
                    break;
                }
            }

            // When we successfully optimize all child group, it's last child.
            if (curChildIndex == groupExpression.arity()) {
                // Not need to do pruning here because it has been done when we get the
                // best expr from the child group

                // TODO: it could update the cost.
                PhysicalProperties outputProperty = ChildOutputPropertyDeriver.getProperties(
                        context.getRequiredProperties(),
                        childrenOutputProperties, groupExpression);

                if (curTotalCost > context.getCostUpperBound()) {
                    break;
                }

                /* update current group statistics and re-compute costs. */
                if (groupExpression.children().stream().anyMatch(group -> group.getStatistics() != null)) {
                    return;
                }
                PlanContext planContext = new PlanContext(groupExpression);
                // TODO: calculate stats. ??????
                groupExpression.getOwnerGroup().setStatistics(planContext.getStatistics());

                enforce(outputProperty, childrenInputProperties);

                if (curTotalCost < context.getCostUpperBound()) {
                    context.setCostUpperBound(curTotalCost);
                }
            }

            // Reset child idx and total cost
            prevChildIndex = -1;
            curChildIndex = 0;
            curTotalCost = 0;
        }
    }

    private void enforce(PhysicalProperties outputProperty, List<PhysicalProperties> childrenInputProperties) {

        // groupExpression can satisfy its own output property
        putProperty(groupExpression, outputProperty, outputProperty, childrenInputProperties);
        // groupExpression can satisfy the ANY type output property
        putProperty(groupExpression, outputProperty, new PhysicalProperties(), childrenInputProperties);

        EnforceMissingPropertiesHelper enforceMissingPropertiesHelper = new EnforceMissingPropertiesHelper(context,
                groupExpression, curTotalCost);

        PhysicalProperties requestedProperties = context.getRequiredProperties();
        if (!outputProperty.meet(requestedProperties)) {
            Pair<PhysicalProperties, Double> pair = enforceMissingPropertiesHelper.enforceProperty(outputProperty,
                    requestedProperties);
            PhysicalProperties addEnforcedProperty = pair.first;
            curTotalCost = pair.second;

            // enforcedProperty is superset of requiredProperty
            if (!addEnforcedProperty.equals(requestedProperties)) {
                putProperty(groupExpression.getOwnerGroup().getBestExpression(addEnforcedProperty),
                        requestedProperties, requestedProperties, Lists.newArrayList(outputProperty));
            }
        } else {
            if (!outputProperty.equals(requestedProperties)) {
                putProperty(groupExpression, outputProperty, requestedProperties, childrenInputProperties);
            }
        }
    }

    private void putProperty(GroupExpression groupExpression,
            PhysicalProperties outputProperty,
            PhysicalProperties requiredProperty,
            List<PhysicalProperties> inputProperties) {
        if (groupExpression.updateLowestCostTable(requiredProperty, inputProperties, curTotalCost)) {
            // Each group expression need to record the outputProperty satisfy what requiredProperty,
            // because group expression can generate multi outputProperty. eg. Join may have shuffle local
            // and shuffle join two types outputProperty.
            groupExpression.putOutputPropertiesMap(outputProperty, requiredProperty);
        }
        this.groupExpression.getOwnerGroup().setBestPlan(groupExpression,
                curTotalCost, requiredProperty);
    }


    /**
     * Shallow clone (ignore clone propertiesListList and groupExpression).
     */
    @Override
    public Object clone() {
        CostAndEnforcerJob task;
        try {
            task = (CostAndEnforcerJob) super.clone();
        } catch (CloneNotSupportedException ignored) {
            ignored.printStackTrace();
            return null;
        }
        return task;
    }
}

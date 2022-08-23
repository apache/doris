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
import org.apache.doris.nereids.stats.StatsCalculator;

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
    // [ child item: [leftProperties, rightPropertie]]
    // [ [Properties {"", ANY}, Properties {"", BROADCAST}],
    //   [Properties {"", SHUFFLE_JOIN}, Properties {"", SHUFFLE_JOIN}]]
    private List<List<PhysicalProperties>> requestChildrenPropertyList;

    private List<GroupExpression> childrenBestGroupExprList;
    private final List<PhysicalProperties> childrenOutputProperty = Lists.newArrayList();

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
    @Override
    public void execute() {
        // Do init logic of root plan/groupExpr of `subplan`, only run once per task.
        if (curChildIndex == -1) {
            curTotalCost = 0;

            // Get property from groupExpression plan (it's root of subplan).
            RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(context);
            requestChildrenPropertyList = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

            curChildIndex = 0;
        }

        for (; curPropertyPairIndex < requestChildrenPropertyList.size(); curPropertyPairIndex++) {
            // children input properties
            List<PhysicalProperties> requestChildrenProperty = requestChildrenPropertyList.get(curPropertyPairIndex);

            // Calculate cost of groupExpression and update total cost
            if (curChildIndex == 0 && prevChildIndex == -1) {
                curTotalCost += CostCalculator.calculateCost(groupExpression);
            }

            // Handle all child plannode.
            for (; curChildIndex < groupExpression.arity(); curChildIndex++) {
                PhysicalProperties requestChildProperty = requestChildrenProperty.get(curChildIndex);
                Group childGroup = groupExpression.child(curChildIndex);

                // Whether the child group was optimized for this requestChildProperty according to
                // the result of returning.
                Optional<Pair<Double, GroupExpression>> lowestCostPlanOpt = childGroup.getLowestCostPlan(
                        requestChildProperty);

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
                    JobContext jobContext = new JobContext(context.getPlannerContext(), requestChildProperty,
                            newCostUpperBound);
                    pushTask(new OptimizeGroupJob(childGroup, jobContext));
                    return;
                }

                GroupExpression lowestCostExpr = lowestCostPlanOpt.get().second;

                PhysicalProperties childOutputProperty = lowestCostExpr.getPropertyFromMap(requestChildProperty);
                // add childOutputProperty of children into childrenOutputProperty
                childrenOutputProperty.add(childOutputProperty);
                requestChildrenProperty.set(curChildIndex, childOutputProperty);

                curTotalCost += lowestCostExpr.getLowestCostTable().get(requestChildProperty).first;
                if (curTotalCost > context.getCostUpperBound()) {
                    break;
                }
            }

            // When we successfully optimize all child group, it's last child.
            if (curChildIndex == groupExpression.arity()) {
                // Not need to do pruning here because it has been done when we get the
                // best expr from the child group

                ChildOutputPropertyDeriver childOutputPropertyDeriver = new ChildOutputPropertyDeriver(
                        context.getRequiredProperties(), childrenOutputProperty, curTotalCost);
                PhysicalProperties outputProperty = childOutputPropertyDeriver.getOutputProperties(groupExpression);
                curTotalCost = childOutputPropertyDeriver.getCurTotalCost();

                if (curTotalCost > context.getCostUpperBound()) {
                    break;
                }

                // update current group statistics and re-compute costs.
                if (groupExpression.children().stream().anyMatch(group -> group.getStatistics() == null)) {
                    return;
                }
                StatsCalculator.estimate(groupExpression);

                // record map { outputProperty -> outputProperty }, { ANY -> outputProperty },
                recordPropertyAndCost(groupExpression, outputProperty, outputProperty, requestChildrenProperty);
                recordPropertyAndCost(groupExpression, outputProperty, PhysicalProperties.ANY, requestChildrenProperty);

                enforce(outputProperty, requestChildrenProperty);

                if (curTotalCost < context.getCostUpperBound()) {
                    context.setCostUpperBound(curTotalCost);
                }
            }

            // Reset child idx and total cost
            childrenOutputProperty.clear();
            prevChildIndex = -1;
            curChildIndex = 0;
            curTotalCost = 0;
        }
    }

    private void enforce(PhysicalProperties outputProperty, List<PhysicalProperties> requestChildrenProperty) {
        EnforceMissingPropertiesHelper enforceMissingPropertiesHelper = new EnforceMissingPropertiesHelper(context,
                groupExpression, curTotalCost);

        PhysicalProperties requestedProperties = context.getRequiredProperties();
        if (!outputProperty.satisfy(requestedProperties)) {
            PhysicalProperties addEnforcedProperty = enforceMissingPropertiesHelper.enforceProperty(outputProperty,
                    requestedProperties);
            curTotalCost = enforceMissingPropertiesHelper.getCurTotalCost();

            // enforcedProperty is superset of requiredProperty
            if (!addEnforcedProperty.equals(requestedProperties)) {
                recordPropertyAndCost(groupExpression.getOwnerGroup().getBestPlan(addEnforcedProperty),
                        requestedProperties, requestedProperties, Lists.newArrayList(outputProperty));
            }
        } else {
            if (!outputProperty.equals(requestedProperties)) {
                recordPropertyAndCost(groupExpression, outputProperty, requestedProperties, requestChildrenProperty);
            }
        }
    }

    private void recordPropertyAndCost(GroupExpression groupExpression,
            PhysicalProperties outputProperty,
            PhysicalProperties requestProperty,
            List<PhysicalProperties> inputProperties) {
        if (groupExpression.updateLowestCostTable(requestProperty, inputProperties, curTotalCost)) {
            // Each group expression need to save { outputProperty --> requestProperty }
            groupExpression.putOutputPropertiesMap(outputProperty, requestProperty);
        }
        this.groupExpression.getOwnerGroup().setBestPlan(groupExpression,
                curTotalCost, requestProperty);
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

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
import org.apache.doris.nereids.properties.ChildrenPropertiesRegulator;
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

    // cost of current plan tree
    private double curTotalCost;
    // cost of current plan node
    private double curNodeCost;

    // List of request property to children
    // Example: Physical Hash Join
    // [ child item: [leftProperties, rightPropertie]]
    // [ [Properties {"", ANY}, Properties {"", BROADCAST}],
    //   [Properties {"", SHUFFLE_JOIN}, Properties {"", SHUFFLE_JOIN}]]
    private List<List<PhysicalProperties>> requestChildrenPropertiesList;
    // index of List<request property to children>
    private int requestPropertiesIndex = 0;

    private List<GroupExpression> lowestCostChildren = Lists.newArrayList();
    private final List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList();

    // current child index of travsing all children
    private int curChildIndex = -1;
    // child index in the last time of travsing all children
    private int prevChildIndex = -1;

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
            curNodeCost = 0;
            curTotalCost = 0;
            curChildIndex = 0;
            // List<request property to children>
            // [ child item: [leftProperties, rightPropertie]]
            // like :[ [Properties {"", ANY}, Properties {"", BROADCAST}],
            //         [Properties {"", SHUFFLE_JOIN}, Properties {"", SHUFFLE_JOIN}] ]
            RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(context);
            requestChildrenPropertiesList = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        }

        for (; requestPropertiesIndex < requestChildrenPropertiesList.size(); requestPropertiesIndex++) {
            // Get one from List<request property to children>
            // like: [ Properties {"", ANY}, Properties {"", BROADCAST} ],
            List<PhysicalProperties> requestChildrenProperties
                    = requestChildrenPropertiesList.get(requestPropertiesIndex);

            // Calculate cost
            if (curChildIndex == 0 && prevChildIndex == -1) {
                curNodeCost = CostCalculator.calculateCost(groupExpression);
                curTotalCost += curNodeCost;
            }

            // Handle all child plan node.
            for (; curChildIndex < groupExpression.arity(); curChildIndex++) {
                PhysicalProperties requestChildProperty = requestChildrenProperties.get(curChildIndex);
                Group childGroup = groupExpression.child(curChildIndex);

                // Whether the child group was optimized for this requestChildProperty according to
                // the result of returning.
                Optional<Pair<Double, GroupExpression>> lowestCostPlanOpt
                        = childGroup.getLowestCostPlan(requestChildProperty);

                if (!lowestCostPlanOpt.isPresent()) {
                    // prevChildIndex >= curChildIndex mean that it is the second time we come here.
                    // So, we cannot get the lowest cost plan from current requested children properties.
                    // This is mean we should prune the current set of child due to cost prune.
                    if (prevChildIndex >= curChildIndex) {
                        break;
                    }

                    // This child isn't optimized, create new job to optimize it.
                    // Meaning that optimize recursively by derive job.
                    prevChildIndex = curChildIndex;
                    pushJob(clone());
                    double newCostUpperBound = context.getCostUpperBound() - curTotalCost;
                    JobContext jobContext = new JobContext(context.getCascadesContext(),
                            requestChildProperty, newCostUpperBound);
                    pushJob(new OptimizeGroupJob(childGroup, jobContext));
                    return;
                }

                GroupExpression lowestCostExpr = lowestCostPlanOpt.get().second;
                lowestCostChildren.add(lowestCostExpr);
                PhysicalProperties outputProperties = lowestCostExpr.getOutputProperties(requestChildProperty);
                childrenOutputProperties.add(outputProperties);
                requestChildrenProperties.set(curChildIndex, outputProperties);

                curTotalCost += lowestCostExpr.getLowestCostTable().get(requestChildProperty).first;
                if (curTotalCost > context.getCostUpperBound()) {
                    break;
                }
            }

            // This mean that we successfully optimize all child groups.
            if (curChildIndex == groupExpression.arity()) {

                // to ensure distributionSpec has been added sufficiently.
                ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(
                        groupExpression, lowestCostChildren, childrenOutputProperties, context);
                double enforceCost = regulator.adjustChildrenProperties();
                curTotalCost += enforceCost;

                // Not need to do pruning here because it has been done when we get the
                // best expr from the child group
                ChildOutputPropertyDeriver childOutputPropertyDeriver
                        = new ChildOutputPropertyDeriver(childrenOutputProperties);
                PhysicalProperties outputProperty = childOutputPropertyDeriver.getOutputProperties(groupExpression);

                // update current group statistics and re-compute costs.
                if (groupExpression.children().stream().anyMatch(group -> group.getStatistics() == null)) {
                    // if we come here, mean that we have some error in stats calculator and should fix it.
                    return;
                }
                StatsCalculator.estimate(groupExpression);

                curTotalCost -= curNodeCost;
                curNodeCost = CostCalculator.calculateCost(groupExpression);
                curTotalCost += curNodeCost;

                // colocate join and bucket shuffle join cannot add enforce to satisfy required property.
                if (enforce(outputProperty, requestChildrenProperties)) {

                    // record map { outputProperty -> outputProperty }, { ANY -> outputProperty },
                    recordPropertyAndCost(groupExpression, outputProperty, outputProperty, requestChildrenProperties);
                    recordPropertyAndCost(groupExpression, outputProperty, PhysicalProperties.ANY,
                            requestChildrenProperties);

                    if (curTotalCost < context.getCostUpperBound()) {
                        context.setCostUpperBound(curTotalCost);
                    }
                }
            }

            clear();
        }
    }

    private boolean enforce(PhysicalProperties outputProperty, List<PhysicalProperties> requestChildrenProperty) {
        PhysicalProperties requiredProperties = context.getRequiredProperties();

        EnforceMissingPropertiesHelper enforceMissingPropertiesHelper
                = new EnforceMissingPropertiesHelper(context, groupExpression, curTotalCost);
        if (!outputProperty.satisfy(requiredProperties)) {
            PhysicalProperties addEnforcedProperty = enforceMissingPropertiesHelper
                    .enforceProperty(outputProperty, requiredProperties);
            curTotalCost = enforceMissingPropertiesHelper.getCurTotalCost();

            // enforcedProperty is superset of requiredProperty
            if (!addEnforcedProperty.equals(requiredProperties)) {
                recordPropertyAndCost(groupExpression.getOwnerGroup().getBestPlan(addEnforcedProperty),
                        addEnforcedProperty, requiredProperties, Lists.newArrayList(outputProperty));
            }
        } else {
            if (!outputProperty.equals(requiredProperties)) {
                recordPropertyAndCost(groupExpression, outputProperty, requiredProperties, requestChildrenProperty);
            }
        }

        return true;
    }

    private void recordPropertyAndCost(GroupExpression groupExpression,
            PhysicalProperties outputProperty,
            PhysicalProperties requestProperty,
            List<PhysicalProperties> inputProperties) {
        if (groupExpression.updateLowestCostTable(requestProperty, inputProperties, curTotalCost, false)) {
            // Each group expression need to save { outputProperty --> requestProperty }
            groupExpression.putOutputPropertiesMap(outputProperty, requestProperty);
        }
        this.groupExpression.getOwnerGroup().setBestPlan(groupExpression, curTotalCost, requestProperty, false);
    }

    private void clear() {
        childrenOutputProperties.clear();
        lowestCostChildren.clear();
        prevChildIndex = -1;
        curChildIndex = 0;
        curTotalCost = 0;
        curNodeCost = 0;
    }

    /**
     * Shallow clone (ignore clone propertiesListList and groupExpression).
     */
    @Override
    public CostAndEnforcerJob clone() {
        CostAndEnforcerJob job;
        try {
            // TODO: need to implement this method
            job = (CostAndEnforcerJob) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            throw new RuntimeException("clone cost and enforcer job failed.");
        }
        return job;
    }
}

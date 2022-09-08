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
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
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
    private List<List<PhysicalProperties>> requestChildrenPropertyList;
    // index of List<request property to children>
    private int requestPropertyIndex = 0;

    private List<GroupExpression> childrenBestGroupExprList = Lists.newArrayList();
    private final List<PhysicalProperties> childrenOutputProperty = Lists.newArrayList();

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
            requestChildrenPropertyList = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        }

        for (; requestPropertyIndex < requestChildrenPropertyList.size(); requestPropertyIndex++) {
            // Get one from List<request property to children>
            // like: [ Properties {"", ANY}, Properties {"", BROADCAST} ],
            List<PhysicalProperties> requestChildrenProperty = requestChildrenPropertyList.get(requestPropertyIndex);

            // Calculate cost
            if (curChildIndex == 0 && prevChildIndex == -1) {
                curNodeCost = CostCalculator.calculateCost(groupExpression);
                curTotalCost += curNodeCost;
            }

            // Handle all child plan node.
            for (; curChildIndex < groupExpression.arity(); curChildIndex++) {
                PhysicalProperties requestChildProperty = requestChildrenProperty.get(curChildIndex);
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

                PhysicalProperties outputProperties = lowestCostExpr.getOutputProperties(requestChildProperty);
                // add outputProperties of children into childrenOutputProperty
                childrenOutputProperty.add(outputProperties);
                requestChildrenProperty.set(curChildIndex, outputProperties);

                curTotalCost += lowestCostExpr.getLowestCostTable().get(requestChildProperty).first;
                if (curTotalCost > context.getCostUpperBound()) {
                    break;
                }
            }

            // This mean that we successfully optimize all child groups.
            if (curChildIndex == groupExpression.arity()) {
                // Not need to do pruning here because it has been done when we get the
                // best expr from the child group
                ChildOutputPropertyDeriver childOutputPropertyDeriver
                        = new ChildOutputPropertyDeriver(childrenOutputProperty);
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
                if (enforce(outputProperty, requestChildrenProperty)) {

                    // record map { outputProperty -> outputProperty }, { ANY -> outputProperty },
                    recordPropertyAndCost(groupExpression, outputProperty, outputProperty, requestChildrenProperty);
                    recordPropertyAndCost(groupExpression, outputProperty, PhysicalProperties.ANY,
                            requestChildrenProperty);

                    if (curTotalCost < context.getCostUpperBound()) {
                        context.setCostUpperBound(curTotalCost);
                    }
                }
            }

            clear();
        }
    }

    private boolean enforce(PhysicalProperties outputProperty, List<PhysicalProperties> requestChildrenProperty) {
        EnforceMissingPropertiesHelper enforceMissingPropertiesHelper
                = new EnforceMissingPropertiesHelper(context, groupExpression, curTotalCost);
        PhysicalProperties requestedProperties = context.getRequiredProperties();

        DistributionSpec requiredDistribution = requestedProperties.getDistributionSpec();
        boolean mustEnforceDistribution = false;
        if (requiredDistribution instanceof DistributionSpecHash) {
            DistributionSpecHash hash = (DistributionSpecHash) requiredDistribution;
            if (!hash.getShuffleType().couldEnforced() && !outputProperty.getDistributionSpec().satisfy(hash)) {
                return false;
            }
            mustEnforceDistribution = hash.getShuffleType().mustEnforced();
        }

        if (!outputProperty.satisfy(requestedProperties) || mustEnforceDistribution) {
            PhysicalProperties addEnforcedProperty = enforceMissingPropertiesHelper
                    .enforceProperty(outputProperty, requestedProperties, mustEnforceDistribution);
            curTotalCost = enforceMissingPropertiesHelper.getCurTotalCost();

            // enforcedProperty is superset of requiredProperty
            if (!addEnforcedProperty.equals(requestedProperties) || mustEnforceDistribution) {
                recordPropertyAndCost(groupExpression.getOwnerGroup().getBestPlan(addEnforcedProperty),
                        requestedProperties, requestedProperties, Lists.newArrayList(outputProperty));
            }
        } else {
            if (!outputProperty.equals(requestedProperties)) {
                recordPropertyAndCost(groupExpression, outputProperty, requestedProperties, requestChildrenProperty);
            }
        }

        return true;
    }

    private void recordPropertyAndCost(GroupExpression groupExpression,
            PhysicalProperties outputProperty,
            PhysicalProperties requestProperty,
            List<PhysicalProperties> inputProperties) {
        if (groupExpression.updateLowestCostTable(requestProperty, inputProperties, curTotalCost)) {
            // Each group expression need to save { outputProperty --> requestProperty }
            groupExpression.putOutputPropertiesMap(outputProperty, requestProperty);
        }
        this.groupExpression.getOwnerGroup().setBestPlan(groupExpression, curTotalCost, requestProperty);
    }

    private void clear() {
        childrenOutputProperty.clear();
        childrenBestGroupExprList.clear();
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

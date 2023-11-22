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
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.minidump.NereidsTracer;
import org.apache.doris.nereids.properties.ChildOutputPropertyDeriver;
import org.apache.doris.nereids.properties.ChildrenPropertiesRegulator;
import org.apache.doris.nereids.properties.EnforceMissingPropertiesHelper;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestPropertyDeriver;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Job to compute cost and add enforcer.
 * Inspired by NoisePage and ORCA-Paper.
 */
public class CostAndEnforcerJob extends Job implements Cloneable {

    private static final Logger LOG = LogManager.getLogger(CostAndEnforcerJob.class);

    // GroupExpression to optimize
    private final GroupExpression groupExpression;

    // cost of current plan tree
    private Cost curTotalCost;
    // cost of current plan node
    private Cost curNodeCost;

    // List of request property to children
    // Example: Physical Hash Join
    // [ child item: [leftProperties, rightProperties]]
    // [ [Properties {"", ANY}, Properties {"", BROADCAST}],
    //   [Properties {"", SHUFFLE_JOIN}, Properties {"", SHUFFLE_JOIN}]]
    private List<List<PhysicalProperties>> requestChildrenPropertiesList;
    private final List<List<PhysicalProperties>> outputChildrenPropertiesList = new ArrayList<>();

    // index of List<request property to children>
    private int requestPropertiesIndex = 0;

    private final List<GroupExpression> lowestCostChildren = Lists.newArrayList();

    // current child index of traversing all children
    private int curChildIndex = -1;
    // child index in the last time of traversing all children
    private int prevChildIndex = -1;

    public CostAndEnforcerJob(GroupExpression groupExpression, JobContext context) {
        super(JobType.OPTIMIZE_CHILDREN, context);
        this.groupExpression = groupExpression;
    }

    private ConnectContext getConnectContext() {
        return context.getCascadesContext().getConnectContext();
    }

    private SessionVariable getSessionVariable() {
        return context.getCascadesContext().getConnectContext().getSessionVariable();
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
        if (groupExpression.isUnused()) {
            return;
        }

        SessionVariable sessionVariable = getSessionVariable();

        countJobExecutionTimesOfGroupExpressions(groupExpression);
        // Do init logic of root plan/groupExpr of `subplan`, only run once per task.
        if (curChildIndex == -1) {
            curNodeCost = Cost.zero(sessionVariable);
            curTotalCost = Cost.zero(sessionVariable);
            curChildIndex = 0;
            // List<request property to children>
            // [ child item: [leftProperties, rightProperties]]
            // like :[ [Properties {"", ANY}, Properties {"", BROADCAST}],
            //         [Properties {"", SHUFFLE_JOIN}, Properties {"", SHUFFLE_JOIN}] ]
            RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(getConnectContext(), context);
            requestChildrenPropertiesList = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
            for (List<PhysicalProperties> requestChildrenProperties : requestChildrenPropertiesList) {
                outputChildrenPropertiesList.add(new ArrayList<>(requestChildrenProperties));
            }
        }

        for (; requestPropertiesIndex < requestChildrenPropertiesList.size(); requestPropertiesIndex++) {
            // Get one from List<request property to children>
            // like: [ Properties {"", ANY}, Properties {"", BROADCAST} ],
            List<PhysicalProperties> requestChildrenProperties
                    = requestChildrenPropertiesList.get(requestPropertiesIndex);
            List<PhysicalProperties> outputChildrenProperties
                    = outputChildrenPropertiesList.get(requestPropertiesIndex);
            // Calculate cost
            if (curChildIndex == 0 && prevChildIndex == -1) {
                curNodeCost = CostCalculator.calculateCost(getConnectContext(), groupExpression,
                        requestChildrenProperties);
                groupExpression.setCost(curNodeCost);
                curTotalCost = curNodeCost;
            }

            // Handle all child plan node.
            for (; curChildIndex < groupExpression.arity(); curChildIndex++) {
                PhysicalProperties requestChildProperty = requestChildrenProperties.get(curChildIndex);
                Group childGroup = groupExpression.child(curChildIndex);

                // Whether the child group was optimized for this requestChildProperty according to
                // the result of returning.
                Optional<Pair<Cost, GroupExpression>> lowestCostPlanOpt
                        = childGroup.getLowestCostPlan(requestChildProperty);

                if (!lowestCostPlanOpt.isPresent()) {
                    // prevChildIndex >= curChildIndex mean that it is the second time we come here.
                    // So, we cannot get the lowest cost plan from current requested children properties.
                    // This is mean we should prune the current set of child due to cost prune.
                    if (prevChildIndex >= curChildIndex) {
                        // if run here, means that the child group will not generate the lowest cost plan map currently.
                        // and lowest cost children's size will not be equals to arity().
                        break;
                    }

                    // This child isn't optimized, create new job to optimize it.
                    // Meaning that optimize recursively by derive job.
                    prevChildIndex = curChildIndex;
                    pushJob(clone());
                    double newCostUpperBound = context.getCostUpperBound() - curTotalCost.getValue();
                    JobContext jobContext = new JobContext(context.getCascadesContext(),
                            requestChildProperty, newCostUpperBound);
                    pushJob(new OptimizeGroupJob(childGroup, jobContext));
                    return;
                }

                // when the child group finish the optimizeGroupJob, the code will begin to run.
                GroupExpression lowestCostExpr = lowestCostPlanOpt.get().second;
                lowestCostChildren.add(lowestCostExpr);
                PhysicalProperties outputProperties = lowestCostExpr.getOutputProperties(requestChildProperty);

                // record child's outputProperties.this is safety because `childGroup.getLowestCostPlan(current
                // plan's requestChildProperty).getOutputProperties(current plan's requestChildProperty) == child
                // plan's outputProperties`, the outputProperties must satisfy the origin requestChildProperty
                outputChildrenProperties.set(curChildIndex, outputProperties);
                curTotalCost = CostCalculator.addChildCost(
                        getConnectContext(),
                        groupExpression.getPlan(),
                        curNodeCost,
                        lowestCostExpr.getCostValueByProperties(requestChildProperty),
                        curChildIndex);

                // Not performing lower bound group pruning here is to avoid redundant optimization of children.
                // For example:
                //      Group1 : betterExpr, currentExpr(child: Group2), otherExpr(child: Group)
                //      steps
                //          1. CostAndEnforce(currentExpr) with upperBound betterExpr.cost
                //          2. OptimizeGroup(Group2) with upperBound bestExpr.cost - currentExpr.nodeCost
                //          3. CostAndEnforce(Expr in Group2) trigger here and exit
                //              ...
                //          n.  CostAndEnforce(otherExpr) can trigger optimize group2 again for the same requireProp

                // the request child properties will be covered by the output properties
                // that corresponding to the request properties. so if we run a costAndEnforceJob of the same
                // group expression, that request child properties will be different of this.
            }

            // This mean that we successfully optimize all child groups.
            // if break when running the loop above, the condition must be false.
            if (curChildIndex == groupExpression.arity()) {
                if (!calculateEnforce(requestChildrenProperties, outputChildrenProperties)) {
                    return; // if error exists, return
                }
                if (curTotalCost.getValue() < context.getCostUpperBound()) {
                    context.setCostUpperBound(curTotalCost.getValue());
                }
            }
            clear();
        }
    }

    /**
     * calculate enforce
     *
     * @return false if error occurs, the caller will return.
     */
    private boolean calculateEnforce(List<PhysicalProperties> requestChildrenProperties,
            List<PhysicalProperties> outputChildrenProperties) {
        // to ensure distributionSpec has been added sufficiently.
        // it's certain that lowestCostChildren is equals to arity().
        ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(groupExpression,
                lowestCostChildren, outputChildrenProperties, requestChildrenProperties, context);
        boolean success = regulator.adjustChildrenProperties();
        if (!success) {
            // invalid enforce, return.
            return false;
        }

        // Not need to do pruning here because it has been done when we get the
        // best expr from the child group
        ChildOutputPropertyDeriver childOutputPropertyDeriver
                = new ChildOutputPropertyDeriver(outputChildrenProperties);
        // the physical properties the group expression support for its parent.
        PhysicalProperties outputProperty = childOutputPropertyDeriver.getOutputProperties(getConnectContext(),
                groupExpression);

        // update current group statistics and re-compute costs.
        if (groupExpression.children().stream().anyMatch(group -> group.getStatistics() == null)
                && groupExpression.getOwnerGroup().getStatistics() == null) {
            // if we come here, mean that we have some error in stats calculator and should fix it.
            LOG.warn("Nereids try to calculate cost without stats for group expression {}", groupExpression);
            return false;
        }

        // recompute cost after adjusting property
        curNodeCost = CostCalculator.calculateCost(getConnectContext(), groupExpression, requestChildrenProperties);
        groupExpression.setCost(curNodeCost);
        curTotalCost = curNodeCost;
        for (int i = 0; i < outputChildrenProperties.size(); i++) {
            PhysicalProperties childProperties = outputChildrenProperties.get(i);
            curTotalCost = CostCalculator.addChildCost(
                    getConnectContext(),
                    groupExpression.getPlan(),
                    curTotalCost,
                    groupExpression.child(i).getLowestCostPlan(childProperties).get().first,
                    i);
        }

        // record map { outputProperty -> outputProperty }, { ANY -> outputProperty },
        recordPropertyAndCost(groupExpression, outputProperty, PhysicalProperties.ANY, outputChildrenProperties);
        recordPropertyAndCost(groupExpression, outputProperty, outputProperty, outputChildrenProperties);
        enforce(outputProperty, outputChildrenProperties);
        return true;
    }

    /**
     * add enforce node
     *
     * @param outputProperty the group expression's out property
     * @param outputChildrenProperty the children's output properties of this group expression.
     */
    private void enforce(PhysicalProperties outputProperty, List<PhysicalProperties> outputChildrenProperty) {
        PhysicalProperties requiredProperties = context.getRequiredProperties();
        if (outputProperty.satisfy(requiredProperties)) {
            if (!outputProperty.equals(requiredProperties)) {
                recordPropertyAndCost(groupExpression, outputProperty, requiredProperties, outputChildrenProperty);
            }
            return;
        }

        if (context.getRequiredProperties().isDistributionOnlyProperties()) {
            // For properties without an orderSpec, enforceMissingPropertiesHelper always adds a distributor
            // above this group expression. The cost of the distributor is equal to the cost of the groupExpression
            // plus the cost of the distributor. The distributor remains unchanged for different groupExpressions.
            // Therefore, if there is a better groupExpr, it is preferable to enforce the better groupExpr.
            // Consequently, we can avoid this enforcement.
            Optional<Pair<Cost, GroupExpression>> bestExpr = groupExpression.getOwnerGroup()
                    .getLowestCostPlan(context.getRequiredProperties());
            double bestCost = bestExpr
                    .map(costGroupExpressionPair -> costGroupExpressionPair.first.getValue())
                    .orElse(Double.POSITIVE_INFINITY);
            if (curTotalCost.getValue() > bestCost) {
                return;
            }
        }

        EnforceMissingPropertiesHelper enforceMissingPropertiesHelper
                = new EnforceMissingPropertiesHelper(getConnectContext(), groupExpression, curTotalCost);
        PhysicalProperties addEnforcedProperty = enforceMissingPropertiesHelper
                .enforceProperty(outputProperty, requiredProperties);
        curTotalCost = enforceMissingPropertiesHelper.getCurTotalCost();

        // enforcedProperty is superset of requiredProperty
        if (!addEnforcedProperty.equals(requiredProperties)) {
            recordPropertyAndCost(groupExpression.getOwnerGroup().getBestPlan(addEnforcedProperty),
                    addEnforcedProperty, requiredProperties, Lists.newArrayList(outputProperty));
        }
    }

    /**
     * record property and cost
     *
     * @param groupExpression the target group expression
     * @param outputProperty the child output physical corresponding to the required property of the group expression.
     * @param requestProperty mentioned above
     * @param inputProperties request children output properties.
     */
    private void recordPropertyAndCost(GroupExpression groupExpression,
            PhysicalProperties outputProperty,
            PhysicalProperties requestProperty,
            List<PhysicalProperties> inputProperties) {
        if (groupExpression.updateLowestCostTable(requestProperty, inputProperties, curTotalCost)) {
            // Each group expression need to save { outputProperty --> requestProperty }
            groupExpression.putOutputPropertiesMap(outputProperty, requestProperty);
        }
        this.groupExpression.getOwnerGroup().setBestPlan(groupExpression, curTotalCost, requestProperty);
        NereidsTracer.logPropertyAndCostEvent(groupExpression.getOwnerGroup().getGroupId(),
                groupExpression.children(), groupExpression.getPlan(), requestProperty, curTotalCost);
    }

    private void clear() {
        lowestCostChildren.clear();
        prevChildIndex = -1;
        curChildIndex = 0;
        curTotalCost = Cost.zero(getSessionVariable());
        curNodeCost = Cost.zero(getSessionVariable());
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
            LOG.warn("", e);
            throw new RuntimeException("clone cost and enforcer job failed.");
        }
        return job;
    }
}

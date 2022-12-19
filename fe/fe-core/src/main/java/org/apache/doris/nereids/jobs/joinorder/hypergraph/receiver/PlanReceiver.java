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

package org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.ChildOutputPropertyDeriver;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestPropertyDeriver;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The Receiver is used for cached the plan that has been emitted and build the new plan
 */
public class PlanReceiver implements AbstractReceiver {
    // limit define the max number of csg-cmp pair in this Receiver
    HashMap<Long, Group> planTable = new HashMap<>();
    int limit;
    int emitCount = 0;

    JobContext jobContext;

    public PlanReceiver() {
        throw new RuntimeException("");
    }

    public PlanReceiver(int limit) {
        throw new RuntimeException("");
    }

    public PlanReceiver(JobContext jobContext, int limit) {
        this.jobContext = jobContext;
        this.limit = limit;
    }


    /**
     * Emit a new plan from bottom to top
     *
     * @param left the bitmap of left child tree
     * @param right the bitmap of the right child tree
     * @param edges the join conditions that can be added in this operator
     * @return the left and the right can be connected by the edge
     */
    @Override
    public boolean emitCsgCmp(long left, long right, List<Edge> edges,
            HashMap<Long, NamedExpression> projectExpression) {
        Preconditions.checkArgument(planTable.containsKey(left));
        Preconditions.checkArgument(planTable.containsKey(right));
        Memo memo = jobContext.getCascadesContext().getMemo();
        emitCount += 1;
        if (emitCount > limit) {
            return false;
        }

        GroupPlan leftPlan = new GroupPlan(planTable.get(left));
        GroupPlan rightPlan = new GroupPlan(planTable.get(right));

        // First, we implement all possible physical group expression and copy them in memo.
        // In this step, we don't generate logical expression because they are useless in DPhy.
        // In fact, we will do that after choosing the best physical plan for the cascades optimizer
        List<Expression> hashConjuncts = new ArrayList<>();
        List<Expression> otherConjuncts = new ArrayList<>();
        JoinType joinType = extractJoinTypeAndConjuncts(edges, hashConjuncts, otherConjuncts);

        LogicalProperties logicalProperties = new LogicalProperties(
                () -> JoinUtils.getJoinOutput(joinType, leftPlan, rightPlan));
        long fullKey = LongBitmap.newBitmapUnion(left, right);
        if (!planTable.containsKey(fullKey)) {
            planTable.put(fullKey, memo.newGroup(logicalProperties));
        }
        Group group = planTable.get(fullKey);

        List<Plan> physicalJoins = proposePhysicalJoin(joinType, leftPlan, rightPlan, hashConjuncts, otherConjuncts,
                logicalProperties);
        List<GroupExpression> physicalExpressions = new ArrayList<>();
        for (Plan plan : physicalJoins) {
            CopyInResult copyInResult = memo.copyIn(plan, group, false);
            physicalExpressions.add(copyInResult.correspondingExpression);
        }

        // Then we generate all possible properties for distributed physical plan. In this step, we calculate
        // cost and cache the winner for all possible properties.
        for (GroupExpression groupExpression : physicalExpressions) {
            List<PhysicalProperties> propertiesList = proposeDistributedJoin(groupExpression);
            for (PhysicalProperties physicalProperties : propertiesList) {
                group.setBestPlan(groupExpression, groupExpression.getCostByProperties(physicalProperties),
                        physicalProperties);
            }
        }

        //Finally, we propose the project expression for this group
        LogicalProject project = proposePhysicalProject(projectExpression, fullKey, new GroupPlan(group));
        if (!project.getProjects().isEmpty()) {
            memo.copyIn(project, null, false);
        }

        return true;
    }

    private List<PhysicalProperties> proposeDistributedJoin(GroupExpression groupExpression) {
        if (!groupExpression.isStatDerived()) {
            jobContext.getCascadesContext().pushJob(new DeriveStatsJob(groupExpression,
                    jobContext.getCascadesContext().getCurrentJobContext()));
            jobContext.getCascadesContext().getJobScheduler().executeJobPool(jobContext.getCascadesContext());
        }
        List<PhysicalProperties> outputProperties = new ArrayList<>();
        //TODO: consider more general property generated
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(this.jobContext);
        List<List<PhysicalProperties>> requestChildrenPropertiesList
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        // TODO: consider pruning
        double cost = 0;
        for (List<PhysicalProperties> childrenProperties : requestChildrenPropertiesList) {
            PhysicalProperties leftProperties = childrenProperties.get(0);
            Group leftGroup = groupExpression.child(0);
            cost += costAndEnforce(leftGroup, leftProperties);

            PhysicalProperties rightProperties = childrenProperties.get(1);
            Group rightGroup = groupExpression.child(1);
            cost += costAndEnforce(rightGroup, rightProperties);

            cost += CostCalculator.calculateCost(groupExpression);
            ChildOutputPropertyDeriver childOutputPropertyDeriver
                    = new ChildOutputPropertyDeriver(childrenProperties);
            PhysicalProperties outputProperty = childOutputPropertyDeriver.getOutputProperties(groupExpression);
            if (groupExpression.updateLowestCostTable(jobContext.getRequiredProperties(), childrenProperties, cost)) {
                groupExpression.putOutputPropertiesMap(outputProperty, outputProperty);
                groupExpression.putOutputPropertiesMap(outputProperty, PhysicalProperties.ANY);
            }
        }
        return outputProperties;
    }

    private double costAndEnforce(Group group, PhysicalProperties requireProperty) {
        Optional<Pair<Double, GroupExpression>> lowestCostPlanOpt
                = group.getLowestCostPlan(requireProperty);
        if (!lowestCostPlanOpt.isPresent()) {
            jobContext.getCascadesContext().pushJob(new OptimizeGroupJob(group,
                    new JobContext(jobContext.getCascadesContext(), requireProperty, Double.MAX_VALUE)));
            jobContext.getCascadesContext().getJobScheduler().executeJobPool(jobContext.getCascadesContext());
            lowestCostPlanOpt = group.getLowestCostPlan(requireProperty);
        }
        return lowestCostPlanOpt.get().first;
    }

    private List<Plan> proposePhysicalJoin(JoinType joinType, Plan left, Plan right, List<Expression> hashConjuncts,
            List<Expression> otherConjuncts, LogicalProperties logicalProperties) {
        // Check whether only NSL can be performed
        if ((joinType.isInnerJoin() && hashConjuncts.isEmpty()) || joinType.isCrossJoin()) {
            return Lists.newArrayList(
                    new PhysicalNestedLoopJoin<>(joinType, hashConjuncts, otherConjuncts, logicalProperties, left,
                            right),
                    new PhysicalNestedLoopJoin<>(joinType.swap(), hashConjuncts, otherConjuncts, logicalProperties,
                            right, left));
        }
        return Lists.newArrayList(
                new PhysicalHashJoin<>(joinType, hashConjuncts, otherConjuncts, logicalProperties, left, right),
                new PhysicalHashJoin<>(joinType.swap(), hashConjuncts, otherConjuncts, logicalProperties, right, left));
    }

    private JoinType extractJoinTypeAndConjuncts(List<Edge> edges, List<Expression> hashConjuncts,
            List<Expression> otherConjuncts) {
        JoinType joinType = null;
        for (Edge edge : edges) {
            Preconditions.checkArgument(joinType == null || joinType == edge.getJoinType());
            joinType = edge.getJoinType();
            Expression expression = edge.getExpression();
            if (expression instanceof EqualTo) {
                hashConjuncts.add(edge.getExpression());
            } else {
                otherConjuncts.add(expression);
            }
        }
        return joinType;
    }

    @Override
    public void addGroup(long bitmap, Group group) {
        planTable.put(bitmap, group);
    }

    @Override
    public boolean contain(long bitmap) {
        return planTable.containsKey(bitmap);
    }

    @Override
    public void reset() {
        planTable.clear();
        emitCount = 0;
    }

    @Override
    public Group getBestPlan(long bitmap) {
        Preconditions.checkArgument(planTable.containsKey(bitmap));
        Group root = planTable.get(bitmap);
        makeLogicalExpression(root);
        return root;
    }

    private void makeLogicalExpression(Group root) {
        if (!root.getLogicalExpressions().isEmpty()) {
            return;
        }
        Plan physicalPlan = root.getPhysicalExpressions().get(0).getPlan();
        for (GroupExpression groupExpression : root.getPhysicalExpressions()) {
            for (Group child : groupExpression.children()) {
                makeLogicalExpression(child);
            }
        }
        Plan logicalPlan;
        if (physicalPlan instanceof LogicalProject) {
            PhysicalProject physicalProject = (PhysicalProject) physicalPlan;
            logicalPlan = new LogicalProject<>(physicalProject.getProjects(),
                    physicalProject.child(0));
        } else if (physicalPlan instanceof PhysicalHashJoin) {
            PhysicalHashJoin physicalJoin = (PhysicalHashJoin) physicalPlan;
            logicalPlan = new LogicalJoin<>(physicalJoin.getJoinType(), physicalJoin.getHashJoinConjuncts(),
                    physicalJoin.getOtherJoinConjuncts(), physicalJoin.child(0), physicalJoin.child(1));
        } else if (physicalPlan instanceof PhysicalNestedLoopJoin) {
            PhysicalNestedLoopJoin physicalJoin = (PhysicalNestedLoopJoin) physicalPlan;
            logicalPlan = new LogicalJoin<>(physicalJoin.getJoinType(), physicalJoin.getHashJoinConjuncts(),
                    physicalJoin.getOtherJoinConjuncts(), physicalJoin.child(0), physicalJoin.child(1));
        } else {
            throw new RuntimeException("DPhyp can only handle join and project operator");
        }
        jobContext.getCascadesContext().getMemo().copyIn(logicalPlan, root, false);
    }

    private LogicalProject proposePhysicalProject(HashMap<Long, NamedExpression> projectExpression, long fullKey,
            Plan child) {
        List<Long> bitmaps = projectExpression.keySet().stream().filter(bitmap -> LongBitmap.isSubset(bitmap, fullKey))
                .collect(Collectors.toList());
        List<NamedExpression> projects = new ArrayList<>();
        for (long bitmap : bitmaps) {
            projects.add(projectExpression.get(bitmap));
            projectExpression.remove(bitmap);
        }

        return new LogicalProject(projects, child);
    }
}


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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.cascades.CostAndEnforcerJob;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The Receiver is used for cached the plan that has been emitted and build the new plan
 */
public class PlanReceiver implements AbstractReceiver {
    // limit define the max number of csg-cmp pair in this Receiver
    HashMap<Long, Group> planTable = new HashMap<>();
    HashMap<Long, BitSet> usdEdges = new HashMap<>();
    int limit;
    int emitCount = 0;

    JobContext jobContext;

    HyperGraph hyperGraph;
    final Set<Slot> finalOutputs;

    public PlanReceiver() {
        throw new RuntimeException("");
    }

    public PlanReceiver(int limit) {
        throw new RuntimeException("");
    }

    public PlanReceiver(JobContext jobContext, int limit, HyperGraph hyperGraph, Set<Slot> outputs) {
        this.jobContext = jobContext;
        this.limit = limit;
        this.hyperGraph = hyperGraph;
        this.finalOutputs = outputs;
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
    public boolean emitCsgCmp(long left, long right, List<Edge> edges) {
        Preconditions.checkArgument(planTable.containsKey(left));
        Preconditions.checkArgument(planTable.containsKey(right));

        Memo memo = jobContext.getCascadesContext().getMemo();
        emitCount += 1;
        if (emitCount > limit) {
            return false;
        }

        GroupPlan leftPlan = new GroupPlan(planTable.get(left));
        GroupPlan rightPlan = new GroupPlan(planTable.get(right));

        // First, we implement all possible physical plans
        // In this step, we don't generate logical expression because they are useless in DPhyp.
        List<Expression> hashConjuncts = new ArrayList<>();
        List<Expression> otherConjuncts = new ArrayList<>();
        JoinType joinType = extractJoinTypeAndConjuncts(edges, hashConjuncts, otherConjuncts);
        long fullKey = LongBitmap.newBitmapUnion(left, right);

        List<Plan> physicalJoins = proposeAllPhysicalJoins(joinType, leftPlan, rightPlan, hashConjuncts,
                otherConjuncts);
        List<Plan> physicalPlans = proposeProject(physicalJoins, edges, left, right);

        // Second, we copy all physical plan to Group and generate properties and calculate cost
        if (!planTable.containsKey(fullKey)) {
            planTable.put(fullKey, memo.newGroup(physicalPlans.get(0).getLogicalProperties()));
        }
        Group group = planTable.get(fullKey);
        for (Plan plan : physicalPlans) {
            CopyInResult copyInResult = memo.copyIn(plan, group, false);
            GroupExpression physicalExpression = copyInResult.correspondingExpression;
            proposeAllDistributedPlans(physicalExpression);
        }

        return true;
    }

    private Set<Slot> calculateRequiredSlots(long left, long right, List<Edge> edges) {
        Set<Slot> outputSlots = new HashSet<>(this.finalOutputs);
        BitSet bitSet = new BitSet();
        bitSet.or(usdEdges.get(left));
        bitSet.or(usdEdges.get(right));
        for (Edge edge : edges) {
            bitSet.set(edge.getIndex());
        }
        // required output slots = final outputs + slot of unused edges
        usdEdges.put(LongBitmap.newBitmapUnion(left, right), bitSet);
        for (Edge edge : hyperGraph.getEdges()) {
            if (!bitSet.get(edge.getIndex())) {
                outputSlots.addAll(edge.getExpression().getInputSlots());
            }
        }
        return outputSlots;
    }

    private void proposeAllDistributedPlans(GroupExpression groupExpression) {
        jobContext.getCascadesContext().pushJob(new CostAndEnforcerJob(groupExpression,
                new JobContext(jobContext.getCascadesContext(), PhysicalProperties.ANY, Double.MAX_VALUE)));
        if (!groupExpression.isStatDerived()) {
            jobContext.getCascadesContext().pushJob(new DeriveStatsJob(groupExpression,
                    jobContext.getCascadesContext().getCurrentJobContext()));
        }
        jobContext.getCascadesContext().getJobScheduler().executeJobPool(jobContext.getCascadesContext());
    }

    private List<Plan> proposeAllPhysicalJoins(JoinType joinType, Plan left, Plan right, List<Expression> hashConjuncts,
            List<Expression> otherConjuncts) {
        // Check whether only NSL can be performed
        LogicalProperties joinProperties = new LogicalProperties(
                () -> JoinUtils.getJoinOutput(joinType, left, right));
        if (JoinUtils.shouldNestedLoopJoin(joinType, hashConjuncts)) {
            return Lists.newArrayList(
                    new PhysicalNestedLoopJoin<>(joinType, hashConjuncts, otherConjuncts, joinProperties, left,
                            right),
                    new PhysicalNestedLoopJoin<>(joinType.swap(), hashConjuncts, otherConjuncts, joinProperties,
                            right, left));
        } else {
            return Lists.newArrayList(
                    new PhysicalHashJoin<>(joinType, hashConjuncts, otherConjuncts, JoinHint.NONE, joinProperties,
                            left,
                            right),
                    new PhysicalHashJoin<>(joinType.swap(), hashConjuncts, otherConjuncts, JoinHint.NONE,
                            joinProperties,
                            right, left));
        }
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
        usdEdges.put(bitmap, new BitSet());
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
        // If there are some rules relied on the logical join, we need to make logical Expression
        // However, it cost 15% of total optimized time.
        makeLogicalExpression(root);
        return root;
    }

    private void makeLogicalExpression(Group root) {
        if (!root.getLogicalExpressions().isEmpty()) {
            return;
        }

        // only makeLogicalExpression for those winners
        Set<GroupExpression> hasGenerated = new HashSet<>();
        for (PhysicalProperties physicalProperties : root.getAllProperties()) {
            GroupExpression groupExpression = root.getBestPlan(physicalProperties);
            if (hasGenerated.contains(groupExpression) || groupExpression.getPlan() instanceof PhysicalDistribute) {
                continue;
            }
            hasGenerated.add(groupExpression);

            // process child first
            Plan physicalPlan = groupExpression.getPlan();
            for (Group child : groupExpression.children()) {
                makeLogicalExpression(child);
            }

            Plan logicalPlan;
            if (physicalPlan instanceof PhysicalProject) {
                PhysicalProject physicalProject = (PhysicalProject) physicalPlan;
                logicalPlan = new LogicalProject<>(physicalProject.getProjects(),
                        physicalProject.child(0));
            } else if (physicalPlan instanceof AbstractPhysicalJoin) {
                AbstractPhysicalJoin physicalJoin = (AbstractPhysicalJoin) physicalPlan;
                logicalPlan = new LogicalJoin<>(physicalJoin.getJoinType(), physicalJoin.getHashJoinConjuncts(),
                        physicalJoin.getOtherJoinConjuncts(), JoinHint.NONE, physicalJoin.child(0),
                        physicalJoin.child(1));
            } else {
                throw new RuntimeException("DPhyp can only handle join and project operator");
            }
            // shadow all join order rule
            CopyInResult copyInResult = jobContext.getCascadesContext().getMemo().copyIn(logicalPlan, root, false);
            for (Rule rule : jobContext.getCascadesContext().getRuleSet().getJoinOrderRule()) {
                copyInResult.correspondingExpression.setApplied(rule);
            }
            for (Rule rule : jobContext.getCascadesContext().getRuleSet().getImplementationRules()) {
                copyInResult.correspondingExpression.setApplied(rule);
            }
        }
    }

    private List<Plan> proposeProject(List<Plan> allChild, List<Edge> usedEdges, long left, long right) {
        List<Plan> res = new ArrayList<>();

        // calculate required columns
        Set<Slot> requireSlots = calculateRequiredSlots(left, right, usedEdges);
        List<Slot> outputs = allChild.get(0).getOutput();
        List<NamedExpression> projects = outputs.stream().filter(e -> requireSlots.contains(e)).collect(
                Collectors.toList());

        // Calculate complex expression
        long fullKey = LongBitmap.newBitmapUnion(left, right);
        Map<Long, NamedExpression> complexExpressionMap = hyperGraph.getComplexProject();
        List<Long> bitmaps = complexExpressionMap.keySet().stream()
                .filter(bitmap -> LongBitmap.isSubset(bitmap, fullKey)).collect(Collectors.toList());

        boolean addComplexProject = false;
        for (long bitmap : bitmaps) {
            projects.add(complexExpressionMap.get(bitmap));
            complexExpressionMap.remove(bitmap);
            addComplexProject = true;
        }

        // propose physical project
        if (projects.isEmpty()) {
            projects.add(ExpressionUtils.selectMinimumColumn(outputs));
        } else if (projects.size() == outputs.size() && !addComplexProject) {
            return allChild;
        }
        LogicalProperties projectProperties = new LogicalProperties(
                () -> projects.stream().map(p -> p.toSlot()).collect(Collectors.toList()));
        for (Plan child : allChild) {
            res.add(new PhysicalProject<>(projects, projectProperties, child));
        }
        return res;
    }
}


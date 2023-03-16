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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The Receiver is used for cached the plan that has been emitted and build the new plan
 */
public class PlanReceiver implements AbstractReceiver {
    // limit define the max number of csg-cmp pair in this Receiver
    HashMap<Long, Group> planTable = new HashMap<>();
    HashMap<Long, BitSet> usdEdges = new HashMap<>();
    HashMap<Long, List<NamedExpression>> projectsOnSubgraph = new HashMap<>();
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

        // check if the missed edges can be correctly connected by add it to edges
        // if not, the plan is invalid because of the missed edges, just return and seek for another valid plan
        if (!processMissedEdges(left, right, edges)) {
            return true;
        }

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
        BitSet usedEdgesBitmap = new BitSet();
        usedEdgesBitmap.or(usdEdges.get(left));
        usedEdgesBitmap.or(usdEdges.get(right));
        for (Edge edge : edges) {
            usedEdgesBitmap.set(edge.getIndex());
        }
        // required output slots = final outputs + slot of unused edges
        usdEdges.put(LongBitmap.newBitmapUnion(left, right), usedEdgesBitmap);
        for (Edge edge : hyperGraph.getEdges()) {
            if (!usedEdgesBitmap.get(edge.getIndex())) {
                outputSlots.addAll(edge.getInputSlots());
            }
        }
        hyperGraph.getComplexProject()
                .values()
                .stream()
                .flatMap(l -> l.stream())
                .forEach(expr -> outputSlots.addAll(expr.getInputSlots()));
        return outputSlots;
    }

    // check if the missed edges can be used to connect left and right together with edges
    // return true if no missed edge or the missed edge can be used to connect left and right
    // the returned edges includes missed edges if there is any.
    private boolean processMissedEdges(long left, long right, List<Edge> edges) {
        boolean canAddMisssedEdges = true;

        // find all reference nodes assume left and right sub graph is connected
        BitSet usedEdgesBitmap = new BitSet();
        usedEdgesBitmap.or(usdEdges.get(left));
        usedEdgesBitmap.or(usdEdges.get(right));
        edges.stream().forEach(edge -> usedEdgesBitmap.set(edge.getIndex()));
        long allReferenceNodes = getAllReferenceNodes(usedEdgesBitmap);

        // check all edges
        // the edge is a missed edge if the edge is not used and its reference nodes is a subset of allReferenceNodes
        for (Edge edge : hyperGraph.getEdges()) {
            if (LongBitmap.isSubset(edge.getReferenceNodes(), allReferenceNodes) && !usedEdgesBitmap.get(
                    edge.getIndex())) {
                // check the missed edge can be used to connect left and right together with edges
                // if the missed edge meet the 2 conditions, it is a valid edge
                // 1. the edge's left child's referenced nodes is subset of the left
                // 2. the edge's original right node is subset of right
                canAddMisssedEdges = canAddMisssedEdges && LongBitmap.isSubset(edge.getLeft(),
                        left) && LongBitmap.isSubset(edge.getOriginalRight(), right);

                // always add the missed edge to edges
                // because the caller will return immediately if canAddMisssedEdges is false
                edges.add(edge);
            }
        }
        return canAddMisssedEdges;
    }

    private long getAllReferenceNodes(BitSet edgesBitmap) {
        long nodes = LongBitmap.newBitmap();
        for (int i = edgesBitmap.nextSetBit(0); i >= 0; i = edgesBitmap.nextSetBit(i + 1)) {
            nodes = LongBitmap.or(nodes, hyperGraph.getEdge(i).getReferenceNodes());
        }
        return nodes;
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
                    new PhysicalNestedLoopJoin<>(joinType, hashConjuncts, otherConjuncts,
                            Optional.empty(), joinProperties,
                            left, right),
                    new PhysicalNestedLoopJoin<>(joinType.swap(), hashConjuncts, otherConjuncts, Optional.empty(),
                            joinProperties,
                            right, left));
        } else {
            return Lists.newArrayList(
                    new PhysicalHashJoin<>(joinType, hashConjuncts, otherConjuncts, JoinHint.NONE, Optional.empty(),
                            joinProperties,
                            left, right),
                    new PhysicalHashJoin<>(joinType.swap(), hashConjuncts, otherConjuncts, JoinHint.NONE,
                            Optional.empty(),
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
            for (Expression expression : edge.getExpressions()) {
                if (expression instanceof EqualTo) {
                    hashConjuncts.add(expression);
                } else {
                    otherConjuncts.add(expression);
                }
            }
        }
        return joinType;
    }

    private boolean extractIsMarkJoin(List<Edge> edges) {
        boolean isMarkJoin = false;
        JoinType joinType = null;
        for (Edge edge : edges) {
            Preconditions.checkArgument(joinType == null || joinType == edge.getJoinType());
            isMarkJoin = edge.getJoin().isMarkJoin() || isMarkJoin;
            joinType = edge.getJoinType();
        }
        return isMarkJoin;
    }

    @Override
    public void addGroup(long bitmap, Group group) {
        Preconditions.checkArgument(LongBitmap.getCardinality(bitmap) == 1);
        usdEdges.put(bitmap, new BitSet());
        Plan plan = proposeProject(Lists.newArrayList(new GroupPlan(group)), new ArrayList<>(), bitmap, bitmap).get(0);
        if (!(plan instanceof GroupPlan)) {
            CopyInResult copyInResult = jobContext.getCascadesContext().getMemo().copyIn(plan, null, false);
            group = copyInResult.correspondingExpression.getOwnerGroup();
        }
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
        projectsOnSubgraph.clear();
        usdEdges.clear();
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
                        physicalJoin.getOtherJoinConjuncts(), JoinHint.NONE, physicalJoin.getMarkJoinSlotReference(),
                        physicalJoin.child(0), physicalJoin.child(1));
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

    private List<Plan> proposeProject(List<Plan> allChild, List<Edge> edges, long left, long right) {
        long fullKey = LongBitmap.newBitmapUnion(left, right);
        List<Slot> outputs = allChild.get(0).getOutput();
        Set<Slot> outputSet = allChild.get(0).getOutputSet();
        if (!projectsOnSubgraph.containsKey(fullKey)) {
            List<NamedExpression> projects = new ArrayList<>();
            // Calculate complex expression
            Map<Long, List<NamedExpression>> complexExpressionMap = hyperGraph.getComplexProject();
            List<Long> bitmaps = complexExpressionMap.keySet().stream()
                    .filter(bitmap -> LongBitmap.isSubset(bitmap, fullKey)).collect(Collectors.toList());

            for (long bitmap : bitmaps) {
                projects.addAll(complexExpressionMap.get(bitmap));
                complexExpressionMap.remove(bitmap);
            }

            // calculate required columns
            Set<Slot> requireSlots = calculateRequiredSlots(left, right, edges);
            outputs.stream()
                    .filter(e -> requireSlots.contains(e))
                    .forEach(e -> projects.add(e));

            // propose physical project
            if (projects.isEmpty()) {
                projects.add(ExpressionUtils.selectMinimumColumn(outputs));
            }
            projectsOnSubgraph.put(fullKey, projects);
        }
        List<NamedExpression> allProjects = projectsOnSubgraph.get(fullKey);
        if (outputSet.equals(new HashSet<>(allProjects))) {
            return allChild;
        }
        while (true) {
            Set<Slot> childOutputSet = allChild.get(0).getOutputSet();
            List<NamedExpression> projects = allProjects.stream()
                    .filter(expr ->
                            childOutputSet.containsAll(expr.getInputSlots()) || childOutputSet.contains(expr.toSlot()))
                    .collect(Collectors.toList());
            if (!outputSet.equals(new HashSet<>(projects))) {
                LogicalProperties projectProperties = new LogicalProperties(
                        () -> projects.stream().map(p -> p.toSlot()).collect(Collectors.toList()));
                allChild = allChild.stream()
                        .map(c -> new PhysicalProject<>(projects, projectProperties, c))
                        .collect(Collectors.toList());
            }
            if (projects.size() == 0) {
                throw new RuntimeException("dphyer fail process project");
            }
            if (projects.size() == allProjects.size()) {
                break;
            }
        }
        return allChild;
    }
}


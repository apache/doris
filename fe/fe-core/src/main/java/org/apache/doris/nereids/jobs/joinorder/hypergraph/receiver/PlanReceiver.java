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
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    HashMap<Long, List<NamedExpression>> complexProjectMap = new HashMap<>();
    int limit;
    int emitCount = 0;

    JobContext jobContext;

    HyperGraph hyperGraph;
    final Set<Slot> finalOutputs;

    public PlanReceiver(JobContext jobContext, int limit, HyperGraph hyperGraph, Set<Slot> outputs) {
        this.jobContext = jobContext;
        this.limit = limit;
        this.hyperGraph = hyperGraph;
        this.finalOutputs = outputs;
    }


    /**
     * Emit a new plan from bottom to top
     * <p>
     * The purpose of EmitCsgCmp is to combine the optimal plans for S1 and S2 into a csg-cmp-pair.
     * It requires calculating the proper join predicate and costs of the resulting joins.
     * In the end, update dpTables.
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

        processMissedEdges(left, right, edges);

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

    // be aware that the requiredOutputSlots is a superset of the actual output of current node
    // check proposeProject method to get how to create a project node for the outputs of current node.
    private Set<Slot> calculateRequiredSlots(long left, long right, List<Edge> edges) {
        // required output slots = final outputs + slot of unused edges + complex project exprs(if there is any)
        // 1. add finalOutputs to requiredOutputSlots
        Set<Slot> requiredOutputSlots = new HashSet<>(this.finalOutputs);
        BitSet usedEdgesBitmap = new BitSet();
        usedEdgesBitmap.or(usdEdges.get(left));
        usedEdgesBitmap.or(usdEdges.get(right));
        for (Edge edge : edges) {
            usedEdgesBitmap.set(edge.getIndex());
        }

        // 2. add unused edges' input slots to requiredOutputSlots
        usdEdges.put(LongBitmap.newBitmapUnion(left, right), usedEdgesBitmap);
        for (Edge edge : hyperGraph.getEdges()) {
            if (!usedEdgesBitmap.get(edge.getIndex())) {
                requiredOutputSlots.addAll(edge.getInputSlots());
            }
        }

        // 3. add input slots of all complex projects which should be done by all upper level (parents) nodes
        // dphyper enumerate subsets before supersets, so all subsets' complex projects should be excluded here
        // because it's been processed by subsets already
        long fullKey = LongBitmap.newBitmapUnion(left, right);
        hyperGraph.getComplexProject().entrySet().stream()
                .filter(l -> !LongBitmap.isSubset(l.getKey(), fullKey))
                .flatMap(l -> l.getValue().stream())
                .forEach(expr -> requiredOutputSlots.addAll(expr.getInputSlots()));
        return requiredOutputSlots;
    }

    // add any missed edge into edges to connect left and right
    private void processMissedEdges(long left, long right, List<Edge> edges) {
        // find all used edges
        BitSet usedEdgesBitmap = new BitSet();
        usedEdgesBitmap.or(usdEdges.get(left));
        usedEdgesBitmap.or(usdEdges.get(right));
        edges.forEach(edge -> usedEdgesBitmap.set(edge.getIndex()));

        // find all referenced nodes
        long allReferenceNodes = LongBitmap.or(left, right);

        // find the edge which is not in usedEdgesBitmap and its referenced nodes is subset of allReferenceNodes
        for (Edge edge : hyperGraph.getEdges()) {
            long referenceNodes =
                    LongBitmap.newBitmapUnion(edge.getOriginalLeft(), edge.getOriginalRight());
            if (LongBitmap.isSubset(referenceNodes, allReferenceNodes)
                    && !usedEdgesBitmap.get(edge.getIndex())) {
                // add the missed edge to edges
                edges.add(edge);
            }
        }
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
        emitCount = 0;
        planTable.clear();
        usdEdges.clear();
        complexProjectMap.clear();
        complexProjectMap.putAll(hyperGraph.getComplexProject());
    }

    @Override
    public Group getBestPlan(long bitmap) {
        Group root = planTable.get(bitmap);
        Preconditions.checkState(root != null);
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

            // process child first, plan's child may be changed due to mergeGroup
            Plan physicalPlan = groupExpression.getPlan();
            for (Group child : groupExpression.children()) {
                makeLogicalExpression(child);
            }

            Plan logicalPlan;
            if (physicalPlan instanceof PhysicalProject) {
                PhysicalProject physicalProject = (PhysicalProject) physicalPlan;
                logicalPlan = new LogicalProject<>(physicalProject.getProjects(),
                        new GroupPlan(groupExpression.child(0)));
            } else if (physicalPlan instanceof AbstractPhysicalJoin) {
                AbstractPhysicalJoin physicalJoin = (AbstractPhysicalJoin) physicalPlan;
                logicalPlan = new LogicalJoin<>(physicalJoin.getJoinType(), physicalJoin.getHashJoinConjuncts(),
                        physicalJoin.getOtherJoinConjuncts(), JoinHint.NONE, physicalJoin.getMarkJoinSlotReference(),
                        groupExpression.children().stream().map(g -> new GroupPlan(g)).collect(Collectors.toList()));
            } else {
                throw new RuntimeException("DPhyp can only handle join and project operator");
            }
            jobContext.getCascadesContext().getMemo().copyIn(logicalPlan, root, false);
        }
    }

    private List<Plan> proposeProject(List<Plan> allChild, List<Edge> edges, long left, long right) {
        long fullKey = LongBitmap.newBitmapUnion(left, right);
        List<Slot> outputs = allChild.get(0).getOutput();
        Set<Slot> outputSet = allChild.get(0).getOutputSet();
        List<NamedExpression> allProjects = Lists.newArrayList();

        List<NamedExpression> complexProjects = new ArrayList<>();
        // Calculate complex expression should be done by current(fullKey) node
        // the complex projects includes final output of current node(the complex project of fullKey)
        // and any complex projects don't belong to subsets of fullKey except that fullKey is not a join node
        List<Long> bitmaps = complexProjectMap.keySet().stream().filter(bitmap -> LongBitmap
                        .isSubset(bitmap, fullKey)
                        && ((!LongBitmap.isSubset(bitmap, left) && !LongBitmap.isSubset(bitmap, right))
                        || left == right))
                .collect(Collectors.toList());

        // complexProjectMap is created by a bottom up traverse of join tree, so child node is put before parent node
        // in the bitmaps
        for (long bitmap : bitmaps) {
            if (complexProjects.isEmpty()) {
                complexProjects = complexProjectMap.get(bitmap);
            } else {
                // The top project of (T1, T2, T3) is different after reorder
                // we need merge Project1 and Project2 as Project4 after reorder
                // T1 join T2 join T3:
                //    Project1(a, e + f)
                //        join(a = e)
                //            Project2(a, b + d as e)
                //                join(a = c)
                //                    T1(a, b)
                //                    T2(c, d)
                //        T3(e, f)
                //
                // after reorder:
                // T1 join T3 join T2:
                //    Project4(a, b + d + f)
                //        join(a = c)
                //            Project3(a, b, f)
                //                join(a = e)
                //                    T1(a, b)
                //                    T3(e, f)
                //        T2(c, d)
                //
                complexProjects =
                        PlanUtils.mergeProjections(complexProjects, complexProjectMap.get(bitmap));
            }
        }
        allProjects.addAll(complexProjects);

        // calculate required columns by all parents
        Set<Slot> requireSlots = calculateRequiredSlots(left, right, edges);

        // add output slots belong to required slots to project list
        allProjects.addAll(outputs.stream().filter(e -> requireSlots.contains(e))
                .collect(Collectors.toList()));

        // propose physical project
        if (allProjects.isEmpty()) {
            allProjects.add(ExpressionUtils.selectMinimumColumn(outputs));
        }
        if (outputSet.equals(new HashSet<>(allProjects))) {
            return allChild;
        }

        Set<Slot> childOutputSet = allChild.get(0).getOutputSet();
        List<NamedExpression> projects = allProjects.stream()
                .filter(expr ->
                        childOutputSet.containsAll(expr.getInputSlots()))
                .collect(Collectors.toList());
        if (!outputSet.equals(new HashSet<>(projects))) {
            LogicalProperties projectProperties = new LogicalProperties(
                    () -> projects.stream().map(p -> p.toSlot()).collect(Collectors.toList()));
            allChild = allChild.stream()
                    .map(c -> new PhysicalProject<>(projects, projectProperties, c))
                    .collect(Collectors.toList());
        }
        Preconditions.checkState(!projects.isEmpty() && projects.size() == allProjects.size());

        return allChild;
    }
}

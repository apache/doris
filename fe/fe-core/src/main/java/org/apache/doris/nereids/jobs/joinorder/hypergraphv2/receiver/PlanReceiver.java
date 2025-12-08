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

package org.apache.doris.nereids.jobs.joinorder.hypergraphv2.receiver;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupExpressionJob;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.edge.Edge;
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The Receiver is used for cached the plan that has been emitted and build the new plan, it's the dp table in paper
 */
public class PlanReceiver extends AbstractReceiver {
    // dp table to cache all valid sub-plans
    HashMap<Long, Group> planTable = new HashMap<>();
    HashMap<Long, BitSet> usdEdges = new HashMap<>();

    // limit define the max number of csg-cmp pair in this Receiver
    int limit;
    int emitCount = 0;

    JobContext jobContext;

    HyperGraph hyperGraph;

    final Set<Slot> finalRequiredSlots;
    final List<NamedExpression> finalProjects;
    long allNodeBitmap;
    long startTime = System.currentTimeMillis();
    long timeLimit = ConnectContext.get().getSessionVariable().joinReorderTimeLimit;
    boolean fullKeyEmitted = false;
    EmitState emitState = EmitState.NONE;
    boolean missingEdgeFail = false;
    boolean conflictRuleFail = false;

    boolean joinTypeError = false;

    public PlanReceiver(JobContext jobContext, int limit, HyperGraph hyperGraph) {
        this.jobContext = jobContext;
        this.limit = limit;
        this.hyperGraph = hyperGraph;
        this.finalProjects = hyperGraph.getFinalProjects();
        this.finalRequiredSlots = ExpressionUtils.getInputSlotSet(finalProjects);
        this.allNodeBitmap = hyperGraph.getNodesMap();
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
    public EmitState emitCsgCmp(long left, long right, List<Edge> edges) {
        Preconditions.checkArgument(planTable.containsKey(left));
        Preconditions.checkArgument(planTable.containsKey(right));
        missingEdgeFail = false;
        conflictRuleFail = false;
        joinTypeError = false;
        fullKeyEmitted = false;
        if (LongBitmap.newBitmapUnion(left, right) == allNodeBitmap) {
            fullKeyEmitted = true;
        }
        if (!processMissedEdges(left, right, edges)) {
            if (fullKeyEmitted) {
                missingEdgeFail = true;
            }
            return EmitState.CONTINUE;
        }

        emitCount += 1;
//        if (emitCount > limit || System.currentTimeMillis() - startTime > timeLimit) {
//            return false;
//        }

        if (emitCount > limit) {
            if (fullKeyEmitted) {
                emitState = EmitState.FAIL;
            }
            return EmitState.FAIL;
        }
        if (!checkConflictRule(left, right, edges)) {
            if (fullKeyEmitted) {
                conflictRuleFail = true;
            }
            return EmitState.CONTINUE;
        }
        if ((edges.get(0).getLeftExtendedNodes() & left) == 0) {
            // swap left and right
            long tmp = left;
            left = right;
            right = tmp;
        }

        Memo memo = jobContext.getCascadesContext().getMemo();
        GroupPlan leftPlan = new GroupPlan(planTable.get(left));
        GroupPlan rightPlan = new GroupPlan(planTable.get(right));

        // First, we implement all possible physical plans
        // In this step, we don't generate logical expression because they are useless in DPhyp.
        List<Expression> hashConjuncts = new ArrayList<>();
        List<Expression> otherConjuncts = new ArrayList<>();

        JoinType joinType = Edge.extractJoinTypeAndConjuncts(edges, hashConjuncts, otherConjuncts);
        if (joinType == null) {
            if (fullKeyEmitted) {
                joinTypeError = true;
            }
            return EmitState.CONTINUE;
        }
        long fullKey = LongBitmap.newBitmapUnion(left, right);

        LogicalPlan logicalPlan = proposeJoin(joinType, leftPlan, rightPlan, hashConjuncts,
                otherConjuncts);

        logicalPlan = proposeProject(logicalPlan, edges, left, right);

        // Second, we copy all physical plan to Group and generate properties and calculate cost
        if (!planTable.containsKey(fullKey)) {
            planTable.put(fullKey, memo.newGroup(logicalPlan.getLogicalProperties()));
        }
        Group group = planTable.get(fullKey);
        CopyInResult copyInResult = memo.copyIn(logicalPlan, group, false, planTable);
        proposeAllDistributedPlans(copyInResult.correspondingExpression);

        return EmitState.SUCCESS;
    }

    // be aware that the requiredOutputSlots is a superset of the actual output of current node
    // check proposeProject method to get how to create a project node for the outputs of current node.
    private Set<Slot> calculateRequiredSlots(long left, long right, List<Edge> edges) {
        // required output slots = final outputs + slot of unused edges + complex project exprs(if there is any)
        // 1. add finalOutputs to requiredOutputSlots
        Set<Slot> requiredOutputSlots = new HashSet<>(this.finalRequiredSlots);
        BitSet usedEdgesBitmap = new BitSet();
        usedEdgesBitmap.or(usdEdges.get(left));
        usedEdgesBitmap.or(usdEdges.get(right));
        for (Edge edge : edges) {
            usedEdgesBitmap.set(edge.getIndex());
        }

        // 2. add unused edges' input slots to requiredOutputSlots
        usdEdges.put(LongBitmap.newBitmapUnion(left, right), usedEdgesBitmap);
        for (Edge edge : hyperGraph.getJoinEdges()) {
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

    // add all missed edge into edges to connect left and right, considering sql bellow:
    // select * from t0 join t1 on t0.c1 = t1.c1 join t2 on t0.c2 = t2.c2 and t0.c1 = t1.c1 + t2.c2;
    // the hyperGraph's joinEdges is:
    // joinEdges = {RegularImmutableList@18366}  size = 3
    // 0 = {Edge@18370} "<{0} --INNER_JOIN-- {1}>"
    // 1 = {Edge@18371} "<{0} --INNER_JOIN-- {2}>"
    // 2 = {Edge@18372} "<{0, 1} --INNER_JOIN-- {2}>"
    // the hyper predicate t0.c1 = t1.c1 + t2.c2 is encoded as hyper edge 2.
    // The hyper edge(Edge2) means 0 and 1 must be joined before join 2 according to the paper.
    // Unfortunately, it's not correct, because we can join 0 and 2 first then join 1.
    // Ideally, we should create new Edge <{0, 2} --INNER_JOIN-- {1}> based on Edge2 to solve the problem,
    // The root cause is hyper predicate should be encoded as one or more hyper edges in different scenarios.
    // But we are not able to do so in all cases (complex expression and outer joins).
    // So we use processMissedEdges to find all valid edges when join 0, 1, 2 as fallback plan.
    private boolean processMissedEdges(long left, long right, List<Edge> edges) {
        // find all used edges
        BitSet usedEdgesBitmap = new BitSet();
        usedEdgesBitmap.or(usdEdges.get(left));
        usedEdgesBitmap.or(usdEdges.get(right));
        edges.forEach(edge -> usedEdgesBitmap.set(edge.getIndex()));

        // find all referenced nodes
        long allReferenceNodes = LongBitmap.or(left, right);

        // find the edge which is not in usedEdgesBitmap and its referenced nodes is subset of allReferenceNodes
        for (Edge edge : hyperGraph.getJoinEdges()) {
            // TODO long referenceNodes = LongBitmap.newBitmapUnion(edge.getLeftRequiredNodes(), edge.getRightRequiredNodes());
            if (LongBitmap.isSubset(edge.getReferenceNodes(), allReferenceNodes)
                    && !usedEdgesBitmap.get(edge.getIndex())) {
                if (edge.isEnforcedOrder()) {
                    return false;
                } else {
                    // add the missed edge to edges
                    edges.add(edge);
                }
            }
        }
        return true;
    }

    private void proposeAllDistributedPlans(GroupExpression groupExpression) {
        jobContext.getCascadesContext().pushJob(new OptimizeGroupExpressionJob(groupExpression,
                new JobContext(jobContext.getCascadesContext(), PhysicalProperties.ANY, Double.MAX_VALUE)));
        if (!groupExpression.isStatDerived()) {
            jobContext.getCascadesContext().pushJob(new DeriveStatsJob(groupExpression,
                    jobContext.getCascadesContext().getCurrentJobContext()));
        }
        jobContext.getCascadesContext().getJobScheduler().executeJobPool(jobContext.getCascadesContext());
    }

    private LogicalPlan proposeJoin(JoinType joinType, Plan left, Plan right, List<Expression> hashConjuncts,
            List<Expression> otherConjuncts) {
        return new LogicalJoin<>(joinType, hashConjuncts, otherConjuncts, left, right, null);
    }

    @Override
    public void addGroup(long bitmap, Group group) {
        Preconditions.checkArgument(LongBitmap.getCardinality(bitmap) == 1);
        usdEdges.put(bitmap, new BitSet());
        Plan plan = proposeProject(new GroupPlan(group), new ArrayList<>(), bitmap, bitmap);
        if (!(plan instanceof GroupPlan)) {
            CopyInResult copyInResult = jobContext.getCascadesContext().getMemo().copyIn(plan, null, false, planTable);
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
        startTime = System.currentTimeMillis();
        emitState = EmitState.NONE;
        fullKeyEmitted = false;
    }

    @Override
    public Group getBestPlan(long bitmap) {
        Group group = planTable.get(bitmap);
        return group;
    }

    private LogicalPlan proposeProject(LogicalPlan join, List<Edge> edges, long left, long right) {
        List<Slot> outputs = join.getOutput();
        Set<Slot> outputSet = join.getOutputSet();
        // calculate required columns by all parents
        Set<Slot> requireSlots = calculateRequiredSlots(left, right, edges);
        List<NamedExpression> allProjects = new ArrayList<>(outputs.size());
        for (Slot slot : outputs) {
            if (requireSlots.contains(slot)) {
                allProjects.add(slot);
            }
        }

        // propose logical project
        if (allProjects.isEmpty()) {
            allProjects.add(ExpressionUtils.selectMinimumColumn(outputs));
        }
//        if (LongBitmap.newBitmapUnion(left, right) == allNodeBitmap && !finalProjects.equals(allProjects)) {
//            // add final project for the join cluster
//            return new LogicalProject<>(finalProjects, join);
//        }
        if (LongBitmap.newBitmapUnion(left, right) == allNodeBitmap && !outputSet.equals(new HashSet<>(finalProjects))) {
            // add final project for the join cluster
            return new LogicalProject<>(finalProjects, join);
        }
        else {
            if (outputSet.equals(new HashSet<>(allProjects))) {
                return join;
            }

            Set<Slot> childOutputSet = join.getOutputSet();
            List<NamedExpression> projects = allProjects.stream()
                    .filter(expr ->
                            childOutputSet.containsAll(expr.getInputSlots()))
                    .collect(Collectors.toList());
            LogicalPlan project = join;
            if (!outputSet.equals(new HashSet<>(projects))) {
                project = new LogicalProject<>(projects, join);
            }
            Preconditions.checkState(!projects.isEmpty() && projects.size() == allProjects.size(),
                    " there are some projects left %s %s", projects, allProjects);
            return project;
        }
    }
}

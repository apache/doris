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
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupExpressionJob;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
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
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    long startTime = System.currentTimeMillis();
    long timeLimit = ConnectContext.get().getSessionVariable().joinReorderTimeLimit;

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
    public boolean emitCsgCmp(long left, long right, List<JoinEdge> edges) {
        Preconditions.checkArgument(planTable.containsKey(left));
        Preconditions.checkArgument(planTable.containsKey(right));
        processMissedEdges(left, right, edges);

        emitCount += 1;
        if (emitCount > limit || System.currentTimeMillis() - startTime > timeLimit) {
            return false;
        }

        Memo memo = jobContext.getCascadesContext().getMemo();
        GroupPlan leftPlan = new GroupPlan(planTable.get(left));
        GroupPlan rightPlan = new GroupPlan(planTable.get(right));

        // First, we implement all possible physical plans
        // In this step, we don't generate logical expression because they are useless in DPhyp.
        List<Expression> hashConjuncts = new ArrayList<>();
        List<Expression> otherConjuncts = new ArrayList<>();

        JoinType joinType = JoinEdge.extractJoinTypeAndConjuncts(edges, hashConjuncts, otherConjuncts);
        if (joinType == null) {
            return true;
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

        return true;
    }

    // be aware that the requiredOutputSlots is a superset of the actual output of current node
    // check proposeProject method to get how to create a project node for the outputs of current node.
    private Set<Slot> calculateRequiredSlots(long left, long right, List<JoinEdge> edges) {
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

    // add any missed edge into edges to connect left and right
    private void processMissedEdges(long left, long right, List<JoinEdge> edges) {
        // find all used edges
        BitSet usedEdgesBitmap = new BitSet();
        usedEdgesBitmap.or(usdEdges.get(left));
        usedEdgesBitmap.or(usdEdges.get(right));
        edges.forEach(edge -> usedEdgesBitmap.set(edge.getIndex()));

        // find all referenced nodes
        long allReferenceNodes = LongBitmap.or(left, right);

        // find the edge which is not in usedEdgesBitmap and its referenced nodes is subset of allReferenceNodes
        for (JoinEdge edge : hyperGraph.getJoinEdges()) {
            long referenceNodes = LongBitmap.newBitmapUnion(edge.getLeftRequiredNodes(), edge.getRightRequiredNodes());
            if (LongBitmap.isSubset(referenceNodes, allReferenceNodes)
                    && !usedEdgesBitmap.get(edge.getIndex())) {
                // add the missed edge to edges
                edges.add(edge);
            }
        }
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
        complexProjectMap.clear();
        complexProjectMap.putAll(hyperGraph.getComplexProject());
        startTime = System.currentTimeMillis();
    }

    @Override
    public Group getBestPlan(long bitmap) {
        return planTable.get(bitmap);
    }

    private LogicalPlan proposeProject(LogicalPlan join, List<JoinEdge> edges, long left, long right) {
        long fullKey = LongBitmap.newBitmapUnion(left, right);
        List<Slot> outputs = join.getOutput();
        Set<Slot> outputSet = join.getOutputSet();

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
        bitmaps.sort(Long::compare);
        for (long bitmap : bitmaps) {
            if (complexProjects.isEmpty()) {
                complexProjects.addAll(complexProjectMap.get(bitmap));
            } else {
                // Rewrite project expression by its children
                complexProjects.addAll(
                        PlanUtils.mergeProjections(complexProjects, complexProjectMap.get(bitmap)));
            }
        }

        // calculate required columns by all parents
        Set<Slot> requireSlots = calculateRequiredSlots(left, right, edges);
        List<NamedExpression> allProjects = Stream.concat(
                outputs.stream().filter(requireSlots::contains),
                complexProjects.stream().filter(e -> requireSlots.contains(e.toSlot()))
        ).collect(Collectors.toList());

        // propose logical project
        if (allProjects.isEmpty()) {
            allProjects.add(ExpressionUtils.selectMinimumColumn(outputs));
        }
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

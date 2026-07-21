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

package org.apache.doris.nereids.jobs.joinorder.hypergraphv2;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.DPhyperNode;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The graph is a join graph, whose node is the leaf plan and edge is a join operator.
 * It's used for join ordering
 */
public class HyperGraph {
    // Long-> nodes collection; BitSet-> its containing edges. containing edges mean the edge's reference nodes
    // is subset of nodes collection
    private final Map<Long, BitSet> treeEdgesCache = new LinkedHashMap<>();
    private final List<Edge> joinEdges;
    private final List<AbstractNode> nodes;
    private final List<NamedExpression> finalProjects;
    // Each value is a flattened list of projected aliases for a given bitmap.
    // Cross-layer references (e.g., z = x + 1 referencing x = COALESCE(v, 0))
    // are resolved at flush time so that only a single layer is stored.
    private final Map<Long, List<NamedExpression>> nodeToProjectedAliases;
    private final CascadesContext ctx;

    HyperGraph(List<NamedExpression> finalProjects, List<Edge> joinEdges, List<AbstractNode> nodes,
               Map<Long, List<NamedExpression>> nodeToProjectedAliases, CascadesContext ctx) {
        this.finalProjects = ImmutableList.copyOf(finalProjects);
        this.joinEdges = ImmutableList.copyOf(joinEdges);
        this.nodes = ImmutableList.copyOf(nodes);
        this.nodeToProjectedAliases = nodeToProjectedAliases;
        this.ctx = ctx;
    }

    /**
     * the project with alias and slot, without volatile or non-movable expressions.
     * Projects containing volatile (e.g. uuid()) or non-movable (e.g. assert_true())
     * expressions are treated as join-cluster boundaries (like aggregate nodes).
     */
    public static boolean isValidProject(Plan plan) {
        if (!(plan instanceof LogicalProject)) {
            return false;
        }
        return ((LogicalProject<? extends Plan>) plan).getProjects().stream()
                .allMatch(e -> (e instanceof Slot || e instanceof Alias)
                        && !e.containsVolatileExpression()
                        && !e.containsType(NoneMovableFunction.class));
    }

    /**
     * inner join group without mark slot
     */
    public static boolean isValidJoin(Plan plan) {
        if (!(plan instanceof LogicalJoin)) {
            return false;
        }
        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
        return !join.isMarkJoin() && !join.isLeadingJoin() && !join.hasDistributeHint()
                && !join.getJoinType().isAsofJoin();
    }

    public static Builder builderForDPhyper(Group group, CascadesContext ctx) {
        return new Builder().buildHyperGraphForDPhyper(group, ctx);
    }

    public CascadesContext getCtx() {
        return ctx;
    }

    public List<NamedExpression> getFinalProjects() {
        return finalProjects;
    }

    public List<Edge> getJoinEdges() {
        return joinEdges;
    }

    public List<AbstractNode> getNodes() {
        return nodes;
    }

    public long getNodesMap() {
        return LongBitmap.newBitmapBetween(0, nodes.size());
    }

    public Edge getJoinEdge(int index) {
        return joinEdges.get(index);
    }

    public boolean hasProjectedAliases() {
        return !nodeToProjectedAliases.isEmpty();
    }

    /**
     * find all literal alias should be projected after left join right
     */
    public List<NamedExpression> getProjectedAliases(long left, long right) {
        ImmutableList.Builder<NamedExpression> aliasList = ImmutableList.builder();
        if (left == right) {
            List<NamedExpression> as = nodeToProjectedAliases.get(left);
            if (as != null) {
                aliasList.addAll(as);
            }
        } else {
            long nodes = LongBitmap.newBitmapUnion(left, right);
            for (Map.Entry<Long, List<NamedExpression>> entry : nodeToProjectedAliases.entrySet()) {
                if (!LongBitmap.isSubset(entry.getKey(), left) && !LongBitmap.isSubset(entry.getKey(), right)
                        && LongBitmap.isSubset(entry.getKey(), nodes)) {
                    aliasList.addAll(entry.getValue());
                }
            }
        }
        return aliasList.build();
    }

    /**
     * Returns ordered Project layers for the given left/right nodes.
     * Each inner list is one LogicalProject layer; layers are ordered bottom-up.
     * PlanReceiver emits each layer as a separate LogicalProject to preserve
     * materialization boundaries for volatile expressions.
     * Uses isSubset(key, nodes) rather than exact match because DPHyp may
     * build a superset of the key without ever building the key independently
     * (e.g., {A,B} forms only as part of {A,B,C}). Without isSubset the layer
     * would be lost entirely. The trade-off: a volatile alias may materialize
     * above unrelated nodes inside the same nullable subtree — this is inherent
     * to DPHyp reordering within inner-join clusters.
     */
    public List<List<NamedExpression>> getProjectedAliasLayers(long left, long right) {
        List<NamedExpression> aliases = getProjectedAliases(left, right);
        if (aliases.isEmpty()) {
            return new ArrayList<>();
        }
        return ImmutableList.of(aliases);
    }

    /**
     * Returns the union of input slots of all projected aliases whose bitmap
     * is a superset of the given nodes. Used by PlanReceiver to preserve
     * columns in intermediate join outputs that are needed by pending aliases
     * (both those emitted now and those deferred to a later join).
     * Without this, calculateRequiredSlots prunes base columns like A.v/B.v
     * that a pending alias s=A.v+B.v depends on, and CheckAfterRewrite fails.
     * Uses isOverlap (not isSubset) because DPHyp can build mixed subplans
     * like {A,C} that only partially overlap with an alias's source {A,B};
     * inputs from the overlapping part (A.v) must still be preserved.
     */
    public Set<Slot> getAllAliasInputSlotsForNodes(long nodes) {
        Set<Slot> result = new HashSet<>();
        for (Map.Entry<Long, List<NamedExpression>> entry : nodeToProjectedAliases.entrySet()) {
            if (LongBitmap.isOverlap(nodes, entry.getKey())) {
                for (NamedExpression alias : entry.getValue()) {
                    result.addAll(alias.getInputSlots());
                }
            }
        }
        return result;
    }

    /**
     * Returns true if the edge can be safely used as a join predicate between
     * left and right. An edge is unsafe when it references a projected alias
     * whose source bitmap spans both children — the alias layer is emitted by
     * proposeProject (after proposeJoin), so the join predicate cannot see it.
     * Such edges must wait for a later join step where the alias source is
     * fully contained in one child.
     *
     * <p>This guards against missed-edge fallback consuming a projected alias
     * across a split source endpoint:
     * <pre>
     *   Edge {A,B}--{C} with predicate s=C.t, where s has source {A,B}.
     *   When left={A,C}, right={B}: s spans both children, so unsafe.
     *   When left={A,B}, right={C}: {A,B} subset of left, so safe.
     * </pre>
     */
    public boolean isEdgeSafeForJoin(Edge edge, long left, long right) {
        if (!hasProjectedAliases()) {
            return true;
        }
        List<NamedExpression> splitAliases = getProjectedAliases(left, right);
        if (splitAliases.isEmpty()) {
            return true;
        }
        Set<ExprId> splitAliasExprIds = new HashSet<>();
        for (NamedExpression alias : splitAliases) {
            splitAliasExprIds.add(alias.getExprId());
        }
        for (Slot slot : edge.getInputSlots()) {
            if (splitAliasExprIds.contains(slot.getExprId())) {
                return false;
            }
        }
        return true;
    }

    // find edges to connect left and right node
    public BitSet findConnectionEdges(long left, long right) {
        BitSet operatorEdgesMap = new BitSet();
        operatorEdgesMap.or(getEdgesInTree(LongBitmap.or(left, right)));
        operatorEdgesMap.andNot(getEdgesInTree(left));
        operatorEdgesMap.andNot(getEdgesInTree(right));
        return operatorEdgesMap;
    }

    /**
     * Returns all edges in the tree
     */
    private BitSet getEdgesInTree(long treeNodesMap) {
        if (!treeEdgesCache.containsKey(treeNodesMap)) {
            BitSet edgesMap = new BitSet();
            for (Edge edge : joinEdges) {
                if (LongBitmap.isSubset(edge.getReferenceNodes(), treeNodesMap)) {
                    edgesMap.set(edge.getIndex());
                }
            }
            treeEdgesCache.put(treeNodesMap, edgesMap);
        }
        return treeEdgesCache.get(treeNodesMap);
    }

    /**
     * Graph simplifier need to update the edge for join ordering
     *
     * @param edgeIndex The index of updated edge
     * @param newLeft   The new left of updated edge
     * @param newRight  The new right of update edge
     */
    public void modifyEdge(int edgeIndex, long newLeft, long newRight) {
        // When modify an edge in hyper graph, we need to update the left and right nodes
        // For these nodes that are only in the old edge, we need remove the edge from them
        // For these nodes that are only in the new edge, we need to add the edge to them
        Edge edge = joinEdges.get(edgeIndex);
        if (treeEdgesCache.containsKey(edge.getReferenceNodes())) {
            treeEdgesCache.get(edge.getReferenceNodes()).set(edgeIndex, false);
        }
        joinEdges.get(edgeIndex).setLeftExtendedNodes(newLeft);
        joinEdges.get(edgeIndex).setRightExtendedNodes(newRight);
        if (treeEdgesCache.containsKey(edge.getReferenceNodes())) {
            treeEdgesCache.get(edge.getReferenceNodes()).set(edgeIndex, true);
        }
    }

    /**
     * For the given hyperGraph, make a textual representation in the form
     * of a dotty graph. You can save this to a file and then use Graphviz
     * to render this it a graphical representation of the hyperGraph for
     * easier debugging, e.g. like this:
     * <p>
     * dot -Tps graph.dot > graph.ps
     * display graph.ps
     */
    public String toDottyHyperGraph() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("digraph G {  # %d edges%n", joinEdges.size()));
        List<String> graphvisNodes = new ArrayList<>();
        for (AbstractNode node : nodes) {
            String nodeName = node.getName();
            // nodeID is used to identify the node with the same name
            String nodeID = nodeName;
            while (graphvisNodes.contains(nodeID)) {
                nodeID += "_";
            }
            double rowCount = (node instanceof DPhyperNode) ? ((DPhyperNode) node).getRowCount() : -1;
            builder.append(String.format("  %s [label=\"%s %n rowCount=%.2f\"];%n", nodeID, nodeName, rowCount));
            graphvisNodes.add(nodeName);
        }
        for (int i = 0; i < joinEdges.size(); i += 1) {
            Edge edge = joinEdges.get(i);
            // TODO: add cardinality to label
            String label = String.format("%.2f", edge.getSelectivity());
            if (joinEdges.get(i).isSimple()) {
                String arrowHead = "";
                if (edge.getJoin().getJoinType() == JoinType.INNER_JOIN) {
                    arrowHead = ",arrowhead=none";
                }

                int leftIndex = LongBitmap.lowestOneIndex(edge.getLeftExtendedNodes());
                int rightIndex = LongBitmap.lowestOneIndex(edge.getRightExtendedNodes());
                builder.append(String.format("%s -> %s [label=\"%s\"%s]%n", graphvisNodes.get(leftIndex),
                        graphvisNodes.get(rightIndex), label, arrowHead));
            } else {
                // Hyper edge is considered as a tiny virtual node
                builder.append(String.format("e%d [shape=circle, width=.001, label=\"\"]%n", i));

                String leftLabel = "";
                String rightLabel = "";
                if (LongBitmap.getCardinality(edge.getLeftExtendedNodes()) == 1) {
                    rightLabel = label;
                } else {
                    leftLabel = label;
                }

                String finalLeftLabel = leftLabel;
                for (int nodeIndex : LongBitmap.getIterator(edge.getLeftExtendedNodes())) {
                    builder.append(
                            String.format("%s -> e%d [arrowhead=none, label=\"%s\"]%n", graphvisNodes.get(nodeIndex), i,
                                    finalLeftLabel));
                }

                String finalRightLabel = rightLabel;
                for (int nodeIndex : LongBitmap.getIterator(edge.getRightExtendedNodes())) {
                    builder.append(
                            String.format("%s -> e%d [arrowhead=none, label=\"%s\"]%n", graphvisNodes.get(nodeIndex), i,
                                    finalRightLabel));
                }
            }
        }
        builder.append("}\n");
        return builder.toString();
    }

    /**
     * Builder of HyperGraph
     */
    public static class Builder {
        private final List<Edge> joinEdges = new ArrayList<>();
        private final List<AbstractNode> nodes = new ArrayList<>();
        // Key: hyper node's output slots, the slots come from simple slotReference or Alias
        // value: the long bitmap value representing hyper node(simple node or joined nodes)
        // addDPHyperNode method add slots from simple node
        // addAlias method add slots from both simple node and joined nodes, depending on the alias's input slots
        private final HashMap<Slot, Long> slotToHyperNodeMap = new LinkedHashMap<>();

        private final Map<Long, List<NamedExpression>> nodeToProjectedAliases = new LinkedHashMap<>();

        // Accumulates aliases for the current Project layer. Reset for each new Project
        // in buildForDPhyper so that each source Project forms a separate layer,
        // preserving materialization boundaries for volatile expressions.
        private List<NamedExpression> currentProjectedAliasLayer = null;

        private Set<Slot> finalOutputs;

        private CascadesContext ctx;

        private ExpressionRewriteContext expressionRewriteCtx;

        private Map<Slot, Expression> aliasReplaceMap = new LinkedHashMap<>();

        private List<NamedExpression> finalProjects = new ArrayList<>();

        private long nonInnerJoinedNodes = 0;

        public List<AbstractNode> getNodes() {
            return nodes;
        }

        private Builder buildHyperGraphForDPhyper(Group group, CascadesContext ctx) {
            this.ctx = ctx;
            this.expressionRewriteCtx = new ExpressionRewriteContext(ctx);
            finalOutputs = group.getLogicalProperties().getOutputSet();
            this.buildForDPhyper(group.getLogicalExpression());
            for (Slot slot : finalOutputs) {
                Expression expression = aliasReplaceMap.get(slot);
                if (expression != null) {
                    finalProjects.add(new Alias(slot.getExprId(), expression, slot.getName()));
                } else {
                    finalProjects.add(slot);
                }
            }
            return this;
        }

        public HyperGraph build() {
            return new HyperGraph(finalProjects, joinEdges, nodes, nodeToProjectedAliases, ctx);
        }

        public void updateNode(int idx, Group group) {
            Preconditions.checkArgument(nodes.get(idx) instanceof DPhyperNode);
            nodes.set(idx, ((DPhyperNode) nodes.get(idx)).withGroup(group));
        }

        /**
         * Build Graph for DPhyper in bottom-up way, it means build node first, then edges
         *
         * @param groupExpression the groupExpression contains root plan node of this join cluster
         * @return BitSet is the latest join edges' index in joinEdges, Long is all the subtree nodes of the
         *      latest join edges index
         */
        private Pair<BitSet, Long> buildForDPhyper(GroupExpression groupExpression) {
            return buildForDPhyper(groupExpression, false);
        }

        private Pair<BitSet, Long> buildForDPhyper(GroupExpression groupExpression, boolean isNullableSide) {
            // process Project
            if (isValidProject(groupExpression.getPlan())) {
                LogicalProject<?> project = (LogicalProject<?>) groupExpression.getPlan();
                Pair<BitSet, Long> res = buildForDPhyper(
                        groupExpression.child(0).getLogicalExpressions().get(0), isNullableSide);
                // Start a new layer for this Project. Each source Project becomes one
                // LogicalProject layer, preserving materialization boundaries for
                // volatile expressions (e.g., uuid()) that PlanUtils.canMergeWithProjections
                // would otherwise reject.
                List<NamedExpression> savedLayer = this.currentProjectedAliasLayer;
                this.currentProjectedAliasLayer = new ArrayList<>();
                for (NamedExpression expr : project.getProjects()) {
                    if (expr instanceof Alias) {
                        this.addAlias((Alias) expr, res.second, isNullableSide);
                    }
                }
                // Flush the layer if non-empty. If aliases for this key already
                // exist, resolve cross-layer references (e.g., z = x + 1 where
                // x was defined by an earlier Project on the same subtree) and
                // merge into the existing single layer. Cross-layer resolution
                // is safe because volatile/non-movable expressions are already
                // excluded by isValidProject.
                if (!this.currentProjectedAliasLayer.isEmpty()) {
                    long key = res.second;
                    List<NamedExpression> existing = nodeToProjectedAliases.get(key);
                    if (existing != null) {
                        Map<Slot, Expression> replaceMap = new LinkedHashMap<>();
                        for (NamedExpression a : existing) {
                            if (a instanceof Alias) {
                                replaceMap.put(a.toSlot(), ((Alias) a).child());
                            }
                        }
                        for (NamedExpression expr : currentProjectedAliasLayer) {
                            existing.add((NamedExpression) ExpressionUtils.replace(expr, replaceMap));
                        }
                    } else {
                        nodeToProjectedAliases.put(key,
                                new ArrayList<>(currentProjectedAliasLayer));
                    }
                }
                this.currentProjectedAliasLayer = savedLayer;
                return res;
            }

            // process Join
            if (isValidJoin(groupExpression.getPlan())) {
                LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) groupExpression.getPlan();
                JoinType joinType = join.getJoinType();
                // Determine if children are on the nullable side:
                // - For LEFT OUTER JOIN, the right child is nullable
                // - For RIGHT OUTER JOIN, the left child is nullable
                // - For FULL OUTER JOIN, both children are nullable
                // - If we're already inside a nullable context, propagate down
                boolean leftNullable = isNullableSide
                        || joinType.isRightOuterJoin()
                        || joinType.isAsofRightOuterJoin()
                        || joinType.isFullOuterJoin();
                boolean rightNullable = isNullableSide
                        || joinType.isLeftOuterJoin()
                        || joinType.isAsofLeftOuterJoin()
                        || joinType.isFullOuterJoin();
                Pair<BitSet, Long> left = buildForDPhyper(
                        groupExpression.child(0).getLogicalExpressions().get(0), leftNullable);
                Pair<BitSet, Long> right = buildForDPhyper(
                        groupExpression.child(1).getLogicalExpressions().get(0), rightNullable);
                return Pair.of(this.addJoin(join, left, right), LongBitmap.or(left.second, right.second));
            }

            // process Other Node
            int idx = this.addDPHyperNode(groupExpression.getOwnerGroup());
            return Pair.of(new BitSet(), LongBitmap.newBitmap(idx));
        }

        /**
         * Store the relation between Alias Slot and Original Slot and its expression
         * e.g.,
         * a = b
         * |--- project((c + d) as b)
         * <p>
         * a = b
         * |--- project((c + 1) as b)
         *
         * @param alias The alias Expression in project Operator
         */
        public boolean addAlias(Alias alias, long subTreeNodes, boolean isNullableSide) {
            Slot aliasSlot = alias.toSlot();
            if (slotToHyperNodeMap.containsKey(aliasSlot)) {
                return true;
            }
            long bitmap = LongBitmap.newBitmap();
            // find the nodes produce the input slots, the nodes is subset of subTreeNodes
            for (Slot slot : alias.getInputSlots()) {
                bitmap = LongBitmap.or(bitmap, slotToHyperNodeMap.get(slot));
            }
            // The case hit when there are some constant aliases such as:
            // select * from t1 join (
            //          select *, 1 as b1 from t2)
            //              on t1.b = b1
            // just reference them all for this slot
            boolean addToReplaceMap = true;
            if (bitmap == 0) {
                bitmap = subTreeNodes;
                addToReplaceMap = false;
                // Constant aliases go into the current Project layer (set up by
                // buildForDPhyper) and will be flushed to nodeToProjectedAliases
                // keyed by the layer's subtree bitmap after the Project is processed.
                if (currentProjectedAliasLayer != null) {
                    currentProjectedAliasLayer.add(alias);
                }
            }
            Preconditions.checkArgument(bitmap > 0, "slot must belong to some table");
            boolean mustStayInCurrentAliasLayer = isNullableSide && !(alias.child() instanceof Slot);
            // Map nullable-side alias slots to subTreeNodes instead of the minimal
            // referenced bitmap. Otherwise a later join predicate like s=C.k sees s as
            // {B} (its input slot) and creates a {B}--{C} edge, allowing DPHyp to join
            // B and C before A — but s can only be emitted when {A,B} is complete.
            // Using subTreeNodes (e.g. {A,B}) forces the predicate edge to require the
            // full source subtree, matching the emission key in nodeToProjectedAliases.
            // Note: always use subTreeNodes for nullable-side aliases. A Slot-forwarding
            // alias (e.g. s=A.k) that shares a Project with expression aliases cannot
            // safely use the minimal bitmap — its layer only emits at {A,B}, so exposing
            // it as {A} would let DPHyp form predicate edges before the alias exists.
            slotToHyperNodeMap.put(aliasSlot, mustStayInCurrentAliasLayer ? subTreeNodes : bitmap);
            // Do not add aliases on the nullable side of outer joins to aliasReplaceMap.
            // Aliases on the nullable side (e.g., COALESCE(v, 0) AS dv on the right side of
            // a LEFT JOIN) must execute BEFORE the outer join's null-extension.
            // If added to aliasReplaceMap, they would be unwrapped and reconstructed above
            // the outer join by PlanReceiver.proposeProject(), changing execution order
            // and producing wrong results.
            // Instead, add them to the current Project layer. buildForDPhyper flushes each
            // layer to nodeToProjectedAliases keyed by subTreeNodes so the alias is only
            // projected when the full original subtree is available, preserving the
            // execution boundary and volatile materialization order.
            // No replaceNameExpression here: nullable-side aliases are stored as
            // independent layers that reference child-output slots, so expansion is
            // unnecessary and could trigger expression-limit failures for large chains
            // (the same reason PlanUtils.tryMergeProjections keeps layers).
            if (addToReplaceMap && mustStayInCurrentAliasLayer) {
                if (currentProjectedAliasLayer != null) {
                    // Resolve forwarding aliases (e.g., x → A.v) through
                    // aliasReplaceMap before storing the layer body.  A
                    // join-separated chain may first resolve a lower Slot
                    // alias into aliasReplaceMap and then build a later
                    // expression alias that references it.  Without this
                    // substitution the stored layer references a slot (x)
                    // that no child outputs, and CheckAfterRewrite fails.
                    // replaceNameExpression is safe here — it only replaces
                    // Alias slots whose source is already fully built in
                    // the same nullable subtree.
                    Alias resolved = (Alias) ExpressionUtils.replaceNameExpression(alias, aliasReplaceMap);
                    currentProjectedAliasLayer.add(resolved);
                }
                return true;
            }
            alias = (Alias) ExpressionUtils.replaceNameExpression(alias, aliasReplaceMap);
            if (addToReplaceMap) {
                aliasReplaceMap.put(aliasSlot, alias.child());
            }
            return true;
        }

        /**
         * add end node to HyperGraph
         *
         * @param group The group that is the end node in graph
         * @return return the node index
         */
        private int addDPHyperNode(Group group) {
            for (Slot slot : group.getLogicalExpression().getPlan().getOutput()) {
                slotToHyperNodeMap.put(slot, LongBitmap.newBitmap(nodes.size()));
            }
            nodes.add(new DPhyperNode(nodes.size(), group));
            return nodes.size() - 1;
        }

        private long calReferencedNodesBySlots(Set<Slot> slots) {
            long bitmap = LongBitmap.newBitmap();
            for (Slot slot : slots) {
                Preconditions.checkArgument(slotToHyperNodeMap.containsKey(slot));
                bitmap = LongBitmap.or(bitmap, slotToHyperNodeMap.get(slot));
            }
            return bitmap;
        }

        /**
         * try to add edge for join group ( only works for inner join!!! )
         * 1. consider original join node's conjuncts:
         * inner join conjuncts: t1.a = t2.a and t1.b = t3.b
         * 2. extract join conjuncts:
         * nodeToConjuncts contains 2 elements : (t1 and t2, t1.a = t2.a), (t1 and t3, t1.b = t3.b)
         * 3. based on nodeToConjuncts, create 2 new join node and edge:
         * inner join1 : t1.a = t2.a
         * inner join2 : t1.b = t3.b
         *
         * @param join                The join plan
         * @param leftChildEdgeNodes  first level left child edges and all nodes in the left tree
         * @param rightChildEdgeNodes first level right child edges and all nodes in the right tree
         * @return BitSet indicate the join edges' index in joinEdges produced by @param join
         */
        private BitSet addJoin(LogicalJoin<?, ?> join, Pair<BitSet, Long> leftChildEdgeNodes,
                               Pair<BitSet, Long> rightChildEdgeNodes) {
            // the join conjuncts may only use subset of leftNodes and rightNodes
            // extract inner join conjuncts to requiredNodesToConjuncts map
            // requiredNodesToConjuncts:
            // Pair<Long, Long> -> [leftNodes, rightNodes] only contains conjuncts' input slots referenced nodes
            // Pair<List<Expression>, List<Expression>> -> [hashConjuncts, otherConjuncts]
            long leftSubtreeNodes = leftChildEdgeNodes.second;
            long rightSubtreeNodes = rightChildEdgeNodes.second;
            Map<Pair<Long, Long>, Pair<List<Expression>, List<Expression>>> requiredNodesToConjuncts =
                    new LinkedHashMap<>();
            if (join.getExpressions().isEmpty()) {
                return addCrossJoin(join, leftChildEdgeNodes, rightChildEdgeNodes);
            } else {
                Pair<Long, Long> ends = calculateEndsBySlots(join.getInputSlots(), leftSubtreeNodes, rightSubtreeNodes);
                if (join.getJoinType().isInnerJoin()) {
                    // we split inner join conjuncts if the ends touch no nonInnerJoinedNodes;
                    if (LongBitmap.isOverlap(nonInnerJoinedNodes, ends.first) || LongBitmap.isOverlap(
                            nonInnerJoinedNodes, ends.second)) {
                        requiredNodesToConjuncts.put(ends,
                                Pair.of(join.getHashJoinConjuncts(), join.getOtherJoinConjuncts()));
                    } else {
                        // split join conjuncts to edges
                        for (Expression expression : join.getHashJoinConjuncts()) {
                            ends = calculateEndsBySlots(expression.getInputSlots(), leftSubtreeNodes,
                                    rightSubtreeNodes);
                            if (!requiredNodesToConjuncts.containsKey(ends)) {
                                requiredNodesToConjuncts.put(ends, Pair.of(new ArrayList<>(), new ArrayList<>()));
                            }
                            requiredNodesToConjuncts.get(ends).first.add(expression);
                        }
                        for (Expression expression : join.getOtherJoinConjuncts()) {
                            ends = calculateEndsBySlots(expression.getInputSlots(), leftSubtreeNodes,
                                    rightSubtreeNodes);
                            if (!requiredNodesToConjuncts.containsKey(ends)) {
                                requiredNodesToConjuncts.put(ends, Pair.of(new ArrayList<>(), new ArrayList<>()));
                            }
                            requiredNodesToConjuncts.get(ends).second.add(expression);
                        }
                    }
                } else {
                    nonInnerJoinedNodes |= LongBitmap.or(leftSubtreeNodes, rightSubtreeNodes);
                    requiredNodesToConjuncts.put(ends,
                            Pair.of(join.getHashJoinConjuncts(), join.getOtherJoinConjuncts()));
                }
            }

            BitSet curJoinEdges = new BitSet();
            for (Map.Entry<Pair<Long, Long>, Pair<List<Expression>, List<Expression>>> entry : requiredNodesToConjuncts
                    .entrySet()) {
                List<Expression> newHashConjuncts = ExpressionUtils.replace(entry.getValue().first, aliasReplaceMap);
                List<Expression> newOtherConjuncts = ExpressionUtils.replace(entry.getValue().second, aliasReplaceMap);
                LogicalJoin<?, ?> singleJoin =
                        new LogicalJoin<>(join.getJoinType(), newHashConjuncts, newOtherConjuncts,
                                new DistributeHint(DistributeType.NONE), join.getMarkJoinSlotReference(),
                                Lists.newArrayList(join.left(), join.right()), null);
                Pair<Long, Long> ends = entry.getKey();
                // we can see Edge contains following info
                // 1. LogicalJoin operator
                // 2. edgeIndex in joinEdges
                // 3. left and right first level child edge
                // 4. left and right subtree nodes
                // 5. left and right required nodes(produced by join conjuncts)
                Edge edge = new Edge(singleJoin, joinEdges.size(), leftChildEdgeNodes.first, rightChildEdgeNodes.first,
                        leftChildEdgeNodes.second, rightChildEdgeNodes.second, ends.first, ends.second);
                curJoinEdges.set(edge.getIndex());
                joinEdges.add(edge);
            }
            curJoinEdges.stream().forEach(
                    i -> ConflictRulesMaker.makeConflictRules(joinEdges.get(i), joinEdges, expressionRewriteCtx));
            return curJoinEdges;
        }

        private BitSet addCrossJoin(LogicalJoin<?, ?> join, Pair<BitSet, Long> leftChildEdgeNodes,
                                    Pair<BitSet, Long> rightChildEdgeNodes) {
            BitSet curJoinEdges = new BitSet();
            long leftSubtreeNodes = leftChildEdgeNodes.second;
            long rightSubtreeNodes = rightChildEdgeNodes.second;
            Pair<Long, Long> ends =
                    calculateEndsBySlots(Sets.union(join.left().getOutputSet(), join.right().getOutputSet()),
                            leftSubtreeNodes, rightSubtreeNodes);
            Edge edge = new Edge(join, joinEdges.size(), leftChildEdgeNodes.first, rightChildEdgeNodes.first,
                    leftSubtreeNodes, rightSubtreeNodes, ends.first, ends.second);
            curJoinEdges.set(edge.getIndex());
            joinEdges.add(edge);
            curJoinEdges.stream().forEach(
                    i -> ConflictRulesMaker.makeConflictRules(joinEdges.get(i), joinEdges, expressionRewriteCtx));
            return curJoinEdges;
        }

        // calculate the ends of an expression by its input slots.
        private Pair<Long, Long> calculateEndsBySlots(Set<Slot> usedSlots, long leftSubtreeNodes,
                                                      long rightSubtreeNodes) {
            long referenceNodes = calReferencedNodesBySlots(usedSlots);
            long left = LongBitmap.newBitmapIntersect(referenceNodes, leftSubtreeNodes);
            long right = LongBitmap.newBitmapIntersect(referenceNodes, rightSubtreeNodes);
            // handle degenerate predicates
            if (left == 0) {
                left = leftSubtreeNodes;
            }
            if (right == 0) {
                right = rightSubtreeNodes;
            }
            return Pair.of(left, right);
        }
    }
}

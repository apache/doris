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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/PlanNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.common.TreeNode;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.planner.normalize.ExprNormalizeVisitor;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TPlan;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Each PlanNode represents a single relational operator
 * and encapsulates the information needed by the planner to
 * make optimization decisions.
 * <p/>
 * finalize(): Computes internal state, such as keys for scan nodes; gets called once on
 * the root of the plan tree before the call to toThrift(). Also finalizes the set
 * of conjuncts, such that each remaining one requires all of its referenced slots to
 * be materialized (ie, can be evaluated by calling GetValue(), rather than being
 * implicitly evaluated as part of a scan key).
 * <p/>
 * conjuncts: Each node has a list of conjuncts that can be executed in the context of
 * this node, ie, they only reference tuples materialized by this node or one of
 * its children (= are bound by tupleIds).
 */
public abstract class PlanNode extends TreeNode<PlanNode> {
    protected String planNodeName;

    protected PlanNodeId id;  // unique w/in plan tree; assigned by planner
    protected long limit; // max. # of rows to be returned; 0: no limit
    protected long offset;

    // ids materialized by the tree rooted at this node
    protected ArrayList<TupleId> tupleIds;

    protected List<Expr> conjuncts = Lists.newArrayList();

    // Conjuncts used to filter the original load file.
    // In the load execution plan, the difference between "preFilterConjuncts" and "conjuncts" is that
    // conjuncts are used to filter the data after column conversion and mapping,
    // while fileFilterConjuncts directly filter the content read from the source data.
    // That is, the data processing flow is:
    //
    //  1. Read data from source.
    //  2. Filter data by using "preFilterConjuncts".
    //  3. Do column mapping and transforming.
    //  4. Filter data by using "conjuncts".
    protected List<Expr> preFilterConjuncts = Lists.newArrayList();

    // Fragment that this PlanNode is executed in. Valid only after this PlanNode has been
    // assigned to a fragment. Set and maintained by enclosing PlanFragment.
    protected PlanFragment fragment;

    // estimate of the output cardinality of this node; set in computeStats();
    // invalid: -1
    protected long cardinality;

    protected long cardinalityAfterFilter = -1;

    // number of nodes on which the plan tree rooted at this node would execute;
    // set in computeStats(); invalid: -1
    protected int numNodes;

    // sum of tupleIds' avgSerializedSizes; set in computeStats()
    protected float avgRowSize;

    // Most of the plan node has the same numInstance as its (left) child, except some special nodes, such as
    // 1. scan node, whose numInstance is calculated according to its data distribution
    // 2. exchange node, which is gather distribution
    // 3. union node, whose numInstance is the sum of its children's numInstance
    // ...
    // only special nodes need to call setNumInstances() and getNumInstances() from attribute numInstances
    protected int numInstances;

    // Runtime filters assigned to this node.
    protected List<RuntimeFilter> runtimeFilters = new ArrayList<>();

    protected List<SlotId> outputSlotIds;

    protected TupleDescriptor outputTupleDesc;
    protected List<Expr> projectList;
    private final List<TupleDescriptor> intermediateOutputTupleDescList = Lists.newArrayList();
    private final List<List<Expr>> intermediateProjectListList = Lists.newArrayList();

    protected int nereidsId = -1;

    // Per-child hash-distribution key exprs: childrenDistributeExprLists.get(i) is the expr list
    // used to (re)partition this node's i-th child's input — consumed by getChildDistributeExprList()
    // when deriving local-exchange keys.
    protected List<List<Expr>> childrenDistributeExprLists = new ArrayList<>();
    // This node's own output hash-distribution key exprs — serialized to BE for its LocalExchange /
    // shuffle (see distributeExprLists()).
    protected List<Expr> distributeExprLists = new ArrayList<>();

    protected PlanNode(PlanNodeId id, List<TupleId> tupleIds, String planNodeName) {
        this.id = id;
        this.limit = -1;
        this.offset = 0;
        // make a copy, just to be on the safe side
        this.tupleIds = Lists.newArrayList(tupleIds);
        this.cardinality = -1;
        this.planNodeName = "V" + planNodeName;
        this.numInstances = 1;
    }

    protected PlanNode(PlanNodeId id, String planNodeName) {
        this.id = id;
        this.limit = -1;
        this.tupleIds = Lists.newArrayList();
        this.cardinality = -1;
        this.planNodeName = "V" + planNodeName;
        this.numInstances = 1;
    }

    /**
     * Copy ctor. Also passes in new id.
     */
    protected PlanNode(PlanNodeId id, PlanNode node, String planNodeName) {
        this.id = id;
        this.limit = node.limit;
        this.offset = node.offset;
        this.tupleIds = Lists.newArrayList(node.tupleIds);
        this.conjuncts = Expr.cloneList(node.conjuncts, null);

        this.cardinality = -1;
        this.planNodeName = "V" + planNodeName;
        this.numInstances = 1;
    }

    public String getPlanNodeName() {
        return planNodeName;
    }

    /**
     * Clears tblRefIds_, tupleIds_, and nullableTupleIds_.
     */
    protected void clearTupleIds() {
        tupleIds.clear();
    }

    protected void setPlanNodeName(String s) {
        this.planNodeName = s;
    }

    public PlanNodeId getId() {
        return id;
    }

    public PlanFragmentId getFragmentId() {
        return fragment.getFragmentId();
    }

    public int getFragmentSeqenceNum() {
        return fragment.getFragmentSequenceNum();
    }

    public void setFragment(PlanFragment fragment) {
        this.fragment = fragment;
    }

    public PlanFragment getFragment() {
        return fragment;
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
    }

    /**
     * Set the limit to the given limit only if the limit hasn't been set, or the new limit
     * is lower.
     *
     * @param limit
     */
    public void setLimit(long limit) {
        if (this.limit == -1 || (limit != -1 && this.limit > limit)) {
            this.limit = limit;
        }
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public boolean hasLimit() {
        return limit > -1;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }

    /**
     * only used for rf ndv computation
     */
    public long getCardinality() {
        return cardinality;
    }

    /**
     * only used for rf ndv computation
     */
    public long getCardinalityAfterFilter() {
        if (cardinalityAfterFilter < 0) {
            return cardinality;
        } else {
            return cardinalityAfterFilter;
        }
    }

    public int getNumNodes() {
        return numNodes;
    }

    public ArrayList<TupleId> getTupleIds() {
        Preconditions.checkState(tupleIds != null);
        return tupleIds;
    }

    public List<TupleId> getOutputTupleIds() {
        if (outputTupleDesc != null) {
            return Lists.newArrayList(outputTupleDesc.getId());
        }
        return tupleIds;
    }

    public List<Expr> getConjuncts() {
        return conjuncts;
    }

    /**
     * NOTICE: this function is only used for explain
     */
    public static Expr convertConjunctsToAndCompoundPredicate(List<Expr> conjuncts) {
        List<Expr> targetConjuncts = Lists.newArrayList(conjuncts);
        while (targetConjuncts.size() > 1) {
            List<Expr> newTargetConjuncts = Lists.newArrayList();
            for (int i = 0; i < targetConjuncts.size(); i += 2) {
                Expr expr = i + 1 < targetConjuncts.size()
                        ? new CompoundPredicate(CompoundPredicate.Operator.AND, targetConjuncts.get(i),
                        targetConjuncts.get(i + 1), true) : targetConjuncts.get(i);
                newTargetConjuncts.add(expr);
            }
            targetConjuncts = newTargetConjuncts;
        }

        Preconditions.checkArgument(targetConjuncts.size() == 1);
        return targetConjuncts.get(0);
    }

    public void addConjuncts(List<Expr> conjuncts) {
        if (conjuncts == null) {
            return;
        }
        for (Expr conjunct : conjuncts) {
            addConjunct(conjunct);
        }
    }

    public void addConjunct(Expr conjunct) {
        if (conjuncts == null) {
            conjuncts = Lists.newArrayList();
        }
        if (!conjuncts.contains(conjunct)) {
            conjuncts.add(conjunct);
        }
    }

    public void addPreFilterConjuncts(List<Expr> conjuncts) {
        if (conjuncts == null) {
            return;
        }
        this.preFilterConjuncts.addAll(conjuncts);
    }

    public String getExplainString() {
        return getExplainString("", "", TExplainLevel.VERBOSE);
    }

    /**
     * Generate the explain plan tree. The plan will be in the form of:
     * <p/>
     * root
     * |
     * |----child 2
     * |      limit:1
     * |
     * |----child 3
     * |      limit:2
     * |
     * child 1
     * <p/>
     * The root node header line will be prefixed by rootPrefix and the remaining plan
     * output will be prefixed by prefix.
     */
    protected final String getExplainString(String rootPrefix, String prefix, TExplainLevel detailLevel) {
        StringBuilder expBuilder = new StringBuilder();
        String detailPrefix = prefix;
        boolean traverseChildren = children != null
                && children.size() > 0
                && !(this instanceof ExchangeNode);
        // if (children != null && children.size() > 0) {
        if (traverseChildren) {
            detailPrefix += "|  ";
        } else {
            detailPrefix += "   ";
        }

        // Print the current node
        // The plan node header line will be prefixed by rootPrefix and the remaining details
        // will be prefixed by detailPrefix.
        expBuilder.append(rootPrefix + id.asInt() + ":" + planNodeName);
        if (nereidsId != -1) {
            expBuilder.append("(" + nereidsId + ")");
        }
        expBuilder.append("\n");
        expBuilder.append(getNodeExplainString(detailPrefix, detailLevel));
        if (!runtimeFilters.isEmpty()) {
            expBuilder.append(detailPrefix).append("runtime filters: ");
            expBuilder.append(getRuntimeFilterExplainString());
        }
        if (limit != -1) {
            expBuilder.append(detailPrefix + "limit: " + limit + "\n");
        }
        if (!CollectionUtils.isEmpty(projectList)) {
            expBuilder.append(detailPrefix).append("final projections: ")
                .append(getExplainString(projectList)).append("\n");
            expBuilder.append(detailPrefix).append("final project output tuple id: ")
                    .append(outputTupleDesc.getId().asInt()).append("\n");
        }
        if (!intermediateProjectListList.isEmpty()) {
            int layers = intermediateProjectListList.size();
            for (int i = layers - 1; i >= 0; i--) {
                expBuilder.append(detailPrefix).append("intermediate projections: ")
                        .append(getExplainString(intermediateProjectListList.get(i))).append("\n");
                expBuilder.append(detailPrefix).append("intermediate tuple id: ")
                        .append(intermediateOutputTupleDescList.get(i).getId().asInt()).append("\n");
            }
        }
        if (!CollectionUtils.isEmpty(childrenDistributeExprLists)) {
            for (List<Expr> distributeExprList : childrenDistributeExprLists) {
                expBuilder.append(detailPrefix).append("distribute expr lists: ")
                    .append(getExplainString(distributeExprList)).append("\n");
            }
        }
        // Output Tuple Ids only when explain plan level is set to verbose
        if (detailLevel.equals(TExplainLevel.VERBOSE)) {
            expBuilder.append(detailPrefix + "tuple ids: ");
            for (TupleId tupleId : tupleIds) {
                expBuilder.append(tupleId.asInt() + " ");
            }
            expBuilder.append("\n");
        }

        // Print the children
        // if (children != null && children.size() > 0) {
        if (traverseChildren) {
            expBuilder.append(detailPrefix + "\n");
            String childHeadlinePrefix = prefix + "|----";
            String childDetailPrefix = prefix + "|    ";
            for (int i = 1; i < children.size(); ++i) {
                expBuilder.append(
                        children.get(i).getExplainString(childHeadlinePrefix, childDetailPrefix,
                                detailLevel));
                expBuilder.append(childDetailPrefix + "\n");
            }
            expBuilder.append(children.get(0).getExplainString(prefix, prefix, detailLevel));
        }
        return expBuilder.toString();
    }

    private String getPlanNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder expBuilder = new StringBuilder();
        expBuilder.append(getNodeExplainString(prefix, detailLevel));
        if (limit != -1) {
            expBuilder.append(prefix + "limit: " + limit + "\n");
        }
        if (!CollectionUtils.isEmpty(projectList)) {
            expBuilder.append(prefix).append("projections: ").append(getExplainString(projectList)).append("\n");
            expBuilder.append(prefix).append("project output tuple id: ")
                    .append(outputTupleDesc.getId().asInt()).append("\n");
        }
        return expBuilder.toString();
    }

    public void getExplainStringMap(TExplainLevel detailLevel, Map<Integer, String> planNodeMap) {
        planNodeMap.put(id.asInt(), getPlanNodeExplainString("", detailLevel));
        for (int i = 0; i < children.size(); ++i) {
            children.get(i).getExplainStringMap(detailLevel, planNodeMap);
        }
    }

    /**
     * Return the node-specific details.
     * Subclass should override this function.
     * Each line should be prefix by detailPrefix.
     */
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return "";
    }

    // Convert this plan node, including all children, to its Thrift representation.
    public TPlan treeToThrift() {
        TPlan result = new TPlan();
        treeToThriftHelper(result);
        return result;
    }

    // Append a flattened version of this plan node, including all children, to 'container'.
    private void treeToThriftHelper(TPlan container) {
        TPlanNode msg = new TPlanNode();
        msg.node_id = id.asInt();
        msg.setNereidsId(nereidsId);
        msg.setIsSerialOperator(isSerialOperatorOnBe(ConnectContext.get()));
        msg.num_children = children.size();
        msg.limit = limit;
        for (TupleId tid : tupleIds) {
            msg.addToRowTuples(tid.asInt());
        }
        msg.setNullableTuples(Collections.emptyList());

        for (Expr e : conjuncts) {
            msg.addToConjuncts(ExprToThriftVisitor.treeToThrift(e));
        }

        // Serialize any runtime filters
        for (RuntimeFilter filter : runtimeFilters) {
            msg.addToRuntimeFilters(filter.toThrift());
        }

        msg.compact_data = false;
        if (outputSlotIds != null) {
            for (SlotId slotId : outputSlotIds) {
                msg.addToOutputSlotIds(slotId.asInt());
            }
        }
        if (!CollectionUtils.isEmpty(childrenDistributeExprLists)) {
            for (List<Expr> exprList : childrenDistributeExprLists) {
                msg.addToDistributeExprLists(new ArrayList<>());
                for (Expr expr : exprList) {
                    msg.distribute_expr_lists.get(msg.distribute_expr_lists.size() - 1)
                            .add(ExprToThriftVisitor.treeToThrift(expr));
                }
            }
        }
        toThrift(msg);
        container.addToNodes(msg);

        if (outputTupleDesc != null) {
            msg.setOutputTupleId(outputTupleDesc.getId().asInt());
        }
        if (projectList != null) {
            for (Expr expr : projectList) {
                msg.addToProjections(ExprToThriftVisitor.treeToThrift(expr));
            }
        }

        if (!intermediateOutputTupleDescList.isEmpty()) {
            intermediateOutputTupleDescList
                    .forEach(
                            tupleDescriptor -> msg.addToIntermediateOutputTupleIdList(tupleDescriptor.getId().asInt()));
        }

        if (!intermediateProjectListList.isEmpty()) {
            intermediateProjectListList.forEach(
                    projectList -> msg.addToIntermediateProjectionsList(
                            projectList.stream().map(expr -> ExprToThriftVisitor.treeToThrift(expr))
                                    .collect(Collectors.toList())));
        }

        if (this instanceof ExchangeNode) {
            msg.num_children = 0;
            return;
        } else {
            msg.num_children = children.size();
            for (PlanNode child : children) {
                child.treeToThriftHelper(container);
            }
        }
    }

    protected void computeNumNodes() {
        if (!children.isEmpty()) {
            numNodes = getChild(0).numNodes;
        }
    }

    protected void capCardinalityAtLimit() {
        if (hasLimit()) {
            cardinality = cardinality == -1 ? limit : Math.min(cardinality, limit);
        }
    }

    public void init() throws UserException {}

    // Convert this plan node into msg (excluding children), which requires setting
    // the node type and the node-specific field.
    protected abstract void toThrift(TPlanNode msg);

    public TNormalizedPlanNode normalize(Normalizer normalizer) {
        TNormalizedPlanNode normalizedPlan = new TNormalizedPlanNode();
        normalizedPlan.setNodeId(normalizer.normalizePlanId(id.asInt()));
        normalizedPlan.setNumChildren(children.size());
        Set<Integer> tupleIds = this.tupleIds
                .stream()
                .map(Id::asInt)
                .collect(Collectors.toSet());
        normalizedPlan.setTupleIds(
                tupleIds.stream()
                    .map(normalizer::normalizeTupleId)
                    .collect(Collectors.toSet())
        );
        normalize(normalizedPlan, normalizer);
        normalizeConjuncts(normalizedPlan, normalizer);
        normalizeProjects(normalizedPlan, normalizer);
        normalizedPlan.setLimit(limit);
        return normalizedPlan;
    }

    public void normalize(TNormalizedPlanNode normalizedPlan, Normalizer normalizer) {
        throw new IllegalStateException("Unsupported normalization");
    }

    protected void normalizeProjects(TNormalizedPlanNode normalizedPlanNode, Normalizer normalizer) {
        throw new IllegalStateException("Unsupported normalize project for " + getClass().getSimpleName());
    }

    public List<TExpr> normalizeProjects(
            List<SlotDescriptor> outputSlotDescs, List<Expr> projects, Normalizer normalizer) {
        Map<SlotId, Expr> outputSlotToProject = Maps.newLinkedHashMap();
        for (int i = 0; i < outputSlotDescs.size(); i++) {
            SlotId slotId = outputSlotDescs.get(i).getId();
            Expr projectExpr = projects.get(i);
            if (projectExpr instanceof SlotRef) {
                int outputSlotId = slotId.asInt();
                int refId = ((SlotRef) projectExpr).getSlotId().asInt();
                normalizer.setSlotIdToNormalizeId(outputSlotId, normalizer.normalizeSlotId(refId));
            }
            outputSlotToProject.put(slotId, projectExpr);
        }
        return normalizeProjects(outputSlotToProject, normalizer);
    }

    protected void normalizeConjuncts(TNormalizedPlanNode normalizedPlan, Normalizer normalizer) {
        normalizedPlan.setConjuncts(normalizeExprs(getConjuncts(), normalizer));
    }

    protected List<TExpr> normalizeProjects(Map<SlotId, Expr> project, Normalizer normalizer) {
        List<Pair<SlotId, TExpr>> sortByTExpr = Lists.newArrayListWithCapacity(project.size());
        for (Entry<SlotId, Expr> kv : project.entrySet()) {
            SlotId slotId = kv.getKey();
            Expr expr = kv.getValue();
            TExpr thriftExpr = ExprNormalizeVisitor.normalize(expr, normalizer);
            sortByTExpr.add(Pair.of(slotId, thriftExpr));
        }
        sortByTExpr.sort(Comparator.comparing(Pair::value));

        // we should normalize slot id by fix order, then the upper nodes can reference the same normalized slot id
        for (Pair<SlotId, TExpr> pair : sortByTExpr) {
            int originOutputSlotId = pair.first.asInt();
            normalizer.normalizeSlotId(originOutputSlotId);
        }

        return sortByTExpr.stream().map(Pair::value).collect(Collectors.toList());
    }

    public static List<TExpr> normalizeExprs(Collection<? extends Expr> exprs, Normalizer normalizer) {
        List<TExpr> normalizedWithoutSort = Lists.newArrayListWithCapacity(exprs.size());
        for (Expr expr : exprs) {
            normalizedWithoutSort.add(ExprNormalizeVisitor.normalize(expr, normalizer));
        }
        normalizedWithoutSort.sort(Comparator.naturalOrder());
        return normalizedWithoutSort;
    }

    public static String getExplainString(List<? extends Expr> exprs) {
        if (exprs == null) {
            return "";
        }
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < exprs.size(); ++i) {
            if (i > 0) {
                output.append(", ");
            }
            output.append(exprs.get(i).accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
        }
        return output.toString();
    }

    public int getNumInstances() {
        return this.children.get(0).getNumInstances();
    }

    public void setNumInstances(int numInstances) {
        this.numInstances = numInstances;
    }

    @Deprecated
    public void appendTrace(StringBuilder sb) {
        sb.append(planNodeName);
        if (!children.isEmpty()) {
            sb.append("(");
            int idx = 0;
            for (PlanNode child : children) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                child.appendTrace(sb);
            }
            sb.append(")");
        }
    }

    /**
     * find planNode recursively based on the planNodeId
     */
    @Deprecated
    public static PlanNode findPlanNodeFromPlanNodeId(PlanNode root, PlanNodeId id) {
        if (root == null || root.getId() == null || id == null) {
            return null;
        } else if (root.getId().equals(id)) {
            return root;
        } else {
            for (PlanNode child : root.getChildren()) {
                PlanNode retNode = findPlanNodeFromPlanNodeId(child, id);
                if (retNode != null) {
                    return retNode;
                }
            }
            return null;
        }
    }

    public String getPlanTreeExplainStr() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(getId().asInt()).append(": ").append(getPlanNodeName()).append("]");
        sb.append("\n[Fragment: ").append(getFragmentSeqenceNum()).append("]");
        sb.append("\n").append(getNodeExplainString("", TExplainLevel.BRIEF));
        return sb.toString();
    }

    protected void addRuntimeFilter(RuntimeFilter filter) {
        runtimeFilters.add(filter);
    }

    protected String getRuntimeFilterExplainString() {
        if (runtimeFilters.isEmpty()) {
            return "";
        }
        List<String> filtersStr = new ArrayList<>();
        for (RuntimeFilter filter : runtimeFilters) {
            filtersStr.add(filter.getExplainString(getId()));
        }
        return Joiner.on(", ").join(filtersStr) + "\n";
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(getId().asInt()).append(": ").append(getPlanNodeName()).append("]");
        sb.append("\nFragment: ").append(getFragmentId().asInt()).append("]");
        sb.append("\n").append(getNodeExplainString("", TExplainLevel.BRIEF));
        return sb.toString();
    }

    /**
     * Supplement the information be needed for nodes generated by the new optimizer.
     */
    public void finalizeForNereids() throws UserException {

    }

    public void setOutputTupleDesc(TupleDescriptor outputTupleDesc) {
        this.outputTupleDesc = outputTupleDesc;
    }

    public TupleDescriptor getOutputTupleDesc() {
        return outputTupleDesc;
    }

    public void setProjectList(List<Expr> projectList) {
        this.projectList = projectList;
    }

    public List<Expr> getProjectList() {
        return projectList;
    }

    public List<Expr> getPointQueryProjectList() {
        if (CollectionUtils.isEmpty(projectList) || intermediateProjectListList.isEmpty()) {
            return projectList;
        }

        List<Expr> flattenedProjectList = Expr.cloneList(projectList);
        for (int i = intermediateProjectListList.size() - 1; i >= 0; --i) {
            flattenedProjectList = Expr.cloneList(
                    flattenedProjectList,
                    createPointQueryProjectionSmap(intermediateOutputTupleDescList.get(i),
                            intermediateProjectListList.get(i)));
        }
        return flattenedProjectList;
    }

    public void setCardinalityAfterFilter(long cardinalityAfterFilter) {
        this.cardinalityAfterFilter = cardinalityAfterFilter;
    }

    protected TPushAggOp pushDownAggNoGroupingOp = TPushAggOp.NONE;

    public void setPushDownAggNoGrouping(TPushAggOp pushDownAggNoGroupingOp) {
        this.pushDownAggNoGroupingOp = pushDownAggNoGroupingOp;
    }

    public void setChildrenDistributeExprLists(List<List<Expr>> childrenDistributeExprLists) {
        this.childrenDistributeExprLists = childrenDistributeExprLists;
    }

    public TPushAggOp getPushDownAggNoGroupingOp() {
        return pushDownAggNoGroupingOp;
    }

    public void setNereidsId(int nereidsId) {
        this.nereidsId = nereidsId;
    }

    public void addIntermediateOutputTupleDescList(TupleDescriptor tupleDescriptor) {
        intermediateOutputTupleDescList.add(tupleDescriptor);
    }

    public void addIntermediateProjectList(List<Expr> exprs) {
        intermediateProjectListList.add(exprs);
    }

    private ExprSubstitutionMap createPointQueryProjectionSmap(
            TupleDescriptor outputTupleDesc, List<Expr> projectionExprs) {
        List<SlotDescriptor> outputSlots = outputTupleDesc.getSlots();
        Preconditions.checkState(outputSlots.size() == projectionExprs.size(),
                "point query projection slot size %s does not match expr size %s",
                outputSlots.size(), projectionExprs.size());

        ExprSubstitutionMap substitutionMap = new ExprSubstitutionMap();
        for (int i = 0; i < outputSlots.size(); ++i) {
            substitutionMap.put(new SlotRef(outputSlots.get(i)), projectionExprs.get(i));
        }
        return substitutionMap;
    }

    public <T extends PlanNode> List<T> collectInCurrentFragment(Predicate<PlanNode> predicate) {
        List<PlanNode> result = Lists.newArrayList();
        foreachDownInCurrentFragment(child -> {
            if (predicate.test(child)) {
                result.add(child);
            }
        });
        return (List) result;
    }

    /** foreachDownInCurrentFragment */
    public void foreachDownInCurrentFragment(Consumer<PlanNode> visitor) {
        int currentFragmentId = getFragmentId().asInt();
        foreachDown(child -> {
            PlanNode childNode = (PlanNode) child;
            if (childNode.getFragmentId().asInt() != currentFragmentId) {
                return false;
            }
            visitor.accept(childNode);
            return true;
        });
    }

    /**
     * Node-level "is this operator inherently serial" property — answers without looking
     * at the fragment.  Default false; subclasses override (e.g. finalized agg without key,
     * UNPARTITIONED ExchangeNode with merge sort).
     *
     * Use ONLY in framework-internal places where we are already iterating within a
     * fragment whose serial-source mode is fixed: {@link #shouldResetSerialFlagForChild}
     * inputs, {@link #createLocalExchange} heavy-op gate, and child.isSerialNode() checks
     * embedded inside an enforceRequire path.  Do NOT use it when computing a
     * {@link LocalExchangeNode.LocalExchangeTypeRequire} on a child — call
     * {@link #isSerialOperatorOnBe} instead.
     */
    public boolean isSerialNode() {
        return false;
    }

    /**
     * Whether this node will be reported to BE as {@code is_serial_operator=true}, i.e. it
     * actually runs with one task on BE.  Composes {@link #isSerialNode} with the fragment's
     * {@code useSerialSource(context)} — when the fragment is not in serial-source mode
     * even an isSerialNode()=true operator still runs with N tasks.
     *
     * <p>This is the API to use when deciding what {@code LocalExchangeTypeRequire} to
     * declare for a child in {@code enforceAndDeriveLocalExchange}.  Using
     * {@link #isSerialNode} directly there will compute the wrong require under
     * non-serial-source fragments and misses the {@code hasSerialScanNode()} contribution
     * that {@link ExchangeNode#isSerialOperatorOnBe} layers in.  Getting this wrong
     * silently produces wrong results (serial child feeds N-task parent without LE).
     *
     * <p>Must match the condition in {@code toThrift()/treeToThriftHelper()}; subclasses
     * (ExchangeNode) override to fold in {@code hasSerialScanNode()}.
     */
    public boolean isSerialOperatorOnBe(ConnectContext context) {
        return fragment != null && isSerialNode() && fragment.useSerialSource(context);
    }

    /**
     * "I depend on hash distribution for correctness, not just performance optimization."
     * Used by UnionNode to decide whether to propagate hash requirement to its inputs:
     * when a downstream operator requires shuffle for correctness, Union must pre-shuffle
     * its inputs so the merged output is hash-distributed.
     *
     * Default is false; only operators that truly need hash for correctness override
     * (finalize AggSink with group keys, HashJoin PARTITIONED/BUCKET_SHUFFLE, Intersect,
     * Except, analytic SortNode, partition-by AnalyticEvalNode).  Operators that request
     * hash for performance only (StreamingAgg pre-agg with enable_local_exchange_before_agg)
     * MUST NOT override — that would cause SetOperationNode to over-insert HASH LE on
     * every union branch even when nothing downstream actually needs correctness shuffling.
     *
     * Mirrors BE's OperatorBase::is_shuffled_operator().
     *
     * <h3>Propagation example — multi-distinct over UNION</h3>
     * <pre>
     *   AggGlobal(finalize, hasKeys)             ← override = true (chain start)
     *     └─ Agg(DISTINCT_LOCAL, !finalize)      ← override = false, inherits via
     *                                              inheritedShuffled in enforceRequire 1b
     *          └─ Agg(FIRST_MERGE, !finalize)    ← override = false, inherits
     *               └─ Agg(FIRST_LOCAL, ...)     ← override = false, inherits
     *                    └─ Union                ← reads inheritedShuffled=true and
     *                                              pre-shuffles each branch
     *                         ├─ Scan_t1
     *                         └─ Scan_t2
     * </pre>
     *
     * Only the top-level correctness consumer needs to override true.  Mid-chain
     * merge / local phases do NOT need to — the flag flows down through
     * {@link PlanTranslatorContext#hasShuffleForCorrectnessAncestor} automatically as long
     * as every link in the chain requires HASH or NOOP (see {@code enforceRequire} step 1b).
     *
     * <h3>What happens if you forget to override</h3>
     * <ul>
     *   <li><b>Short chain (top consumer directly above Union)</b>: Union doesn't
     *       pre-shuffle its branches, but {@code enforceRequire} inserts a fallback
     *       LE(HASH) between the consumer and Union.  Data result is still correct,
     *       but the plan shape differs from BE-planned mode (one extra fan-in→fan-out).</li>
     *   <li><b>Long chain</b>: same outcome as short chain, because the fallback LE
     *       is inserted at the consumer/Union boundary regardless of chain length.</li>
     *   <li><b>The real wrong-result risk</b> is when {@code enforceRequire}'s fallback
     *       LE is skipped — e.g. Layer 1 skip when a serial ancestor sits between the
     *       consumer and Union.  In practice top-level correctness consumers (finalize
     *       agg, hash join, etc.) are not under serial ancestors so this is rare, but
     *       the override is the principled fix.</li>
     * </ul>
     */
    public boolean requiresShuffleForCorrectness() {
        return false;
    }

    public boolean hasSerialChildren() {
        if (children.isEmpty()) {
            return isSerialNode();
        }
        return children.stream().allMatch(PlanNode::hasSerialChildren);
    }

    public boolean hasSerialScanChildren() {
        if (children.isEmpty()) {
            return false;
        }
        return children.stream().anyMatch(PlanNode::hasSerialScanChildren);
    }

    protected void printNestedColumns(StringBuilder output, String prefix, TupleDescriptor tupleDesc) {
        boolean printNestedColumnsHeader = true;
        for (SlotDescriptor slot : tupleDesc.getSlots()) {
            String prunedType = null;
            if (slot.getColumn() != null && !slot.getType().equals(slot.getColumn().getType())) {
                prunedType = slot.getType().toString();
            }
            String displayAllAccessPathsString = null;
            if (slot.getDisplayAllAccessPaths() != null
                    && slot.getDisplayAllAccessPaths() != null
                    && !slot.getDisplayAllAccessPaths().isEmpty()) {
                if (this instanceof IcebergScanNode) {
                    displayAllAccessPathsString = mergeIcebergAccessPathsWithId(
                            slot.getAllAccessPaths(),
                            slot.getDisplayAllAccessPaths()
                    );
                } else {
                    displayAllAccessPathsString = slot.getDisplayAllAccessPaths()
                            .stream()
                            .map(a -> StringUtils.join(a.getPath(), "."))
                            .collect(Collectors.joining(", "));
                }
            }
            String displayPredicateAccessPathsString = null;
            if (slot.getDisplayPredicateAccessPaths() != null
                    && slot.getDisplayPredicateAccessPaths() != null
                    && !slot.getDisplayPredicateAccessPaths().isEmpty()) {
                if (this instanceof IcebergScanNode) {
                    displayPredicateAccessPathsString = mergeIcebergAccessPathsWithId(
                            slot.getPredicateAccessPaths(),
                            slot.getDisplayPredicateAccessPaths()
                    );
                } else {
                    displayPredicateAccessPathsString = slot.getPredicateAccessPaths()
                            .stream()
                            .map(a -> StringUtils.join(a.getPath(), "."))
                            .collect(Collectors.joining(", "));
                }
            }


            List<String> subColLables = slot.getSubColLables();
            if (prunedType == null
                    && displayAllAccessPathsString == null
                    && displayPredicateAccessPathsString == null
                    && (subColLables == null || subColLables.isEmpty())) {
                continue;
            }

            if (printNestedColumnsHeader) {
                output.append(prefix).append("nested columns:\n");
                printNestedColumnsHeader = false;
            }
            output.append(prefix).append("  ").append(slot.getColumn().getName()).append(":\n");
            output.append(prefix).append("    origin type: ").append(slot.getColumn().getType()).append("\n");
            if (prunedType != null) {
                output.append(prefix).append("    pruned type: ").append(prunedType).append("\n");
            }
            if (subColLables != null && !subColLables.isEmpty()) {
                output.append(prefix).append("    sub path: [")
                        .append(String.join(".", subColLables)).append("]\n");
            }
            if (displayAllAccessPathsString != null) {
                output.append(prefix).append("    all access paths: [")
                        .append(displayAllAccessPathsString).append("]\n");
            }
            if (displayPredicateAccessPathsString != null) {
                output.append(prefix).append("    predicate access paths: [")
                        .append(displayPredicateAccessPathsString).append("]\n");
            }
        }
    }

    private String mergeIcebergAccessPathsWithId(
            List<ColumnAccessPath> accessPaths, List<ColumnAccessPath> displayAccessPaths) {
        List<String> mergeDisplayAccessPaths = Lists.newArrayList();
        for (int i = 0; i < displayAccessPaths.size(); i++) {
            ColumnAccessPath displayAccessPath = displayAccessPaths.get(i);
            ColumnAccessPath idAccessPath = accessPaths.get(i);
            List<String> nameAccessPathStrings = displayAccessPath.getPath();
            List<String> idAccessPathStrings = idAccessPath.getPath();

            List<String> mergedPath = new ArrayList<>();
            for (int j = 0; j < idAccessPathStrings.size(); j++) {
                String name = nameAccessPathStrings.get(j);
                String id = idAccessPathStrings.get(j);
                if (name.equals(id)) {
                    mergedPath.add(name);
                } else {
                    mergedPath.add(name + "(" + id + ")");
                }
            }
            mergeDisplayAccessPaths.add(StringUtils.join(mergedPath, "."));
        }
        return StringUtils.join(mergeDisplayAccessPaths, ", ");
    }

    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(
            PlanTranslatorContext translatorContext, PlanNode parent, LocalExchangeTypeRequire parentRequire) {
        ArrayList<PlanNode> newChildren = Lists.newArrayList();
        for (int i = 0; i < children.size(); i++) {
            Pair<PlanNode, LocalExchangeType> childOutput
                    = enforceRequire(translatorContext, children.get(i), i, LocalExchangeTypeRequire.noRequire());
            newChildren.add(childOutput.first);
        }
        this.children = newChildren;
        return Pair.of(this, LocalExchangeType.NOOP);
    }

    /**
     * Unified framework method: propagate serial flag → recurse child → satisfy check → Layer 1 skip → insert LE.
     * Replaces the old enforceChild/enforceChildExchange/forceEnforceChildExchange trio.
     *
     * <h3>Data flow</h3>
     * <ul>
     *   <li><b>serial-ancestor flag</b> ({@link PlanTranslatorContext#hasSerialAncestorInPipeline})
     *       — flows root → leaf during traversal.  Mirrors BE's
     *       {@code any_of(operators[idx..end], is_serial_operator)} check used by
     *       {@code _add_local_exchange} to skip LE insertion when an ancestor in the same
     *       pipeline is already serial.  Reset at pipeline boundaries via
     *       {@link #shouldResetSerialFlagForChild}.</li>
     *   <li><b>shuffle-for-correctness flag</b>
     *       ({@link PlanTranslatorContext#hasShuffleForCorrectnessAncestor}) — also flows
     *       root → leaf.  Mirrors BE's {@code _followed_by_shuffled_operator}: tells a
     *       child whether some downstream operator depends on hash distribution for
     *       correctness, so {@code SetOperationNode} can pre-shuffle union branches.</li>
     *   <li><b>return value</b> {@code Pair<PlanNode, LocalExchangeType>} — first is the
     *       (possibly LE-wrapped) child; second is the actual output distribution as
     *       observed by the parent.  Caller's {@code require.satisfy(output)} decides
     *       whether more LE is needed.</li>
     *   <li><b>parent.require</b> describes the constraint on the child output —
     *       computed inside the parent's {@code enforceAndDeriveLocalExchange} per child.</li>
     * </ul>
     *
     * <h3>Invariants</h3>
     * <ul>
     *   <li>Where a serial → non-serial transition needs redistribution, framework step 3 inserts
     *       the LE (e.g. a serial source fanned out via PASSTHROUGH).  This is not a hard invariant:
     *       a serial child feeding a parent that requires PASSTHROUGH / noRequire (TableFunction,
     *       NLJ, Agg) is already correct and needs no LE, so it is intentionally not validated by a
     *       post-pass.</li>
     *   <li>{@code LocalExchangeNode} itself is always non-serial — setting it serial
     *       would defeat its purpose of fanning a 1-task pipeline back to N tasks.</li>
     *   <li>For pipeline-breaking parents ({@code shouldResetSerialFlagForChild=true}),
     *       the child starts a fresh pipeline so {@code hasSerialAncestor} is reset; the
     *       node's own {@code isSerialNode()} still composes in for the child's view.</li>
     *   <li>{@code RequireHash} accepts any hash flavour; {@code RequireSpecific} demands
     *       an exact match (with the one PASSTHROUGH/ADAPTIVE_PASSTHROUGH compatibility).
     *       Pick the looser one whenever correctness allows — see
     *       {@link LocalExchangeNode.LocalExchangeTypeRequire}.</li>
     * </ul>
     *
     * <h3>Layers</h3>
     * Layer 1 (shouldSkipLE): mirrors BE's need_to_local_exchange — skip when this node or
     * an ancestor in the same pipeline is serial (operators[idx..end] has serial → skip).
     * Layer 2 (require/output): each Node declares require and output in enforceAndDeriveLocalExchange.
     */
    protected Pair<PlanNode, LocalExchangeType> enforceRequire(
            PlanTranslatorContext translatorContext, PlanNode child, int childIndex,
            LocalExchangeTypeRequire require) {
        // 1. Propagate serial-ancestor flag to child.
        // For pipeline-splitting operators (shouldReset=true, e.g. non-streaming AGG):
        //   Drop inherited serial flag from parent (parent is in a different pipeline),
        //   but keep this node's own serial status (child is in the same pipeline as this
        //   node's sink, e.g. Exchange is in AGG_Sink pipeline).
        // For non-splitting operators (shouldReset=false, e.g. streaming AGG):
        //   Inherit parent's serial flag + this node's own.
        boolean inheritedSerial = shouldResetSerialFlagForChild(childIndex)
                ? false : translatorContext.hasSerialAncestorInPipeline(this);
        // Use isSerialOperatorOnBe (= isSerialNode && fragment.useSerialSource) instead of the
        // raw isSerialNode().  BE's OperatorBase reads the Thrift `is_serial_operator` flag —
        // which is what FE writes via isSerialOperatorOnBe — so when the fragment is not in
        // serial-source mode, BE treats this operator as non-serial regardless of isSerialNode.
        // Using isSerialNode here would set the child's serial-ancestor flag wider than BE's
        // view and over-skip required LocalExchanges downstream.
        boolean childHasSerialAncestor = inheritedSerial
                || isSerialOperatorOnBe(translatorContext.getConnectContext());
        translatorContext.setHasSerialAncestorInPipeline(child, childHasSerialAncestor);

        // 1b. Propagate shuffle-for-correctness-ancestor flag to child.
        // Mirrors BE's _followed_by_shuffled_operator: a downstream operator needs hash
        // distribution for correctness, and the chain to here goes through HASH or NOOP
        // requirements (so the dependency is preserved).
        //   propagate = ((inheritedShuffled || self.requiresShuffleForCorrectness)
        //                && require is hash)
        //            || (inheritedShuffled && require is noop/passthrough)
        boolean inheritedShuffled = translatorContext.hasShuffleForCorrectnessAncestor(this);
        boolean selfOrInheritedShuffled = inheritedShuffled || requiresShuffleForCorrectness();
        boolean requireIsHash = require.preferType().isHashShuffle();
        boolean requireIsNoop = require.preferType() == LocalExchangeNode.LocalExchangeType.NOOP;
        boolean childShuffledAncestor = (selfOrInheritedShuffled && requireIsHash)
                || (inheritedShuffled && requireIsNoop);
        translatorContext.setHasShuffleForCorrectnessAncestor(child, childShuffledAncestor);

        // 2. Recurse child (Layer 2: child declares its own require/output)
        Pair<PlanNode, LocalExchangeType> childOutput =
                child.enforceAndDeriveLocalExchange(translatorContext, this, require);

        // Steps 2.5 and 3 both react to a serial child but address different concerns:
        //   - Step 2.5 rewrites the OUTPUT-side view (what we tell satisfy/parent about
        //     the child's actual distribution).  A serial pipeline runs with 1 task so
        //     its distribution claim is meaningless — flatten to NOOP so the satisfy
        //     check below doesn't get fooled by a stale "I output BUCKET_HASH" claim.
        //   - Step 3 rewrites the REQUIRE-side decision (what we want from the child).
        //     If we previously asked for nothing (noRequire) but the child turns out
        //     to be serial and we're not, upgrade to requirePassthrough so an LE is
        //     inserted to restore parallelism.

        // 2.5. Serial child override (output side): if child is serial on BE, force its
        //      reported output to NOOP.  Distribution is irrelevant when the child runs
        //      with 1 task; downstream parallelism is restored either by step 3 (LE
        //      insertion) or skipped entirely by step 4b (we're also serial).
        if (childOutput.first.isSerialOperatorOnBe(translatorContext.getConnectContext())) {
            childOutput = Pair.of(childOutput.first, LocalExchangeType.NOOP);
        }

        // 3. Framework-level serial child check (require side, mirrors BE base class
        //    required_data_distribution): if child will be serial on BE but this node is
        //    not serial, the pipeline has a 1-task serial child feeding an N-task non-serial
        //    parent.  Without LE, pipeline splits (AGG/JOIN) create paired pipelines with
        //    mismatched num_tasks → crash.  Upgrade noRequire to requirePassthrough so an
        //    LE is inserted below to restore parallelism.
        if (require instanceof LocalExchangeNode.NoRequire
                && childOutput.first.isSerialOperatorOnBe(translatorContext.getConnectContext())
                && !isSerialOperatorOnBe(translatorContext.getConnectContext())) {
            require = LocalExchangeTypeRequire.requirePassthrough();
        }

        // 4. Satisfy check: child output meets requirement → done
        if (require.satisfy(childOutput.second)) {
            return childOutput;
        }

        // 4. Layer 1: skip LE when serial operator or ancestor in same pipeline
        // Equivalent to BE's need_to_local_exchange: any_of(operators[idx..end], is_serial) → skip.
        // Use isSerialOperatorOnBe (not isSerialNode) because BE's Pipeline::need_to_local_exchange
        // checks op->is_serial_operator() which reads the Thrift flag set from isSerialOperatorOnBe;
        // when fragment.useSerialSource is false, BE treats this node as non-serial.
        if (translatorContext.hasSerialAncestorInPipeline(this)
                || isSerialOperatorOnBe(translatorContext.getConnectContext())) {
            return childOutput;
        }

        // 5. Resolve exchange type and create LE node
        LocalExchangeType preferType = AddLocalExchange.resolveExchangeType(require);
        List<Expr> distributeExprs = getLocalExchangeDistributeExprs(childIndex, selfOrInheritedShuffled);
        PlanNode leNode = createLocalExchange(translatorContext, childOutput.first, preferType, distributeExprs);
        return Pair.of(leNode, preferType);
    }

    /**
     * Create a LocalExchangeNode wrapping child with the given exchange type.
     * No child-type skip — matches BE's _add_local_exchange which inserts LE for any child
     * type without checking instanceof.
     *
     * Handles heavy-ops bottleneck avoidance (mirrors BE pipeline_fragment_context.cpp):
     * when upstream has 1 task (serial source) and exchange is heavy (hash/bucket/adaptive),
     * insert a PASSTHROUGH fan-out first to avoid single-task bottleneck on the heavy
     * exchange sink. Only applies to local-shuffle (pooling scan) fragments.
     */
    protected PlanNode createLocalExchange(PlanTranslatorContext translatorContext,
            PlanNode child, LocalExchangeType exchangeType, List<Expr> distributeExprs) {
        if (fragment != null && fragment.useSerialSource(translatorContext.getConnectContext())
                && exchangeType.isHeavyOperation() && child.isSerialNode()) {
            PlanNode ptNode = new LocalExchangeNode(translatorContext.nextPlanNodeId(),
                    child, LocalExchangeType.PASSTHROUGH, null);
            return new LocalExchangeNode(translatorContext.nextPlanNodeId(), ptNode,
                    exchangeType, distributeExprs);
        }
        return new LocalExchangeNode(translatorContext.nextPlanNodeId(), child,
                exchangeType, distributeExprs);
    }

    /**
     * Whether the child at {@code childIndex} starts a new pipeline context, causing
     * its serial-ancestor flag to be reset to {@code false} rather than inherited from this node.
     * Override to return {@code true} for pipeline-splitting nodes (LocalExchangeNode) and nodes
     * whose children run in an independent pipeline segment (SortNode before analytic, etc.).
     */
    protected boolean shouldResetSerialFlagForChild(int childIndex) {
        return false;
    }

    protected List<Expr> getChildDistributeExprList(int childIndex) {
        if ((childrenDistributeExprLists == null || childrenDistributeExprLists.size() <= childIndex)) {
            return null;
        } else {
            return childrenDistributeExprLists.get(childIndex);
        }
    }

    /**
     * Return distribute exprs used as the hash key when {@link #enforceRequire} inserts a
     * LocalExchange between this node and {@code child[childIndex]}.  Default returns the
     * child's output distribution ({@code childrenDistributeExprLists[childIndex]}).
     *
     * <p>Subclasses override this to mirror BE-specific {@code _partition_exprs} logic.  For
     * example BE's {@code AggSinkOperatorX::update_operator} picks
     * {@code grouping_exprs} when {@code !_followed_by_shuffled_operator && !has_distinct},
     * even though the child outputs a different (hash) distribution — and the LE inserted
     * before the streaming preagg must partition by {@code grouping_exprs} so a local
     * partial reduce actually collapses same-key rows.  Using the default (child
     * distribution) here would scatter same-group rows across instances and degrade the
     * preagg to a no-op, also breaking row-arrival order at downstream merge-finalize.
     *
     * @param childIndex which child
     * @param followedByShuffled whether the chain at this node is followed by a shuffled
     *        operator (mirrors BE's {@code _followed_by_shuffled_operator})
     */
    protected List<Expr> getLocalExchangeDistributeExprs(int childIndex, boolean followedByShuffled) {
        return getChildDistributeExprList(childIndex);
    }

    /**
     * Returns the operator's own semantically-defined partition expressions
     * (e.g. GROUP BY exprs for aggregation, PARTITION BY exprs for analytic).
     * Corresponds to BE's fallback path: tnode.agg_node.grouping_exprs /
     * tnode.analytic_node.partition_exprs when _followed_by_shuffled_operator=false.
     * Override in subclasses that have intrinsic partition keys.
     */
    protected List<Expr> getSemanticPartitionExprs() {
        return null;
    }

    /**
     * Returns true if there are effective (non-empty) partition expressions,
     * mirroring BE's _partition_exprs logic:
     *   _followed_by_shuffled_operator=true  → distribute_expr_lists[0] (child distribute key)
     *   _followed_by_shuffled_operator=false → semantic partition exprs (grouping / partition by)
     * parentRequire.preferType().isHashShuffle() corresponds to _followed_by_shuffled_operator=true.
     */
    protected boolean hasPartitionExprs(LocalExchangeTypeRequire parentRequire) {
        if (parentRequire.preferType().isHashShuffle()) {
            List<Expr> childExprs = getChildDistributeExprList(0);
            return childExprs != null && !childExprs.isEmpty();
        }
        List<Expr> semanticExprs = getSemanticPartitionExprs();
        return semanticExprs != null && !semanticExprs.isEmpty();
    }

    public List<List<Expr>> getChildrenDistributeExprLists() {
        return childrenDistributeExprLists;
    }

    public List<Expr> getDistributeExprLists() {
        return distributeExprLists;
    }

    public void setDistributeExprLists(List<Expr> distributeExprLists) {
        if (distributeExprLists == null) {
            this.distributeExprLists = Collections.emptyList();
        } else {
            this.distributeExprLists = distributeExprLists;
        }
    }
}

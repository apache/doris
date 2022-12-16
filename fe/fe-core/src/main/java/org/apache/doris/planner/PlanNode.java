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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BitmapFilterPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprId;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.TreeNode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.statistics.PlanStats;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFunctionBinaryType;
import org.apache.doris.thrift.TPlan;
import org.apache.doris.thrift.TPlanNode;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
public abstract class PlanNode extends TreeNode<PlanNode> implements PlanStats {

    protected String planNodeName;

    protected PlanNodeId id;  // unique w/in plan tree; assigned by planner
    protected PlanFragmentId fragmentId;  // assigned by planner after fragmentation step
    protected long limit; // max. # of rows to be returned; 0: no limit

    // ids materialized by the tree rooted at this node
    protected ArrayList<TupleId> tupleIds;

    // ids of the TblRefs "materialized" by this node; identical with tupleIds_
    // if the tree rooted at this node only materializes BaseTblRefs;
    // useful during plan generation
    protected ArrayList<TupleId> tblRefIds;

    // A set of nullable TupleId produced by this node. It is a subset of tupleIds.
    // A tuple is nullable within a particular plan tree if it's the "nullable" side of
    // an outer join, which has nothing to do with the schema.
    protected Set<TupleId> nullableTupleIds = Sets.newHashSet();

    protected List<Expr> conjuncts = Lists.newArrayList();

    protected Expr vconjunct = null;

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

    protected Expr vpreFilterConjunct = null;

    // Fragment that this PlanNode is executed in. Valid only after this PlanNode has been
    // assigned to a fragment. Set and maintained by enclosing PlanFragment.
    protected PlanFragment fragment;

    // estimate of the output cardinality of this node; set in computeStats();
    // invalid: -1
    protected long cardinality;

    // number of nodes on which the plan tree rooted at this node would execute;
    // set in computeStats(); invalid: -1
    protected int numNodes;

    // sum of tupleIds' avgSerializedSizes; set in computeStats()
    protected float avgRowSize;

    //  Node should compact data.
    protected boolean compactData;
    protected int numInstances;

    // Runtime filters assigned to this node.
    protected List<RuntimeFilter> runtimeFilters = new ArrayList<>();

    private boolean cardinalityIsDone = false;

    protected List<SlotId> outputSlotIds;

    protected StatisticalType statisticalType = StatisticalType.DEFAULT;
    protected StatsDeriveResult statsDeriveResult;

    protected TupleDescriptor outputTupleDesc;

    protected List<Expr> projectList;

    protected PlanNode(PlanNodeId id, ArrayList<TupleId> tupleIds, String planNodeName,
            StatisticalType statisticalType) {
        this.id = id;
        this.limit = -1;
        // make a copy, just to be on the safe side
        this.tupleIds = Lists.newArrayList(tupleIds);
        this.tblRefIds = Lists.newArrayList(tupleIds);
        this.cardinality = -1;
        this.planNodeName = VectorizedUtil.isVectorized() ? "V" + planNodeName : planNodeName;
        this.numInstances = 1;
        this.statisticalType = statisticalType;
    }

    protected PlanNode(PlanNodeId id, String planNodeName, StatisticalType statisticalType) {
        this.id = id;
        this.limit = -1;
        this.tupleIds = Lists.newArrayList();
        this.tblRefIds = Lists.newArrayList();
        this.cardinality = -1;
        this.planNodeName = VectorizedUtil.isVectorized() ? "V" + planNodeName : planNodeName;
        this.numInstances = 1;
        this.statisticalType = statisticalType;
    }

    /**
     * Copy ctor. Also passes in new id.
     */
    protected PlanNode(PlanNodeId id, PlanNode node, String planNodeName, StatisticalType statisticalType) {
        this.id = id;
        this.limit = node.limit;
        this.tupleIds = Lists.newArrayList(node.tupleIds);
        this.tblRefIds = Lists.newArrayList(node.tblRefIds);
        this.nullableTupleIds = Sets.newHashSet(node.nullableTupleIds);
        this.conjuncts = Expr.cloneList(node.conjuncts, null);
        this.cardinality = -1;
        this.compactData = node.compactData;
        this.planNodeName = VectorizedUtil.isVectorized() ? "V" + planNodeName : planNodeName;
        this.numInstances = 1;
        this.statisticalType = statisticalType;
    }

    public String getPlanNodeName() {
        return planNodeName;
    }

    public StatsDeriveResult getStatsDeriveResult() {
        return statsDeriveResult;
    }

    public StatisticalType getStatisticalType() {
        return statisticalType;
    }

    public void setStatsDeriveResult(StatsDeriveResult statsDeriveResult) {
        this.statsDeriveResult = statsDeriveResult;
    }

    /**
     * Sets tblRefIds_, tupleIds_, and nullableTupleIds_.
     * The default implementation is a no-op.
     */
    public void computeTupleIds() {
        Preconditions.checkState(children.isEmpty() || !tupleIds.isEmpty());
    }

    /**
     * Clears tblRefIds_, tupleIds_, and nullableTupleIds_.
     */
    protected void clearTupleIds() {
        tblRefIds.clear();
        tupleIds.clear();
        nullableTupleIds.clear();
    }

    protected void setPlanNodeName(String s) {
        this.planNodeName = s;
    }

    public PlanNodeId getId() {
        return id;
    }

    public void setId(PlanNodeId id) {
        Preconditions.checkState(this.id == null);
        this.id = id;
    }

    public PlanFragmentId getFragmentId() {
        return fragment.getFragmentId();
    }

    public int getFragmentSeqenceNum() {
        return fragment.getFragmentSequenceNum();
    }

    public void setFragmentId(PlanFragmentId id) {
        fragmentId = id;
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

    public boolean hasLimit() {
        return limit > -1;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }

    public long getCardinality() {
        return cardinality;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public float getAvgRowSize() {
        return avgRowSize;
    }

    /**
     * Set the value of compactData in all children.
     */
    public void setCompactData(boolean on) {
        this.compactData = on;
        for (PlanNode child : this.getChildren()) {
            child.setCompactData(on);
        }
    }

    public void unsetLimit() {
        limit = -1;
    }

    protected List<TupleId> getAllScanTupleIds() {
        List<TupleId> tupleIds = Lists.newArrayList();
        List<ScanNode> scanNodes = Lists.newArrayList();
        collectAll(Predicates.instanceOf(ScanNode.class), scanNodes);
        for (ScanNode node : scanNodes) {
            tupleIds.addAll(node.getTupleIds());
        }
        return tupleIds;
    }

    public void resetTupleIds(ArrayList<TupleId> tupleIds) {
        this.tupleIds = tupleIds;
    }

    public ArrayList<TupleId> getTupleIds() {
        Preconditions.checkState(tupleIds != null);
        return tupleIds;
    }

    public ArrayList<TupleId> getTblRefIds() {
        return tblRefIds;
    }

    public void setTblRefIds(ArrayList<TupleId> ids) {
        tblRefIds = ids;
    }

    public ArrayList<TupleId> getOutputTblRefIds() {
        return tblRefIds;
    }

    public List<TupleId> getOutputTupleIds() {
        return tupleIds;
    }

    public Set<TupleId> getNullableTupleIds() {
        Preconditions.checkState(nullableTupleIds != null);
        return nullableTupleIds;
    }

    public List<Expr> getConjuncts() {
        return conjuncts;
    }

    @Override
    public List<StatsDeriveResult> getChildrenStats() {
        List<StatsDeriveResult> statsDeriveResultList = Lists.newArrayList();
        for (PlanNode child : children) {
            statsDeriveResultList.add(child.getStatsDeriveResult());
        }
        return statsDeriveResultList;
    }

    protected void initCompoundPredicate(Expr expr) {
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            compoundPredicate.setType(Type.BOOLEAN);
            List<Type> args = new ArrayList<>();
            args.add(Type.BOOLEAN);
            args.add(Type.BOOLEAN);
            Function function = new Function(new FunctionName("", compoundPredicate.getOp().toString()),
                    args, Type.BOOLEAN, false);
            function.setBinaryType(TFunctionBinaryType.BUILTIN);
            expr.setFn(function);
        }

        for (Expr child : expr.getChildren()) {
            initCompoundPredicate(child);
        }
    }

    protected Expr convertConjunctsToAndCompoundPredicate(List<Expr> conjuncts) {
        List<Expr> targetConjuncts = Lists.newArrayList(conjuncts);
        while (targetConjuncts.size() > 1) {
            List<Expr> newTargetConjuncts = Lists.newArrayList();
            for (int i = 0; i < targetConjuncts.size(); i += 2) {
                Expr expr = i + 1 < targetConjuncts.size()
                        ? new CompoundPredicate(CompoundPredicate.Operator.AND, targetConjuncts.get(i),
                        targetConjuncts.get(i + 1)) : targetConjuncts.get(i);
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

    public void setAssignedConjuncts(Set<ExprId> conjuncts) {
        assignedConjuncts = conjuncts;
    }

    public Set<ExprId> getAssignedConjuncts() {
        return assignedConjuncts;
    }

    public void transferConjuncts(PlanNode recipient) {
        recipient.vconjunct = vconjunct;
        vconjunct = null;

        recipient.conjuncts.addAll(conjuncts);
        conjuncts.clear();
    }

    public void addPreFilterConjuncts(List<Expr> conjuncts) {
        if (conjuncts == null) {
            return;
        }
        this.preFilterConjuncts.addAll(conjuncts);
    }

    /**
     * Call computeStatAndMemLayout() for all materialized tuples.
     */
    protected void computeTupleStatAndMemLayout(Analyzer analyzer) {
        for (TupleId id : tupleIds) {
            analyzer.getDescTbl().getTupleDesc(id).computeStatAndMemLayout();
        }
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
        expBuilder.append(rootPrefix + id.asInt() + ":" + planNodeName + "\n");
        expBuilder.append(getNodeExplainString(detailPrefix, detailLevel));
        if (limit != -1) {
            expBuilder.append(detailPrefix + "limit: " + limit + "\n");
        }
        if (!CollectionUtils.isEmpty(projectList)) {
            expBuilder.append(detailPrefix).append("projections: ").append(getExplainString(projectList)).append("\n");
            expBuilder.append(detailPrefix).append("project output tuple id: ")
                    .append(outputTupleDesc.getId().asInt()).append("\n");
        }
        // Output Tuple Ids only when explain plan level is set to verbose
        if (detailLevel.equals(TExplainLevel.VERBOSE)) {
            expBuilder.append(detailPrefix + "tuple ids: ");
            for (TupleId tupleId : tupleIds) {
                String nullIndicator = nullableTupleIds.contains(tupleId) ? "N" : "";
                expBuilder.append(tupleId.asInt() + nullIndicator + " ");
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
        msg.num_children = children.size();
        msg.limit = limit;
        for (TupleId tid : tupleIds) {
            msg.addToRowTuples(tid.asInt());
            msg.addToNullableTuples(nullableTupleIds.contains(tid));
        }
        // `conjuncts` is never needed on vectorized engine except scan nodes which use them as push-down predicates.
        if (this instanceof ScanNode || !VectorizedUtil.isVectorized()) {
            for (Expr e : conjuncts) {
                msg.addToConjuncts(e.treeToThrift());
            }
        }

        // Serialize any runtime filters
        for (RuntimeFilter filter : runtimeFilters) {
            msg.addToRuntimeFilters(filter.toThrift());
        }

        if (vconjunct != null) {
            msg.vconjunct = vconjunct.treeToThrift();
        }

        msg.compact_data = compactData;
        if (outputSlotIds != null) {
            for (SlotId slotId : outputSlotIds) {
                msg.addToOutputSlotIds(slotId.asInt());
            }
        }
        toThrift(msg);
        container.addToNodes(msg);
        if (projectList != null) {
            for (Expr expr : projectList) {
                msg.addToProjections(expr.treeToThrift());
            }
        }
        if (outputTupleDesc != null) {
            msg.setOutputTupleId(outputTupleDesc.getId().asInt());
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

    /**
     * Computes internal state, including planner-relevant statistics.
     * Call this once on the root of the plan tree before calling toThrift().
     * Subclasses need to override this.
     */
    public void finalize(Analyzer analyzer) throws UserException {
        for (PlanNode child : children) {
            child.finalize(analyzer);
        }
        computeNumNodes();
        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            computeOldCardinality();
        }
    }

    protected void computeNumNodes() {
        if (!children.isEmpty()) {
            numNodes = getChild(0).numNodes;
        }
    }

    /**
     * Computes planner statistics: avgRowSize.
     * Subclasses need to override this.
     * Assumes that it has already been called on all children.
     * This is broken out of finalize() so that it can be called separately
     * from finalize() (to facilitate inserting additional nodes during plan
     * partitioning w/o the need to call finalize() recursively on the whole tree again).
     */
    protected void computeStats(Analyzer analyzer) throws UserException {
        avgRowSize = 0.0F;
        for (TupleId tid : tupleIds) {
            TupleDescriptor desc = analyzer.getTupleDesc(tid);
            avgRowSize += desc.getAvgSerializedSize();
        }
    }

    /**
     * This function will calculate the cardinality when the old join reorder algorithm is enabled.
     * This value is used to determine the distributed way(broadcast of shuffle) of join in the distributed planning.
     *
     * If the new join reorder and the old join reorder have the same cardinality calculation method,
     *   also the calculation is completed in the init(),
     *   there is no need to override this function.
     */
    protected void computeOldCardinality() {
    }

    protected void capCardinalityAtLimit() {
        if (hasLimit()) {
            cardinality = cardinality == -1 ? limit : Math.min(cardinality, limit);
        }
    }

    protected ExprSubstitutionMap outputSmap;

    // global state of planning wrt conjunct assignment; used by planner as a shortcut
    // to avoid having to pass assigned conjuncts back and forth
    // (the planner uses this to save and reset the global state in between join tree
    // alternatives)
    protected Set<ExprId> assignedConjuncts;

    protected ExprSubstitutionMap withoutTupleIsNullOutputSmap;

    public ExprSubstitutionMap getOutputSmap() {
        return outputSmap;
    }

    public void setOutputSmap(ExprSubstitutionMap smap) {
        outputSmap = smap;
    }

    public void setWithoutTupleIsNullOutputSmap(ExprSubstitutionMap smap) {
        withoutTupleIsNullOutputSmap = smap;
    }

    public ExprSubstitutionMap getWithoutTupleIsNullOutputSmap() {
        return withoutTupleIsNullOutputSmap == null ? outputSmap : withoutTupleIsNullOutputSmap;
    }

    public void init() throws UserException {}

    public void init(Analyzer analyzer) throws UserException {
        assignConjuncts(analyzer);
        createDefaultSmap(analyzer);
    }

    /**
     * Assign remaining unassigned conjuncts.
     */
    protected void assignConjuncts(Analyzer analyzer) {
        List<Expr> unassigned = analyzer.getUnassignedConjuncts(this);
        for (Expr unassignedConjunct : unassigned) {
            addConjunct(unassignedConjunct);
        }
        analyzer.markConjunctsAssigned(unassigned);
    }

    /**
     * Returns an smap that combines the children's smaps.
     */
    protected ExprSubstitutionMap getCombinedChildSmap() {
        if (getChildren().size() == 0) {
            return new ExprSubstitutionMap();
        }

        if (getChildren().size() == 1) {
            return getChild(0).getOutputSmap();
        }

        ExprSubstitutionMap result = ExprSubstitutionMap.combine(
                getChild(0).getOutputSmap(), getChild(1).getOutputSmap());

        for (int i = 2; i < getChildren().size(); ++i) {
            result = ExprSubstitutionMap.combine(result, getChild(i).getOutputSmap());
        }

        return result;
    }

    protected ExprSubstitutionMap getCombinedChildWithoutTupleIsNullSmap() {
        if (getChildren().size() == 0) {
            return new ExprSubstitutionMap();
        }
        if (getChildren().size() == 1) {
            return getChild(0).getWithoutTupleIsNullOutputSmap();
        }
        ExprSubstitutionMap result = ExprSubstitutionMap.combine(
                getChild(0).getWithoutTupleIsNullOutputSmap(),
                getChild(1).getWithoutTupleIsNullOutputSmap());

        for (int i = 2; i < getChildren().size(); ++i) {
            result = ExprSubstitutionMap.combine(
                    result, getChild(i).getWithoutTupleIsNullOutputSmap());
        }

        return result;
    }

    /**
     * Sets outputSmap_ to compose(existing smap, combined child smap). Also
     * substitutes conjuncts_ using the combined child smap.
     *
     * @throws AnalysisException
     */
    protected void createDefaultSmap(Analyzer analyzer) throws UserException {
        ExprSubstitutionMap combinedChildSmap = getCombinedChildSmap();
        outputSmap =
                ExprSubstitutionMap.compose(outputSmap, combinedChildSmap, analyzer);

        conjuncts = Expr.substituteList(conjuncts, outputSmap, analyzer, false);
    }

    /**
     * Appends ids of slots that need to be materialized for this tree of nodes.
     * By default, only slots referenced by conjuncts need to be materialized
     * (the rationale being that only conjuncts need to be evaluated explicitly;
     * exprs that are turned into scan predicates, etc., are evaluated implicitly).
     */
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        for (PlanNode childNode : children) {
            childNode.getMaterializedIds(analyzer, ids);
        }
        Expr.getIds(getConjuncts(), null, ids);
    }

    // Convert this plan node into msg (excluding children), which requires setting
    // the node type and the node-specific field.
    protected abstract void toThrift(TPlanNode msg);

    protected String debugString() {
        // not using Objects.toStrHelper because
        StringBuilder output = new StringBuilder();
        output.append("preds=" + Expr.debugString(conjuncts));
        output.append(" limit=" + Long.toString(limit));
        return output.toString();
    }

    protected String getExplainString(List<? extends Expr> exprs) {
        if (exprs == null) {
            return "";
        }
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < exprs.size(); ++i) {
            if (i > 0) {
                output.append(", ");
            }
            output.append(exprs.get(i).toSql());
        }
        return output.toString();
    }

    /**
     * Returns true if stats-related variables are valid.
     */
    protected boolean hasValidStats() {
        return (numNodes == -1 || numNodes >= 0) && (cardinality == -1 || cardinality >= 0);
    }

    public int getNumInstances() {
        return numInstances;
    }

    public void setNumInstances(int numInstances) {
        this.numInstances = numInstances;
    }

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
     * Returns the estimated combined selectivity of all conjuncts. Uses heuristics to
     * address the following estimation challenges:
     * 1. The individual selectivities of conjuncts may be unknown.
     * 2. Two selectivities, whether known or unknown, could be correlated. Assuming
     * independence can lead to significant underestimation.
     * <p>
     * The first issue is addressed by using a single default selectivity that is
     * representative of all conjuncts with unknown selectivities.
     * The second issue is addressed by an exponential backoff when multiplying each
     * additional selectivity into the final result.
     */
    protected static double computeCombinedSelectivity(List<Expr> conjuncts) {
        // Collect all estimated selectivities.
        List<Double> selectivities = new ArrayList<>();
        for (Expr e : conjuncts) {
            if (e.hasSelectivity()) {
                selectivities.add(e.getSelectivity());
            }
        }
        if (selectivities.size() != conjuncts.size()) {
            // Some conjuncts have no estimated selectivity. Use a single default
            // representative selectivity for all those conjuncts.
            selectivities.add(Expr.DEFAULT_SELECTIVITY);
        }
        // Sort the selectivities to get a consistent estimate, regardless of the original
        // conjunct order. Sort in ascending order such that the most selective conjunct
        // is fully applied.
        Collections.sort(selectivities);
        double result = 1.0;
        // selectivity = 1 * (s1)^(1/1) * (s2)^(1/2) * ... * (sn-1)^(1/(n-1)) * (sn)^(1/n)
        for (int i = 0; i < selectivities.size(); ++i) {
            // Exponential backoff for each selectivity multiplied into the final result.
            result *= Math.pow(selectivities.get(i), 1.0 / (double) (i + 1));
        }
        // Bound result in [0, 1]
        return Math.max(0.0, Math.min(1.0, result));
    }

    protected double computeSelectivity() {
        for (Expr expr : conjuncts) {
            expr.setSelectivity();
        }
        return computeCombinedSelectivity(conjuncts);
    }

    /**
     * Compute the product of the selectivity of all conjuncts.
     * This function is used for old cardinality in finalize()
     */
    protected double computeOldSelectivity() {
        double prod = 1.0;
        for (Expr e : conjuncts) {
            if (e.getSelectivity() < 0) {
                return -1.0;
            }
            prod *= e.getSelectivity();
        }
        return prod;
    }

    // Compute the cardinality after applying conjuncts based on 'preConjunctCardinality'.
    protected void applyConjunctsSelectivity() {
        if (cardinality == -1) {
            return;
        }
        applySelectivity();
    }

    // Compute the cardinality after applying conjuncts with 'selectivity', based on
    // 'preConjunctCardinality'.
    private void applySelectivity() {
        double selectivity = computeSelectivity();
        Preconditions.checkState(cardinality >= 0);
        double preConjunctCardinality = cardinality;
        cardinality = Math.round(cardinality * selectivity);
        // don't round cardinality down to zero for safety.
        if (cardinality == 0 && preConjunctCardinality > 0) {
            cardinality = 1;
        }
    }

    public String getPlanTreeExplainStr() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(getId().asInt()).append(": ").append(getPlanNodeName()).append("]");
        sb.append("\n[Fragment: ").append(getFragmentSeqenceNum()).append("]");
        sb.append("\n").append(getNodeExplainString("", TExplainLevel.BRIEF));
        return sb.toString();
    }

    public ScanNode getScanNodeInOneFragmentByTupleId(TupleId tupleId) {
        if (this instanceof ScanNode && tupleIds.contains(tupleId)) {
            return (ScanNode) this;
        } else if (!(this instanceof ExchangeNode)) {
            for (PlanNode planNode : children) {
                ScanNode scanNode = planNode.getScanNodeInOneFragmentByTupleId(tupleId);
                if (scanNode != null) {
                    return scanNode;
                }
            }
        }
        return null;
    }

    protected void addRuntimeFilter(RuntimeFilter filter) {
        runtimeFilters.add(filter);
    }

    protected Collection<RuntimeFilter> getRuntimeFilters() {
        return runtimeFilters;
    }

    public void clearRuntimeFilters() {
        runtimeFilters.clear();
    }

    protected String getRuntimeFilterExplainString(boolean isBuildNode, boolean isBrief) {
        if (runtimeFilters.isEmpty()) {
            return "";
        }
        List<String> filtersStr = new ArrayList<>();
        for (RuntimeFilter filter : runtimeFilters) {
            StringBuilder filterStr = new StringBuilder();
            filterStr.append(filter.getFilterId());
            if (!isBrief) {
                filterStr.append("[");
                filterStr.append(filter.getType().toString().toLowerCase());
                filterStr.append("]");
                if (isBuildNode) {
                    filterStr.append(" <- ");
                    filterStr.append(filter.getSrcExpr().toSql());
                } else {
                    filterStr.append(" -> ");
                    filterStr.append(filter.getTargetExpr(getId()).toSql());
                }
            }
            filtersStr.add(filterStr.toString());
        }
        return Joiner.on(", ").join(filtersStr) + "\n";
    }

    protected String getRuntimeFilterExplainString(boolean isBuildNode) {
        return getRuntimeFilterExplainString(isBuildNode, false);
    }

    public void convertToVectorized() {
        List<Expr> conjunctsExcludeBitmapFilter = Lists.newArrayList();
        for (Expr expr : conjuncts) {
            if (!(expr instanceof BitmapFilterPredicate)) {
                conjunctsExcludeBitmapFilter.add(expr);
            }
        }
        if (!conjunctsExcludeBitmapFilter.isEmpty()) {
            vconjunct = convertConjunctsToAndCompoundPredicate(conjunctsExcludeBitmapFilter);
            initCompoundPredicate(vconjunct);
        }

        if (!preFilterConjuncts.isEmpty()) {
            vpreFilterConjunct = convertConjunctsToAndCompoundPredicate(preFilterConjuncts);
            initCompoundPredicate(vpreFilterConjunct);
        }

        for (PlanNode child : children) {
            child.convertToVectorized();
        }
    }

    /**
     * If an plan node implements this method, the plan node itself supports project optimization.
     * @param requiredSlotIdSet: The upper plan node's requirement slot set for the current plan node.
     *                        The requiredSlotIdSet could be null when the upper plan node cannot
     *                         calculate the required slot.
     * @param analyzer
     * @throws NotImplementedException
     *
     * For example:
     * Query: select a.k1 from a, b where a.k1=b.k1
     * PlanNodeTree:
     *     output exprs: a.k1
     *           |
     *     hash join node
     *   (input slots: a.k1, b.k1)
     *        |      |
     *  scan a(k1)   scan b(k1)
     *
     * Function params: requiredSlotIdSet = a.k1
     * After function:
     *     hash join node
     *   (output slots: a.k1)
     *   (input slots: a.k1, b.k1)
     */
    public void initOutputSlotIds(Set<SlotId> requiredSlotIdSet, Analyzer analyzer) throws NotImplementedException {
        throw new NotImplementedException("The `initOutputSlotIds` hasn't been implemented in " + planNodeName);
    }

    public void projectOutputTuple() throws NotImplementedException {
        throw new NotImplementedException("The `projectOutputTuple` hasn't been implemented in " + planNodeName + ". "
        + "But it does not affect the project optimizer");
    }

    /**
     * If an plan node implements this method, its child plan node has the ability to implement the project.
     * The return value of this method will be used as
     *     the input(requiredSlotIdSet) of child plan node method initOutputSlotIds.
     * That is to say, only when the plan node implements this method,
     *     its children can realize project optimization.
     *
     * @return The requiredSlotIdSet of this plan node
     * @throws NotImplementedException
     * PlanNodeTree:
     *         agg node(group by a.k1)
     *           |
     *     hash join node(a.k1=b.k1)
     *        |      |
     *  scan a(k1)   scan b(k1)
     * After function:
     *         agg node
     *    (required slots: a.k1)
     */
    public Set<SlotId> computeInputSlotIds(Analyzer analyzer) throws NotImplementedException {
        throw new NotImplementedException("The `computeInputSlotIds` hasn't been implemented in " + planNodeName);
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
    public void finalizeForNereids() {

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
}

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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/PlanFragment.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.TreeNode;
import org.apache.doris.common.util.ProfileStatistics;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TResultSinkType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PlanFragments form a tree structure via their ExchangeNodes. A tree of fragments
 * connected in that way forms a plan. The output of a plan is produced by the root
 * fragment and is either the result of the query or an intermediate result
 * needed by a different plan (such as a hash table).
 *
 * Plans are grouped into cohorts based on the consumer of their output: all
 * plans that materialize intermediate results for a particular consumer plan
 * are grouped into a single cohort.
 *
 * A PlanFragment encapsulates the specific tree of execution nodes that
 * are used to produce the output of the plan fragment, as well as output exprs,
 * destination node, etc. If there are no output exprs, the full row that is
 * produced by the plan root is marked as materialized.
 *
 * A plan fragment can have one or many instances, each of which in turn is executed by
 * an individual node and the output sent to a specific instance of the destination
 * fragment (or, in the case of the root fragment, is materialized in some form).
 *
 * A hash-partitioned plan fragment is the result of one or more hash-partitioning data
 * streams being received by plan nodes in this fragment. In the future, a fragment's
 * data partition could also be hash partitioned based on a scan node that is reading
 * from a physically hash-partitioned table.
 *
 * The sequence of calls is:
 * - c'tor
 * - assemble with getters, etc.
 * - finalize()
 * - toThrift()
 *
 * TODO: the tree of PlanNodes is connected across fragment boundaries, which makes
 *   it impossible search for things within a fragment (using TreeNode functions);
 *   fix that
 */
public class PlanFragment extends TreeNode<PlanFragment> {
    private static final Logger LOG = LogManager.getLogger(PlanFragment.class);

    // id for this plan fragment
    private PlanFragmentId fragmentId;
    // nereids planner and original planner generate fragments in different order.
    // This makes nereids fragment id different from that of original planner, and
    // hence different from that in profile.
    // in original planner, fragmentSequenceNum is fragmentId, and in nereids planner,
    // fragmentSequenceNum is the id displayed in profile
    private int fragmentSequenceNum;
    // private PlanId planId_;
    // private CohortId cohortId_;

    // root of plan tree executed by this fragment
    private PlanNode planRoot;

    // exchange node to which this fragment sends its output
    private ExchangeNode destNode;

    // if null, outputs the entire row produced by planRoot
    private ArrayList<Expr> outputExprs;

    // created in finalize() or set in setSink()
    protected DataSink sink;

    // data source(or sender) of specific partition in the fragment;
    // an UNPARTITIONED fragment is executed on only a single node
    private DataPartition dataPartition;

    // specification of the actually input partition of this fragment when transmitting to be.
    // By default, the value of the data partition in planner and the data partition transmitted to be are the same.
    // So this attribute is empty.
    // But sometimes the planned value and the serialized value are inconsistent. You need to set this value.
    // At present, this situation only occurs in the fragment where the scan node is located.
    // Since the data partition expression of the scan node is actually constructed from the schema of the table,
    //   the expression is not analyzed.
    // This will cause this expression to not be serialized correctly and transmitted to be.
    // In this case, you need to set this attribute to DataPartition RANDOM to avoid the problem.
    private DataPartition dataPartitionForThrift;

    // specification of how the output of this fragment is partitioned (i.e., how
    // it's sent to its destination);
    // if the output is UNPARTITIONED, it is being broadcast
    protected DataPartition outputPartition;

    // Whether query statistics is sent with every batch. In order to get the query
    // statistics correctly when query contains limit, it is necessary to send query
    // statistics with every batch, or only in close.
    private boolean transferQueryStatisticsWithEveryBatch;

    // TODO: SubstitutionMap outputSmap;
    // substitution map to remap exprs onto the output of this fragment, to be applied
    // at destination fragment

    // specification of the number of parallel when fragment is executed
    // default value is 1
    private int parallelExecNum = 1;

    // The runtime filter id that produced
    private Set<RuntimeFilterId> builderRuntimeFilterIds;
    // The runtime filter id that is expected to be used
    private Set<RuntimeFilterId> targetRuntimeFilterIds;

    // has colocate plan node
    private boolean hasColocatePlanNode = false;

    private TResultSinkType resultSinkType = TResultSinkType.MYSQL_PROTOCAL;

    /**
     * C'tor for fragment with specific partition; the output is by default broadcast.
     */
    public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition) {
        this.fragmentId = id;
        this.planRoot = root;
        this.dataPartition = partition;
        this.outputPartition = DataPartition.UNPARTITIONED;
        this.transferQueryStatisticsWithEveryBatch = false;
        this.builderRuntimeFilterIds = new HashSet<>();
        this.targetRuntimeFilterIds = new HashSet<>();
        setParallelExecNumIfExists();
        setFragmentInPlanTree(planRoot);
    }

    public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition, DataPartition partitionForThrift) {
        this(id, root, partition);
        this.dataPartitionForThrift = partitionForThrift;
    }

    public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition,
            Set<RuntimeFilterId> builderRuntimeFilterIds, Set<RuntimeFilterId> targetRuntimeFilterIds) {
        this(id, root, partition);
        this.builderRuntimeFilterIds = new HashSet<>(builderRuntimeFilterIds);
        this.targetRuntimeFilterIds = new HashSet<>(targetRuntimeFilterIds);
    }

    /**
     * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
     * Does not traverse the children of ExchangeNodes because those must belong to a
     * different fragment.
     */
    public void setFragmentInPlanTree(PlanNode node) {
        if (node == null) {
            return;
        }
        node.setFragment(this);
        if (node instanceof ExchangeNode) {
            return;
        }
        for (PlanNode child : node.getChildren()) {
            setFragmentInPlanTree(child);
        }
    }

    /**
     * Assign ParallelExecNum by PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM in SessionVariable for synchronous request
     * Assign ParallelExecNum by default value for Asynchronous request
     */
    public void setParallelExecNumIfExists() {
        if (ConnectContext.get() != null) {
            parallelExecNum = ConnectContext.get().getSessionVariable().getParallelExecInstanceNum();
        }
    }

    // Manually set parallel exec number
    // Currently for broker load
    public void setParallelExecNum(int parallelExecNum) {
        this.parallelExecNum = parallelExecNum;
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = Expr.cloneList(outputExprs, null);
    }

    public void resetOutputExprs(TupleDescriptor tupleDescriptor) {
        this.outputExprs = Lists.newArrayList();
        for (SlotDescriptor slotDescriptor : tupleDescriptor.getSlots()) {
            SlotRef slotRef = new SlotRef(slotDescriptor);
            outputExprs.add(slotRef);
        }
    }

    public ArrayList<Expr> getOutputExprs() {
        return outputExprs;
    }

    public void setBuilderRuntimeFilterIds(RuntimeFilterId rid) {
        this.builderRuntimeFilterIds.add(rid);
    }

    public void setTargetRuntimeFilterIds(RuntimeFilterId rid) {
        this.targetRuntimeFilterIds.add(rid);
    }

    public void setHasColocatePlanNode(boolean hasColocatePlanNode) {
        this.hasColocatePlanNode = hasColocatePlanNode;
    }

    public void setResultSinkType(TResultSinkType resultSinkType) {
        this.resultSinkType = resultSinkType;
    }

    public boolean hasColocatePlanNode() {
        return hasColocatePlanNode;
    }

    public void setDataPartition(DataPartition dataPartition) {
        this.dataPartition = dataPartition;
    }

    /**
     * Finalize plan tree and create stream sink, if needed.
     */
    public void finalize(StatementBase stmtBase) {
        if (sink != null) {
            return;
        }
        if (destNode != null) {
            Preconditions.checkState(sink == null);
            // we're streaming to an exchange node
            DataStreamSink streamSink = new DataStreamSink(destNode.getId());
            streamSink.setOutputPartition(outputPartition);
            streamSink.setFragment(this);
            sink = streamSink;
        } else {
            if (planRoot == null) {
                // only output expr, no FROM clause
                // "select 1 + 2"
                return;
            }
            Preconditions.checkState(sink == null);
            QueryStmt queryStmt = stmtBase instanceof QueryStmt ? (QueryStmt) stmtBase : null;
            if (queryStmt != null && queryStmt.hasOutFileClause()) {
                sink = new ResultFileSink(planRoot.getId(), queryStmt.getOutFileClause(), queryStmt.getColLabels());
            } else {
                // add ResultSink
                // we're streaming to an result sink
                sink = new ResultSink(planRoot.getId(), resultSinkType);
            }
        }
    }

    /**
     * Return the number of nodes on which the plan fragment will execute.
     * invalid: -1
     */
    public int getNumNodes() {
        return dataPartition == DataPartition.UNPARTITIONED ? 1 : planRoot.getNumNodes();
    }

    public int getParallelExecNum() {
        return parallelExecNum;
    }

    public TPlanFragment toThrift() {
        TPlanFragment result = new TPlanFragment();
        if (planRoot != null) {
            result.setPlan(planRoot.treeToThrift());
        }
        if (outputExprs != null) {
            result.setOutputExprs(Expr.treesToThrift(outputExprs));
        }
        if (sink != null) {
            result.setOutputSink(sink.toThrift());
        }
        if (dataPartitionForThrift == null) {
            result.setPartition(dataPartition.toThrift());
        } else {
            result.setPartition(dataPartitionForThrift.toThrift());
        }

        // TODO chenhao , calculated by cost
        result.setMinReservationBytes(0);
        result.setInitialReservationTotalClaims(0);
        return result;
    }

    public String getExplainString(TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        Preconditions.checkState(dataPartition != null);
        if (CollectionUtils.isNotEmpty(outputExprs)) {
            str.append("  OUTPUT EXPRS:\n    ");
            str.append(outputExprs.stream().map(Expr::toSql).collect(Collectors.joining("\n    ")));
        }
        str.append("\n");
        str.append("  PARTITION: " + dataPartition.getExplainString(explainLevel) + "\n");
        str.append("  HAS_COLO_PLAN_NODE: " + hasColocatePlanNode + "\n");
        str.append("\n");
        if (sink != null) {
            str.append(sink.getExplainString("  ", explainLevel) + "\n");
        }
        if (planRoot != null) {
            str.append(planRoot.getExplainString("  ", "  ", explainLevel));
        }
        return str.toString();
    }

    public String getExplainStringToProfile(TExplainLevel explainLevel, ProfileStatistics statistics, int fragmentIdx) {
        StringBuilder str = new StringBuilder();
        Preconditions.checkState(dataPartition != null);
        if (CollectionUtils.isNotEmpty(outputExprs)) {
            str.append("  OUTPUT EXPRS:\n    ");
            str.append(outputExprs.stream().map(Expr::toSql).collect(Collectors.joining("\n    ")));
        }
        str.append("\n");
        str.append("  PARTITION: " + dataPartition.getExplainString(explainLevel) + "\n");
        if (sink != null) {
            str.append(sink.getExplainStringToProfile("  ", explainLevel, statistics, fragmentIdx) + "\n");
        }
        if (planRoot != null) {
            str.append(planRoot.getExplainStringToProfile("  ", "  ", explainLevel, statistics));
        }
        return str.toString();
    }

    /**
     * Returns true if this fragment is partitioned.
     */
    public boolean isPartitioned() {
        return (dataPartition.getType() != TPartitionType.UNPARTITIONED);
    }

    public void updateDataPartition(DataPartition dataPartition) {
        if (this.dataPartition == DataPartition.UNPARTITIONED) {
            return;
        }
        this.dataPartition = dataPartition;
    }

    public PlanFragmentId getId() {
        return fragmentId;
    }

    public PlanFragment getDestFragment() {
        if (destNode == null) {
            return null;
        }
        return destNode.getFragment();
    }

    public void setDestination(ExchangeNode destNode) {
        this.destNode = destNode;
        PlanFragment dest = getDestFragment();
        Preconditions.checkNotNull(dest);
        dest.addChild(this);
    }

    public DataPartition getDataPartition() {
        return dataPartition;
    }

    public DataPartition getOutputPartition() {
        return outputPartition;
    }

    public void setOutputPartition(DataPartition outputPartition) {
        this.outputPartition = outputPartition;
    }

    public PlanNode getPlanRoot() {
        return planRoot;
    }

    public void setPlanRoot(PlanNode root) {
        planRoot = root;
        setFragmentInPlanTree(planRoot);
    }

    /**
     * Adds a node as the new root to the plan tree. Connects the existing
     * root as the child of newRoot.
     */
    public void addPlanRoot(PlanNode newRoot) {
        Preconditions.checkState(newRoot.getChildren().size() == 1);
        newRoot.setChild(0, planRoot);
        planRoot = newRoot;
        planRoot.setFragment(this);
    }

    public DataSink getSink() {
        return sink;
    }

    public void setSink(DataSink sink) {
        Preconditions.checkState(this.sink == null);
        Preconditions.checkNotNull(sink);
        sink.setFragment(this);
        this.sink = sink;
    }

    public void resetSink(DataSink sink) {
        sink.setFragment(this);
        this.sink = sink;
    }

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public Set<RuntimeFilterId> getBuilderRuntimeFilterIds() {
        return builderRuntimeFilterIds;
    }

    public Set<RuntimeFilterId> getTargetRuntimeFilterIds() {
        return targetRuntimeFilterIds;
    }

    public void clearRuntimeFilters() {
        builderRuntimeFilterIds.clear();
        targetRuntimeFilterIds.clear();
    }

    public void setTransferQueryStatisticsWithEveryBatch(boolean value) {
        transferQueryStatisticsWithEveryBatch = value;
    }

    public boolean isTransferQueryStatisticsWithEveryBatch() {
        return transferQueryStatisticsWithEveryBatch;
    }

    public int getFragmentSequenceNum() {
        if (ConnectContext.get().getSessionVariable().isEnableNereidsPlanner()) {
            return fragmentSequenceNum;
        } else {
            return fragmentId.asInt();
        }
    }

    public void setFragmentSequenceNum(int seq) {
        fragmentSequenceNum = seq;
    }
}

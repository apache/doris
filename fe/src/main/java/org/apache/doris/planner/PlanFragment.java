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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.common.UserException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.TreeNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanFragment;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.List;

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
 * is produced by the plan root is marked as materialized.
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
    private final static Logger LOG = LogManager.getLogger(PlanFragment.class);

    // id for this plan fragment
    private PlanFragmentId fragmentId;
    // private PlanId planId_;
    // private CohortId cohortId_;

    // root of plan tree executed by this fragment
    private PlanNode planRoot;

    // exchange node to which this fragment sends its output
    private ExchangeNode destNode;

    // if null, outputs the entire row produced by planRoot
    private ArrayList<Expr> outputExprs;

    // created in finalize() or set in setSink()
    private DataSink sink;

    // specification of the partition of the input of this fragment;
    // an UNPARTITIONED fragment is executed on only a single node
    // TODO: improve this comment, "input" is a bit misleading
    private final DataPartition dataPartition;

    // specification of how the output of this fragment is partitioned (i.e., how
    // it's sent to its destination);
    // if the output is UNPARTITIONED, it is being broadcast
    private DataPartition outputPartition;

    // Whether query statistics is sent with every batch. In order to get the query
    // statistics correctly when query contains limit, it is necessary to send query 
    // statistics with every batch, or only in close.
    private boolean transferQueryStatisticsWithEveryBatch;

    // TODO: SubstitutionMap outputSmap;
    // substitution map to remap exprs onto the output of this fragment, to be applied
    // at destination fragment

    /**
     * C'tor for fragment with specific partition; the output is by default broadcast.
     */
    public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition) {
        this.fragmentId = id;
        this.planRoot = root;
        this.dataPartition = partition;
        this.outputPartition = DataPartition.UNPARTITIONED;
        this.transferQueryStatisticsWithEveryBatch = false;
        setFragmentInPlanTree(planRoot);
    }

    /**
     * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
     * Does not traverse the children of ExchangeNodes because those must belong to a
     * different fragment.
     */
    public void setFragmentInPlanTree(PlanNode node) {
        if (node == null) return;
        node.setFragment(this);
        if (node instanceof ExchangeNode) return;
        for (PlanNode child : node.getChildren()) setFragmentInPlanTree(child);
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = Expr.cloneList(outputExprs, null);
    }

    /**
     * Finalize plan tree and create stream sink, if needed.
     */
    public void finalize(Analyzer analyzer, boolean validateFileFormats)
            throws UserException, NotImplementedException {
        if (sink != null) {
            return;
        }
        if (destNode != null) {
            Preconditions.checkState(sink == null);
            // we're streaming to an exchange node
            DataStreamSink streamSink = new DataStreamSink(destNode.getId());
            streamSink.setPartition(outputPartition);
            streamSink.setFragment(this);
            sink = streamSink;
        } else {
            if (planRoot == null) {
                // only output expr, no FROM clause
                // "select 1 + 2"
                return;
            }
            // add ResultSink
            Preconditions.checkState(sink == null);
            // we're streaming to an exchange node
            ResultSink bufferSink = new ResultSink(planRoot.getId());
            sink = bufferSink;
        }
    }

    /**
     * Return the number of nodes on which the plan fragment will execute.
     * invalid: -1
     */
    public int getNumNodes() {
        return dataPartition == DataPartition.UNPARTITIONED ? 1 : planRoot.getNumNodes();
    }

    public TPlanFragment toThrift() {
        TPlanFragment result = new TPlanFragment();
        if (planRoot != null) {
            result.setPlan(planRoot.treeToThrift());
        }
        if (outputExprs != null) {
            result.setOutput_exprs(Expr.treesToThrift(outputExprs));
        }
        if (sink != null) {
            result.setOutput_sink(sink.toThrift());
        }
        result.setPartition(dataPartition.toThrift());

        // TODO chenhao , calculated by cost
        result.setMin_reservation_bytes(0);
        result.setInitial_reservation_total_claims(0);
        return result;
    }

    public String getExplainString(TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        Preconditions.checkState(dataPartition != null);
        str.append(" OUTPUT EXPRS:");
        if (outputExprs != null) {
            for (Expr e : outputExprs) {
                str.append(e.toSql() + " | ");
            }
        }
        str.append("\n");
        str.append("  PARTITION: " + dataPartition.getExplainString(explainLevel) + "\n");
        if (sink != null) {
            str.append(sink.getExplainString("  ", explainLevel) + "\n");
        }
        if (planRoot != null) {
            str.append(planRoot.getExplainString("  ", "  ", explainLevel));
        }
        return str.toString();
    }

    /**
     * Returns true if this fragment is partitioned.
     */
    public boolean isPartitioned() {
        return (dataPartition.getType() != TPartitionType.UNPARTITIONED);
    }

    public PlanFragment getDestFragment() {
        if (destNode == null) return null;
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

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public void setTransferQueryStatisticsWithEveryBatch(boolean value) {
        transferQueryStatisticsWithEveryBatch = value;
    }

    public boolean isTransferQueryStatisticsWithEveryBatch() {
        return transferQueryStatisticsWithEveryBatch;
    }
}

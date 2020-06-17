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

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPartitionType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * The distributed planner is responsible for creating an executable, distributed plan
 * from a single-node plan that can be sent to the backend.
 */
public class DistributedPlanner {
    private final static Logger LOG = LogManager.getLogger(DistributedPlanner.class);

    private final PlannerContext ctx_;

    public DistributedPlanner(PlannerContext ctx) {
        ctx_ = ctx;
    }

    /**
     * Create plan fragments for a single-node plan considering a set of execution options.
     * The fragments are returned in a list such that element i of that list can
     * only consume output of the following fragments j > i.
     *
     * TODO: take data partition of the plan fragments into account; in particular,
     * coordinate between hash partitioning for aggregation and hash partitioning
     * for analytic computation more generally than what createQueryPlan() does
     * right now (the coordination only happens if the same select block does both
     * the aggregation and analytic computation).
     */
    public ArrayList<PlanFragment> createPlanFragments(
            PlanNode singleNodePlan) throws UserException, AnalysisException {
        Preconditions.checkState(!ctx_.isSingleNodeExec());
        // AnalysisContext.AnalysisResult analysisResult = ctx_.getAnalysisResult();
        QueryStmt queryStmt = ctx_.getQueryStmt();
        ArrayList<PlanFragment> fragments = Lists.newArrayList();
        // For inserts or CTAS, unless there is a limit, leave the root fragment
        // partitioned, otherwise merge everything into a single coordinator fragment,
        // so we can pass it back to the client.
        boolean isPartitioned = false;
        // if ((analysisResult.isInsertStmt() || analysisResult.isCreateTableAsSelectStmt()
        //         || analysisResult.isUpdateStmt() || analysisResult.isDeleteStmt())
        //         && !singleNodePlan.hasLimit()) {
        //     Preconditions.checkState(!queryStmt.hasOffset());
        //     isPartitioned = true;
        // }
        if (ctx_.isInsert() && !singleNodePlan.hasLimit()) {
            Preconditions.checkState(!queryStmt.hasOffset());
            isPartitioned = true;
        }
        long perNodeMemLimit = ctx_.getQueryOptions().mem_limit;
        if (LOG.isDebugEnabled()) {
            LOG.debug("create plan fragments");
            LOG.debug("memlimit=" + Long.toString(perNodeMemLimit));
        }
        createPlanFragments(singleNodePlan, isPartitioned, perNodeMemLimit, fragments);
        return fragments;
    }

    private boolean isFragmentPartitioned(PlanFragment fragment) {
        return fragment.isPartitioned() && fragment.getPlanRoot().getNumInstances() > 1;
    }

    PlanFragment createInsertFragment(
            PlanFragment inputFragment, InsertStmt stmt, ArrayList<PlanFragment> fragments)
            throws UserException {
        Table targetTable = stmt.getTargetTable();
        Boolean isRepart = stmt.isRepartition();
        // When inputFragment is partitioned:
        //      1. If target table is partitioned, we need repartitioned. Or a merge node if hint has "NOSHUFFLE"
        //      1.a: If target table is random partitioned, return inputFragment
        //      2. If target table is not partitioned, we must have a merge node
        // When inputFragment is not partitioned:
        //      1. If target table is partitioned, we can return inputFragment; or repartition if hints has "SHUFFLE"
        //      2. If target table is not partitioned, return inputFragment
        boolean needRepartition = false;
        boolean needMerge = false;
        if (isFragmentPartitioned(inputFragment)) {
            if (targetTable.isPartitioned()) {
                if (stmt.getDataPartition().getType() == TPartitionType.RANDOM) {
                    return inputFragment;
                }
                if (isRepart != null && !isRepart) {
                    needMerge = true;
                } else {
                    needRepartition = true;
                }
            } else {
                needMerge = true;
            }
        } else {
            if (targetTable.isPartitioned()) {
                if (isRepart != null && isRepart) {
                    needRepartition = true;
                } else {
                    return inputFragment;
                }
            } else {
                return inputFragment;
            }
        }

        // Need a merge node to merge all partition of input framgent
        if (needMerge) {
            PlanFragment newInputFragment = createMergeFragment(inputFragment);
            fragments.add(newInputFragment);
            return newInputFragment;
        }

        // Following is repartition logic
        Preconditions.checkState(needRepartition);

        ExchangeNode exchNode = new ExchangeNode(ctx_.getNextNodeId(), inputFragment.getPlanRoot(), false);
        exchNode.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
        // exchNode.computeStats(analyzer);
        // exchNode.createDefaultSmap(analyzer);
        exchNode.init(ctx_.getRootAnalyzer());
        DataPartition dataPartition = stmt.getDataPartition();
        PlanFragment fragment = new PlanFragment(ctx_.getNextFragmentId(), exchNode, dataPartition);
        inputFragment.setDestination(exchNode);
        inputFragment.setOutputPartition(dataPartition);
        fragments.add(fragment);
        return fragment;
    }

    /**
     * Return plan fragment that produces result of 'root'; recursively creates
     * all input fragments to the returned fragment.
     * If a new fragment is created, it is appended to 'fragments', so that
     * each fragment is preceded by those from which it consumes the output.
     * If 'isPartitioned' is false, the returned fragment is unpartitioned;
     * otherwise it may be partitioned, depending on whether its inputs are
     * partitioned; the partition function is derived from the inputs.
     */
    private PlanFragment createPlanFragments(
            PlanNode root, boolean isPartitioned,
            long perNodeMemLimit, ArrayList<PlanFragment> fragments) throws UserException, AnalysisException {
        ArrayList<PlanFragment> childFragments = Lists.newArrayList();
        for (PlanNode child : root.getChildren()) {
            // allow child fragments to be partitioned, unless they contain a limit clause
            // (the result set with the limit constraint needs to be computed centrally);
            // merge later if needed
            boolean childIsPartitioned = !child.hasLimit();
            // Do not fragment the subplan of a SubplanNode since it is executed locally.
            // TODO()
            // if (root instanceof SubplanNode && child == root.getChild(1)) continue;
            childFragments.add(
                    createPlanFragments(child, childIsPartitioned, perNodeMemLimit, fragments));
        }

        PlanFragment result = null;
        if (root instanceof ScanNode) {
            result = createScanFragment(root);
            fragments.add(result);
        } else if (root instanceof HashJoinNode) {
            Preconditions.checkState(childFragments.size() == 2);
            result = createHashJoinFragment((HashJoinNode) root, childFragments.get(1),
                    childFragments.get(0), perNodeMemLimit, fragments);
        } else if (root instanceof CrossJoinNode) {
            result = createCrossJoinFragment((CrossJoinNode) root, childFragments.get(1),
                    childFragments.get(0));
        } else if (root instanceof SelectNode) {
            result = createSelectNodeFragment((SelectNode) root, childFragments);
        } else if (root instanceof OlapRewriteNode) {
            result = createOlapRewriteNodeFragment((OlapRewriteNode) root, childFragments);
        } else if (root instanceof SetOperationNode) {
            result = createSetOperationNodeFragment((SetOperationNode) root, childFragments, fragments);
        } else if (root instanceof MergeNode) {
            result = createMergeNodeFragment((MergeNode) root, childFragments, fragments);
        } else if (root instanceof AggregationNode) {
            result = createAggregationFragment((AggregationNode) root, childFragments.get(0), fragments);
        } else if (root instanceof SortNode) {
            if (((SortNode) root).isAnalyticSort()) {
                // don't parallelize this like a regular SortNode
                result = createAnalyticFragment((SortNode) root, childFragments.get(0), fragments);
            } else {
                result = createOrderByFragment((SortNode) root, childFragments.get(0));
            }
        } else if (root instanceof AnalyticEvalNode) {
            result = createAnalyticFragment(root, childFragments.get(0), fragments);
        } else if (root instanceof EmptySetNode) {
            result = new PlanFragment(ctx_.getNextFragmentId(), root, DataPartition.UNPARTITIONED);
        } else if (root instanceof RepeatNode) {
            result = createRepeatNodeFragment((RepeatNode) root, childFragments.get(0), fragments);
        } else if (root instanceof AssertNumRowsNode) {
            result = createAssertFragment(root, childFragments.get(0));
        } else {
            throw new UserException(
                    "Cannot create plan fragment for this node type: " + root.getExplainString());
        }
        // move 'result' to end, it depends on all of its children
        fragments.remove(result);
        fragments.add(result);

        if (!isPartitioned && result.isPartitioned() && result.getPlanRoot().getNumInstances() > 1) {
            result = createMergeFragment(result);
            fragments.add(result);
        }

        return result;
    }

    /**
     * Return unpartitioned fragment that merges the input fragment's output via
     * an ExchangeNode.
     * Requires that input fragment be partitioned.
     */
    private PlanFragment createMergeFragment(PlanFragment inputFragment)
            throws UserException {
        Preconditions.checkState(inputFragment.isPartitioned());

        // exchange node clones the behavior of its input, aside from the conjuncts
        ExchangeNode mergePlan =
                new ExchangeNode(ctx_.getNextNodeId(), inputFragment.getPlanRoot(), false);
        mergePlan.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
        mergePlan.init(ctx_.getRootAnalyzer());
        Preconditions.checkState(mergePlan.hasValidStats());
        PlanFragment fragment = new PlanFragment(ctx_.getNextFragmentId(), mergePlan, DataPartition.UNPARTITIONED);
        inputFragment.setDestination(mergePlan);
        return fragment;
    }

    /**
     * Create new randomly-partitioned fragment containing a single scan node.
     * TODO: take bucketing into account to produce a naturally hash-partitioned
     * fragment
     * TODO: hbase scans are range-partitioned on the row key
     */
    private PlanFragment createScanFragment(PlanNode node) {
        if (node instanceof MysqlScanNode) {
            return new PlanFragment(ctx_.getNextFragmentId(), node, DataPartition.UNPARTITIONED);
        } else if (node instanceof SchemaScanNode) {
            return new PlanFragment(ctx_.getNextFragmentId(), node, DataPartition.UNPARTITIONED);
        } else {
            // es scan node, olap scan node are random partitioned
            return new PlanFragment(ctx_.getNextFragmentId(), node, DataPartition.RANDOM);
        }
    }

    /**
     * Creates either a broadcast join or a repartitioning join, depending on the expected cost. If any of the inputs to
     * the cost computation is unknown, it assumes the cost will be 0. Costs being equal, it'll favor partitioned over
     * broadcast joins. If perNodeMemLimit > 0 and the size of the hash table for a broadcast join is expected to exceed
     * that mem limit, switches to partitioned join instead. TODO: revisit the choice of broadcast as the default TODO:
     * don't create a broadcast join if we already anticipate that this will exceed the query's memory budget.
     */
    private PlanFragment createHashJoinFragment(HashJoinNode node, PlanFragment rightChildFragment,
                                                PlanFragment leftChildFragment, long perNodeMemLimit,
                                                ArrayList<PlanFragment> fragments)
            throws UserException {
        // broadcast: send the rightChildFragment's output to each node executing
        // the leftChildFragment; the cost across all nodes is proportional to the
        // total amount of data sent

        // NOTICE:
        // for now, only MysqlScanNode and OlapScanNode has Cardinality.
        // OlapScanNode's cardinality is calculated by row num and data size,
        // and MysqlScanNode's cardinality is always 0.
        // Other ScanNode's cardinality is -1.
        //
        // So if there are other kind of scan node in join query, it won't be able to calculate the cost of
        // join normally and result in both "broadcastCost" and "partitionCost" be 0. And this will lead
        // to a SHUFFLE join.
        PlanNode rhsTree = rightChildFragment.getPlanRoot();
        long rhsDataSize = 0;
        long broadcastCost = 0;
        if (rhsTree.getCardinality() != -1 && leftChildFragment.getNumNodes() != -1) {
            rhsDataSize = Math.round((double) rhsTree.getCardinality() * rhsTree.getAvgRowSize());
            broadcastCost = rhsDataSize * leftChildFragment.getNumNodes();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("broadcast: cost=" + Long.toString(broadcastCost));
            LOG.debug("card=" + Long.toString(rhsTree.getCardinality()) + " row_size="
                    + Float.toString(rhsTree.getAvgRowSize()) + " #nodes="
                    + Integer.toString(leftChildFragment.getNumNodes()));
        }

        // repartition: both left- and rightChildFragment are partitioned on the
        // join exprs
        // TODO: take existing partition of input fragments into account to avoid
        // unnecessary repartitioning
        PlanNode lhsTree = leftChildFragment.getPlanRoot();
        long partitionCost = 0;
        if (lhsTree.getCardinality() != -1 && rhsTree.getCardinality() != -1) {
            partitionCost = Math.round(
                    (double) lhsTree.getCardinality() * lhsTree.getAvgRowSize() + (double) rhsTree
                            .getCardinality() * rhsTree.getAvgRowSize());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("partition: cost=" + Long.toString(partitionCost));
            LOG.debug("lhs card=" + Long.toString(lhsTree.getCardinality()) + " row_size="
                    + Float.toString(lhsTree.getAvgRowSize()));
            LOG.debug("rhs card=" + Long.toString(rhsTree.getCardinality()) + " row_size="
                    + Float.toString(rhsTree.getAvgRowSize()));
            LOG.debug(rhsTree.getExplainString());
        }

        boolean doBroadcast;
        // we do a broadcast join if
        // - we're explicitly told to do so
        // - or if it's cheaper and we weren't explicitly told to do a partitioned join
        // - and we're not doing a full or right outer join (those require the left-hand
        //   side to be partitioned for correctness)
        // - and the expected size of the hash tbl doesn't exceed perNodeMemLimit
        // we set partition join as default when broadcast join cost equals partition join cost
        if (node.getJoinOp() != JoinOperator.RIGHT_OUTER_JOIN
                && node.getJoinOp() != JoinOperator.FULL_OUTER_JOIN
                && (perNodeMemLimit == 0 || Math.round(
                (double) rhsDataSize * PlannerContext.HASH_TBL_SPACE_OVERHEAD) <= perNodeMemLimit)
                && (node.getInnerRef().isBroadcastJoin() || (!node.getInnerRef().isPartitionJoin()
                && broadcastCost < partitionCost))) {
            doBroadcast = true;
        } else {
            doBroadcast = false;
        }

        List<String> reason = Lists.newArrayList();
        if (canColocateJoin(node, leftChildFragment, rightChildFragment, reason)) {
            node.setColocate(true, "");
            //node.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);
            node.setChild(0, leftChildFragment.getPlanRoot());
            node.setChild(1, rightChildFragment.getPlanRoot());
            leftChildFragment.setPlanRoot(node);
            fragments.remove(rightChildFragment);
            return leftChildFragment;
        } else {
            node.setColocate(false, reason.get(0));
        }

        if (doBroadcast) {
            node.setDistributionMode(HashJoinNode.DistributionMode.BROADCAST);
            // Doesn't create a new fragment, but modifies leftChildFragment to execute
            // the join; the build input is provided by an ExchangeNode, which is the
            // destination of the rightChildFragment's output
            node.setChild(0, leftChildFragment.getPlanRoot());
            connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
            leftChildFragment.setPlanRoot(node);

            // Push down the predicates constructed by the right child when the
            // join op is inner join or left semi join.
            if (node.getJoinOp().isInnerJoin() || node.getJoinOp().isLeftSemiJoin()) {
                node.setIsPushDown(true);
            } else {
                node.setIsPushDown(false);
            }  
            return leftChildFragment;
        } else {
            node.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);
            // Create a new parent fragment containing a HashJoin node with two
            // ExchangeNodes as inputs; the latter are the destinations of the
            // left- and rightChildFragments, which now partition their output
            // on their respective join exprs.
            // The new fragment is hash-partitioned on the lhs input join exprs.
            // TODO: create equivalence classes based on equality predicates

            // first, extract join exprs
            List<BinaryPredicate> eqJoinConjuncts = node.getEqJoinConjuncts();
            List<Expr> lhsJoinExprs = Lists.newArrayList();
            List<Expr> rhsJoinExprs = Lists.newArrayList();
            for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
                // no remapping necessary
                lhsJoinExprs.add(eqJoinPredicate.getChild(0).clone(null));
                rhsJoinExprs.add(eqJoinPredicate.getChild(1).clone(null));
            }

            // create the parent fragment containing the HashJoin node
            DataPartition lhsJoinPartition = new DataPartition(TPartitionType.HASH_PARTITIONED,
                    Expr.cloneList(lhsJoinExprs, null));
            ExchangeNode lhsExchange =
                    new ExchangeNode(ctx_.getNextNodeId(), leftChildFragment.getPlanRoot(), false);
            lhsExchange.setNumInstances(leftChildFragment.getPlanRoot().getNumInstances());
            lhsExchange.init(ctx_.getRootAnalyzer());

            DataPartition rhsJoinPartition =
                    new DataPartition(TPartitionType.HASH_PARTITIONED, rhsJoinExprs);
            ExchangeNode rhsExchange =
                    new ExchangeNode(ctx_.getNextNodeId(), rightChildFragment.getPlanRoot(), false);
            rhsExchange.setNumInstances(rightChildFragment.getPlanRoot().getNumInstances());
            rhsExchange.init(ctx_.getRootAnalyzer());

            node.setChild(0, lhsExchange);
            node.setChild(1, rhsExchange);
            PlanFragment joinFragment = new PlanFragment(ctx_.getNextFragmentId(), node, lhsJoinPartition);
            // connect the child fragments
            leftChildFragment.setDestination(lhsExchange);
            leftChildFragment.setOutputPartition(lhsJoinPartition);
            rightChildFragment.setDestination(rhsExchange);
            rightChildFragment.setOutputPartition(rhsJoinPartition);

            return joinFragment;
        }
    }

    private boolean canColocateJoin(HashJoinNode node, PlanFragment leftChildFragment, PlanFragment rightChildFragment,
            List<String> cannotReason) {
        if (Config.disable_colocate_join) {
            cannotReason.add("Disabled");
            return false;
        }

        if (ConnectContext.get().getSessionVariable().isDisableColocateJoin()) {
            cannotReason.add("Session disabled");
            return false;
        }

        PlanNode leftRoot = leftChildFragment.getPlanRoot();
        PlanNode rightRoot = rightChildFragment.getPlanRoot();

        //leftRoot should be ScanNode or HashJoinNode, rightRoot should be ScanNode
        if (leftRoot instanceof OlapScanNode && rightRoot instanceof OlapScanNode) {
            return canColocateJoin(node, leftRoot, rightRoot, cannotReason);
        }

        if (leftRoot instanceof HashJoinNode && rightRoot instanceof OlapScanNode) {
            while (leftRoot instanceof HashJoinNode) {
                if (((HashJoinNode)leftRoot).isColocate()) {
                    leftRoot = leftRoot.getChild(0);
                } else {
                    cannotReason.add("left hash join node can not do colocate");
                    return false;
                }
            }
            if (leftRoot instanceof OlapScanNode) {
                return canColocateJoin(node, leftRoot, rightRoot, cannotReason);
            }
        }

        cannotReason.add("Node type not match");
        return false;
    }

    //the table must be colocate
    //the colocate group must be stable
    //the eqJoinConjuncts must contain the distributionColumns
    private boolean canColocateJoin(HashJoinNode node, PlanNode leftRoot, PlanNode rightRoot,
            List<String> cannotReason) {
        OlapTable leftTable = ((OlapScanNode) leftRoot).getOlapTable();
        OlapTable rightTable = ((OlapScanNode) rightRoot).getOlapTable();

        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();

        //1 the table must be colocate
        if (!colocateIndex.isSameGroup(leftTable.getId(), rightTable.getId())) {
            cannotReason.add("table not in same group");
            return false;
        }

        //2 the colocate group must be stable
        GroupId groupId = colocateIndex.getGroup(leftTable.getId());
        if (colocateIndex.isGroupUnstable(groupId)) {
            cannotReason.add("group is not stable");
            return false;
        }

        DistributionInfo leftDistribution = leftTable.getDefaultDistributionInfo();
        DistributionInfo rightDistribution = rightTable.getDefaultDistributionInfo();

        if (leftDistribution instanceof HashDistributionInfo && rightDistribution instanceof HashDistributionInfo) {
            List<Column> leftColumns = ((HashDistributionInfo) leftDistribution).getDistributionColumns();
            List<Column> rightColumns = ((HashDistributionInfo) rightDistribution).getDistributionColumns();

            List<BinaryPredicate> eqJoinConjuncts = node.getEqJoinConjuncts();
            for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
                Expr lhsJoinExpr = eqJoinPredicate.getChild(0);
                Expr rhsJoinExpr = eqJoinPredicate.getChild(1);
                if (lhsJoinExpr.unwrapSlotRef() == null || rhsJoinExpr.unwrapSlotRef() == null) {
                    continue;
                }

                SlotDescriptor leftSlot = lhsJoinExpr.unwrapSlotRef().getDesc();
                SlotDescriptor rightSlot = rhsJoinExpr.unwrapSlotRef().getDesc();

                //3 the eqJoinConjuncts must contain the distributionColumns
                if (leftColumns.contains(leftSlot.getColumn()) && rightColumns.contains(rightSlot.getColumn())) {
                    return true;
                }
            }
        }

        cannotReason.add("column not match");
        return false;
    }

    /**
     * Modifies the leftChildFragment to execute a cross join. The right child input is provided by an ExchangeNode,
     * which is the destination of the rightChildFragment's output.
     */
    private PlanFragment createCrossJoinFragment(
            CrossJoinNode node, PlanFragment rightChildFragment, PlanFragment leftChildFragment)
            throws UserException {
        // The rhs tree is going to send data through an exchange node which effectively
        // compacts the data. No reason to do it again at the rhs root node.
        rightChildFragment.getPlanRoot().setCompactData(false);
        node.setChild(0, leftChildFragment.getPlanRoot());
        connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
        leftChildFragment.setPlanRoot(node);
        return leftChildFragment;
    }

    /**
     * Creates an unpartitioned fragment that merges the outputs of all of its children (with a single ExchangeNode),
     * corresponding to the 'mergeNode' of the non-distributed plan. Each of the child fragments receives a MergeNode as
     * a new plan root (with the child fragment's plan tree as its only input), so that each child fragment's output is
     * mapped onto the MergeNode's result tuple id. TODO: if this is implementing a UNION DISTINCT, the parent of the
     * mergeNode is a duplicate-removing AggregationNode, which might make sense to apply to the children as well, in
     * order to reduce the amount of data that needs to be sent to the parent; augment the planner to decide whether
     * that would reduce the runtime. TODO: since the fragment that does the merge is unpartitioned, it can absorb all
     * child fragments that are also unpartitioned
     */
    private PlanFragment createMergeNodeFragment(MergeNode mergeNode,
                                                 ArrayList<PlanFragment> childFragments,
                                                 ArrayList<PlanFragment> fragments)
            throws UserException {
        Preconditions.checkState(mergeNode.getChildren().size() == childFragments.size());

        // If the mergeNode only has constant exprs, return it in an unpartitioned fragment.
        if (mergeNode.getChildren().isEmpty()) {
            Preconditions.checkState(!mergeNode.getConstExprLists().isEmpty());
            return new PlanFragment(ctx_.getNextFragmentId(), mergeNode, DataPartition.UNPARTITIONED);
        }

        // create an ExchangeNode to perform the merge operation of mergeNode;
        // the ExchangeNode retains the generic PlanNode parameters of mergeNode
        ExchangeNode exchNode = new ExchangeNode(ctx_.getNextNodeId(), mergeNode, true);
        exchNode.setNumInstances(1);
        exchNode.init(ctx_.getRootAnalyzer());
        PlanFragment parentFragment =
                new PlanFragment(ctx_.getNextFragmentId(), exchNode, DataPartition.UNPARTITIONED);

        // we don't expect to be parallelizing a MergeNode that was inserted solely
        // to evaluate conjuncts (ie, that doesn't explicitly materialize its output)
        Preconditions.checkState(mergeNode.getTupleIds().size() == 1);

        for (int i = 0; i < childFragments.size(); ++i) {
            PlanFragment childFragment = childFragments.get(i);
            // create a clone of mergeNode; we want to keep the limit and conjuncts
            MergeNode childMergeNode = new MergeNode(ctx_.getNextNodeId(), mergeNode);
            List<Expr> resultExprs = Expr.cloneList(mergeNode.getResultExprLists().get(i), null);
            childMergeNode.addChild(childFragment.getPlanRoot(), resultExprs);
            childFragment.setPlanRoot(childMergeNode);
            childFragment.setDestination(exchNode);
        }

        // Add an unpartitioned child fragment with a MergeNode for the constant exprs.
        if (!mergeNode.getConstExprLists().isEmpty()) {
            MergeNode childMergeNode = new MergeNode(ctx_.getNextNodeId(), mergeNode);
            childMergeNode.init(ctx_.getRootAnalyzer());
            childMergeNode.getConstExprLists().addAll(mergeNode.getConstExprLists());
            // Clear original constant exprs to make sure nobody else picks them up.
            mergeNode.getConstExprLists().clear();
            PlanFragment childFragment =
                    new PlanFragment(ctx_.getNextFragmentId(), childMergeNode, DataPartition.UNPARTITIONED);
            childFragment.setPlanRoot(childMergeNode);
            childFragment.setDestination(exchNode);
            childFragments.add(childFragment);
            fragments.add(childFragment);
        }
        return parentFragment;
    }

    /**
     * Returns a new fragment with a UnionNode as its root. The data partition of the
     * returned fragment and how the data of the child fragments is consumed depends on the
     * data partitions of the child fragments:
     * - All child fragments are unpartitioned or partitioned: The returned fragment has an
     *   UNPARTITIONED or RANDOM data partition, respectively. The UnionNode absorbs the
     *   plan trees of all child fragments.
     * - Mixed partitioned/unpartitioned child fragments: The returned fragment is
     *   RANDOM partitioned. The plan trees of all partitioned child fragments are absorbed
     *   into the UnionNode. All unpartitioned child fragments are connected to the
     *   UnionNode via a RANDOM exchange, and remain unchanged otherwise.
     */
    private PlanFragment createSetOperationNodeFragment(
            SetOperationNode setOperationNode, ArrayList<PlanFragment> childFragments,
            ArrayList<PlanFragment> fragments) throws UserException {
        Preconditions.checkState(setOperationNode.getChildren().size() == childFragments.size());

        // A UnionNode could have no children or constant selects if all of its operands
        // were dropped because of constant predicates that evaluated to false.
        if (setOperationNode.getChildren().isEmpty()) {
            return new PlanFragment(
                    ctx_.getNextFragmentId(), setOperationNode, DataPartition.UNPARTITIONED);
        }

        Preconditions.checkState(!childFragments.isEmpty());
        int numUnpartitionedChildFragments = 0;
        for (int i = 0; i < childFragments.size(); ++i) {
            if (!childFragments.get(i).isPartitioned()) ++numUnpartitionedChildFragments;
        }

        // remove all children to avoid them being tagged with the wrong
        // fragment (in the PlanFragment c'tor; we haven't created ExchangeNodes yet)
        setOperationNode.clearChildren();

        // If all child fragments are unpartitioned, return a single unpartitioned fragment
        // with a UnionNode that merges all child fragments.
        if (numUnpartitionedChildFragments == childFragments.size()) {
            PlanFragment setOperationFragment = new PlanFragment(
                    ctx_.getNextFragmentId(), setOperationNode, DataPartition.UNPARTITIONED);
            // Absorb the plan trees of all childFragments into unionNode
            // and fix up the fragment tree in the process.
            for (int i = 0; i < childFragments.size(); ++i) {
                setOperationNode.addChild(childFragments.get(i).getPlanRoot());
                setOperationFragment.setFragmentInPlanTree(setOperationNode.getChild(i));
                setOperationFragment.addChildren(childFragments.get(i).getChildren());
            }
            setOperationNode.init(ctx_.getRootAnalyzer());
            // All child fragments have been absorbed into unionFragment.
            fragments.removeAll(childFragments);
            return setOperationFragment;
        }

        // There is at least one partitioned child fragment.
        PlanFragment setOperationFragment = new PlanFragment(ctx_.getNextFragmentId(), setOperationNode,
                DataPartition.RANDOM);
        for (int i = 0; i < childFragments.size(); ++i) {
            PlanFragment childFragment = childFragments.get(i);
            /* if (childFragment.isPartitioned() && childFragment.getPlanRoot().getNumInstances() > 1) {
             *  // absorb the plan trees of all partitioned child fragments into unionNode
             *  unionNode.addChild(childFragment.getPlanRoot());
             *  unionFragment.setFragmentInPlanTree(unionNode.getChild(i));
             *  unionFragment.addChildren(childFragment.getChildren());
             *  fragments.remove(childFragment);
             * } else {
             *  // dummy entry for subsequent addition of the ExchangeNode
             *  unionNode.addChild(null);
             *  // Connect the unpartitioned child fragments to unionNode via a random exchange.
             *  connectChildFragment(unionNode, i, unionFragment, childFragment);
             *  childFragment.setOutputPartition(DataPartition.RANDOM);
             * }
             */

            // UnionNode should't be absorbed by childFragment, because it reduce 
            // the degree of concurrency.
            // chenhao16 add
            // dummy entry for subsequent addition of the ExchangeNode
            setOperationNode.addChild(null);
            // Connect the unpartitioned child fragments to SetOperationNode via a random exchange.
            connectChildFragment(setOperationNode, i, setOperationFragment, childFragment);
            childFragment.setOutputPartition(
                    DataPartition.hashPartitioned(setOperationNode.getMaterializedResultExprLists_().get(i)));
        }
        setOperationNode.init(ctx_.getRootAnalyzer());
        return setOperationFragment;
    }

    /**
     * Adds the SelectNode as the new plan root to the child fragment and returns the child fragment.
     */
    private PlanFragment createSelectNodeFragment(SelectNode selectNode, ArrayList<PlanFragment> childFragments) {
        Preconditions.checkState(selectNode.getChildren().size() == childFragments.size());
        PlanFragment childFragment = childFragments.get(0);
        // set the child explicitly, an ExchangeNode might have been inserted
        // (whereas selectNode.child[0] would point to the original child)
        selectNode.setChild(0, childFragment.getPlanRoot());
        childFragment.setPlanRoot(selectNode);
        return childFragment;
    }

    private PlanFragment createOlapRewriteNodeFragment(
            OlapRewriteNode olapRewriteNode, ArrayList<PlanFragment> childFragments) {
        Preconditions.checkState(olapRewriteNode.getChildren().size() == childFragments.size());
        PlanFragment childFragment = childFragments.get(0);
        olapRewriteNode.setChild(0, childFragment.getPlanRoot());
        childFragment.setPlanRoot(olapRewriteNode);
        return childFragment;
    }

    /**
     * Replace node's child at index childIdx with an ExchangeNode that receives its input from childFragment.
     */
    private void connectChildFragment(
            PlanNode node, int childIdx,
            PlanFragment parentFragment, PlanFragment childFragment)
            throws UserException {
        ExchangeNode exchangeNode = new ExchangeNode(ctx_.getNextNodeId(), childFragment.getPlanRoot(), false);
        exchangeNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        exchangeNode.init(ctx_.getRootAnalyzer());
        exchangeNode.setFragment(parentFragment);
        node.setChild(childIdx, exchangeNode);
        childFragment.setDestination(exchangeNode);
    }

    /**
     * Create a new fragment containing a single ExchangeNode that consumes the output
     * of childFragment, set the destination of childFragment to the new parent
     * and the output partition of childFragment to that of the new parent.
     * TODO: the output partition of a child isn't necessarily the same as the data
     * partition of the receiving parent (if there is more materialization happening
     * in the parent, such as during distinct aggregation). Do we care about the data
     * partition of the parent being applicable to the *output* of the parent (it's
     * correct for the input).
     */
    private PlanFragment createParentFragment(PlanFragment childFragment, DataPartition parentPartition)
            throws UserException {
        ExchangeNode exchangeNode = new ExchangeNode(ctx_.getNextNodeId(), childFragment.getPlanRoot(), false);
        exchangeNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        exchangeNode.init(ctx_.getRootAnalyzer());
        PlanFragment parentFragment = new PlanFragment(ctx_.getNextFragmentId(), exchangeNode, parentPartition);
        childFragment.setDestination(exchangeNode);
        childFragment.setOutputPartition(parentPartition);
        return parentFragment;
    }

    /**
     * Returns a fragment that materializes the aggregation result of 'node'.
     * If the child fragment is partitioned, the result fragment will be partitioned on
     * the grouping exprs of 'node'.
     * If 'node' is phase 1 of a 2-phase DISTINCT aggregation, this will simply
     * add 'node' to the child fragment and return the child fragment; the new
     * fragment will be created by the subsequent call of createAggregationFragment()
     * for the phase 2 AggregationNode.
     */
    private PlanFragment createAggregationFragment(
            AggregationNode node, PlanFragment childFragment, ArrayList<PlanFragment> fragments)
            throws UserException {
        if (!childFragment.isPartitioned()) {
            // nothing to distribute; do full aggregation directly within childFragment
            childFragment.addPlanRoot(node);
            return childFragment;
        }

        if (node.getAggInfo().isDistinctAgg()) {
            // 'node' is phase 1 of a DISTINCT aggregation; the actual agg fragment
            // will get created in the next createAggregationFragment() call
            // for the parent AggregationNode
            childFragment.addPlanRoot(node);
            return childFragment;
        }

        // check size
        if (childFragment.getPlanRoot().getNumInstances() <= 1) {
            childFragment.addPlanRoot(node);
            return childFragment;
        }

        // 2nd phase of DISTINCT aggregation
        boolean isDistinct = node.getChild(0) instanceof AggregationNode
                && ((AggregationNode) (node.getChild(0))).getAggInfo().isDistinctAgg();
        if (isDistinct) {
            return createPhase2DistinctAggregationFragment(node, childFragment, fragments);
        } else {
            return createMergeAggregationFragment(node, childFragment);
        }
    }

    private PlanFragment createRepeatNodeFragment(
            RepeatNode repeatNode, PlanFragment childFragment, ArrayList<PlanFragment> fragments)
            throws UserException {
        repeatNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        childFragment.addPlanRoot(repeatNode);
        return childFragment;
    }

    /**
     * Returns a fragment that materializes the final result of an aggregation where
     * 'childFragment' is a partitioned fragment and 'node' is not part of a distinct
     * aggregation.
     */
    private PlanFragment createMergeAggregationFragment(AggregationNode node, PlanFragment childFragment)
            throws UserException {
        Preconditions.checkArgument(childFragment.isPartitioned());
        ArrayList<Expr> groupingExprs = node.getAggInfo().getGroupingExprs();
        boolean hasGrouping = !groupingExprs.isEmpty();

        DataPartition parentPartition = null;
        if (hasGrouping) {
            List<Expr> partitionExprs = node.getAggInfo().getPartitionExprs();
            if (partitionExprs == null) partitionExprs = groupingExprs;
            // boolean childHasCompatPartition = ctx_.getRootAnalyzer().equivSets(partitionExprs,
            //         childFragment.getDataPartition().getPartitionExprs());
            // if (childHasCompatPartition && !childFragment.refsNullableTupleId(partitionExprs)) {
            //     // The data is already partitioned on the required expressions. We can do the
            //     // aggregation in the child fragment without an extra merge step.
            //     // An exchange+merge step is required if the grouping exprs reference a tuple
            //     // that is made nullable in 'childFragment' to bring NULLs from outer-join
            //     // non-matches together.
            //     childFragment.addPlanRoot(node);
            //     return childFragment;
            // }
            // the parent fragment is partitioned on the grouping exprs;
            // substitute grouping exprs to reference the *output* of the agg, not the input
            partitionExprs = Expr.substituteList(partitionExprs,
                    node.getAggInfo().getIntermediateSmap(), ctx_.getRootAnalyzer(), false);
            parentPartition = DataPartition.hashPartitioned(partitionExprs);
        } else {
            // the parent fragment is unpartitioned
            parentPartition = DataPartition.UNPARTITIONED;
        }

        // the original aggregation materializes the intermediate agg tuple and goes
        // into the child fragment; merge aggregation materializes the output agg tuple
        // and goes into a parent fragment
        childFragment.addPlanRoot(node);
        node.setIntermediateTuple();

        node.setIsPreagg(ctx_);

        // if there is a limit, we need to transfer it from the pre-aggregation
        // node in the child fragment to the merge aggregation node in the parent
        long limit = node.getLimit();
        node.unsetLimit();
        node.unsetNeedsFinalize();

        // place a merge aggregation step in a new fragment
        PlanFragment mergeFragment = createParentFragment(childFragment, parentPartition);
        AggregationNode mergeAggNode = new AggregationNode(ctx_.getNextNodeId(),
                mergeFragment.getPlanRoot(), node.getAggInfo().getMergeAggInfo());
        mergeAggNode.init(ctx_.getRootAnalyzer());
        mergeAggNode.setLimit(limit);
        // Merge of non-grouping agg only processes one tuple per Impala daemon - codegen
        // will cost more than benefit.
        if (!hasGrouping) {
            // TODO(zc)
            // mergeFragment.getPlanRoot().setDisableCodegen(true);
            // mergeAggNode.setDisableCodegen(true);
        }

        // HAVING predicates can only be evaluated after the merge agg step
        node.transferConjuncts(mergeAggNode);
        // Recompute stats after transferring the conjuncts_ (order is important).
        node.computeStats(ctx_.getRootAnalyzer());
        mergeFragment.getPlanRoot().computeStats(ctx_.getRootAnalyzer());
        mergeAggNode.computeStats(ctx_.getRootAnalyzer());
        // Set new plan root after updating stats.
        mergeFragment.addPlanRoot(mergeAggNode);

        return mergeFragment;
    }

    /**
     * Returns a fragment that materialises the final result of a distinct aggregation
     * where 'childFragment' is a partitioned fragment with the first phase aggregation
     * as its root and 'node' is the second phase of the distinct aggregation.
     */
    private PlanFragment createPhase2DistinctAggregationFragment(
            AggregationNode node,
            PlanFragment childFragment, ArrayList<PlanFragment> fragments) throws UserException {
        ArrayList<Expr> groupingExprs = node.getAggInfo().getGroupingExprs();
        boolean hasGrouping = !groupingExprs.isEmpty();

        // The first-phase aggregation node is already in the child fragment.
        Preconditions.checkState(node.getChild(0) == childFragment.getPlanRoot());

        AggregateInfo firstPhaseAggInfo = ((AggregationNode) node.getChild(0)).getAggInfo();
        List<Expr> partitionExprs = null;
        boolean isMultiDistinct = node.getAggInfo().isMultiDistinct();
        if (hasGrouping) {
            // We need to do
            // - child fragment:
            //   * phase-1 aggregation
            // - merge fragment, hash-partitioned on grouping exprs:
            //   * merge agg of phase 1
            //   * phase 2 agg
            // The output partition exprs of the child are the (input) grouping exprs of the
            // parent. The grouping exprs reference the output tuple of the 1st phase, but the
            // partitioning happens on the intermediate tuple of the 1st phase.
            partitionExprs = Expr.substituteList(
                    groupingExprs, firstPhaseAggInfo.getOutputToIntermediateSmap(),
                    ctx_.getRootAnalyzer(), false);
        } else {
            // We need to do
            // - child fragment:
            //   * phase-1 aggregation
            // - merge fragment 1, hash-partitioned on distinct exprs:
            //   * merge agg of phase 1
            //   * phase 2 agg
            // - merge fragment 2, unpartitioned:
            //   * merge agg of phase 2
            if (!isMultiDistinct) {
                partitionExprs = Expr.substituteList(firstPhaseAggInfo.getGroupingExprs(),
                        firstPhaseAggInfo.getIntermediateSmap(), ctx_.getRootAnalyzer(), false);
            }
        }

        PlanFragment mergeFragment = null;
        boolean childHasCompatPartition = false; // analyzer..equivSets(partitionExprs,
        // childFragment.getDataPartition().getPartitionExprs());
        if (childHasCompatPartition) {
            // The data is already partitioned on the required expressions, we can skip the
            // phase 1 merge step.
            childFragment.addPlanRoot(node);
            mergeFragment = childFragment;
        } else {
            DataPartition mergePartition =
                    partitionExprs == null ? DataPartition.UNPARTITIONED : DataPartition.hashPartitioned(partitionExprs);
            // Convert the existing node to a preaggregation.
            AggregationNode preaggNode = (AggregationNode)node.getChild(0);
            
            preaggNode.setIsPreagg(ctx_);

            // place a merge aggregation step for the 1st phase in a new fragment
            mergeFragment = createParentFragment(childFragment, mergePartition);
            AggregateInfo phase1MergeAggInfo = firstPhaseAggInfo.getMergeAggInfo();
            AggregationNode phase1MergeAggNode =
                    new AggregationNode(ctx_.getNextNodeId(), preaggNode, phase1MergeAggInfo);
            phase1MergeAggNode.init(ctx_.getRootAnalyzer());
            phase1MergeAggNode.unsetNeedsFinalize();
            phase1MergeAggNode.setIntermediateTuple();
            mergeFragment.addPlanRoot(phase1MergeAggNode);

            // the 2nd-phase aggregation consumes the output of the merge agg;
            // if there is a limit, it had already been placed with the 2nd aggregation
            // step (which is where it should be)
            mergeFragment.addPlanRoot(node);
        }

        if (!hasGrouping && !isMultiDistinct) {
            // place the merge aggregation of the 2nd phase in an unpartitioned fragment;
            // add preceding merge fragment at end
            if (mergeFragment != childFragment) fragments.add(mergeFragment);

            node.unsetNeedsFinalize();
            node.setIntermediateTuple();
            // Any limit should be placed in the final merge aggregation node
            long limit = node.getLimit();
            node.unsetLimit();
            mergeFragment = createParentFragment(mergeFragment, DataPartition.UNPARTITIONED);
            AggregateInfo phase2MergeAggInfo = node.getAggInfo().getMergeAggInfo();
            AggregationNode phase2MergeAggNode = new AggregationNode(ctx_.getNextNodeId(), node,
                    phase2MergeAggInfo);
            phase2MergeAggNode.init(ctx_.getRootAnalyzer());
            // Transfer having predicates. If hasGrouping == true, the predicates should
            // instead be evaluated by the 2nd phase agg (the predicates are already there).
            node.transferConjuncts(phase2MergeAggNode);
            phase2MergeAggNode.setLimit(limit);
            mergeFragment.addPlanRoot(phase2MergeAggNode);
        }
        return mergeFragment;
    }

    /**
     * Returns a fragment that produces the output of either an AnalyticEvalNode
     * or of the SortNode that provides the input to an AnalyticEvalNode.
     * ('node' can be either an AnalyticEvalNode or a SortNode).
     * The returned fragment is either partitioned on the Partition By exprs or
     * unpartitioned in the absence of such exprs.
     */
    private PlanFragment createAnalyticFragment(
            PlanNode node, PlanFragment childFragment, List<PlanFragment> fragments)
            throws UserException, AnalysisException {
        Preconditions.checkState(
                node instanceof SortNode || node instanceof AnalyticEvalNode);

        if (node instanceof AnalyticEvalNode) {
            AnalyticEvalNode analyticNode = (AnalyticEvalNode) node;

            if (analyticNode.getPartitionExprs().isEmpty()
                    && analyticNode.getOrderByElements().isEmpty()) {
                // no Partition-By/Order-By exprs: compute analytic exprs in single
                // unpartitioned fragment
                PlanFragment fragment = childFragment;
                if (childFragment.isPartitioned()) {
                    fragment = createParentFragment(childFragment, DataPartition.UNPARTITIONED);
                }
                fragment.addPlanRoot(analyticNode);
                return fragment;
            } else {
                analyticNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
                childFragment.addPlanRoot(analyticNode);
                return childFragment;
            }
        }

        SortNode sortNode = (SortNode) node;
        Preconditions.checkState(sortNode.isAnalyticSort());
        PlanFragment analyticFragment = childFragment;
        if (sortNode.getInputPartition() != null) {
            sortNode.getInputPartition().substitute(
                    childFragment.getPlanRoot().getOutputSmap(), ctx_.getRootAnalyzer());

            // Make sure the childFragment's output is partitioned as required by the sortNode.
            // Even if the fragment and the sort partition exprs are equal, an exchange is
            // required if the sort partition exprs reference a tuple that is made nullable in
            // 'childFragment' to bring NULLs from outer-join non-matches together.
            DataPartition sortPartition = sortNode.getInputPartition();
            if (!childFragment.getDataPartition().equals(sortPartition)) {
                    // TODO(zc) || childFragment.refsNullableTupleId(sortPartition.getPartitionExprs())) {
                analyticFragment = createParentFragment(childFragment, sortNode.getInputPartition());
            }
        }

        analyticFragment.addPlanRoot(sortNode);
        return analyticFragment;
    }

    /**
     * Returns a new unpartitioned fragment that materializes the result of the given
     * SortNode. If the child fragment is partitioned, returns a new fragment with a
     * sort-merging exchange that merges the results of the partitioned sorts.
     * The offset and limit are adjusted in the child and parent plan nodes to produce
     * the correct result.
     */
    private PlanFragment createOrderByFragment(
            SortNode node, PlanFragment childFragment)
            throws UserException {
        node.setChild(0, childFragment.getPlanRoot());
        childFragment.addPlanRoot(node);
        if (!childFragment.isPartitioned()) {
            return childFragment;
        }

        // Remember original offset and limit.
        boolean hasLimit = node.hasLimit();
        long limit = node.getLimit();
        long offset = node.getOffset();

        // Create a new fragment for a sort-merging exchange.
        PlanFragment mergeFragment = createParentFragment(childFragment, DataPartition.UNPARTITIONED);
        ExchangeNode exchNode = (ExchangeNode) mergeFragment.getPlanRoot();

        // Set limit, offset and merge parameters in the exchange node.
        exchNode.unsetLimit();
        if (hasLimit) {
            exchNode.setLimit(limit);
        }
        exchNode.setMergeInfo(node.getSortInfo(), offset);

        // Child nodes should not process the offset. If there is a limit,
        // the child nodes need only return (offset + limit) rows.
        SortNode childSortNode = (SortNode) childFragment.getPlanRoot();
        Preconditions.checkState(node == childSortNode);
        if (hasLimit) {
            childSortNode.unsetLimit();
            childSortNode.setLimit(limit + offset);
        }
        childSortNode.setOffset(0);
        childSortNode.computeStats(ctx_.getRootAnalyzer());
        exchNode.computeStats(ctx_.getRootAnalyzer());

        return mergeFragment;
    }

    private PlanFragment createAssertFragment(PlanNode assertRowCountNode, PlanFragment inputFragment)
            throws UserException {
        Preconditions.checkState(assertRowCountNode instanceof AssertNumRowsNode);
        if (!inputFragment.isPartitioned()) {
            inputFragment.addPlanRoot(assertRowCountNode);
            return inputFragment;
        }

        // Create a new fragment for assert row count node
        PlanFragment mergeFragment = createParentFragment(inputFragment, DataPartition.UNPARTITIONED);
        ExchangeNode exchNode = (ExchangeNode) mergeFragment.getPlanRoot();
        mergeFragment.addPlanRoot(assertRowCountNode);

        // reset the stat of assert row count node
        exchNode.computeStats(ctx_.getRootAnalyzer());
        assertRowCountNode.computeStats(ctx_.getRootAnalyzer());

        return mergeFragment;
    }

}

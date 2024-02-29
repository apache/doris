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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DistributedPlanner.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPartitionType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * The distributed planner is responsible for creating an executable, distributed plan
 * from a single-node plan that can be sent to the backend.
 */
public class DistributedPlanner {
    private static final Logger LOG = LogManager.getLogger(DistributedPlanner.class);

    private final PlannerContext ctx;

    public DistributedPlanner(PlannerContext ctx) {
        this.ctx = ctx;
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
        Preconditions.checkState(!ctx.isSingleNodeExec());
        // AnalysisContext.AnalysisResult analysisResult = ctx_.getAnalysisResult();
        QueryStmt queryStmt = ctx.getQueryStmt();
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
        if (ctx.isInsert() && !singleNodePlan.hasLimit()) {
            Preconditions.checkState(!queryStmt.hasOffset());
            isPartitioned = true;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("create plan fragments");
        }
        createPlanFragments(singleNodePlan, isPartitioned, fragments);
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
            if (targetTable.isPartitionDistributed()) {
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
            if (targetTable.isPartitionDistributed()) {
                if (isRepart != null && isRepart) {
                    needRepartition = true;
                } else {
                    return inputFragment;
                }
            } else {
                return inputFragment;
            }
        }

        // Need a merge node to merge all partition of input fragment
        if (needMerge) {
            PlanFragment newInputFragment = createMergeFragment(inputFragment);
            fragments.add(newInputFragment);
            return newInputFragment;
        }

        // Following is repartition logic
        Preconditions.checkState(needRepartition);

        ExchangeNode exchNode = new ExchangeNode(ctx.getNextNodeId(), inputFragment.getPlanRoot(), false);
        exchNode.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
        // exchNode.computeStats(analyzer);
        // exchNode.createDefaultSmap(analyzer);
        exchNode.init(ctx.getRootAnalyzer());
        DataPartition dataPartition = stmt.getDataPartition();
        PlanFragment fragment = new PlanFragment(ctx.getNextFragmentId(), exchNode, dataPartition);
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
            PlanNode root, boolean isPartitioned, ArrayList<PlanFragment> fragments) throws UserException {
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
                    createPlanFragments(child, childIsPartitioned, fragments));
        }

        PlanFragment result = null;
        if (root instanceof ScanNode) {
            result = createScanFragment(root);
            fragments.add(result);
        } else if (root instanceof TableFunctionNode) {
            result = createTableFunctionFragment(root, childFragments.get(0));
        } else if (root instanceof HashJoinNode) {
            Preconditions.checkState(childFragments.size() == 2);
            result = createHashJoinFragment((HashJoinNode) root,
                    childFragments.get(1), childFragments.get(0), fragments);
        } else if (root instanceof NestedLoopJoinNode) {
            result = createNestedLoopJoinFragment((NestedLoopJoinNode) root, childFragments.get(1),
                    childFragments.get(0));
        } else if (root instanceof SelectNode) {
            result = createSelectNodeFragment((SelectNode) root, childFragments);
        } else if (root instanceof SetOperationNode) {
            result = createSetOperationNodeFragment((SetOperationNode) root, childFragments, fragments);
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
            result = new PlanFragment(ctx.getNextFragmentId(), root, DataPartition.UNPARTITIONED);
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
        if ((!isPartitioned && result.isPartitioned() && result.getPlanRoot().getNumInstances() > 1)
                || (!(root instanceof SortNode) && root.hasOffset())) {
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
        Preconditions.checkState(inputFragment.isPartitioned() || inputFragment.getPlanRoot().hasOffset());

        // exchange node clones the behavior of its input, aside from the conjuncts
        ExchangeNode mergePlan =
                new ExchangeNode(ctx.getNextNodeId(), inputFragment.getPlanRoot(), false);
        PlanNode inputRoot = inputFragment.getPlanRoot();
        if (inputRoot.hasOffset()) {
            long limit = inputRoot.getOffset() + inputRoot.getLimit();
            inputRoot.unsetLimit();
            inputRoot.setLimit(limit);
        }
        mergePlan.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
        mergePlan.init(ctx.getRootAnalyzer());
        Preconditions.checkState(mergePlan.hasValidStats());
        PlanFragment fragment = new PlanFragment(ctx.getNextFragmentId(), mergePlan, DataPartition.UNPARTITIONED);
        inputFragment.setDestination(mergePlan);
        return fragment;
    }

    /**
     * Create new randomly-partitioned fragment containing a single scan node.
     * TODO: take bucketing into account to produce a naturally hash-partitioned
     * fragment
     * TODO: hbase scans are range-partitioned on the row key
     */
    private PlanFragment createScanFragment(PlanNode node) throws UserException {
        if (node instanceof MysqlScanNode) {
            return new PlanFragment(ctx.getNextFragmentId(), node, DataPartition.UNPARTITIONED);
        } else if (node instanceof OlapScanNode) {
            // olap scan node
            OlapScanNode olapScanNode = (OlapScanNode) node;
            return new PlanFragment(ctx.getNextFragmentId(), node,
                    olapScanNode.constructInputPartitionByDistributionInfo(), DataPartition.RANDOM);
        } else {
            // other scan nodes are random partitioned: es, broker
            return new PlanFragment(ctx.getNextFragmentId(), node, DataPartition.RANDOM);
        }
    }

    private PlanFragment createTableFunctionFragment(PlanNode node, PlanFragment childFragment) {
        Preconditions.checkState(node instanceof TableFunctionNode);
        node.setChild(0, childFragment.getPlanRoot());
        node.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        childFragment.addPlanRoot(node);
        return childFragment;
    }

    /**
     * There are 4 kinds of distributed hash join methods in Doris:
     * Colocate, Bucket Shuffle, Broadcast, Shuffle
     * The priority between these four distributed execution methods is following:
     * Colocate > Bucket Shuffle > Broadcast > Shuffle
     * This function is mainly used to choose the most suitable distributed method for the 'node',
     * and transform it into PlanFragment.
     */
    private PlanFragment createHashJoinFragment(
            HashJoinNode node, PlanFragment rightChildFragment,
            PlanFragment leftChildFragment, ArrayList<PlanFragment> fragments)
            throws UserException {
        List<String> reason = Lists.newArrayList();
        if (canColocateJoin(node, leftChildFragment, rightChildFragment, reason)) {
            node.setColocate(true, "");
            node.setChild(0, leftChildFragment.getPlanRoot());
            node.setChild(1, rightChildFragment.getPlanRoot());
            leftChildFragment.setPlanRoot(node);
            fragments.remove(rightChildFragment);
            leftChildFragment.setHasColocatePlanNode(true);
            return leftChildFragment;
        } else {
            node.setColocate(false, reason.get(0));
        }

        // bucket shuffle join is better than broadcast and shuffle join
        // it can reduce the network cost of join, so doris chose it first
        List<Expr> rhsPartitionExprs = Lists.newArrayList();
        if (canBucketShuffleJoin(node, leftChildFragment, rhsPartitionExprs)) {
            node.setDistributionMode(HashJoinNode.DistributionMode.BUCKET_SHUFFLE);
            DataPartition rhsJoinPartition =
                    new DataPartition(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, rhsPartitionExprs);
            ExchangeNode rhsExchange =
                    new ExchangeNode(ctx.getNextNodeId(), rightChildFragment.getPlanRoot(), false);
            rhsExchange.setNumInstances(rightChildFragment.getPlanRoot().getNumInstances());
            rhsExchange.init(ctx.getRootAnalyzer());

            node.setChild(0, leftChildFragment.getPlanRoot());
            node.setChild(1, rhsExchange);
            leftChildFragment.setPlanRoot(node);

            rightChildFragment.setDestination(rhsExchange);
            rightChildFragment.setOutputPartition(rhsJoinPartition);

            return leftChildFragment;
        }

        JoinCostEvaluation joinCostEvaluation = new JoinCostEvaluation(node, rightChildFragment, leftChildFragment);
        boolean doBroadcast;
        // we do a broadcast join if
        // - we're explicitly told to do so
        // - or if it's cheaper and we weren't explicitly told to do a partitioned join
        // - and we're not doing a full or right outer join (those require the left-hand
        //   side to be partitioned for correctness)
        // - and the expected size of the hash tbl doesn't exceed autoBroadcastThreshold
        // we set partition join as default when broadcast join cost equals partition join cost

        if (node.getJoinOp() == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
            doBroadcast = true;
        } else if (node.getJoinOp() != JoinOperator.RIGHT_OUTER_JOIN
                && node.getJoinOp() != JoinOperator.FULL_OUTER_JOIN) {
            if (node.getInnerRef().isBroadcastJoin()) {
                // respect user join hint
                doBroadcast = true;
            } else if (!node.getInnerRef().isPartitionJoin() && joinCostEvaluation.isBroadcastCostSmaller()
                    && joinCostEvaluation.constructHashTableSpace()
                    <= ctx.getRootAnalyzer().getAutoBroadcastJoinThreshold()) {
                doBroadcast = true;
            } else {
                doBroadcast = false;
            }
        } else {
            doBroadcast = false;
        }
        if (doBroadcast) {
            node.setDistributionMode(HashJoinNode.DistributionMode.BROADCAST);
            // Doesn't create a new fragment, but modifies leftChildFragment to execute
            // the join; the build input is provided by an ExchangeNode, which is the
            // destination of the rightChildFragment's output
            node.setChild(0, leftChildFragment.getPlanRoot());
            connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
            leftChildFragment.setPlanRoot(node);
            ((ExchangeNode) node.getChild(1)).setRightChildOfBroadcastHashJoin(true);
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
                    new ExchangeNode(ctx.getNextNodeId(), leftChildFragment.getPlanRoot(), false);
            lhsExchange.setNumInstances(leftChildFragment.getPlanRoot().getNumInstances());
            lhsExchange.init(ctx.getRootAnalyzer());

            DataPartition rhsJoinPartition =
                    new DataPartition(TPartitionType.HASH_PARTITIONED, rhsJoinExprs);
            ExchangeNode rhsExchange =
                    new ExchangeNode(ctx.getNextNodeId(), rightChildFragment.getPlanRoot(), false);
            rhsExchange.setNumInstances(rightChildFragment.getPlanRoot().getNumInstances());
            rhsExchange.init(ctx.getRootAnalyzer());

            node.setChild(0, lhsExchange);
            node.setChild(1, rhsExchange);
            PlanFragment joinFragment = new PlanFragment(ctx.getNextFragmentId(), node, lhsJoinPartition);
            // connect the child fragments
            leftChildFragment.setDestination(lhsExchange);
            leftChildFragment.setOutputPartition(lhsJoinPartition);
            rightChildFragment.setDestination(rhsExchange);
            rightChildFragment.setOutputPartition(rhsJoinPartition);

            return joinFragment;
        }
    }

    /**
     * Colocate Join can be performed when the following 4 conditions are met at the same time.
     * 1. Join operator is not NULL_AWARE_LEFT_ANTI_JOIN
     * 2. Session variables disable_colocate_plan = false
     * 3. There is no join hints in HashJoinNode
     * 4. There are no exchange node between source scan node and HashJoinNode.
     * 5. The scan nodes which are related by EqConjuncts in HashJoinNode are colocate and group can be matched.
     */
    private boolean canColocateJoin(HashJoinNode node, PlanFragment leftChildFragment, PlanFragment rightChildFragment,
                                    List<String> cannotReason) {
        // Condition1
        if (node.getJoinOp() == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
            cannotReason.add(DistributedPlanColocateRule.NULL_AWARE_LEFT_ANTI_JOIN_MUST_BROADCAST);
            return false;
        }

        // Condition2
        if (ConnectContext.get().getSessionVariable().isDisableColocatePlan()) {
            cannotReason.add(DistributedPlanColocateRule.SESSION_DISABLED);
            return false;
        }

        // Condition3: If user have a join hint to use proper way of join, can not be colocate join
        if (node.getInnerRef().hasJoinHints()) {
            cannotReason.add(DistributedPlanColocateRule.HAS_JOIN_HINT);
            return false;
        }

        // Condition4:
        // If there is an exchange node between the HashJoinNode and their real associated ScanNode,
        //   it means that the data has been rehashed.
        // The rehashed data can no longer be guaranteed to correspond to the left and right buckets,
        //   and naturally cannot be colocate
        Map<Pair<OlapScanNode, OlapScanNode>, List<BinaryPredicate>> scanNodeWithJoinConjuncts = Maps.newHashMap();
        for (BinaryPredicate eqJoinPredicate : node.getEqJoinConjuncts()) {
            OlapScanNode leftScanNode = genSrcScanNode(eqJoinPredicate.getChild(0), leftChildFragment, cannotReason);
            if (leftScanNode == null) {
                return false;
            }
            OlapScanNode rightScanNode = genSrcScanNode(eqJoinPredicate.getChild(1), rightChildFragment, cannotReason);
            if (rightScanNode == null) {
                return false;
            }
            Pair<OlapScanNode, OlapScanNode> eqPair = Pair.of(leftScanNode, rightScanNode);
            List<BinaryPredicate> predicateList = scanNodeWithJoinConjuncts.get(eqPair);
            if (predicateList == null) {
                predicateList = Lists.newArrayList();
                scanNodeWithJoinConjuncts.put(eqPair, predicateList);
            }
            predicateList.add(eqJoinPredicate);
        }

        // Condition5
        return dataDistributionMatchEqPredicate(scanNodeWithJoinConjuncts, cannotReason);
    }

    private OlapScanNode genSrcScanNode(Expr expr, PlanFragment planFragment, List<String> cannotReason) {
        SlotRef slotRef = expr.getSrcSlotRef();
        if (slotRef == null) {
            cannotReason.add(DistributedPlanColocateRule.TRANSFORMED_SRC_COLUMN);
            return null;
        }
        ScanNode scanNode = planFragment.getPlanRoot()
                .getScanNodeInOneFragmentBySlotRef(slotRef);
        if (scanNode == null) {
            cannotReason.add(DistributedPlanColocateRule.REDISTRIBUTED_SRC_DATA);
            return null;
        }
        if (scanNode instanceof OlapScanNode) {
            return (OlapScanNode) scanNode;
        } else {
            cannotReason.add(DistributedPlanColocateRule.SUPPORT_ONLY_OLAP_TABLE);
            return null;
        }
    }

    private boolean dataDistributionMatchEqPredicate(Map<Pair<OlapScanNode, OlapScanNode>,
            List<BinaryPredicate>> scanNodeWithJoinConjuncts, List<String> cannotReason) {
        // If left table and right table is same table and they select same single partition or no partition
        // they are naturally colocate relationship no need to check colocate group
        for (Map.Entry<Pair<OlapScanNode, OlapScanNode>, List<BinaryPredicate>> entry
                : scanNodeWithJoinConjuncts.entrySet()) {
            OlapScanNode leftScanNode = entry.getKey().first;
            OlapScanNode rightScanNode = entry.getKey().second;
            List<BinaryPredicate> eqPredicates = entry.getValue();
            if (!dataDistributionMatchEqPredicate(eqPredicates, leftScanNode, rightScanNode, cannotReason)) {
                return false;
            }
        }
        return true;
    }


    //the table must be colocate
    //the colocate group must be stable
    //the eqJoinConjuncts must contain the distributionColumns
    private boolean dataDistributionMatchEqPredicate(List<BinaryPredicate> eqJoinPredicates, OlapScanNode leftRoot,
                                                     OlapScanNode rightRoot, List<String> cannotReason) {
        OlapTable leftTable = leftRoot.getOlapTable();
        OlapTable rightTable = rightRoot.getOlapTable();

        // if left table and right table is same table and they select same single partition or no partition
        // they are naturally colocate relationship no need to check colocate group
        Collection<Long> leftPartitions = leftRoot.getSelectedPartitionIds();
        Collection<Long> rightPartitions = rightRoot.getSelectedPartitionIds();

        // For UT or no partition is selected, getSelectedIndexId() == -1, see selectMaterializedView()
        boolean hitSameIndex = (leftTable.getId() == rightTable.getId())
                && (leftRoot.getSelectedIndexId() != -1 && rightRoot.getSelectedIndexId() != -1)
                && (leftRoot.getSelectedIndexId() == rightRoot.getSelectedIndexId());

        boolean noNeedCheckColocateGroup = hitSameIndex && (leftPartitions.equals(rightPartitions))
                && (leftPartitions.size() <= 1);

        if (!noNeedCheckColocateGroup) {
            ColocateTableIndex colocateIndex = Env.getCurrentColocateIndex();

            //1 the table must be colocate
            if (!colocateIndex.isSameGroup(leftTable.getId(), rightTable.getId())) {
                cannotReason.add(DistributedPlanColocateRule.TABLE_NOT_IN_THE_SAME_GROUP);
                return false;
            }

            //2 the colocate group must be stable
            GroupId groupId = colocateIndex.getGroup(leftTable.getId());
            if (colocateIndex.isGroupUnstable(groupId)) {
                cannotReason.add(DistributedPlanColocateRule.COLOCATE_GROUP_IS_NOT_STABLE);
                return false;
            }
        }

        DistributionInfo leftDistribution = leftTable.getDefaultDistributionInfo();
        DistributionInfo rightDistribution = rightTable.getDefaultDistributionInfo();

        if (leftDistribution instanceof HashDistributionInfo && rightDistribution instanceof HashDistributionInfo) {
            List<Column> leftDistributeColumns = ((HashDistributionInfo) leftDistribution).getDistributionColumns();
            List<Column> rightDistributeColumns = ((HashDistributionInfo) rightDistribution).getDistributionColumns();

            List<Column> leftJoinColumns = new ArrayList<>();
            List<Column> rightJoinColumns = new ArrayList<>();
            for (BinaryPredicate eqJoinPredicate : eqJoinPredicates) {
                SlotRef lhsSlotRef = eqJoinPredicate.getChild(0).getSrcSlotRef();
                SlotRef rhsSlotRef = eqJoinPredicate.getChild(1).getSrcSlotRef();
                Preconditions.checkState(lhsSlotRef != null);
                Preconditions.checkState(rhsSlotRef != null);

                Column leftColumn = lhsSlotRef.getDesc().getColumn();
                Column rightColumn = rhsSlotRef.getDesc().getColumn();
                int leftColumnIndex = leftDistributeColumns.indexOf(leftColumn);
                int rightColumnIndex = rightDistributeColumns.indexOf(rightColumn);

                // eqjoinConjuncts column should have the same order like colocate distribute column
                if (leftColumnIndex == rightColumnIndex && leftColumnIndex != -1) {
                    leftJoinColumns.add(leftColumn);
                    rightJoinColumns.add(rightColumn);
                }
            }

            //3 the join columns should contain all distribute columns to enable colocate join
            if (leftJoinColumns.containsAll(leftDistributeColumns)
                    && rightJoinColumns.containsAll(rightDistributeColumns)) {
                return true;
            }
        }

        cannotReason.add(DistributedPlanColocateRule.INCONSISTENT_DISTRIBUTION_OF_TABLE_AND_QUERY);
        return false;
    }

    private boolean canBucketShuffleJoin(HashJoinNode node, PlanFragment leftChildFragment,
                                         List<Expr> rhsHashExprs) {
        if (node.getJoinOp() == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
            return false;
        }

        if (!ConnectContext.get().getSessionVariable().isEnableBucketShuffleJoin()) {
            return false;
        }
        // If user have a join hint to use proper way of join, can not be bucket shuffle join
        if (node.getInnerRef().hasJoinHints()) {
            return false;
        }

        PlanNode leftRoot = leftChildFragment.getPlanRoot();
        // 1.leftRoot be OlapScanNode
        if (leftRoot instanceof OlapScanNode) {
            return canBucketShuffleJoin(node, leftRoot, rhsHashExprs);
        }

        // 2.leftRoot be hashjoin node
        if (leftRoot instanceof HashJoinNode) {
            while (leftRoot instanceof HashJoinNode) {
                leftRoot = leftRoot.getChild(0);
            }
            if (leftRoot instanceof OlapScanNode) {
                return canBucketShuffleJoin(node, leftRoot, rhsHashExprs);
            }
        }

        return false;
    }

    //the join expr must contian left table distribute column
    private boolean canBucketShuffleJoin(HashJoinNode node, PlanNode leftRoot,
                                         List<Expr> rhsJoinExprs) {
        OlapScanNode leftScanNode = ((OlapScanNode) leftRoot);
        OlapTable leftTable = leftScanNode.getOlapTable();

        //1 the left table has more than one partition or left table is not a stable colocate table
        if (leftScanNode.getSelectedPartitionIds().size() != 1) {
            ColocateTableIndex colocateIndex = Env.getCurrentColocateIndex();
            if (!leftTable.isColocateTable()
                    || colocateIndex.isGroupUnstable(colocateIndex.getGroup(leftTable.getId()))) {
                return false;
            }
        }

        DistributionInfo leftDistribution = leftScanNode.getOlapTable().getDefaultDistributionInfo();

        if (leftDistribution instanceof HashDistributionInfo) {
            // use the table_name + '-' + column_name.toLowerCase() as check condition,
            // as column name in doris is case insensitive and table name is case sensitive
            List<Column> leftDistributeColumns = ((HashDistributionInfo) leftDistribution).getDistributionColumns();
            List<String> leftDistributeColumnNames = leftDistributeColumns.stream()
                    .map(col -> leftTable.getName() + "." + col.getName().toLowerCase()).collect(Collectors.toList());

            List<String> leftJoinColumnNames = new ArrayList<>();
            List<Expr> rightExprs = new ArrayList<>();
            List<BinaryPredicate> eqJoinConjuncts = node.getEqJoinConjuncts();

            for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
                Expr lhsJoinExpr = eqJoinPredicate.getChild(0);
                Expr rhsJoinExpr = eqJoinPredicate.getChild(1);
                if (lhsJoinExpr.unwrapSlotRef() == null || rhsJoinExpr.unwrapSlotRef() == null) {
                    continue;
                }

                SlotRef leftSlot = node.getChild(0).findSrcSlotRef(lhsJoinExpr.unwrapSlotRef());
                if (leftSlot.getTable() instanceof OlapTable
                        && leftScanNode.desc.getSlots().contains(leftSlot.getDesc())) {
                    // table name in SlotRef is not the really name. `select * from test as t`
                    // table name in SlotRef is `t`, but here we need is `test`.
                    leftJoinColumnNames.add(leftSlot.getTable().getName() + "."
                            + leftSlot.getColumnName().toLowerCase());
                    rightExprs.add(rhsJoinExpr);
                }
            }

            //2 the join columns should contains all left table distribute columns to enable bucket shuffle join
            for (int i = 0; i < leftDistributeColumnNames.size(); i++) {
                String distributeColumnName = leftDistributeColumnNames.get(i);
                boolean findRhsExprs = false;
                // check the join column name is same as distribute column name and
                // check the rhs join expr type is same as distribute column
                for (int j = 0; j < leftJoinColumnNames.size(); j++) {
                    if (leftJoinColumnNames.get(j).equals(distributeColumnName)) {
                        // varchar and string type don't need to check the length property
                        if ((rightExprs.get(j).getType().isVarcharOrStringType()
                                && leftDistributeColumns.get(i).getType().isVarcharOrStringType())
                                || (rightExprs.get(j).getType()
                                        .equals(leftDistributeColumns.get(i).getType()))) {
                            rhsJoinExprs.add(rightExprs.get(j));
                            findRhsExprs = true;
                            break;
                        }
                    }
                }

                if (!findRhsExprs) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }

    /**
     * Modifies the leftChildFragment to execute a cross join. The right child input is provided by an ExchangeNode,
     * which is the destination of the rightChildFragment's output.
     */
    private PlanFragment createNestedLoopJoinFragment(
            NestedLoopJoinNode node, PlanFragment rightChildFragment, PlanFragment leftChildFragment)
            throws UserException {
        if (node.canParallelize()) {
            // The rhs tree is going to send data through an exchange node which effectively
            // compacts the data. No reason to do it again at the rhs root node.
            rightChildFragment.getPlanRoot().setCompactData(false);
            node.setChild(0, leftChildFragment.getPlanRoot());
            connectChildFragment(node, 1, leftChildFragment, rightChildFragment);
            leftChildFragment.setPlanRoot(node);
            return leftChildFragment;
        } else {
            // For non-equal nljoin, we should make sure using only one instance to do processing.
            DataPartition lhsJoinPartition = new DataPartition(TPartitionType.UNPARTITIONED);
            ExchangeNode lhsExchange =
                    new ExchangeNode(ctx.getNextNodeId(), leftChildFragment.getPlanRoot(), false);
            lhsExchange.setNumInstances(1);
            lhsExchange.init(ctx.getRootAnalyzer());

            DataPartition rhsJoinPartition =
                    new DataPartition(TPartitionType.UNPARTITIONED);
            ExchangeNode rhsExchange =
                    new ExchangeNode(ctx.getNextNodeId(), rightChildFragment.getPlanRoot(), false);
            rhsExchange.setNumInstances(1);
            rhsExchange.init(ctx.getRootAnalyzer());

            node.setChild(0, lhsExchange);
            node.setChild(1, rhsExchange);
            PlanFragment joinFragment = new PlanFragment(ctx.getNextFragmentId(), node, lhsJoinPartition);
            // connect the child fragments
            leftChildFragment.setDestination(lhsExchange);
            leftChildFragment.setOutputPartition(lhsJoinPartition);
            rightChildFragment.setDestination(rhsExchange);
            rightChildFragment.setOutputPartition(rhsJoinPartition);
            return joinFragment;
        }
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
                    ctx.getNextFragmentId(), setOperationNode, DataPartition.UNPARTITIONED);
        }

        Preconditions.checkState(!childFragments.isEmpty());
        int numUnpartitionedChildFragments = 0;
        for (int i = 0; i < childFragments.size(); ++i) {
            if (!childFragments.get(i).isPartitioned()) {
                ++numUnpartitionedChildFragments;
            }
        }

        // remove all children to avoid them being tagged with the wrong
        // fragment (in the PlanFragment c'tor; we haven't created ExchangeNodes yet)
        setOperationNode.clearChildren();

        // If all child fragments are unpartitioned, return a single unpartitioned fragment
        // with a UnionNode that merges all child fragments.
        if (numUnpartitionedChildFragments == childFragments.size()) {
            PlanFragment setOperationFragment = new PlanFragment(
                    ctx.getNextFragmentId(), setOperationNode, DataPartition.UNPARTITIONED);
            // Absorb the plan trees of all childFragments into unionNode
            // and fix up the fragment tree in the process.
            for (int i = 0; i < childFragments.size(); ++i) {
                setOperationNode.addChild(childFragments.get(i).getPlanRoot());
                setOperationFragment.setFragmentInPlanTree(setOperationNode.getChild(i));
                setOperationFragment.addChildren(childFragments.get(i).getChildren());
            }
            setOperationNode.init(ctx.getRootAnalyzer());
            // All child fragments have been absorbed into unionFragment.
            fragments.removeAll(childFragments);
            return setOperationFragment;
        }

        // There is at least one partitioned child fragment.
        // TODO(ML): here
        PlanFragment setOperationFragment = new PlanFragment(ctx.getNextFragmentId(), setOperationNode,
                new DataPartition(TPartitionType.HASH_PARTITIONED,
                        setOperationNode.getMaterializedResultExprLists().get(0)));
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

            // UnionNode shouldn't be absorbed by childFragment, because it reduce
            // the degree of concurrency.
            // chenhao16 add
            // dummy entry for subsequent addition of the ExchangeNode
            setOperationNode.addChild(null);
            // Connect the unpartitioned child fragments to SetOperationNode via a random exchange.
            connectChildFragment(setOperationNode, i, setOperationFragment, childFragment);
            childFragment.setOutputPartition(
                    DataPartition.hashPartitioned(setOperationNode.getMaterializedResultExprLists().get(i)));
        }
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

    /**
     * Replace node's child at index childIdx with an ExchangeNode that receives its input from childFragment.
     */
    private void connectChildFragment(
            PlanNode node, int childIdx,
            PlanFragment parentFragment, PlanFragment childFragment)
            throws UserException {
        ExchangeNode exchangeNode = new ExchangeNode(ctx.getNextNodeId(), childFragment.getPlanRoot(), false);
        exchangeNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        exchangeNode.init(ctx.getRootAnalyzer());
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
        ExchangeNode exchangeNode = new ExchangeNode(ctx.getNextNodeId(), childFragment.getPlanRoot(), false);
        exchangeNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        exchangeNode.init(ctx.getRootAnalyzer());
        PlanFragment parentFragment = new PlanFragment(ctx.getNextFragmentId(), exchangeNode, parentPartition);
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
            if (canColocateAgg(node.getAggInfo(), childFragment.getDataPartition())) {
                childFragment.addPlanRoot(node);
                childFragment.setHasColocatePlanNode(true);
                return childFragment;
            } else {
                return createMergeAggregationFragment(node, childFragment);
            }
        }
    }

    /**
     * Colocate Agg can be performed when the following 2 conditions are met at the same time.
     * 1. Session variables disable_colocate_plan = false
     * 2. The input data partition of child fragment < agg node partition exprs
     */
    private boolean canColocateAgg(AggregateInfo aggregateInfo, DataPartition childFragmentDataPartition) {
        // Condition1
        if (ConnectContext.get().getSessionVariable().isDisableColocatePlan()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Agg node is not colocate in:" + ConnectContext.get().queryId()
                        + ", reason:" + DistributedPlanColocateRule.SESSION_DISABLED);
            }
            return false;
        }

        // Condition2
        List<Expr> aggPartitionExprs = aggregateInfo.getInputPartitionExprs();
        if (dataPartitionMatchAggInfo(childFragmentDataPartition, aggPartitionExprs)) {
            return true;
        }
        return false;
    }

    /**
     * The aggPartitionExprs should contains all of data partition columns.
     * Since aggPartitionExprs may be derived from the transformation of the lower tuple,
     *   it is necessary to find the source expr of itself firstly.
     * <p>
     * For example:
     * Data Partition: t1.k1, t1.k2
     * Agg Partition Exprs: t1.k1, t1.k2, t1.k3
     * Return: true
     * <p>
     * Data Partition: t1.k1, t1.k2
     * Agg Partition Exprs: t1.k1, t2.k2
     * Return: false
     */
    private boolean dataPartitionMatchAggInfo(DataPartition dataPartition, List<Expr> aggPartitionExprs) {
        TPartitionType partitionType = dataPartition.getType();
        if (partitionType != TPartitionType.HASH_PARTITIONED) {
            return false;
        }
        List<Expr> dataPartitionExprs = dataPartition.getPartitionExprs();
        for (Expr dataPartitionExpr : dataPartitionExprs) {
            boolean match = false;
            for (Expr aggPartitionExpr : aggPartitionExprs) {
                if (aggPartitionExpr.comeFrom(dataPartitionExpr)) {
                    match = true;
                    break;
                }
            }
            if (!match) {
                return false;
            }
        }
        return true;
    }

    private PlanFragment createRepeatNodeFragment(
            RepeatNode repeatNode, PlanFragment childFragment, ArrayList<PlanFragment> fragments)
            throws UserException {
        repeatNode.setNumInstances(childFragment.getPlanRoot().getNumInstances());
        childFragment.addPlanRoot(repeatNode);
        /*
        The Repeat Node will change the data partition of fragment
          when the origin data partition of fragment is HashPartition.
        For example,
        Query: SELECT k1, k2, sum(v1)
               FROM table
               GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ( ))
        Table schema: table distributed by k1
        The Child Fragment:
               Fragment 0
                   Data partition: k1
                   Repeat Node: repeat 3 lines [[0, 1], [0], [1], []]
                   OlapScanNode: table
        Data before Repeat Node is partitioned by k1 such as:
          | Node 1 |  | Node 2 |
          | 1, 1   |  | 2, 1   |
          | 1, 2   |  | 2, 2   |
        Data after Repeat Node is partitioned by RANDOM such as:
          | Node 1 |  | Node 2 |
          | 1, 1   |  | 2, 1   |
          | 1, 2   |  | 2, 2   |
          | null,1 |  | null,1 |
          | null,2 |  | null,2 |
          ...
        The Repeat Node will generate some new rows.
        The distribution of these new rows is completely inconsistent with the original data distribution,
          their distribution is RANDOM.
        Therefore, the data distribution method of the fragment needs to be modified here.
        Only the correct data distribution can make the correct result when judging **colocate**.
         */
        childFragment.updateDataPartition(DataPartition.RANDOM);
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
            if (partitionExprs == null) {
                partitionExprs = groupingExprs;
            }
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
                    node.getAggInfo().getIntermediateSmap(), ctx.getRootAnalyzer(), false);
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

        node.setIsPreagg(ctx);

        // if there is a limit, we need to transfer it from the pre-aggregation
        // node in the child fragment to the merge aggregation node in the parent
        long limit = node.getLimit();
        node.unsetLimit();
        node.unsetNeedsFinalize();

        // place a merge aggregation step in a new fragment
        PlanFragment mergeFragment = createParentFragment(childFragment, parentPartition);
        AggregationNode mergeAggNode = new AggregationNode(ctx.getNextNodeId(),
                mergeFragment.getPlanRoot(), node.getAggInfo().getMergeAggInfo());
        mergeAggNode.init(ctx.getRootAnalyzer());
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
        node.computeStats(ctx.getRootAnalyzer());
        mergeFragment.getPlanRoot().computeStats(ctx.getRootAnalyzer());
        mergeAggNode.computeStats(ctx.getRootAnalyzer());
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
        boolean isUsingSetForDistinct = node.getAggInfo().isUsingSetForDistinct();
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
                    ctx.getRootAnalyzer(), false);
        } else {
            // We need to do
            // - child fragment:
            //   * phase-1 aggregation
            // - merge fragment 1, hash-partitioned on distinct exprs:
            //   * merge agg of phase 1
            //   * phase 2 agg
            // - merge fragment 2, unpartitioned:
            //   * merge agg of phase 2
            if (!isUsingSetForDistinct) {
                partitionExprs = Expr.substituteList(firstPhaseAggInfo.getGroupingExprs(),
                        firstPhaseAggInfo.getIntermediateSmap(), ctx.getRootAnalyzer(), false);
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
            DataPartition mergePartition = partitionExprs == null
                    ? DataPartition.UNPARTITIONED : DataPartition.hashPartitioned(partitionExprs);
            // Convert the existing node to a preaggregation.
            AggregationNode preaggNode = (AggregationNode) node.getChild(0);

            preaggNode.setIsPreagg(ctx);

            // place a merge aggregation step for the 1st phase in a new fragment
            mergeFragment = createParentFragment(childFragment, mergePartition);
            AggregateInfo phase1MergeAggInfo = firstPhaseAggInfo.getMergeAggInfo();
            AggregationNode phase1MergeAggNode =
                    new AggregationNode(ctx.getNextNodeId(), preaggNode, phase1MergeAggInfo);
            phase1MergeAggNode.init(ctx.getRootAnalyzer());
            phase1MergeAggNode.unsetNeedsFinalize();
            phase1MergeAggNode.setIntermediateTuple();
            mergeFragment.addPlanRoot(phase1MergeAggNode);

            // the 2nd-phase aggregation consumes the output of the merge agg;
            // if there is a limit, it had already been placed with the 2nd aggregation
            // step (which is where it should be)
            mergeFragment.addPlanRoot(node);
        }

        if (!hasGrouping && !isUsingSetForDistinct) {
            // place the merge aggregation of the 2nd phase in an unpartitioned fragment;
            // add preceding merge fragment at end
            if (mergeFragment != childFragment) {
                fragments.add(mergeFragment);
            }

            node.unsetNeedsFinalize();
            node.setIntermediateTuple();
            // Any limit should be placed in the final merge aggregation node
            long limit = node.getLimit();
            node.unsetLimit();
            mergeFragment = createParentFragment(mergeFragment, DataPartition.UNPARTITIONED);
            AggregateInfo phase2MergeAggInfo = node.getAggInfo().getMergeAggInfo();
            AggregationNode phase2MergeAggNode = new AggregationNode(ctx.getNextNodeId(), node,
                    phase2MergeAggInfo);
            phase2MergeAggNode.init(ctx.getRootAnalyzer());
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
                    childFragment.getPlanRoot().getOutputSmap(), ctx.getRootAnalyzer());

            // Make sure the childFragment's output is partitioned as required by the sortNode.
            // Even if the fragment and the sort partition exprs are equal, an exchange is
            // required if the sort partition exprs reference a tuple that is made nullable in
            // 'childFragment' to bring NULLs from outer-join non-matches together.
            DataPartition sortPartition = sortNode.getInputPartition();
            // TODO(ML): here
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
        exchNode.setMergeInfo(node.getSortInfo());
        node.setMergeByExchange();
        exchNode.setOffset(offset);

        // Child nodes should not process the offset. If there is a limit,
        // the child nodes need only return (offset + limit) rows.
        SortNode childSortNode = (SortNode) childFragment.getPlanRoot();
        Preconditions.checkState(node == childSortNode);
        if (hasLimit) {
            childSortNode.unsetLimit();
            childSortNode.setLimit(limit + offset);
        }
        childSortNode.setOffset(0);
        childSortNode.computeStats(ctx.getRootAnalyzer());
        exchNode.computeStats(ctx.getRootAnalyzer());

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
        exchNode.computeStats(ctx.getRootAnalyzer());
        assertRowCountNode.computeStats(ctx.getRootAnalyzer());

        return mergeFragment;
    }

}

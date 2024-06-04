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

import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Evaluate the cost of Broadcast and Shuffle Join to choose between the two
 *
 * broadcast: send the rightChildFragment's output to each node executing the leftChildFragment; the cost across
 * all nodes is proportional to the total amount of data sent.
 * shuffle: also called Partitioned Join. That is, small tables and large tables are hashed according to the Join key,
 * and then distributed Join is performed.
 *
 * NOTICE:
 * for now, only MysqlScanNode and OlapScanNode has Cardinality. OlapScanNode's cardinality is calculated by row num
 * and data size, and MysqlScanNode's cardinality is always 1. Other ScanNode's cardinality is -1.
 * So if there are other kind of scan node in join query, it won't be able to calculate the cost of join normally
 * and result in both "broadcastCost" and "partitionCost" be 0. And this will lead to a SHUFFLE join.
 */
public class JoinCostEvaluation {
    private static final Logger LOG = LogManager.getLogger(JoinCostEvaluation.class);

    private final long rhsTreeCardinality;
    private final float rhsTreeAvgRowSize;
    private final int rhsTreeTupleIdNum;
    private final long lhsTreeCardinality;
    private final float lhsTreeAvgRowSize;
    private final int lhsTreeNumNodes;
    private long broadcastCost = 0;
    private long partitionCost = 0;

    JoinCostEvaluation(PlanNode node, PlanFragment rightChildFragment, PlanFragment leftChildFragment) {
        PlanNode rhsTree = rightChildFragment.getPlanRoot();
        rhsTreeCardinality = rhsTree.getCardinality();
        rhsTreeAvgRowSize = rhsTree.getAvgRowSize();
        rhsTreeTupleIdNum = rhsTree.getTupleIds().size();
        PlanNode lhsTree = leftChildFragment.getPlanRoot();
        lhsTreeCardinality = lhsTree.getCardinality();
        lhsTreeAvgRowSize = lhsTree.getAvgRowSize();
        lhsTreeNumNodes = leftChildFragment.getNumNodes();

        String nodeOverview = setNodeOverview(node, rightChildFragment, leftChildFragment);
        broadcastCost(nodeOverview);
        shuffleCost(nodeOverview);
    }

    private String setNodeOverview(PlanNode node, PlanFragment rightChildFragment, PlanFragment leftChildFragment) {
        return "root node id=" + node.getId().toString() + ": " + node.planNodeName
                + " right fragment id=" + rightChildFragment.getFragmentId().toString()
                + " left fragment id=" + leftChildFragment.getFragmentId().toString();
    }

    private void broadcastCost(String nodeOverview) {
        if (rhsTreeCardinality != -1 && lhsTreeNumNodes != -1) {
            broadcastCost = Math.round((double) rhsTreeCardinality * rhsTreeAvgRowSize) * lhsTreeNumNodes;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(nodeOverview);
            LOG.debug("broadcast: cost=" + broadcastCost);
            LOG.debug("rhs card=" + rhsTreeCardinality
                    + " rhs row_size=" + rhsTreeAvgRowSize
                    + " lhs nodes=" + lhsTreeNumNodes);
        }
    }

    /**
     * repartition: both left- and rightChildFragment are partitioned on the join exprs
     * TODO: take existing partition of input fragments into account to avoid unnecessary repartitioning
     */
    private void shuffleCost(String nodeOverview) {
        if (lhsTreeCardinality != -1 && rhsTreeCardinality != -1) {
            partitionCost = Math.round(
                    (double) lhsTreeCardinality * lhsTreeAvgRowSize + (double) rhsTreeCardinality * rhsTreeAvgRowSize);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("nodeOverview: {}", nodeOverview);
            LOG.debug("partition: cost={} ", partitionCost);
            LOG.debug("lhs card={} row_size={}", lhsTreeCardinality, lhsTreeAvgRowSize);
            LOG.debug("rhs card={} row_size={}", rhsTreeCardinality, rhsTreeAvgRowSize);
        }
    }

    /**
     * When broadcastCost and partitionCost are equal, there is no uniform standard for which join implementation
     * is better. Some scenarios are suitable for broadcast join, and some scenarios are suitable for shuffle join.
     * Therefore, we add a SessionVariable to help users choose a better join implementation.
     */
    public boolean isBroadcastCostSmaller()  {
        String joinMethod = ConnectContext.get().getSessionVariable().getPreferJoinMethod();
        if (joinMethod.equalsIgnoreCase("broadcast")) {
            return broadcastCost <= partitionCost;
        } else {
            return broadcastCost < partitionCost;
        }
    }

    /**
     * Estimate the memory cost of constructing Hash Table in Broadcast Join.
     * The memory cost by the Hash Table = ((cardinality/0.75[1]) * 8[2])[3] + (cardinality * avgRowSize)[4]
     * + (nodeArrayLen[5] * 16[6])[7] + (nodeArrayLen * tupleNum[8] * 8[9])[10]. consists of four parts:
     * 1) All bucket pointers. 2) Length of the node array. 3) Overhead of all nodes. 4) Tuple pointers of all nodes.
     * - [1] Expansion factor of the number of HashTable buckets;
     * - [2] The pointer length of each bucket of HashTable;
     * - [3] bucketPointerSpace: The memory cost by all bucket pointers of HashTable;
     * - [4] rhsDataSize: The memory cost by all nodes of HashTable, equal to the amount of data that the right table
     *       participates in the construction of HashTable;
     * - [5] HashTable stores the length of the node array, which is larger than the actual cardinality. The initial
     *       value is 4096. When the storage is full, one-half of the current array length is added each time.
     *       The length of the array after each increment is actually a sequence of numbers:
     *          4096 = pow(3/2, 0) * 4096,
     *          6144 = pow(3/2, 1) * 4096,
     *          9216 = pow(3/2, 2) * 4096,
     *          13824 = pow(3/2, 3) * 4096,
     *       finally need to satisfy len(node array)> cardinality,
     *       so the number of increments = int((ln(cardinality/4096) / ln(3/2)) + 1),
     *       finally len(node array) = pow(3/2, int((ln(cardinality/4096) / ln(3/2)) + 1) * 4096
     * - [6] The overhead length of each node of HashTable, including the next node pointer, Hash value,
     *       and a bool type variable;
     * - [7] nodeOverheadSpace: The memory cost by the overhead of all nodes in the HashTable;
     * - [8] Number of Tuples participating in the build;
     * - [9] The length of each Tuple pointer;
     * - [10] nodeTuplePointerSpace: The memory cost by Tuple pointers of all nodes in HashTable;
     */
    public long constructHashTableSpace() {
        double bucketPointerSpace = ((double) rhsTreeCardinality / 0.75) * 8;
        double nodeArrayLen =
                Math.pow(1.5, (int) ((Math.log((double) rhsTreeCardinality / 4096) / Math.log(1.5)) + 1)) * 4096;
        double nodeOverheadSpace = nodeArrayLen * 16;
        double nodeTuplePointerSpace = nodeArrayLen * rhsTreeTupleIdNum * 8;
        return Math.round((bucketPointerSpace + (double) rhsTreeCardinality * rhsTreeAvgRowSize
                + nodeOverheadSpace + nodeTuplePointerSpace) * PlannerContext.HASH_TBL_SPACE_OVERHEAD);
    }
}

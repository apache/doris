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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

/**
 * Edge in HyperGraph
 */
public class Edge {
    final int index;
    final LogicalJoin join;
    final double selectivity;

    // The endpoints (hyperNodes) of this hyperEdge.
    // left and right may not overlap, and both must have at least one bit set.
    private long left = LongBitmap.newBitmap();
    private long right = LongBitmap.newBitmap();
    private long referenceNodes = LongBitmap.newBitmap();

    /**
     * Create simple edge.
     */
    public Edge(LogicalJoin join, int index) {
        this.index = index;
        this.join = join;
        this.selectivity = 1.0;
    }

    public LogicalJoin getJoin() {
        return join;
    }

    public boolean isSimple() {
        return LongBitmap.getCardinality(left) == 1 && LongBitmap.getCardinality(right) == 1;
    }

    public void addLeftNode(long left) {
        this.left = LongBitmap.or(this.left, left);
        referenceNodes = LongBitmap.or(referenceNodes, left);
    }

    public void addLeftNodes(long... bitmaps) {
        for (long bitmap : bitmaps) {
            this.left = LongBitmap.or(this.left, bitmap);
            referenceNodes = LongBitmap.or(referenceNodes, bitmap);
        }
    }

    public void addRightNode(long right) {
        this.right = LongBitmap.or(this.right, right);
        referenceNodes = LongBitmap.or(referenceNodes, right);
    }

    public void addRightNodes(long... bitmaps) {
        for (long bitmap : bitmaps) {
            LongBitmap.or(this.right, bitmap);
            LongBitmap.or(referenceNodes, bitmap);
        }
    }

    public long getLeft() {
        return left;
    }

    public void setLeft(long left) {
        referenceNodes = LongBitmap.clear(referenceNodes);
        this.left = left;
    }

    public long getRight() {
        return right;
    }

    public void setRight(long right) {
        referenceNodes = LongBitmap.clear(referenceNodes);
        this.right = right;
    }

    public boolean isSub(Edge edge) {
        // When this join reference nodes is a subset of other join, then this join must appear before that join
        long otherBitmap = edge.getReferenceNodes();
        return LongBitmap.isSubset(getReferenceNodes(), otherBitmap);
    }

    public long getReferenceNodes() {
        if (LongBitmap.getCardinality(referenceNodes) == 0) {
            referenceNodes = LongBitmap.newBitmapUnion(left, right);
        }
        return referenceNodes;
    }

    public int getIndex() {
        return index;
    }

    public double getSelectivity() {
        return selectivity;
    }

    private double getRowCount(Plan plan) {
        if (plan instanceof GroupPlan) {
            return ((GroupPlan) plan).getGroup().getStatistics().getRowCount();
        }
        return plan.getGroupExpression().get().getOwnerGroup().getStatistics().getRowCount();
    }

    @Override
    public String toString() {
        return String.format("<%s - %s>", LongBitmap.toString(left), LongBitmap.toString(right));
    }
}


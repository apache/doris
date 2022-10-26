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

package org.apache.doris.nereids.rules.exploration.join.hypergraph;

import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import java.util.BitSet;

class Edge {
    final int index;
    final LogicalJoin join;

    // The endpoints (hypernodes) of this hyperedge.
    // left and right may not overlap, and both must have at least one bit set.
    BitSet left = new BitSet(32);
    BitSet right = new BitSet(32);
    BitSet constraints = new BitSet(32);

    /**
     * Create simple edge.
     */
    public Edge(int index, LogicalJoin join) {
        this.index = index;
        this.join = join;
    }

    public boolean isSimple() {
        return left.cardinality() == 1 && right.cardinality() == 1;
    }

    public void addLeftNode(BitSet left) {
        this.left.or(left);
    }

    public void addRightNode(BitSet right) {
        this.right.or(right);
    }

    public void addConstraintNode(BitSet constraints) {
        this.constraints.or(constraints);
    }

    public BitSet getLeft() {
        return left;
    }

    public BitSet getRight() {
        return right;
    }

    public BitSet getReferenceNodes() {
        // TODO: do we need consider constraints
        BitSet bitSet = new BitSet();
        bitSet.or(left);
        bitSet.or(right);
        return bitSet;
    }

    public Edge reverse() {
        Edge newEdge = new Edge(index, join);
        newEdge.addLeftNode(right);
        newEdge.addRightNode(left);
        newEdge.addConstraintNode(constraints);
        return newEdge;
    }
}


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

package org.apache.doris.nereids.rules.join;

import java.util.BitSet;

/**
 * HyperGraph Edge.
 * Edge just contain one condition.
 * `A.id = B.id And A.name = B.name` contains two edges.
 */
class Edge {
    final int index;
    // The endpoints (hypernodes) of this hyperedge.
    // left and right may not overlap, and both must have at least one bit set.
    final BitSet left = new BitSet(32);
    final BitSet right = new BitSet(32);

    /**
     * Create simple edge.
     */
    public Edge(int index, int leftId, int rightId) {
        this.index = index;
        this.left.set(leftId);
        this.right.set(rightId);
    }
}

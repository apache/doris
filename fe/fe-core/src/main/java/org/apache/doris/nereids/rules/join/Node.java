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

import com.google.common.collect.Lists;

import java.util.BitSet;
import java.util.List;

/**
 * HyperGraph Node.
 */
class Node {
    final int index;
    // We split these into simple edges (only one node on each side) and complex edges (others)
    // because we can often quickly discard all simple edges by testing the set of interesting nodes
    // against the “simple_neighborhood” bitmap.
    List<Integer> complexEdges = Lists.newArrayList();
    List<Integer> simpleEdges = Lists.newArrayList();

    BitSet simpleNeighborhood = new BitSet();

    public Node(int index) {
        this.index = index;
    }

    public void addSimpleEdge(int rightIndex, int edgeIndex) {
        simpleNeighborhood.set(rightIndex);
        simpleEdges.add(edgeIndex);
    }
}

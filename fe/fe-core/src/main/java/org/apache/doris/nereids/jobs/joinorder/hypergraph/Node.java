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
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * HyperGraph Node.
 */
public class Node {
    private final int index;
    // Due to group in Node is base group, so mergeGroup() don't need to consider it.
    private final Group group;
    private final List<Edge> edges = new ArrayList<>();

    public Node(int index, Group group) {
        this.group = group;
        this.index = index;
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public int getIndex() {
        return index;
    }

    public long getNodeMap() {
        return LongBitmap.newBitmap(index);
    }

    public Plan getPlan() {
        return group.getLogicalExpression().getPlan();
    }

    /**
     * Attach all edge in this node if the edge references this node
     *
     * @param edge the edge that references this node
     */
    public void attachEdge(Edge edge) {
        edges.add(edge);
    }

    /**
     * Remove the edge when the edge is modified
     *
     * @param edge The edge should be removed
     */
    public void removeEdge(Edge edge) {
        edges.remove(edge);
    }

    public String getName() {
        return getPlan().getType().name() + index;
    }

    public double getRowCount() {
        return group.getStatistics().getRowCount();
    }

    public Group getGroup() {
        return group;
    }

    public Set<Slot> getOutput() {
        return group.getLogicalExpression().getPlan().getOutputSet();
    }
}

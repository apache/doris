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

import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.Bitmap;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import javax.annotation.Nullable;

/**
 * HyperGraph Node.
 */
public class Node {
    private final int index;
    private Group group;
    private List<Edge> edges = new ArrayList<>();
    // We split these into simple edges (only one node on each side) and complex edges (others)
    // because we can often quickly discard all simple edges by testing the set of interesting nodes
    // against the “simple_neighborhood” bitmap. These data will be calculated before enumerate.
    private List<Edge> complexEdges = new ArrayList<>();
    private BitSet simpleNeighborhood = new BitSet();
    private List<Edge> simpleEdges = new ArrayList<>();
    private BitSet complexNeighborhood = new BitSet();

    public Node(int index, Group group) {
        this.group = group;
        this.index = index;
    }

    /**
     * Try to find the edge between this node and nodes
     *
     * @param nodes the other side of the edge
     * @return The edge between this node and parameters
     */
    @Nullable
    public Edge tryGetEdgeWith(BitSet nodes) {
        if (Bitmap.isOverlap(simpleNeighborhood, nodes)) {
            for (Edge edge : simpleEdges) {
                if (Bitmap.isSubset(edge.getLeft(), nodes) || Bitmap.isSubset(edge.getRight(), nodes)) {
                    return edge;
                }
            }
            throw new RuntimeException(String.format("There is no simple Edge <%d - %s>", index, nodes));
        } else if (Bitmap.isOverlap(complexNeighborhood, nodes)) {
            for (Edge edge : complexEdges) {
                // TODO: Right now we check all edges. But due to the simple cmp, we can only check that the edge with
                // one side that equal to this node
                if ((Bitmap.isSubset(edge.getLeft(), nodes) && Bitmap.isSubset(edge.getRight(),
                        Bitmap.newBitmap(index))) || (Bitmap.isSubset(edge.getRight(), nodes) && Bitmap.isSubset(
                        edge.getLeft(), Bitmap.newBitmap(index)))) {
                    return edge;
                }
            }
        }
        return null;
    }

    public int getIndex() {
        return index;
    }

    public BitSet getNodeMap() {
        return Bitmap.newBitmap(index);
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
}

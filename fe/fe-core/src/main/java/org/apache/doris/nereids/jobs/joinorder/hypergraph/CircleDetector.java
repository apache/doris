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

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * The class implements the algorithm in Online Cycle Detection
 * and Difference Propagation for Pointer Analysis. It adds the
 * edge and detect the circle in graph. Note the vertices in
 * this class are join edge in HyperGraph
 */
public class CircleDetector {
    // record the topological order of each node, named n2i in paper.
    // orders[a] < orders[b] => a -> b
    List<Integer> orders = new ArrayList<>();
    // record the node in certain order, named i2n in paper
    List<Integer> nodes = new ArrayList<>();
    // stored the dependency of each node
    List<Long> directedEdges = new ArrayList<>();
    // the nodes are after than this node
    List<Long> subNodes = new ArrayList<>();

    CircleDetector(int size) {
        for (int i = 0; i < size; i++) {
            orders.add(i);
            nodes.add(i);
            directedEdges.add(LongBitmap.newBitmap());
            subNodes.add(LongBitmap.newBitmap(i));
        }
    }

    /**
     * Try to add edge node1 -> node2, and return true if success
     *
     * @param node1 the before node in directed edge
     * @param node2 the after node in directed edge
     * @return true if add successfully
     */
    public boolean tryAddDirectedEdge(int node1, int node2) {
        // Add dependency node1 -> node2
        if (checkCircleWithEdge(node1, node2)) {
            return false;
        }
        directedEdges.set(node1, LongBitmap.set(directedEdges.get(node1), node2));
        int order1 = orders.get(node1);
        int order2 = orders.get(node2);
        if (order1 >= order2) {
            shift(order2, order1 + 1, subNodes.get(node2));
        }
        int size = subNodes.size();
        for (int i = 0; i < size; i++) {
            // add the subNodes which contains node1 with subNodes of node2.
            long nodes = subNodes.get(i);
            if (LongBitmap.get(nodes, node1)) {
                subNodes.set(i, LongBitmap.or(nodes, subNodes.get(node2)));
            }
        }
        subNodes.set(node1, LongBitmap.or(subNodes.get(node1), subNodes.get(node2)));
        return true;
    }

    /**
     * Delete an edge node1->node2
     *
     * @param node1 the start node of the edge
     * @param node2 the end node of the edge
     */
    public void deleteDirectedEdge(int node1, int node2) {
        Preconditions.checkArgument(LongBitmap.get(directedEdges.get(node1), node2),
                String.format("The edge %d -> %d is not existed", node1, node2));
        int size = subNodes.size();
        for (int i = 0; i < size; i++) {
            subNodes.set(i, LongBitmap.newBitmap());
        }

        size = orders.size();
        for (int i = 0; i < size; i++) {
            getSubNodes(i);
        }
    }

    public List<Integer> getTopologicalOrder() {
        return orders;
    }

    /**
     * Whether there is an edge after add node1 -> node2
     *
     * @param node1 the left node
     * @param node2 the right node
     * @return Whether there is an edge after add node1 -> node2
     */
    public boolean checkCircleWithEdge(int node1, int node2) {
        // return true when there is a circle
        return LongBitmap.get(subNodes.get(node2), node1);
    }

    private long getSubNodes(int node) {
        if (LongBitmap.getCardinality(subNodes.get(node)) != 0) {
            return subNodes.get(node);
        }

        for (int nextNode : LongBitmap.getIterator(directedEdges.get(node))) {
            Preconditions.checkArgument(orders.get(nextNode) > orders.get(node),
                    String.format("node %d must come after node %d", nextNode, node));
            subNodes.set(node, LongBitmap.or(subNodes.get(node), getSubNodes(nextNode)));
        }
        return subNodes.get(node);
    }

    private void shift(int startOrder, int endOrder, long visited) {
        // Reorder the nodes between order1 and order2. We always keep the nodes visited comes
        // before the other nodes and their relative order is not changed. Because those two parts
        // is not connected, we can do it safely.
        List<Integer> shiftNodes = new ArrayList<>();
        for (int o = startOrder; o < endOrder; o++) {
            int node = nodes.get(o);
            if (LongBitmap.get(visited, node)) {
                shiftNodes.add(node);
            } else {
                // the relative orders of visited nodes are not changed
                allocate(node, o - shiftNodes.size());
            }
        }

        int size = shiftNodes.size();
        for (int i = 0; i < size; i++) {
            allocate(shiftNodes.get(i), endOrder + i - size);
        }
    }

    private void allocate(int node, int order) {
        orders.set(node, order);
        nodes.set(order, node);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        int size = directedEdges.size();
        for (int i = 0; i < size; i++) {
            if (LongBitmap.getCardinality(directedEdges.get(i)) != 0) {
                builder.append(String.format("%d -> %s; ", i, directedEdges.get(i)));
            }
        }
        return builder.toString();
    }
}

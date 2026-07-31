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

package org.apache.doris.nereids.jobs.joinorder.hypergraphv2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The class implements the algorithm in Online Cycle Detection
 * and Difference Propagation for Pointer Analysis. It adds the
 * edge and detect the circle in graph. Note the vertices in
 * this class are join edge in HyperGraph
 */
public class CircleDetector {
    private int[] order;                // Topological order of nodes
    private int[] positionOfNode;       // Position of each node in the topological order
    private boolean[] visited;          // DFS visit marker
    private List<Integer> toShift;      // Temporary storage for nodes to be shifted
    private Map<Integer, List<Integer>> edges; // Directed edge relations

    /**
     * Constructor: initializes the topological order and internal structures.
     * @param numVertices Number of nodes in the graph
     */
    public CircleDetector(int numVertices) {
        order = new int[numVertices];
        positionOfNode = new int[numVertices];
        visited = new boolean[numVertices];
        toShift = new ArrayList<>();
        edges = new HashMap<>();
        for (int i = 0; i < numVertices; i++) {
            order[i] = i;
            positionOfNode[i] = i;
            visited[i] = false;
        }
    }

    /**
     * Checks if adding edge aIdx->bIdx would create a cycle.
     * @param aIdx Source node index
     * @param bIdx Destination node index
     * @return true if a cycle would be created, false otherwise
     */
    public boolean edgeWouldCreateCycle(int aIdx, int bIdx) {
        if (aIdx == bIdx) {
            return true;
        }
        int posOfA = positionOfNode[aIdx];
        int posOfB = positionOfNode[bIdx];
        if (posOfA >= posOfB) {
            Arrays.fill(visited, false);
            if (depthFirstSearch(bIdx, posOfA + 1, aIdx)) {
                return true;
            }
            moveAllMarked(posOfB, posOfA + 1);
        }
        return false;
    }

    /**
     * Adds a directed edge aIdx->bIdx. Returns true if a cycle would be created.
     * @param aIdx Source node index
     * @param bIdx Destination node index
     * @return false if a cycle would be created, true otherwise
     */
    public boolean addEdge(int aIdx, int bIdx) {
        if (edgeWouldCreateCycle(aIdx, bIdx)) {
            return false;
        }
        edges.computeIfAbsent(aIdx, k -> new ArrayList<>()).add(bIdx);
        return true;
    }

    /**
     * Deletes the directed edge aIdx->bIdx.
     * @param aIdx Source node index
     * @param bIdx Destination node index
     */
    public void deleteEdge(int aIdx, int bIdx) {
        List<Integer> list = edges.get(aIdx);
        if (list != null && list.remove((Integer) bIdx)) {
            if (list.isEmpty()) {
                edges.remove(aIdx);
            }
            return;
        }
        throw new AssertionError("Edge not found");
    }

    /**
     * DFS helper to check reachability and mark nodes for shifting.
     * @param nodeIdx Current node index
     * @param upperBound Only consider nodes before this position
     * @param nodeIdxToAvoid Target node to detect cycles
     * @return true if a cycle is found, false otherwise
     */
    private boolean depthFirstSearch(int nodeIdx, int upperBound, int nodeIdxToAvoid) {
        if (nodeIdx == nodeIdxToAvoid) {
            return true;
        }
        if (visited[nodeIdx]) {
            return false;
        }
        if (positionOfNode[nodeIdx] >= upperBound) {
            return false;
        }
        visited[nodeIdx] = true;
        List<Integer> dests = edges.getOrDefault(nodeIdx, Collections.emptyList());
        for (int dest : dests) {
            if (positionOfNode[dest] <= positionOfNode[nodeIdx]) {
                continue;
            }
            if (depthFirstSearch(dest, upperBound, nodeIdxToAvoid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Moves all marked nodes to the right of the specified range in the topological order.
     * @param startPos Start position in order array
     * @param newPos End position in order array
     */
    private void moveAllMarked(int startPos, int newPos) {
        toShift.clear();
        for (int i = startPos; i < newPos; i++) {
            int nodeIdx = order[i];
            if (visited[nodeIdx]) {
                toShift.add(nodeIdx);
            } else {
                allocate(nodeIdx, i - toShift.size());
            }
        }
        for (int i = 0; i < toShift.size(); i++) {
            allocate(toShift.get(i), newPos + i - toShift.size());
        }
    }

    /**
     * Assigns a node to a new position in the topological order.
     * @param nodeIdx Node index
     * @param indexInOrder Position in order array
     */
    private void allocate(int nodeIdx, int indexInOrder) {
        order[indexInOrder] = nodeIdx;
        positionOfNode[nodeIdx] = indexInOrder;
    }

    /**
     * Returns the current topological order of nodes.
     * @return Array of node indices in topological order
     */
    public int[] getOrder() {
        return Arrays.copyOf(order, order.length);
    }
}

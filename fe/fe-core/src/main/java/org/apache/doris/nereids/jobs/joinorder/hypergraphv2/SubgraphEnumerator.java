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

import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.bitmap.LongBitmapSubsetIterator;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.DPhyperNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.receiver.AbstractReceiver;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * HyperGraph Join Reorder Enumerator.
 * <p>
 * This class implements a join reordering algorithm based on HyperGraph, inspired by the papers:
 * <ul>
 *   <li>"Dynamic Programming Strikes Back"</li>
 *   <li>"Build Query Optimizer"</li>
 * </ul>
 * The core idea is to model tables and join relationships in a query as a hypergraph, then enumerate all
 * possible join orders by recursively generating all connected subgraphs (CSG) and their complements (CMP),
 * and recording/pruning them using a DP table (receiver).
 * <br>
 * Key concepts:
 * <ul>
 *   <li><b>CSG (Connected SubGraph):</b> A connected subgraph in the hypergraph, representing a set of tables
 *   that can be joined together.</li>
 *   <li><b>CMP (Complement SubGraph):</b> The complement of a CSG, representing the remaining tables that can
 *   be joined with the current CSG.</li>
 *   <li><b>EdgeCalculator:</b> Caches the relationship between subgraphs and edges to improve enumeration
 *   efficiency.</li>
 *   <li><b>NeighborhoodCalculator:</b> Calculates the neighborhood nodes of a subgraph for expansion.</li>
 *   <li><b>receiver:</b> The DP table, responsible for recording enumerated subgraphs and their join plans.</li>
 * </ul>
 * <br>
 * Main workflow:
 * <ol>
 *   <li>Initialize the receiver, EdgeCalculator, and NeighborhoodCalculator.</li>
 *   <li>For each node, recursively enumerate all connected subgraphs (CSG), and for each CSG, enumerate all
 *   possible CMPs.</li>
 *   <li>Record and prune using the receiver to avoid redundant or invalid enumerations.</li>
 *   <li>All feasible join plans are finally stored in the receiver.</li>
 * </ol>
 * This implementation supports trace debugging and can output detailed enumeration steps.
 */
public class SubgraphEnumerator {
    public static final Logger LOG = LogManager.getLogger(SubgraphEnumerator.class);
    // Whether to enable trace for detailed enumeration steps
    private final boolean enableTrace = ConnectContext.get().getSessionVariable().enableDpHypTrace;
    // Trace information collector
    private final StringBuilder traceBuilder = new StringBuilder();
    // DP table, records all enumerated CSG/CMP and their join plans
    private AbstractReceiver receiver;
    // The HyperGraph being enumerated
    private HyperGraph hyperGraph;
    // Edge relationship cache to avoid redundant computation
    private EdgeCalculator edgeCalculator;
    // Neighborhood node calculator
    private NeighborhoodCalculator neighborhoodCalculator;

    /**
     * Constructor.
     *
     * @param receiver   DP table to record join plans
     * @param hyperGraph The query's hypergraph
     */
    public SubgraphEnumerator(AbstractReceiver receiver, HyperGraph hyperGraph) {
        this.receiver = receiver;
        this.hyperGraph = hyperGraph;
    }

    /**
     * Main entry for hypergraph enumeration.
     * <p>
     * 1. Initializes the DP table (receiver), edge cache (edgeCalculator), and neighborhood calculator
     *    (neighborhoodCalculator).
     * 2. For each node, recursively enumerates all connected subgraphs (CSG), and for each CSG, enumerates all
     *    possible CMPs.
     * 3. Records all feasible join plans in the receiver.
     *
     * @return true if enumeration succeeds, false otherwise
     */
    public boolean enumerate() {
        if (enableTrace) {
            traceBuilder.append("Query Graph Graphviz: ").append(hyperGraph.toDottyHyperGraph()).append("\n");
        }
        receiver.reset();
        List<AbstractNode> nodes = hyperGraph.getNodes();
        // Init all nodes in Receiver
        // in hyper graph, there are two kinds of elements, join edge and hyper node
        // in plan tree, the LogicalJoin node is translated to join edge in hyper graph
        // and other kind of node is translated to hyper node.
        // so in a join cluster, all hyper nodes may be a simple table or the root node of sub-plan tree
        for (AbstractNode node : nodes) {
            DPhyperNode dPhyperNode = (DPhyperNode) node;
            receiver.addGroup(node.getNodeMap(), dPhyperNode.getGroup());
        }
        int size = nodes.size();

        // Init edgeCalculator
        edgeCalculator = new EdgeCalculator(hyperGraph.getJoinEdges());
        for (AbstractNode node : nodes) {
            edgeCalculator.initSubgraph(node.getNodeMap());
        }

        // Init neighborhoodCalculator
        neighborhoodCalculator = new NeighborhoodCalculator();

        // We skip the last element because it can't generate valid csg-cmp pair
        long forbiddenNodes = LongBitmap.newBitmapBetween(0, size - 1);
        for (int i = size - 1; i >= 0; i--) {
            if (enableTrace) {
                traceBuilder.append("Starting main iteration at node[").append(i).append("]\n");
            }
            long csg = LongBitmap.newBitmap(i);
            forbiddenNodes = LongBitmap.unset(forbiddenNodes, i);
            if (!emitCsg(csg) || !enumerateCsgRec(csg, LongBitmap.clone(forbiddenNodes))) {
                return false;
            }
        }
        if (enableTrace) {
            LOG.info(traceBuilder.toString());
        }
        return true;
    }

    /**
     * Recursively expands the current connected subgraph (CSG), enumerating all larger connected subgraphs
     * containing the current CSG.
     *
     * @param csg            The current connected subgraph as a bitmap
     * @param forbiddenNodes The set of nodes that cannot be expanded (bitmap)
     * @return true if successful, false otherwise
     */
    private boolean enumerateCsgRec(long csg, long forbiddenNodes) {
        long neighborhood = neighborhoodCalculator.calcNeighborhood(csg, forbiddenNodes, edgeCalculator);
        LongBitmapSubsetIterator subsetIterator = LongBitmap.getSubsetIterator(neighborhood);
        if (enableTrace) {
            traceBuilder.append("Expanding connected subgraph, subgraph=[").append(LongBitmap.toString(csg))
                    .append("], neighborhood=[").append(LongBitmap.toString(neighborhood)).append("], forbidden=[")
                    .append(LongBitmap.toString(forbiddenNodes)).append("]\n");
        }
        for (long subset : subsetIterator) {
            long newCsg = LongBitmap.newBitmapUnion(csg, subset);
            if (receiver.contain(newCsg)) {
                if (!emitCsg(newCsg)) {
                    return false;
                }
            }
        }
        forbiddenNodes = LongBitmap.or(forbiddenNodes, neighborhood);
        subsetIterator.reset();
        for (long subset : subsetIterator) {
            long newCsg = LongBitmap.newBitmapUnion(csg, subset);
            edgeCalculator.unionSubGraphs(csg, subset);
            if (!enumerateCsgRec(newCsg, LongBitmap.clone(forbiddenNodes))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Recursively expands the current complement subgraph (CMP), enumerating all larger complements containing
     * the current CMP.
     *
     * @param csg            The current CSG
     * @param cmp            The current CMP
     * @param forbiddenNodes The set of nodes that cannot be expanded
     * @return true if successful, false otherwise
     */
    private boolean enumerateCmpRec(long csg, long cmp, long forbiddenNodes) {
        long neighborhood = neighborhoodCalculator.calcNeighborhood(cmp, forbiddenNodes, edgeCalculator);
        LongBitmapSubsetIterator subsetIterator = new LongBitmapSubsetIterator(neighborhood);
        if (enableTrace) {
            traceBuilder.append("Expanding complement subgraph, subgraph=[").append(LongBitmap.toString(cmp))
                    .append("], neighborhood=[").append(LongBitmap.toString(neighborhood)).append("], forbidden=[")
                    .append(LongBitmap.toString(forbiddenNodes)).append("]\n");
        }
        for (long subset : subsetIterator) {
            long newCmp = LongBitmap.newBitmapUnion(cmp, subset);
            // We need to check whether Cmp is connected and then try to find hyper edge
            if (receiver.contain(newCmp)) {
                // We check all edges for finding an edge.
                List<Edge> edges = edgeCalculator.connectCsgCmp(csg, newCmp);
                if (!edges.isEmpty()) {
                    AbstractReceiver.EmitState emitState = receiver.emitCsgCmp(csg, newCmp, edges);
                    if (emitState == AbstractReceiver.EmitState.SUCCESS) {
                        edgeCalculator.unionSubGraphs(csg, newCmp);
                    } else if (emitState == AbstractReceiver.EmitState.FAIL) {
                        return false;
                    }
                }
            }
        }
        forbiddenNodes = LongBitmap.or(forbiddenNodes, neighborhood);
        subsetIterator.reset();
        for (long subset : subsetIterator) {
            long newCmp = LongBitmap.newBitmapUnion(cmp, subset);
            edgeCalculator.unionSubGraphs(cmp, subset);
            if (!enumerateCmpRec(csg, newCmp, LongBitmap.clone(forbiddenNodes))) {
                return false;
            }
        }
        return true;
    }

    /**
     * For a given connected subgraph (CSG), enumerate all possible complements (CMP) and generate all feasible
     * csg-cmp pairs.
     *
     * @param csg The current connected subgraph
     * @return true if successful, false otherwise
     */
    private boolean emitCsg(long csg) {
        long forbiddenNodes = LongBitmap.newBitmapBetween(0, LongBitmap.nextSetBit(csg, 0));
        forbiddenNodes = LongBitmap.or(forbiddenNodes, csg);
        long neighborhoods = neighborhoodCalculator.calcNeighborhood(csg, LongBitmap.clone(forbiddenNodes),
                edgeCalculator);
        if (enableTrace && LongBitmap.getCardinality(csg) == 1) {
            traceBuilder.append("Emitting connected subgraph, subgraph=[").append(LongBitmap.toString(csg))
                    .append("], neighborhood=[").append(LongBitmap.toString(neighborhoods)).append("], forbidden=[")
                    .append(LongBitmap.toString(forbiddenNodes)).append("]\n");
        }
        for (int nodeIndex : LongBitmap.getReverseIterator(neighborhoods)) {
            long cmp = LongBitmap.newBitmap(nodeIndex);
            // whether there is an edge between csg and cmp
            List<Edge> edges = edgeCalculator.connectCsgCmp(csg, cmp);
            if (!edges.isEmpty()) {
                AbstractReceiver.EmitState emitState = receiver.emitCsgCmp(csg, cmp, edges);
                if (emitState == AbstractReceiver.EmitState.SUCCESS) {
                    edgeCalculator.unionSubGraphs(csg, cmp);
                } else if (emitState == AbstractReceiver.EmitState.FAIL) {
                    return false;
                }
            }

            // In order to avoid enumerate repeated cmp, e.g.,
            //       t1 (csg)
            //      /  \
            //     t2 - t3
            // for csg {t1}, we can get neighborhoods {t2, t3}
            // 1. The cmp is {t3} and expanded from {t3} to {t2, t3}
            // 2. The cmp is {t2} and expanded from {t2} to {t2, t3}
            // We don't want get {t2, t3} twice. So In first enumeration, we
            // can exclude {t2}
            long newForbiddenNodes = LongBitmap.newBitmapBetween(0, nodeIndex);
            newForbiddenNodes = LongBitmap.and(newForbiddenNodes, neighborhoods);
            newForbiddenNodes = LongBitmap.or(newForbiddenNodes, forbiddenNodes);
            if (!enumerateCmpRec(csg, cmp, newForbiddenNodes)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Neighborhood node calculator.
     * <p>
     * Used to calculate the neighborhood node set of a given subgraph (i.e., nodes that can be used to expand
     * the subgraph). Only the minimal set of nodes needed to expand all subgraphs is selected, to avoid redundant
     * expansion.
     */
    static class NeighborhoodCalculator {
        /**
         * Calculate the neighborhood nodes for a given subgraph.
         *
         * @param subgraph       The current subgraph
         * @param forbiddenNodes Nodes that cannot be expanded
         * @param edgeCalculator Edge cache
         * @return Neighborhood node set (bitmap)
         */
        public long calcNeighborhood(long subgraph, long forbiddenNodes, EdgeCalculator edgeCalculator) {
            long neighborhoods = LongBitmap.newBitmap();
            for (Edge edge : edgeCalculator.foundSimpleEdgesContain(subgraph)) {
                //                neighborhoods = LongBitmap.or(neighborhoods, edge.getReferenceNodes());
                neighborhoods = LongBitmap.or(neighborhoods, edge.getReferenceNodes());
            }
            forbiddenNodes = LongBitmap.or(forbiddenNodes, subgraph);
            for (Edge edge : edgeCalculator.foundComplexEdgesContain(subgraph)) {
                long left = edge.getLeftExtendedNodes();
                long right = edge.getRightExtendedNodes();
                if (LongBitmap.isSubset(left, subgraph) && !LongBitmap.isOverlap(right, forbiddenNodes)) {
                    neighborhoods = LongBitmap.set(neighborhoods, LongBitmap.lowestOneIndex(right));
                } else if (LongBitmap.isSubset(right, subgraph) && !LongBitmap.isOverlap(left, forbiddenNodes)) {
                    neighborhoods = LongBitmap.set(neighborhoods, LongBitmap.lowestOneIndex(left));
                }
            }
            neighborhoods = LongBitmap.andNot(neighborhoods, forbiddenNodes);
            return neighborhoods;
        }
    }

    /**
     * Edge relationship cache and calculator.
     * <p>
     * 1. Caches all edges in the hypergraph.
     * 2. Caches, for each subgraph, the edges it contains (split into simple/complex), and the edges connecting
     *    to its complement.
     * 3. Supports efficient merging of subgraphs, finding connecting edges, and determining edge-subgraph
     *    relationships.
     * <ul>
     *   <li><b>containSimpleEdges/containComplexEdges:</b> Edges where one endpoint is fully contained in the
     *   subgraph.</li>
     *   <li><b>overlapEdges:</b> Edges where one endpoint partially overlaps with the subgraph.</li>
     * </ul>
     * These caches greatly improve join enumeration efficiency.
     */
    static class EdgeCalculator {
        // all edges are unchanged during enumerate phase
        final List<Edge> edges;
        // It cached all edges that contained by this subgraph, Note we always
        // use bitset store edge map because the number of edges can be very large
        // We split these into simple edges (only one node on each side) and complex edges (others)
        // because we can often quickly discard all simple edges by testing the set of interesting nodes
        // against the “simple_neighborhood” bitmap. These data will be calculated before enumerate.

        // HashMap<Long, BitSet>: sub-graph to it's edges
        // Long : sub graph nodes' indexes LongBitmap, the index is 0 based, but in LongBitmap, minimal is 1st bit: 1
        // BitSet : edge's index in all join edges, 0 based
        // for each sub graph, we cache its containSimpleEdges and containComplexEdges for neighbor calculation
        // contains means the sub graph contains one whole side end of edge
        HashMap<Long, BitSet> containSimpleEdges = new HashMap<>();
        HashMap<Long, BitSet> containComplexEdges = new HashMap<>();
        // It cached all edges that overlap by this subgraph. All overlap edges must be
        // complex edges, overlap means the sub graph contains part of one side end of edge
        // the overlapEdges are NOT used to connect the sub graph, but used make union two sub-graph faster
        // only overlapEdges may be turned in to containEdges
        HashMap<Long, BitSet> overlapEdges = new HashMap<>();

        EdgeCalculator(List<Edge> edges) {
            this.edges = edges;
        }

        /**
         * Initialize the edge cache for a subgraph.
         *
         * @param subgraph Subgraph node set
         */
        public void initSubgraph(long subgraph) {
            BitSet simpleContains = new BitSet();
            BitSet complexContains = new BitSet();
            BitSet overlaps = new BitSet();
            for (Edge edge : edges) {
                if (isContainEdge(subgraph, edge)) {
                    if (edge.isSimple()) {
                        simpleContains.set(edge.getIndex());
                    } else {
                        complexContains.set(edge.getIndex());
                    }
                } else if (isOverlapEdge(subgraph, edge)) {
                    overlaps.set(edge.getIndex());
                }
            }
            if (containSimpleEdges.containsKey(subgraph)) {
                complexContains.or(containComplexEdges.get(subgraph));
                simpleContains.or(containSimpleEdges.get(subgraph));
            }
            if (overlapEdges.containsKey(subgraph)) {
                overlaps.or(overlapEdges.get(subgraph));
            }
            overlapEdges.put(subgraph, overlaps);
            containSimpleEdges.put(subgraph, simpleContains);
            containComplexEdges.put(subgraph, complexContains);
        }

        /**
         * Merge the edge caches of two subgraphs.
         * <p>
         * Used to quickly merge edge information when expanding subgraphs.
         *
         * @param subgraph1 Subgraph 1
         * @param subgraph2 Subgraph 2
         */
        public void unionSubGraphs(long subgraph1, long subgraph2) {
            // When union two sub graphs, we only need to check overlap edges.
            // However, if all reference nodes are contained by the subgraph,
            // we should remove it.
            if (!containSimpleEdges.containsKey(subgraph1)) {
                initSubgraph(subgraph1);
            }
            if (!containSimpleEdges.containsKey(subgraph2)) {
                initSubgraph(subgraph2);
            }
            long subgraph = LongBitmap.newBitmapUnion(subgraph1, subgraph2);
            if (containSimpleEdges.containsKey(subgraph)) {
                return;
            }
            BitSet simpleContains = new BitSet();
            simpleContains.or(containSimpleEdges.get(subgraph1));
            simpleContains.or(containSimpleEdges.get(subgraph2));
            BitSet complexContains = new BitSet();
            complexContains.or(containComplexEdges.get(subgraph1));
            complexContains.or(containComplexEdges.get(subgraph2));
            BitSet overlaps = new BitSet();
            overlaps.or(overlapEdges.get(subgraph1));
            overlaps.or(overlapEdges.get(subgraph2));
            for (int index : overlaps.stream().toArray()) {
                Edge edge = edges.get(index);
                // some overlap edges may become contains edges
                if (isContainEdge(subgraph, edge)) {
                    overlaps.set(index, false);
                    if (edge.isSimple()) {
                        simpleContains.set(index);
                    } else {
                        complexContains.set(index);
                    }
                }
            }
            simpleContains = removeInvalidEdges(subgraph, simpleContains);
            complexContains = removeInvalidEdges(subgraph, complexContains);
            containSimpleEdges.put(subgraph, simpleContains);
            containComplexEdges.put(subgraph, complexContains);
            overlapEdges.put(subgraph, overlaps);
        }

        /**
         * Find join edges that connect csg and cmp.
         * <p>
         * Only returns join edges that can connect csg and cmp.
         *
         * @param csg Connected subgraph
         * @param cmp Complement subgraph
         * @return List of connecting edges, or empty if not connected
         */
        public List<Edge> connectCsgCmp(long csg, long cmp) {
            Preconditions.checkArgument(
                    containSimpleEdges.containsKey(csg) && containSimpleEdges.containsKey(cmp));
            List<Edge> foundEdges = new ArrayList<>();
            // find all edges contained both by csg and cmp, we use these edges as join condition later
            BitSet edgeMap = new BitSet();
            edgeMap.or(containSimpleEdges.get(csg));
            edgeMap.and(containSimpleEdges.get(cmp));
            BitSet complexes = new BitSet();
            complexes.or(containComplexEdges.get(csg));
            complexes.and(containComplexEdges.get(cmp));
            edgeMap.or(complexes);
            edgeMap.stream().forEach(index -> foundEdges.add(edges.get(index)));
            return foundEdges;
        }

        /**
         * Get all edges (simple + complex) contained in a subgraph.
         *
         * @param subgraph Subgraph
         * @return List of edges
         */
        public List<Edge> foundEdgesContain(long subgraph) {
            BitSet edgeMap = containSimpleEdges.get(subgraph);
            Preconditions.checkState(edgeMap != null);
            edgeMap.or(containComplexEdges.get(subgraph));
            return edgeMap.stream().mapToObj(edges::get).collect(Collectors.toList());
        }

        /**
         * Get all simple edges contained in a subgraph.
         *
         * @param subgraph Subgraph
         * @return List of edges
         */
        public List<Edge> foundSimpleEdgesContain(long subgraph) {
            if (!containSimpleEdges.containsKey(subgraph)) {
                return Collections.emptyList();
            }
            BitSet edgeMap = containSimpleEdges.get(subgraph);
            return edgeMap.stream().mapToObj(edges::get).collect(Collectors.toList());
        }

        /**
         * Get all complex edges contained in a subgraph.
         *
         * @param subgraph Subgraph
         * @return List of edges
         */
        public List<Edge> foundComplexEdgesContain(long subgraph) {
            if (!containComplexEdges.containsKey(subgraph)) {
                return Collections.emptyList();
            }
            BitSet edgeMap = containComplexEdges.get(subgraph);
            return edgeMap.stream().mapToObj(edges::get).collect(Collectors.toList());
        }

        /**
         * Determine if an edge has one endpoint fully contained in the subgraph.
         *
         * @param subgraph Subgraph
         * @param edge     Edge
         * @return true if contained, false otherwise
         */
        private boolean isContainEdge(long subgraph, Edge edge) {
            // one side of the edge completely inside the subgraph, and other side completely outside the subgraph
            return (LongBitmap.isSubset(edge.getLeftExtendedNodes(), subgraph)
                    && !LongBitmap.isOverlap(edge.getRightExtendedNodes(), subgraph))
                    || (LongBitmap.isSubset(edge.getRightExtendedNodes(), subgraph)
                    && !LongBitmap.isOverlap(edge.getLeftExtendedNodes(), subgraph));
        }

        /**
         * Determine if an edge partially overlaps with the subgraph.
         *
         * @param subgraph Subgraph
         * @param edge     Edge
         * @return true if overlaps, false otherwise
         */
        private boolean isOverlapEdge(long subgraph, Edge edge) {
            // one side of the edge overlap subgraph but not inside it, and other side completely outside the subgraph
            return (LongBitmap.isOverlap(edge.getLeftExtendedNodes(), subgraph)
                    && !LongBitmap.isSubset(edge.getLeftExtendedNodes(), subgraph)
                    && !LongBitmap.isOverlap(edge.getRightExtendedNodes(), subgraph))
                    || (LongBitmap.isOverlap(edge.getRightExtendedNodes(), subgraph)
                    && !LongBitmap.isSubset(edge.getRightExtendedNodes(), subgraph)
                    && !LongBitmap.isOverlap(edge.getLeftExtendedNodes(), subgraph));
        }

        /**
         * Remove edges that are no longer contained by the subgraph.
         *
         * @param subgraph Subgraph
         * @param edgeMap  Edge set
         * @return Filtered edge set
         */
        private BitSet removeInvalidEdges(long subgraph, BitSet edgeMap) {
            for (int index : edgeMap.stream().toArray()) {
                Edge edge = edges.get(index);
                if (!isContainEdge(subgraph, edge)) {
                    edgeMap.set(index, false);
                }
            }
            return edgeMap;
        }
    }
}

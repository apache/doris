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
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmapSubsetIterator;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.AbstractReceiver;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

/**
 * This class enumerate all subgraph of HyperGraph. CSG means connected subgraph
 * and CMP means complement subgraph. More details are in Dynamic Programming
 * Strikes Back and Build Query Optimizer.
 */
public class SubgraphEnumerator {
    //The receiver receives the csg and cmp and record them, named DPTable in paper
    AbstractReceiver receiver;
    //The enumerated hyperGraph
    HyperGraph hyperGraph;
    EdgeCalculator edgeCalculator;
    NeighborhoodCalculator neighborhoodCalculator;
    // These caches are used to avoid repetitive computation

    public SubgraphEnumerator(AbstractReceiver receiver, HyperGraph hyperGraph) {
        this.receiver = receiver;
        this.hyperGraph = hyperGraph;
    }

    /**
     * Entry function of enumerating hyperGraph
     *
     * @return whether the hyperGraph is enumerated successfully
     */
    public boolean enumerate() {
        receiver.reset();
        List<Node> nodes = hyperGraph.getNodes();
        // Init all nodes in Receiver
        for (Node node : nodes) {
            receiver.addGroup(node.getNodeMap(), node.getGroup());
        }
        int size = nodes.size();

        // Init edgeCalculator
        edgeCalculator = new EdgeCalculator(hyperGraph.getEdges());
        for (Node node : nodes) {
            edgeCalculator.initSubgraph(node.getNodeMap());
        }

        // Init neighborhoodCalculator
        neighborhoodCalculator = new NeighborhoodCalculator();

        // We skip the last element because it can't generate valid csg-cmp pair
        long forbiddenNodes = LongBitmap.newBitmapBetween(0, size - 1);
        for (int i = size - 2; i >= 0; i--) {
            long csg = LongBitmap.newBitmap(i);
            forbiddenNodes = LongBitmap.unset(forbiddenNodes, i);
            if (!emitCsg(csg) || !enumerateCsgRec(csg, LongBitmap.clone(forbiddenNodes))) {
                return false;
            }
        }
        return true;
    }

    // The general purpose of EnumerateCsgRec is to extend a given set csg, which
    // induces a connected subgraph of G to a larger set with the same property.
    private boolean enumerateCsgRec(long csg, long forbiddenNodes) {
        long neighborhood = neighborhoodCalculator.calcNeighborhood(csg, forbiddenNodes, edgeCalculator);
        LongBitmapSubsetIterator subsetIterator = LongBitmap.getSubsetIterator(neighborhood);
        for (long subset : subsetIterator) {
            long newCsg = LongBitmap.newBitmapUnion(csg, subset);
            if (receiver.contain(newCsg)) {
                edgeCalculator.unionEdges(csg, subset);
                if (!emitCsg(newCsg)) {
                    return false;
                }
            }
        }
        forbiddenNodes = LongBitmap.or(forbiddenNodes, neighborhood);
        subsetIterator.reset();
        for (long subset : subsetIterator) {
            long newCsg = LongBitmap.newBitmapUnion(csg, subset);
            if (!enumerateCsgRec(newCsg, LongBitmap.clone(forbiddenNodes))) {
                return false;
            }
        }
        return true;
    }

    private boolean enumerateCmpRec(long csg, long cmp, long forbiddenNodes) {
        long neighborhood = neighborhoodCalculator.calcNeighborhood(cmp, forbiddenNodes, edgeCalculator);
        LongBitmapSubsetIterator subsetIterator = new LongBitmapSubsetIterator(neighborhood);
        for (long subset : subsetIterator) {
            long newCmp = LongBitmap.newBitmapUnion(cmp, subset);
            // We need to check whether Cmp is connected and then try to find hyper edge
            if (receiver.contain(newCmp)) {
                edgeCalculator.unionEdges(cmp, subset);
                // We check all edges for finding an edge.
                List<Edge> edges = edgeCalculator.connectCsgCmp(csg, newCmp);
                if (edges.isEmpty()) {
                    continue;
                }
                if (!receiver.emitCsgCmp(csg, newCmp, edges)) {
                    return false;
                }
            }
        }
        forbiddenNodes = LongBitmap.or(forbiddenNodes, neighborhood);
        subsetIterator.reset();
        for (long subset : subsetIterator) {
            long newCmp = LongBitmap.newBitmapUnion(cmp, subset);
            if (!enumerateCmpRec(csg, newCmp, LongBitmap.clone(forbiddenNodes))) {
                return false;
            }
        }
        return true;
    }

    // EmitCsg takes as an argument a non-empty, proper subset csg of HyperGraph , which
    // induces a connected subgraph. It is then responsible to generate the seeds for
    // all cmp such that (csg, cmp) becomes a csg-cmp-pair.
    private boolean emitCsg(long csg) {
        long forbiddenNodes = LongBitmap.newBitmapBetween(0, LongBitmap.nextSetBit(csg, 0));
        forbiddenNodes = LongBitmap.or(forbiddenNodes, csg);
        long neighborhoods = neighborhoodCalculator.calcNeighborhood(csg, LongBitmap.clone(forbiddenNodes),
                edgeCalculator);

        for (int nodeIndex : LongBitmap.getReverseIterator(neighborhoods)) {
            long cmp = LongBitmap.newBitmap(nodeIndex);
            // whether there is an edge between csg and cmp
            List<Edge> edges = edgeCalculator.connectCsgCmp(csg, cmp);
            if (edges.isEmpty()) {
                continue;
            }
            if (!receiver.emitCsgCmp(csg, cmp, edges)) {
                return false;
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
            long newForbiddenNodes = LongBitmap.newBitmapBetween(0, nodeIndex + 1);
            newForbiddenNodes = LongBitmap.and(newForbiddenNodes, neighborhoods);
            newForbiddenNodes = LongBitmap.or(newForbiddenNodes, forbiddenNodes);
            if (!enumerateCmpRec(csg, cmp, newForbiddenNodes)) {
                return false;
            }
        }
        return true;
    }

    class NeighborhoodCalculator {
        // This function is used to calculate neighborhoods of given subgraph.
        // Though a direct way is to add all nodes u that satisfies:
        //              <u, v> \in E && v \in subgraph && v \intersect X = empty
        // We don't used it because they can cause some repeated subgraph when
        // expand csg and cmp. In fact, we just need a seed node that can be expanded
        // to all subgraph. That is any one node of hyper nodes. In fact, the neighborhoods
        // is the minimum set that we choose one node from above v.
        public long calcNeighborhood(long subgraph, long forbiddenNodes, EdgeCalculator edgeCalculator) {
            long neighborhoods = LongBitmap.newBitmap();
            for (Edge edge : edgeCalculator.foundSimpleEdgesContain(subgraph)) {
                neighborhoods = LongBitmap.or(neighborhoods, edge.getReferenceNodes());
            }
            forbiddenNodes = LongBitmap.or(forbiddenNodes, subgraph);
            neighborhoods = LongBitmap.andNot(neighborhoods, forbiddenNodes);
            forbiddenNodes = LongBitmap.or(forbiddenNodes, neighborhoods);

            for (Edge edge : edgeCalculator.foundComplexEdgesContain(subgraph)) {
                long left = edge.getLeft();
                long right = edge.getRight();
                if (LongBitmap.isSubset(left, subgraph) && !LongBitmap.isOverlap(right, forbiddenNodes)) {
                    neighborhoods = LongBitmap.set(neighborhoods, LongBitmap.lowestOneIndex(right));
                } else if (LongBitmap.isSubset(right, subgraph) && !LongBitmap.isOverlap(left, forbiddenNodes)) {
                    neighborhoods = LongBitmap.set(neighborhoods, LongBitmap.lowestOneIndex(left));
                }
            }
            return neighborhoods;
        }
    }

    class EdgeCalculator {
        final List<Edge> edges;
        // It cached all edges that contained by this subgraph, Note we always
        // use bitset store edge map because the number of edges can be very large
        // We split these into simple edges (only one node on each side) and complex edges (others)
        // because we can often quickly discard all simple edges by testing the set of interesting nodes
        // against the “simple_neighborhood” bitmap. These data will be calculated before enumerate.

        HashMap<Long, BitSet> containSimpleEdges = new HashMap<>();
        HashMap<Long, BitSet> containComplexEdges = new HashMap<>();
        // It cached all edges that overlap by this subgraph. All this edges must be
        // complex edges
        HashMap<Long, BitSet> overlapEdges = new HashMap<>();

        EdgeCalculator(List<Edge> edges) {
            this.edges = edges;
        }

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

        public void unionEdges(long bitmap1, long bitmap2) {
            // When union two sub graphs, we only need to check overlap edges.
            // However, if all reference nodes are contained by the subgraph,
            // we should remove it.
            if (!containSimpleEdges.containsKey(bitmap1)) {
                initSubgraph(bitmap1);
            }
            if (!containSimpleEdges.containsKey(bitmap2)) {
                initSubgraph(bitmap2);
            }
            long subgraph = LongBitmap.newBitmapUnion(bitmap1, bitmap2);
            if (containSimpleEdges.containsKey(subgraph)) {
                return;
            }
            BitSet simpleContains = new BitSet();
            simpleContains.or(containSimpleEdges.get(bitmap1));
            simpleContains.or(containSimpleEdges.get(bitmap2));
            BitSet complexContains = new BitSet();
            simpleContains.or(containComplexEdges.get(bitmap1));
            simpleContains.or(containComplexEdges.get(bitmap2));
            BitSet overlaps = new BitSet();
            simpleContains.or(overlapEdges.get(bitmap1));
            simpleContains.or(overlapEdges.get(bitmap2));
            for (int index : overlaps.stream().toArray()) {
                Edge edge = edges.get(index);
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

        public List<Edge> connectCsgCmp(long csg, long cmp) {
            Preconditions.checkArgument(
                    containSimpleEdges.containsKey(csg) && containSimpleEdges.containsKey(cmp));
            List<Edge> foundEdges = new ArrayList<>();
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

        public List<Edge> foundEdgesContain(long subgraph) {
            Preconditions.checkArgument(containSimpleEdges.containsKey(subgraph));
            BitSet edgeMap = containSimpleEdges.get(subgraph);
            edgeMap.or(containComplexEdges.get(subgraph));
            List<Edge> foundEdges = new ArrayList<>();
            edgeMap.stream().forEach(index -> foundEdges.add(edges.get(index)));
            return foundEdges;
        }

        public List<Edge> foundSimpleEdgesContain(long subgraph) {
            List<Edge> foundEdges = new ArrayList<>();
            if (!containSimpleEdges.containsKey(subgraph)) {
                return foundEdges;
            }
            BitSet edgeMap = containSimpleEdges.get(subgraph);
            edgeMap.stream().forEach(index -> foundEdges.add(edges.get(index)));
            return foundEdges;
        }

        public List<Edge> foundComplexEdgesContain(long subgraph) {
            List<Edge> foundEdges = new ArrayList<>();
            if (!containComplexEdges.containsKey(subgraph)) {
                return foundEdges;
            }
            BitSet edgeMap = containComplexEdges.get(subgraph);
            edgeMap.stream().forEach(index -> foundEdges.add(edges.get(index)));
            return foundEdges;
        }

        public int getEdgeSizeContain(long subgraph) {
            Preconditions.checkArgument(containSimpleEdges.containsKey(subgraph));
            return containSimpleEdges.get(subgraph).cardinality() + containSimpleEdges.get(subgraph).cardinality();
        }

        private boolean isContainEdge(long subgraph, Edge edge) {
            int containLeft = LongBitmap.isSubset(edge.getLeft(), subgraph) ? 0 : 1;
            int containRight = LongBitmap.isSubset(edge.getRight(), subgraph) ? 0 : 1;
            return containLeft + containRight == 1;
        }

        private boolean isOverlapEdge(long subgraph, Edge edge) {
            int overlapLeft = LongBitmap.isOverlap(edge.getLeft(), subgraph) ? 0 : 1;
            int overlapRight = LongBitmap.isOverlap(edge.getRight(), subgraph) ? 0 : 1;
            return overlapLeft + overlapRight == 1;
        }

        private BitSet removeInvalidEdges(long subgraph, BitSet edgeMap) {
            for (int index : edgeMap.stream().toArray()) {
                Edge edge = edges.get(index);
                if (!isOverlapEdge(subgraph, edge)) {
                    edgeMap.set(index, false);
                }
            }
            return edgeMap;
        }
    }
}


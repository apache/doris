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
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.SubsetIterator;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.AbstractReceiver;

import java.util.BitSet;
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
            receiver.addGroup(node.getBitSet(), node.getGroup());
        }
        hyperGraph.splitEdgesForNodes();
        int size = nodes.size();

        // We skip the last element due to it can't generate valid csg-cmp pair
        BitSet forbiddenNodes = Bitmap.newBitmapBetween(0, size - 1);
        for (int i = size - 2; i >= 0; i--) {
            BitSet csg = Bitmap.newBitmap(i);
            Bitmap.unset(forbiddenNodes, i);
            if (!emitCsg(csg) || !enumerateCsgRec(csg, Bitmap.newBitmap(forbiddenNodes))) {
                return false;
            }
        }
        return true;
    }

    // The general purpose of EnumerateCsgRec is to extend a given set csg, which
    // induces a connected subgraph of G to a larger set with the same property.
    private boolean enumerateCsgRec(BitSet csg, BitSet forbiddenNodes) {
        BitSet neighborhood = calcNeighborhood(csg, forbiddenNodes);
        SubsetIterator subsetIterator = Bitmap.getSubsetIterator(neighborhood);
        for (BitSet subset : subsetIterator) {
            BitSet newCsg = Bitmap.newBitmapUnion(csg, subset);
            if (receiver.contain(newCsg)) {
                if (!emitCsg(newCsg)) {
                    return false;
                }
            }
        }
        Bitmap.or(forbiddenNodes, neighborhood);
        subsetIterator.reset();
        for (BitSet subset : subsetIterator) {
            BitSet newCsg = Bitmap.newBitmapUnion(csg, subset);
            if (!enumerateCsgRec(newCsg, Bitmap.newBitmap(forbiddenNodes))) {
                return false;
            }
        }
        return true;
    }

    private boolean enumerateCmpRec(BitSet csg, BitSet cmp, BitSet forbiddenNodes) {
        BitSet neighborhood = calcNeighborhood(cmp, forbiddenNodes);
        SubsetIterator subsetIterator = new SubsetIterator(neighborhood);
        for (BitSet subset : subsetIterator) {
            BitSet newCmp = Bitmap.newBitmapUnion(cmp, subset);
            // We need to check whether Cmp is connected and then try to find hyper edge
            if (receiver.contain(newCmp)) {
                // We check all edges for finding an edge. That is inefficient.
                // MySQL use full neighborhood for it. Or a hashMap may be useful
                for (Edge edge : hyperGraph.getEdges()) {
                    if (Bitmap.isSubset(edge.getLeft(), csg) && Bitmap.isSubset(edge.getRight(), newCmp) || (
                            Bitmap.isSubset(edge.getLeft(), newCmp) && Bitmap.isSubset(edge.getRight(), csg))) {
                        if (!receiver.emitCsgCmp(csg, newCmp, edge)) {
                            return false;
                        }
                        break;
                    }
                }
            }
        }
        Bitmap.or(forbiddenNodes, neighborhood);
        subsetIterator.reset();
        for (BitSet subset : subsetIterator) {
            BitSet newCmp = Bitmap.newBitmapUnion(cmp, subset);
            if (!enumerateCmpRec(csg, newCmp, Bitmap.newBitmap(forbiddenNodes))) {
                return false;
            }
        }
        return true;
    }

    // EmitCsg takes as an argument a non-empty, proper subset csg of HyperGraph , which
    // induces a connected subgraph. It is then responsible to generate the seeds for
    // all cmp such that (csg, cmp) becomes a csg-cmp-pair.
    private boolean emitCsg(BitSet csg) {
        BitSet forbiddenNodes = Bitmap.newBitmapBetween(0, Bitmap.nextSetBit(csg, 0));
        Bitmap.or(forbiddenNodes, csg);
        BitSet neighborhoods = calcNeighborhood(csg, Bitmap.newBitmap(forbiddenNodes));

        for (int nodeIndex : Bitmap.getReverseIterator(neighborhoods)) {
            BitSet cmp = Bitmap.newBitmap(nodeIndex);
            // whether there is an edge between csg and cmp
            Node cmpNode = hyperGraph.getNode(nodeIndex);
            Edge edge = cmpNode.tryGetEdgeWith(csg);
            if (edge != null) {
                if (!receiver.emitCsgCmp(csg, cmp, edge)) {
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
            BitSet newForbiddenNodes = Bitmap.newBitmapBetween(0, nodeIndex + 1);
            Bitmap.and(newForbiddenNodes, neighborhoods);
            Bitmap.or(newForbiddenNodes, forbiddenNodes);
            if (!enumerateCmpRec(csg, cmp, newForbiddenNodes)) {
                return false;
            }
        }
        return true;
    }

    // This function is used to calculate neighborhoods of given subgraph.
    // Though a direct way is to add all nodes u that satisfies:
    //              <u, v> \in E && v \in subgraph && v \intersect X = empty
    // We don't used it because they can cause some repeated subgraph when
    // expand csg and cmp. In fact, we just need a seed node that can be expanded
    // to all subgraph. That is any one node of hyper nodes. In fact, the neighborhoods
    // is the minimum set that we choose one node from above v.

    // Note there are many tricks implemented in MySQL, such as neighbor cache, complex edges
    // We hope implement them after a benchmark.
    private BitSet calcNeighborhood(BitSet subGraph, BitSet forbiddenNodes) {
        BitSet neighborhoods = Bitmap.newBitmap();
        Bitmap.getIterator(subGraph)
                .forEach(nodeIndex -> Bitmap.or(neighborhoods, hyperGraph.getNode(nodeIndex).getSimpleNeighborhood()));
        Bitmap.andNot(neighborhoods, forbiddenNodes);
        Bitmap.or(forbiddenNodes, subGraph);
        Bitmap.or(forbiddenNodes, neighborhoods);

        for (Edge edge : hyperGraph.getEdges()) {
            BitSet left = edge.getLeft();
            BitSet right = edge.getRight();
            if (Bitmap.isSubset(left, subGraph) && !Bitmap.isOverlap(right, forbiddenNodes)) {
                Bitmap.set(neighborhoods, right.nextSetBit(0));
            } else if (Bitmap.isSubset(right, subGraph) && !Bitmap.isOverlap(left, forbiddenNodes)) {
                Bitmap.set(neighborhoods, left.nextSetBit(0));
            }
        }
        return neighborhoods;
    }
}

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

package org.apache.doris.nereids.rules.joinreorder.hypergraph;

import org.apache.doris.nereids.rules.joinreorder.hypergraph.bitmap.Bitmap;
import org.apache.doris.nereids.rules.joinreorder.hypergraph.bitmap.SubsetIterator;
import org.apache.doris.nereids.rules.joinreorder.hypergraph.receiver.AbstractReceiver;

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

    SubgraphEnumerator(AbstractReceiver receiver, HyperGraph hyperGraph) {
        this.receiver = receiver;
        this.hyperGraph = hyperGraph;
    }

    /**
     * Entry function of enumerating hyperGraph
     *
     * @return whether the hyperGraph is enumerated successfully
     */
    public boolean enumerate() {
        List<Node> nodes = hyperGraph.getNodes();
        // Init all nodes in Receiver
        for (Node node : nodes) {
            receiver.addPlan(node.getBitSet(), node.getPlan());
        }
        int size = nodes.size();

        // We skip the last element due to it can't generate valid csg-cmp pair
        BitSet forbiddenNodes = new BitSet();
        forbiddenNodes.set(0, size - 1);
        for (int i = size - 2; i >= 0; i--) {
            BitSet csg = new BitSet();
            csg.set(i);
            forbiddenNodes.set(i, false);
            emitCsg(csg);
            enumerateCsgRec(csg, cloneBitSet(forbiddenNodes));
        }
        return true;
    }

    // The general purpose of EnumerateCsgRec is to extend a given set csg, which
    // induces a connected subgraph of G to a larger set with the same property.
    private void enumerateCsgRec(BitSet csg, BitSet forbiddenNodes) {
        BitSet neighborhood = calcNeighborhood(csg, forbiddenNodes);
        SubsetIterator subsetIterator = new SubsetIterator(neighborhood);
        for (BitSet subset : subsetIterator) {
            BitSet newCsg = new BitSet();
            newCsg.or(csg);
            newCsg.or(subset);
            if (receiver.contain(newCsg)) {
                emitCsg(newCsg);
            }
        }
        forbiddenNodes.or(neighborhood);
        for (BitSet subset : subsetIterator) {
            BitSet newCsg = new BitSet();
            newCsg.or(csg);
            newCsg.or(subset);
            enumerateCsgRec(newCsg, cloneBitSet(forbiddenNodes));
        }
    }

    private void enumerateCmpRec(BitSet csg, BitSet cmp, BitSet forbiddenNodes) {
        BitSet neighborhood = calcNeighborhood(cmp, forbiddenNodes);
        SubsetIterator subsetIterator = new SubsetIterator(neighborhood);
        for (BitSet subset : subsetIterator) {
            BitSet newCmp = new BitSet();
            newCmp.or(cmp);
            newCmp.or(subset);
            // We need to check whether Cmp is connected and then try to find hyper edge
            if (receiver.contain(newCmp)) {
                // We check all edges for finding an edge. That is inefficient.
                // MySQL use full neighborhood for it. Or a hashMap may be useful
                for (Edge edge : hyperGraph.getEdges()) {
                    if (Bitmap.isSubset(csg, edge.getLeft()) && Bitmap.isSubset(cmp, edge.getRight()) || (
                            Bitmap.isSubset(cmp, edge.getLeft()) && Bitmap.isSubset(csg, edge.getRight()))) {
                        receiver.emitCsgCmp(csg, cmp, edge);
                        break;
                    }
                }
            }
        }
        forbiddenNodes.or(neighborhood);
        for (BitSet subset : subsetIterator) {
            BitSet newCmp = new BitSet();
            newCmp.or(cmp);
            newCmp.or(subset);
            enumerateCmpRec(csg, newCmp, cloneBitSet(forbiddenNodes));
        }
    }

    // EmitCsg takes as an argument a non-empty, proper subset csg of HyperGraph , which
    // induces a connected subgraph. It is then responsible to generate the seeds for
    // all cmp such that (csg, cmp) becomes a csg-cmp-pair.
    private void emitCsg(BitSet csg) {
        BitSet forbiddenNodes = new BitSet();
        forbiddenNodes.set(0, csg.nextSetBit(0));
        forbiddenNodes.or(csg);
        BitSet neighborhoods = calcNeighborhood(csg, cloneBitSet(forbiddenNodes));
        int cardinality = neighborhoods.cardinality();
        int nodeIndex = neighborhoods.size();

        for (int i = cardinality - 1; i >= 0; i--) {
            nodeIndex = neighborhoods.previousSetBit(nodeIndex - 1);
            BitSet cmp = new BitSet();
            cmp.set(nodeIndex);
            // whether there is an edge between csg and cmp
            Node cmpNode = hyperGraph.getNode(nodeIndex);
            Edge edge = cmpNode.tryGetEdgeWith(csg);
            if (edge != null) {
                receiver.emitCsgCmp(csg, cmp, edge);
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
            BitSet newForbiddenNodes = new BitSet();
            newForbiddenNodes.set(0, nodeIndex);
            newForbiddenNodes.or(forbiddenNodes);
            enumerateCmpRec(csg, cmp, newForbiddenNodes);
        }
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
        BitSet neighborhoods = new BitSet();
        subGraph.stream()
                .forEach(nodeIndex -> neighborhoods.or(hyperGraph.getNode(nodeIndex).getSimpleNeighborhood()));
        neighborhoods.andNot(forbiddenNodes);
        forbiddenNodes.or(subGraph);
        forbiddenNodes.or(neighborhoods);

        for (Edge edge : hyperGraph.getEdges()) {
            BitSet left = edge.getLeft();
            BitSet right = edge.getRight();
            if (Bitmap.isSubset(left, subGraph) && !left.intersects(forbiddenNodes)) {
                neighborhoods.set(right.nextSetBit(0));
            } else if (Bitmap.isSubset(right, subGraph) && !right.intersects(forbiddenNodes)) {
                neighborhoods.set(left.nextSetBit(0));
            }
            forbiddenNodes.or(neighborhoods);
        }
        return neighborhoods;
    }

    private BitSet cloneBitSet(BitSet bitSet) {
        BitSet cloned = new BitSet();
        cloned.or(bitSet);
        return cloned;
    }
}

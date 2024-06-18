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

package org.apache.doris.common.util;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Consistent hash algorithm implemented by SortedMap
 */
public class ConsistentHash<K, N> {
    /**
     * Virtual node for consistent hash algorithm
     */
    private class VirtualNode {
        private final int replicaIndex;
        private final N node;

        public VirtualNode(N node, int replicaIndex) {
            this.replicaIndex = replicaIndex;
            this.node = node;
        }

        public N getNode() {
            return node;
        }

        public long hashValue() {
            Hasher hasher = hashFunction.newHasher();
            hasher.putObject(node, nodeFunnel);
            hasher.putInt(replicaIndex);
            long hash = hasher.hash().asLong();
            return hash;
        }
    }

    HashFunction hashFunction;
    Funnel<K> keyFunnel;
    Funnel<N> nodeFunnel;
    private final SortedMap<Long, VirtualNode> ring = new TreeMap<>();
    private final int virtualNumber;

    public ConsistentHash(
            HashFunction hashFunction,
            Funnel<K> keyFunnel,
            Funnel<N> nodeFunnel,
            Collection<N> nodes,
            int virtualNumber) {
        this.hashFunction = hashFunction;
        this.keyFunnel = keyFunnel;
        this.nodeFunnel = nodeFunnel;
        this.virtualNumber = virtualNumber;
        for (N node : nodes) {
            addNode(node);
        }
    }

    public void addNode(N node) {
        for (int i = 0; i < virtualNumber; i++) {
            VirtualNode vNode = new VirtualNode(node, i);
            ring.put(vNode.hashValue(), vNode);
        }
    }

    public void removeNode(N node) {
        for (int i = 0; i < virtualNumber; i++) {
            VirtualNode vNode = new VirtualNode(node, i);
            ring.remove(vNode.hashValue());
        }
    }

    public List<N> getNode(K key, int count) {
        int nodeCount = ring.values().size();
        if (count > nodeCount) {
            count = nodeCount;
        }

        Set<N> uniqueNodes = new LinkedHashSet<>();

        Hasher hasher = hashFunction.newHasher();
        Long hashKey = hasher.putObject(key, keyFunnel).hash().asLong();

        SortedMap<Long, VirtualNode> tailMap = ring.tailMap(hashKey);
        // Start reading from tail
        for (Map.Entry<Long, VirtualNode> entry : tailMap.entrySet()) {
            uniqueNodes.add(entry.getValue().node);
            if (uniqueNodes.size() == count) {
                break;
            }
        }

        if (uniqueNodes.size() < count) {
            // Start reading from the head as we have exhausted tail
            SortedMap<Long, VirtualNode> headMap = ring.headMap(hashKey);
            for (Map.Entry<Long, VirtualNode> entry : headMap.entrySet()) {
                uniqueNodes.add(entry.getValue().node);
                if (uniqueNodes.size() == count) {
                    break;
                }
            }
        }

        return ImmutableList.copyOf(uniqueNodes);
    }
}

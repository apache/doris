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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Consistent hash algorithm implemented by SortedMap
 */
public class ConsistentHash<K, N> {
    public interface HashFunction<T> {
        long hashValue(T object);
    }

    public static class StringHashFunction implements HashFunction<String> {
        private final MD5Hash md5 = new MD5Hash();

        @Override
        public long hashValue(String object) {
            return md5.hash(object);
        }
    }

    private static class MD5Hash {
        MessageDigest instance;

        public MD5Hash() {
            try {
                instance = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                // do nothing.
            }
        }

        long hash(String key) {
            instance.reset();
            instance.update(key.getBytes());
            byte[] digest = instance.digest();

            long h = 0;
            for (int i = 0; i < 4; i++) {
                h <<= 8;
                h |= ((int) digest[i]) & 0xFF;
            }
            return h;
        }
    }

    /**
     * Virtual node for consistent hash algorithm
     */
    private class VirtualNode {
        private final int replicaNumber;
        private final N parent;

        public VirtualNode(N parent, int replicaNumber) {
            this.replicaNumber = replicaNumber;
            this.parent = parent;
        }

        public boolean matches(N node) {
            return nodeHash.hashValue(parent) == nodeHash.hashValue(node);
        }

        public int getReplicaNumber() {
            return replicaNumber;
        }

        public N getParent() {
            return parent;
        }

        public long hashValue() {
            return md5.hash(nodeHash.hashValue(parent) + ":" + replicaNumber);
        }
    }

    private final MD5Hash md5 = new MD5Hash();
    private final HashFunction<K> keyHash;
    private final HashFunction<N> nodeHash;
    private final SortedMap<Long, VirtualNode> ring = new TreeMap<>();
    private final int virtualNumber;

    public ConsistentHash(Collection<N> nodes, int virtualNumber, HashFunction<K> keyHash, HashFunction<N> nodeHash) {
        this.keyHash = keyHash;
        this.nodeHash = nodeHash;
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
        Iterator<Long> it = ring.keySet().iterator();
        while (it.hasNext()) {
            Long key = it.next();
            VirtualNode virtualNode = ring.get(key);
            if (virtualNode.matches(node)) {
                it.remove();
            }
        }
    }

    public N getNode(K key) {
        if (ring.isEmpty()) {
            return null;
        }
        Long hashKey = keyHash.hashValue(key);
        SortedMap<Long, VirtualNode> tailMap = ring.tailMap(hashKey);
        hashKey = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        return ring.get(hashKey).getParent();
    }
}

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

package org.apache.doris.nereids.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class representing an immutable set of elements with equivalence relations.
 */
public class ImmutableEqualSet<T> {
    private final Map<T, T> root;

    ImmutableEqualSet(Map<T, T> root) {
        this.root = ImmutableMap.copyOf(root);
    }

    public static <T> ImmutableEqualSet<T> empty() {
        return new ImmutableEqualSet<>(ImmutableMap.of());
    }

    /**
     * Builder for ImmutableEqualSet.
     */
    public static class Builder<T> {
        private final Map<T, T> parent = new HashMap<>();
        private final Map<T, Integer> size = new HashMap<>();

        /**
         * Add a equal pair
         */
        public void addEqualPair(T a, T b) {
            T root1 = findRoot(a);
            T root2 = findRoot(b);
            if (root1 != root2) {
                parent.put(b, root1);
                findRoot(b);
            }
        }

        private T findRoot(T a) {
            parent.putIfAbsent(a, a); // Ensure that the element is added
            size.putIfAbsent(a, 1); // Initialize size to 1

            if (!parent.get(a).equals(a)) {
                parent.put(a, findRoot(parent.get(a))); // Path compression
            }
            return parent.get(a);
        }

        public ImmutableEqualSet<T> build() {
            parent.keySet().forEach(this::findRoot);
            return new ImmutableEqualSet<>(parent);
        }
    }

    /**
     * Calculate equal set for a except self
     */
    public Set<T> calEqualSet(T a) {
        T ra = root.get(a);
        return root.keySet().stream()
                .filter(t -> root.get(t).equals(ra) && !t.equals(a))
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Calculate all equal set
     */
    public List<Set<T>> calEqualSetList() {
        return root.values()
                .stream()
                .distinct()
                .map(a -> {
                    T ra = root.get(a);
                    return root.keySet().stream()
                            .filter(t -> root.get(t).equals(ra))
                            .collect(ImmutableSet.toImmutableSet());
                }).collect(ImmutableList.toImmutableList());
    }

    public Set<T> getAllItemSet() {
        return ImmutableSet.copyOf(root.keySet());
    }
}

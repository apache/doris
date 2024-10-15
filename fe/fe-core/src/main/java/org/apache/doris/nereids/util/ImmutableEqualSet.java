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
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
        private Map<T, T> parent;

        Builder(Map<T, T> parent) {
            this.parent = parent;
        }

        public Builder() {
            this(new LinkedHashMap<>());
        }

        public Builder(ImmutableEqualSet<T> equalSet) {
            this(new LinkedHashMap<>(equalSet.root));
        }

        /**
         * replace all key value according replace map
         */
        public void replace(Map<T, T> replaceMap) {
            Map<T, T> newMap = new LinkedHashMap<>();
            for (Entry<T, T> entry : parent.entrySet()) {
                newMap.put(replaceMap.getOrDefault(entry.getKey(), entry.getKey()),
                        replaceMap.getOrDefault(entry.getValue(), entry.getValue()));
            }
            parent = newMap;
        }

        /**
         * Remove all not contain in containSet
         * @param containSet the set to contain
         */
        public void removeNotContain(Set<T> containSet) {
            List<Set<T>> equalSetList = calEqualSetList();
            this.parent.clear();
            for (Set<T> equalSet : equalSetList) {
                Set<T> intersect = Sets.intersection(containSet, equalSet);
                if (intersect.size() <= 1) {
                    continue;
                }
                Iterator<T> iterator = intersect.iterator();
                T first = intersect.iterator().next();
                while (iterator.hasNext()) {
                    T next = iterator.next();
                    this.addEqualPair(first, next);
                }
            }
        }

        /**
         * Add a equal pair
         */
        public void addEqualPair(T a, T b) {
            if (!parent.containsKey(a)) {
                parent.put(a, a);
            }
            if (!parent.containsKey(b)) {
                parent.put(b, b);
            }
            T root1 = findRoot(a);
            T root2 = findRoot(b);
            if (root1 != root2) {
                parent.put(root1, root2);
            }
        }

        /**
         * Calculate all equal set
         */
        public List<Set<T>> calEqualSetList() {
            parent.replaceAll((s, v) -> findRoot(s));
            return parent.values()
                    .stream()
                    .distinct()
                    .map(a -> {
                        T ra = parent.get(a);
                        return parent.keySet().stream()
                                .filter(t -> parent.get(t).equals(ra))
                                .collect(ImmutableSet.toImmutableSet());
                    }).collect(ImmutableList.toImmutableList());
        }

        public void addEqualSet(ImmutableEqualSet<T> equalSet) {
            this.parent.putAll(equalSet.root);
        }

        private T findRoot(T a) {
            if (a.equals(parent.get(a))) {
                return parent.get(a);
            }
            return findRoot(parent.get(a));
        }

        public ImmutableEqualSet<T> build() {
            ImmutableMap.Builder<T, T> foldMapBuilder = new ImmutableMap.Builder<>();
            for (T k : parent.keySet()) {
                foldMapBuilder.put(k, findRoot(k));
            }
            return new ImmutableEqualSet<>(foldMapBuilder.build());
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

    public boolean isEmpty() {
        return root.isEmpty();
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

    public boolean isEqual(T l, T r) {
        if (!root.containsKey(l) || !root.containsKey(r)) {
            return false;
        }
        return root.get(l) == root.get(r);
    }

    @Override
    public String toString() {
        return root.toString();
    }
}

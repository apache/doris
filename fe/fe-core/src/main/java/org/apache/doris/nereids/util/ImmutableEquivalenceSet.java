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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * EquivalenceSet
 */
public class ImmutableEquivalenceSet<T> {
    final Map<T, T> root;

    ImmutableEquivalenceSet(Map<T, T> root) {
        this.root = ImmutableMap.copyOf(root);
    }

    public static <T> ImmutableEquivalenceSet<T> of() {
        return new ImmutableEquivalenceSet<>(ImmutableMap.of());
    }

    /**
     * Builder of ImmutableEquivalenceSet
     */
    public static class Builder<T> {
        final Map<T, T> parent = new HashMap<>();

        public void addEqualPair(T a, T b) {
            parent.computeIfAbsent(b, v -> v);
            parent.computeIfAbsent(a, v -> v);
            union(a, b);
        }

        private void union(T a, T b) {
            T root1 = findRoot(a);
            T root2 = findRoot(b);

            if (root1 != root2) {
                parent.put(b, root1);
                findRoot(b);
            }
        }

        private T findRoot(T a) {
            if (!parent.get(a).equals(a)) {
                parent.put(a, findRoot(parent.get(a)));
            }
            return parent.get(a);
        }

        public ImmutableEquivalenceSet<T> build() {
            parent.keySet().forEach(this::findRoot);
            return new ImmutableEquivalenceSet<>(parent);
        }
    }

    /**
     * cal equal set for a except self
     */
    public Set<T> calEqualSet(T a) {
        T ra = root.get(a);
        return root.keySet().stream()
                .filter(t -> root.get(t).equals(ra) && !t.equals(a))
                .collect(ImmutableSet.toImmutableSet());
    }

    public Set<T> getAllItemSet() {
        return ImmutableSet.copyOf(root.keySet());
    }
}

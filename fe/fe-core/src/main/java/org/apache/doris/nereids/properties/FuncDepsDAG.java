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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.Slot;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Function dependence items.
 */
public class FuncDepsDAG {
    class DAGItem {
        Set<Slot> slots;
        Set<DAGItem> parents;
        Set<DAGItem> children;

        public DAGItem(Set<Slot> slots) {
            this.slots = slots;
            this.parents = new HashSet<>();
            this.children = new HashSet<>();
        }
    }

    private Map<Set<Slot>, DAGItem> itemMap;

    public FuncDepsDAG() {
        itemMap = new HashMap<>();
    }

    public void addDeps(Set<Slot> l, Set<Slot> r) {
        DAGItem lNode = getOrCreateNode(l);
        DAGItem rNode = getOrCreateNode(r);
        addEdge(lNode, rNode);
    }

    private DAGItem getOrCreateNode(Set<Slot> slots) {
        return itemMap.computeIfAbsent(slots, DAGItem::new);
    }

    private void addEdge(DAGItem from, DAGItem to) {
        if (!from.children.contains(to)) {
            from.children.add(to);
            to.parents.add(from);
        }
    }

    /**
     * find all func deps
     */
    public FuncDeps findValidFuncDeps(Set<Slot> validSlot) {
        FuncDeps res = new FuncDeps();
        for (Entry<Set<Slot>, DAGItem> entry : itemMap.entrySet()) {
            if (validSlot.containsAll(entry.getKey())) {
                Set<DAGItem> visited = new HashSet<>();
                Set<DAGItem> children = new HashSet<>();
                visited.add(entry.getValue());
                collectAllChildren(validSlot, entry.getValue(), visited, children);
                for (DAGItem child : children) {
                    res.addFuncItems(entry.getValue().slots, child.slots);
                }
            }
        }
        return res;
    }

    private void collectAllChildren(Set<Slot> validSlot, DAGItem root,
            Set<DAGItem> visited, Set<DAGItem> children) {
        for (DAGItem child : root.children) {
            if (visited.contains(child)) {
                continue;
            }
            if (validSlot.containsAll(child.slots)) {
                children.add(child);
            }
            visited.add(child);
            collectAllChildren(validSlot, child, visited, children);
        }
    }
}

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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TreeNode.java
// and modified by Doris

package org.apache.doris.common;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Generic tree structure. Only concrete subclasses of this can be instantiated.
 */
public class TreeNode<NodeType extends TreeNode<NodeType>> {
    @SerializedName("children")
    protected ArrayList<NodeType> children = Lists.newArrayListWithCapacity(2);

    public NodeType getChild(int i) {
        return hasChild(i) ? children.get(i) : null;
    }

    public void addChild(NodeType n) {
        children.add(n);
    }

    public void addChildren(List<? extends NodeType> n) {
        children.addAll(n);
    }

    public boolean hasChild(int i) {
        return children.size() > i;
    }

    public void setChild(int index, NodeType n) {
        children.set(index, n);
    }

    public ArrayList<NodeType> getChildren() {
        return children;
    }

    public void clearChildren() {
        children.clear();
    }

    public void removeNode(int i) {
        if (children != null && i >= 0 && i < children.size()) {
            children.remove(i);
        }
    }

    /**
     * Count the total number of nodes in this tree. Leaf node will return 1.
     * Non-leaf node will include all its children.
     */
    public int numNodes() {
        int numNodes = 1;
        for (NodeType child : children) {
            numNodes += child.numNodes();
        }
        return numNodes;
    }

    /**
     * Add all nodes in the tree that satisfy 'predicate' to the list 'matches'
     * This node is checked first, followed by its children in order. If the node
     * itself matches, the children are skipped.
     */
    public <C extends TreeNode<NodeType>, D extends C> void collect(
            Predicate<? super C> predicate, Collection<D> matches) {
        // TODO: the semantics of this function are very strange. contains()
        // checks using .equals() on the nodes. In the case of literals, slotrefs
        // and maybe others, two different tree node objects can be equal and
        // this function would only return one of them. This is not intuitive.
        // We rely on these semantics to not have duplicate nodes. Investigate this.
        if (predicate.apply((C) this) && !matches.contains(this)) {
            matches.add((D) this);
            return;
        }
        for (NodeType child : children) {
            child.collect(predicate, matches);
        }
    }

    /**
     * Add all nodes in the tree that are of class 'cl' to the list 'matches'.
     * This node is checked first, followed by its children in order. If the node
     * itself is of class 'cl', the children are skipped.
     */
    public <C extends TreeNode<NodeType>, D extends C> void collect(
            Class cl, Collection<D> matches) {
        if (cl.equals(getClass())) {
            matches.add((D) this);
            return;
        }
        for (NodeType child : children) {
            child.collect(cl, matches);
        }
    }

    /**
     * Add all nodes in the tree that satisfy 'predicate' to the list 'matches'
     * This node is checked first, followed by its children in order. All nodes
     * that match in the subtree are added.
     */
    public <C extends TreeNode<NodeType>, D extends C> void collectAll(
            Predicate<? super C> predicate, List<D> matches) {
        if (predicate.apply((C) this)) {
            matches.add((D) this);
        }
        for (NodeType child : children) {
            child.collectAll(predicate, matches);
        }
    }

    /**
     * For each expression in 'nodeList', collect all subexpressions satisfying 'predicate'
     * into 'matches'
     */
    public static <C extends TreeNode<C>, D extends C> void collect(
            Collection<C> nodeList, Predicate<? super C> predicate, Collection<D> matches) {
        for (C node : nodeList) {
            node.collect(predicate, matches);
        }
    }

    /**
     * For each expression in 'nodeList', collect all subexpressions of class 'cl'
     * into 'matches'
     */
    public static <C extends TreeNode<C>, D extends C> void collect(
            Collection<C> nodeList, Class cl, Collection<D> matches) {
        for (C node : nodeList) {
            node.collect(cl, matches);
        }
    }

    public boolean contains(Class cl) {
        if (cl.isAssignableFrom(this.getClass()) && this.getClass().isAssignableFrom(cl)) {
            return true;
        }
        for (NodeType child : children) {
            if (child.contains(cl)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if this node or any of its children satisfy 'predicate'.
     */
    public <C extends TreeNode<NodeType>> boolean contains(
            Predicate<? super C> predicate) {
        if (predicate.apply((C) this)) {
            return true;
        }
        for (NodeType child : children) {
            if (child.contains(predicate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * For each node in nodeList, return true if any subexpression satisfies
     * contains('predicate').
     */
    public static <C extends TreeNode<C>, D extends C> boolean contains(
            Collection<C> nodeList, Predicate<? super C> predicate) {
        for (C node : nodeList) {
            if (node.contains(predicate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if any node in nodeList contains children of class cl.
     */
    public static <C extends TreeNode<C>> boolean contains(
            List<C> nodeList, Class cl) {
        for (C node : nodeList) {
            if (node.contains(cl)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsSubclass(Class cl) {
        if (cl.isAssignableFrom(this.getClass())) {
            return true;
        }
        for (NodeType child : children) {
            if (child.containsSubclass(cl)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return 'this' or first child that is exactly of class 'cl'.
     * Looks for matching children via depth-first, left-to-right traversal.
     */
    public <C extends NodeType> C findFirstOf(Class<C> cl) {
        if (this.getClass().equals(cl)) {
            return (C) this;
        }
        for (NodeType child : children) {
            NodeType result = child.findFirstOf(cl);
            if (result != null) {
                return (C) result;
            }
        }
        return null;
    }

    public interface ThrowingConsumer<T> {
        void accept(T t) throws AnalysisException;
    }

    public void foreach(ThrowingConsumer<TreeNode<NodeType>> func) throws AnalysisException {
        func.accept(this);
        for (NodeType child : getChildren()) {
            child.foreach(func);
        }
    }

    /** anyMatch */
    public boolean anyMatch(Predicate<TreeNode<? extends NodeType>> func) {
        if (func.apply(this)) {
            return true;
        }

        for (NodeType child : children) {
            if (child.anyMatch(func)) {
                return true;
            }
        }
        return false;
    }

    /** foreachDown */
    public void foreachDown(Predicate<TreeNode<NodeType>> visitor) {
        if (!visitor.test(this)) {
            return;
        }

        for (TreeNode<NodeType> child : getChildren()) {
            child.foreachDown(visitor);
        }
    }
}

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

package org.apache.doris.nereids.trees;

import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * interface for all node in Nereids, include plan node and expression.
 *
 * @param <NODE_TYPE> either {@link org.apache.doris.nereids.trees.plans.Plan}
 *                 or {@link org.apache.doris.nereids.trees.expressions.Expression}
 */
public interface TreeNode<NODE_TYPE extends TreeNode<NODE_TYPE>> {

    List<NODE_TYPE> children();

    NODE_TYPE child(int index);

    int arity();

    <T> Optional<T> getMutableState(String key);

    /** getOrInitMutableState */
    default <T> T getOrInitMutableState(String key, Supplier<T> initState) {
        Optional<T> mutableState = getMutableState(key);
        if (!mutableState.isPresent()) {
            T state = initState.get();
            setMutableState(key, state);
            return state;
        }
        return mutableState.get();
    }

    void setMutableState(String key, Object value);

    default NODE_TYPE withChildren(NODE_TYPE... children) {
        return withChildren(Utils.fastToImmutableList(children));
    }

    NODE_TYPE withChildren(List<NODE_TYPE> children);

    default NODE_TYPE withChildren(Function<NODE_TYPE, NODE_TYPE> rewriter) {
        return withChildren((child, index) -> rewriter.apply(child));
    }

    /**
     * rewrite children by a rewriter
     * @param rewriter consume the origin child and child index, then return the new child
     * @return new tree node if any child has changed
     */
    default NODE_TYPE withChildren(BiFunction<NODE_TYPE, Integer, NODE_TYPE> rewriter) {
        Builder<NODE_TYPE> newChildren = ImmutableList.builderWithExpectedSize(arity());
        boolean changed = false;
        for (int i = 0; i < arity(); i++) {
            NODE_TYPE child = child(i);
            NODE_TYPE newChild = rewriter.apply(child, i);
            if (child != newChild) {
                changed = true;
            }
            newChildren.add(newChild);
        }
        return changed ? withChildren(newChildren.build()) : (NODE_TYPE) this;
    }

    /**
     * top-down rewrite short circuit.
     * @param rewriteFunction rewrite function.
     * @return rewritten result.
     */
    default NODE_TYPE rewriteDownShortCircuit(Function<NODE_TYPE, NODE_TYPE> rewriteFunction) {
        NODE_TYPE currentNode = rewriteFunction.apply((NODE_TYPE) this);

        if (currentNode == this) {
            Builder<NODE_TYPE> newChildren = ImmutableList.builderWithExpectedSize(arity());
            boolean changed = false;
            for (NODE_TYPE child : children()) {
                NODE_TYPE newChild = child.rewriteDownShortCircuit(rewriteFunction);
                if (child != newChild) {
                    changed = true;
                }
                newChildren.add(newChild);
            }

            if (changed) {
                currentNode = currentNode.withChildren(newChildren.build());
            }
        }
        return currentNode;
    }

    /**
     * similar to rewriteDownShortCircuit, except that only subtrees, whose root satisfies
     * border predicate are rewritten.
     */
    default NODE_TYPE rewriteDownShortCircuitDown(Function<NODE_TYPE, NODE_TYPE> rewriteFunction,
            Predicate border, boolean aboveBorder) {
        NODE_TYPE currentNode = (NODE_TYPE) this;
        if (border.test(this)) {
            aboveBorder = false;
        }
        if (!aboveBorder) {
            currentNode = rewriteFunction.apply((NODE_TYPE) this);
        }
        if (currentNode == this) {
            Builder<NODE_TYPE> newChildren = ImmutableList.builderWithExpectedSize(arity());
            boolean changed = false;
            for (NODE_TYPE child : children()) {
                NODE_TYPE newChild = child.rewriteDownShortCircuitDown(rewriteFunction, border, aboveBorder);
                if (child != newChild) {
                    changed = true;
                }
                newChildren.add(newChild);
            }

            if (changed) {
                currentNode = currentNode.withChildren(newChildren.build());
            }
        }
        return currentNode;
    }

    /**
     * bottom-up rewrite.
     * @param rewriteFunction rewrite function.
     * @return rewritten result.
     */
    default NODE_TYPE rewriteUp(Function<NODE_TYPE, NODE_TYPE> rewriteFunction) {
        Builder<NODE_TYPE> newChildren = ImmutableList.builderWithExpectedSize(arity());
        boolean changed = false;
        for (NODE_TYPE child : children()) {
            NODE_TYPE newChild = child.rewriteUp(rewriteFunction);
            changed |= child != newChild;
            newChildren.add(newChild);
        }

        NODE_TYPE rewrittenChildren = changed ? withChildren(newChildren.build()) : (NODE_TYPE) this;
        return rewriteFunction.apply(rewrittenChildren);
    }

    /**
     * Foreach treeNode. Top-down traverse implicitly, stop traverse if satisfy test.
     * @param func foreach function
     */
    default void foreach(Predicate<TreeNode<NODE_TYPE>> func) {
        boolean valid = func.test(this);
        if (!valid) {
            for (NODE_TYPE child : children()) {
                child.foreach(func);
            }
        }
    }

    /**
     * Foreach treeNode. Top-down traverse implicitly.
     * @param func foreach function
     */
    default void foreach(Consumer<TreeNode<NODE_TYPE>> func) {
        func.accept(this);
        for (NODE_TYPE child : children()) {
            child.foreach(func);
        }
    }

    /** foreachBreath */
    default void foreachBreath(Predicate<TreeNode<NODE_TYPE>> func) {
        LinkedList<TreeNode<NODE_TYPE>> queue = new LinkedList<>();
        queue.add(this);
        while (!queue.isEmpty()) {
            TreeNode<NODE_TYPE> current = queue.pollFirst();
            if (!func.test(current)) {
                queue.addAll(current.children());
            }
        }
    }

    default void foreachUp(Consumer<TreeNode<NODE_TYPE>> func) {
        for (NODE_TYPE child : children()) {
            child.foreach(func);
        }
        func.accept(this);
    }

    /**
     * iterate top down and test predicate if any matched. Top-down traverse implicitly.
     * @param predicate predicate
     * @return true if any predicate return true
     */
    default boolean anyMatch(Predicate<TreeNode<NODE_TYPE>> predicate) {
        if (predicate.test(this)) {
            return true;
        }
        for (NODE_TYPE child : children()) {
            if (child.anyMatch(predicate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * iterate top down and test predicate if all matched. Top-down traverse implicitly.
     * @param predicate predicate
     * @return true if all predicate return true
     */
    default boolean allMatch(Predicate<TreeNode<NODE_TYPE>> predicate) {
        if (!predicate.test(this)) {
            return false;
        }
        for (NODE_TYPE child : children()) {
            if (!child.allMatch(predicate)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Collect the nodes that satisfied the predicate.
     */
    default <T> Set<T> collect(Predicate<TreeNode<NODE_TYPE>> predicate) {
        ImmutableSet.Builder<TreeNode<NODE_TYPE>> result = ImmutableSet.builder();
        foreach(node -> {
            if (predicate.test(node)) {
                result.add(node);
            }
        });
        return (Set<T>) result.build();
    }

    /**
     * Collect the nodes that satisfied the predicate to list.
     */
    default <T> List<T> collectToList(Predicate<TreeNode<NODE_TYPE>> predicate) {
        ImmutableList.Builder<TreeNode<NODE_TYPE>> result = ImmutableList.builder();
        foreach(node -> {
            if (predicate.test(node)) {
                result.add(node);
            }
        });
        return (List<T>) result.build();
    }

    /**
     * Collect the nodes that satisfied the predicate to set.
     */
    default <T> Set<T> collectToSet(Predicate<TreeNode<NODE_TYPE>> predicate) {
        ImmutableSet.Builder<TreeNode<NODE_TYPE>> result = ImmutableSet.builder();
        foreach(node -> {
            if (predicate.test(node)) {
                result.add(node);
            }
        });
        return (Set<T>) result.build();
    }

    /**
     * Collect the nodes that satisfied the predicate firstly.
     */
    default <T> Optional<T> collectFirst(Predicate<TreeNode<NODE_TYPE>> predicate) {
        List<TreeNode<NODE_TYPE>> result = new ArrayList<>();
        foreach(node -> {
            if (result.isEmpty() && predicate.test(node)) {
                result.add(node);
            }
            return !result.isEmpty();
        });
        return result.isEmpty() ? Optional.empty() : Optional.of((T) result.get(0));
    }

    /**
     * iterate top down and test predicate if contains any instance of the classes
     * @param types classes array
     * @return true if it has any instance of the types
     */
    default boolean containsType(Class... types) {
        return anyMatch(node -> {
            for (Class type : types) {
                if (type.isInstance(node)) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * equals by the full tree nodes
     * @param that other tree node
     * @return true if all the tree is equals
     */
    default boolean deepEquals(TreeNode<?> that) {
        Deque<TreeNode<?>> thisDeque = new ArrayDeque<>();
        Deque<TreeNode<?>> thatDeque = new ArrayDeque<>();

        thisDeque.push(this);
        thatDeque.push(that);

        while (!thisDeque.isEmpty()) {
            if (thatDeque.isEmpty()) {
                // The "that" tree has been fully traversed, but the "this" tree has not; hence they are not equal.
                return false;
            }

            TreeNode<?> currentNodeThis = thisDeque.pop();
            TreeNode<?> currentNodeThat = thatDeque.pop();

            // since TreeNode is immutable, use == to short circuit
            if (currentNodeThis == currentNodeThat) {
                continue;
            }

            // If current nodes are not equal or the number of child nodes differ, return false.
            if (!currentNodeThis.equals(currentNodeThat)
                    || currentNodeThis.arity() != currentNodeThat.arity()) {
                return false;
            }

            // Add child nodes to the deque for further processing.
            for (int i = 0; i < currentNodeThis.arity(); i++) {
                thisDeque.push(currentNodeThis.child(i));
                thatDeque.push(currentNodeThat.child(i));
            }
        }

        // If the "that" tree hasn't been fully traversed, return false.
        return thatDeque.isEmpty();
    }
}

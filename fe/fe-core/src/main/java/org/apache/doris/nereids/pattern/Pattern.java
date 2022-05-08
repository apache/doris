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

package org.apache.doris.nereids.pattern;

import com.google.common.collect.ImmutableList;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.TreeNode;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Pattern node used in pattern matching.
 */
public class Pattern<T extends TreeNode> extends AbstractTreeNode<Pattern<T>> {
    private final NodeType nodeType;
    public static final Pattern MULTI = new Pattern(NodeType.MULTI);
    public static final Pattern ANY = new Pattern(NodeType.ANY);

    public final List<Predicate<T>> predicates;

    /**
     * Constructor for Pattern.
     *
     * @param nodeType node type to matching
     * @param children sub pattern
     */
    public Pattern(NodeType nodeType, Pattern... children) {
        super(NodeType.PATTERN, children);
        this.nodeType = nodeType;
        this.predicates = ImmutableList.of();
    }

    public Pattern(NodeType nodeType, List<Predicate<T>> predicates, Pattern... children) {
        super(NodeType.PATTERN, children);
        this.nodeType = nodeType;
        this.predicates = ImmutableList.copyOf(predicates);
    }

    /**
     * get current type in Pattern.
     *
     * @return node type in pattern
     */
    public NodeType getNodeType() {
        return nodeType;
    }

    /**
     * Return ture if current Pattern match Plan in params.
     *
     * @param root wait to match
     * @return ture if current Pattern match Plan in params
     */
    public boolean matchRoot(T root) {
        if (root == null) {
            return false;
        }

        if (root.children().size() < this.children().size() && !children.contains(MULTI)) {
            return false;
        }

        if (nodeType == NodeType.MULTI || nodeType == NodeType.ANY) {
            return true;
        }

        return getNodeType().equals(root.getType())
                && predicates.stream().allMatch(predicate -> predicate.test(root));
    }

    public boolean matchChildren(T root) {
        for (int i = 0; i < arity(); i++) {
            if (!child(i).match(root.child(i))) {
                return false;
            }
        }
        return true;
    }

    public boolean match(T root) {
        return matchRoot(root) && matchChildren(root);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pattern pattern = (Pattern) o;
        return nodeType == pattern.nodeType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeType);
    }

    @Override
    public List<Pattern> children() {
        return (List) children;
    }

    @Override
    public Pattern child(int index) {
        return (Pattern) children.get(index);
    }
}

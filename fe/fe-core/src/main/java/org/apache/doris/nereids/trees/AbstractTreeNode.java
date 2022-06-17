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


import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.Operator;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract class for plan node in Nereids, include plan node and expression.
 *
 * @param <NODE_TYPE> either {@link org.apache.doris.nereids.trees.plans.Plan}
 *                 or {@link org.apache.doris.nereids.trees.expressions.Expression}
 */
public abstract class AbstractTreeNode<NODE_TYPE extends TreeNode<NODE_TYPE>>
        implements TreeNode<NODE_TYPE> {

    protected final NodeType type;
    protected final List<NODE_TYPE> children;
    // TODO: Maybe we should use a GroupPlan to avoid TreeNode hold the GroupExpression.
    // https://github.com/apache/incubator-doris/pull/9807#discussion_r884829067
    protected final Optional<GroupExpression> groupExpression;


    public AbstractTreeNode(NodeType type, NODE_TYPE... children) {
        this(type, Optional.empty(), children);
    }

    /**
     * Constructor for plan node.
     *
     * @param type node type
     * @param groupExpression group expression related to the operator of this node
     * @param children children of this node
     */
    public AbstractTreeNode(NodeType type, Optional<GroupExpression> groupExpression, NODE_TYPE... children) {
        this.type = type;
        this.children = ImmutableList.copyOf(children);
        this.groupExpression = Objects.requireNonNull(groupExpression, "groupExpression can not be null");
    }

    @Override
    public Operator getOperator() {
        throw new RuntimeException();
    }

    @Override
    public Optional<GroupExpression> getGroupExpression() {
        return groupExpression;
    }

    @Override
    public NODE_TYPE child(int index) {
        return children.get(index);
    }

    @Override
    public List<NODE_TYPE> children() {
        return children;
    }

    @Override
    public NodeType getType() {
        return type;
    }

    public int arity() {
        return children.size();
    }
}

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

/**
 * Abstract class for plan node in Nereids, include plan node and expression.
 *
 * @param <NODE_TYPE> either {@link org.apache.doris.nereids.trees.plans.Plan}
 *                 or {@link org.apache.doris.nereids.trees.expressions.Expression}
 */
public abstract class AbstractTreeNode<NODE_TYPE extends AbstractTreeNode<NODE_TYPE>>
        implements TreeNode<NODE_TYPE> {

    protected final NodeType type;
    protected final List<TreeNode> children;
    protected final GroupExpression groupExpression;


    public AbstractTreeNode(NodeType type, TreeNode... children) {
        this(type, null, children);
    }

    /**
     * Constructor for plan node.
     *
     * @param type node type
     * @param groupExpression group expression related to the operator of this node
     * @param children children of this node
     */
    public AbstractTreeNode(NodeType type, GroupExpression groupExpression, TreeNode... children) {
        this.type = type;
        if (children.length != 0 && children[0] == null) {
            this.children = ImmutableList.of();
        } else {
            this.children = ImmutableList.copyOf(children);
        }
        this.groupExpression = groupExpression;
    }

    @Override
    public Operator getOperator() {
        throw new RuntimeException();
    }

    @Override
    public GroupExpression getGroupExpression() {
        return groupExpression;
    }

    @Override
    public NODE_TYPE newChildren(List<TreeNode> children) {
        throw new RuntimeException();
    }

    @Override
    public NodeType getType() {
        return type;
    }

    @Override
    public <CHILD_TYPE extends TreeNode> List<CHILD_TYPE> children() {
        return (List) children;
    }

    @Override
    public <CHILD_TYPE extends TreeNode> CHILD_TYPE child(int index) {
        return (CHILD_TYPE) children.get(index);
    }

    public int arity() {
        return children.size();
    }
}

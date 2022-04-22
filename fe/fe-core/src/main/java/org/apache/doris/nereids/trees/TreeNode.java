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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Abstract class for all node in Nereids, include plan node and expression.
 *
 * @param <NodeType> either {@link org.apache.doris.nereids.trees.plans.Plan}
 *                 or {@link org.apache.doris.nereids.trees.expressions.Expression}
 */
public abstract class TreeNode<NodeType extends TreeNode<NodeType>> {
    protected final org.apache.doris.nereids.trees.NodeType type;
    protected List<NodeType> children = Lists.newArrayList();

    public TreeNode(org.apache.doris.nereids.trees.NodeType type) {
        this.type = type;
    }

    public NodeType getChild(int i) {
        return children.get(i);
    }

    public void addChild(NodeType child) {
        children.add(child);
    }

    public List<NodeType> getChildren() {
        return children;
    }

    public int arity() {
        return children.size();
    }

    public void replaceChild(int index, NodeType child) {
        children.remove(index);
        children.add(index, child);
    }

    public void removeAllChildren() {
        children.clear();
    }

    public void setChildren(List<NodeType> children) {
        this.children = children;
    }
}

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

// Author: https://github.com/davidsusu/tree-printer

package org.apache.doris.common.treeprinter;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractTreeNodeDecorator extends AbstractTreeNode {

    protected final TreeNode decoratedNode;

    protected final boolean decorable;

    protected final boolean inherit;
    
    protected final boolean forceInherit;

    public AbstractTreeNodeDecorator(TreeNode decoratedNode) {
        this(decoratedNode, decoratedNode.isDecorable(), true, false);
    }

    public AbstractTreeNodeDecorator(TreeNode decoratedNode, boolean decorable) {
        this(decoratedNode, decorable, true, false);
    }

    public AbstractTreeNodeDecorator(TreeNode decoratedNode, boolean decorable, boolean inherit) {
        this(decoratedNode, decorable, inherit, false);
    }

    public AbstractTreeNodeDecorator(TreeNode decoratedNode, boolean decorable, boolean inherit, boolean forceInherit) {
        this.decoratedNode = decoratedNode;
        this.decorable = decorable;
        this.inherit = inherit;
        this.forceInherit = forceInherit;
    }
    
    public TreeNode getDecoratedNode() {
        return decoratedNode;
    }

    @Override
    public TreeNode getOriginalNode() {
        return decoratedNode.getOriginalNode();
    }
    
    @Override
    public int[] getInsets() {
        return decoratedNode.getInsets();
    }
    
    @Override
    public boolean isDecorable() {
        return decorable;
    }

    @Override
    public List<TreeNode> getChildren() {
        List<TreeNode> decoratedChildren = new ArrayList<TreeNode>();
        for (TreeNode childNode: decoratedNode.getChildren()) {
            if (childNode == null) {
                decoratedChildren.add(null);
            } else if (inherit && (forceInherit || childNode.isDecorable())) {
                decoratedChildren.add(decorateChild(childNode));
            } else {
                decoratedChildren.add(childNode);
            }
        }
        return decoratedChildren;
    }

    protected abstract TreeNode decorateChild(TreeNode childNode);
}

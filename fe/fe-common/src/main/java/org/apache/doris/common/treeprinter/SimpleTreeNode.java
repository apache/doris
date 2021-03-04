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

public class SimpleTreeNode extends AbstractTreeNode {

    protected final String content;

    protected final int[] insets;

    protected List<TreeNode> children = new ArrayList<TreeNode>();

    public SimpleTreeNode(String content) {
        this(content, 0, 0, 0, 0);
    }

    public SimpleTreeNode(String content, int... insets) {
        this.content = content;
        this.insets = insets.clone();
    }

    public void addChild(TreeNode childNode) {
        children.add(childNode);
    }

    @Override
    public String getContent() {
        return content;
    }

    @Override
    public int[] getInsets() {
        return new int[] {insets[0], insets[1], insets[2], insets[3]};
    }

    @Override
    public List<TreeNode> getChildren() {
        return new ArrayList<TreeNode>(children);
    }

}

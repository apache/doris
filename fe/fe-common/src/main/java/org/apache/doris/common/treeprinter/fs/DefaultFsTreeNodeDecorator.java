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

package org.apache.doris.common.treeprinter.fs;

import org.apache.doris.common.treeprinter.AbstractTreeNodeDecorator;
import org.apache.doris.common.treeprinter.TreeNode;

import java.io.File;
import java.text.DecimalFormat;

public class DefaultFsTreeNodeDecorator extends AbstractTreeNodeDecorator {

    public DefaultFsTreeNodeDecorator(TreeNode decoratedNode) {
        super(decoratedNode);
    }

    public DefaultFsTreeNodeDecorator(TreeNode decoratedNode, boolean decorable) {
        super(decoratedNode, decorable);
    }

    public DefaultFsTreeNodeDecorator(TreeNode decoratedNode, boolean decorable, boolean inherit) {
        super(decoratedNode, decorable, inherit);
    }
    
    public DefaultFsTreeNodeDecorator(TreeNode decoratedNode, boolean decorable, boolean inherit, boolean forceInherit) {
        super(decoratedNode, decorable, inherit, forceInherit);
    }
    
    @Override
    public String getContent() {
        if (decoratedNode instanceof FsTreeNode) {
            FsTreeNode fsNode = (FsTreeNode)decoratedNode;
            File file = fsNode.getFile();
            if (file.isDirectory()) {
                return "(D) " + file.getName();
            } else {
                return file.getName() + " (" + formatFileSize(file.length()) + ")";
            }
        } else {
            return decoratedNode.getContent();
        }
    }
    
    @Override
    protected TreeNode decorateChild(TreeNode childNode) {
        return new DefaultFsTreeNodeDecorator(childNode, decorable, inherit, forceInherit);
    }
    
    protected String formatFileSize(long fileSize) {
        String[] suffixes = new String[]{" KB", " MB", " GB"};
        double floatingSize = fileSize;
        String suffix = " b";
        for (String _suffix: suffixes) {
            if (floatingSize > 850) {
                floatingSize /= 1024;
                suffix = _suffix;
            }
        }
        return new DecimalFormat("#.##").format(floatingSize) + suffix;
    }

}

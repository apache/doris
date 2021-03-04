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

import org.apache.doris.common.treeprinter.AbstractTreeNode;
import org.apache.doris.common.treeprinter.TreeNode;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class FsTreeNode extends AbstractTreeNode {
    
    private final File file;
    
    private final FileFilter filter;
    
    private final Comparator<File> comparator;
    
    private final boolean decorable;

    public static final FileFilter DEFAULT_FILE_FILTER = new FileFilter() {
        
        @Override
        public boolean accept(File file) {
            return !file.isHidden();
        }
        
    };
    
    public static final Comparator<File> DEFAULT_COMPARATOR = new Comparator<File>() {

        @Override
        public int compare(File file1, File file2) {
            if (file1.isDirectory()) {
                if (!file2.isDirectory()) {
                    return -1;
                }
            } else if (file2.isDirectory()) {
                return 1;
            }
            
            return file1.getName().compareToIgnoreCase(file2.getName());
        }
        
    };
    
    public FsTreeNode() {
        this(new File("."));
    }

    public FsTreeNode(File file) {
        this(file, DEFAULT_FILE_FILTER, DEFAULT_COMPARATOR, true);
    }

    public FsTreeNode(File file, FileFilter filter, Comparator<File> comparator, boolean decorable) {
        this.file = file;
        this.filter = filter;
        this.comparator = comparator;
        this.decorable = decorable;
    }
    
    public File getFile() {
        return file;
    }
    
    @Override
    public String getContent() {
        return file.getName();
    }

    @Override
    public List<TreeNode> getChildren() {
        List<TreeNode> childNodes = new ArrayList<TreeNode>();
        File[] subFileArray = file.listFiles(filter);
        if (subFileArray != null && subFileArray.length > 0) {
            List<File> subFiles = new ArrayList<File>(Arrays.asList(subFileArray));
            Collections.sort(subFiles, comparator);
            for (File subFile: subFiles) {
                childNodes.add(new FsTreeNode(subFile, filter, comparator, decorable));
            }
        }
        return childNodes;
    }

    @Override
    public boolean isDecorable() {
        return decorable;
    }

}

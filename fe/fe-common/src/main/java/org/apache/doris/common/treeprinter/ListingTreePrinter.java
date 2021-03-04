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

import java.util.Collections;
import java.util.List;

public class ListingTreePrinter extends AbstractTreePrinter {

    public static final String[] LINE_STRINGS_ASCII = new String[]{
            "   ", " | ", " |-", " '-", "---"
    };

    public static final String[] LINE_STRINGS_UNICODE = new String[] {
        "   ", " │ ", " ├─", " └─", "───"
    };
    
    private static final int NODE_ROOT = 0;
    private static final int NODE_GENERAL = 1;
    private static final int NODE_LAST = 2;

    private final String liningSpace;
    private final String liningGeneral;
    private final String liningNode;
    private final String liningLastNode;
    private final String liningInset;
    private final boolean displayRoot;
    private final boolean align;

    public ListingTreePrinter() {
        this(true, false);
    }

    public ListingTreePrinter(boolean useUnicode) {
        this(useUnicode, true, false);
    }

    public ListingTreePrinter(boolean displayRoot, boolean align) {
        this(UnicodeMode.isUnicodeDefault(), displayRoot, align);
    }

    public ListingTreePrinter(boolean useUnicode, boolean displayRoot, boolean align) {
        this(
            (useUnicode ? LINE_STRINGS_UNICODE : LINE_STRINGS_ASCII)[0],
            (useUnicode ? LINE_STRINGS_UNICODE : LINE_STRINGS_ASCII)[1],
            (useUnicode ? LINE_STRINGS_UNICODE : LINE_STRINGS_ASCII)[2],
            (useUnicode ? LINE_STRINGS_UNICODE : LINE_STRINGS_ASCII)[3],
            (useUnicode ? LINE_STRINGS_UNICODE : LINE_STRINGS_ASCII)[4],
            displayRoot, align
        );
    }
    
    public ListingTreePrinter(
        String liningSpace, String liningGeneral, String liningNode, String liningLastNode, String liningInset,
        boolean displayRoot, boolean align
    ) {
        this.liningSpace = liningSpace;
        this.liningGeneral = liningGeneral;
        this.liningNode = liningNode;
        this.liningLastNode = liningLastNode;
        this.liningInset = liningInset;
        this.displayRoot = displayRoot;
        this.align = align;
    }

    @Override
    public void print(TreeNode rootNode, Appendable out) {
        printSub(rootNode, out, "", NODE_ROOT, align ? Util.getDepth(rootNode) : 0);
    }
    
    private void printSub(TreeNode node, Appendable out, String prefix, int type, int inset) {
        String content = node.getContent();
        int connectOffset = node.getInsets()[0];
        
        String[] lines = content.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (type == NODE_ROOT) {
                if (displayRoot) {
                    writeln(out, prefix + line);
                }
            } else {
                String itemPrefix;
                if (i < connectOffset) {
                    itemPrefix = liningGeneral;
                } else if (i == connectOffset) {
                    itemPrefix = (type == NODE_LAST) ? liningLastNode : liningNode;
                } else {
                    itemPrefix = (type == NODE_LAST) ? liningSpace : liningGeneral;
                }
                if (inset > 0) {
                    String insetString = (i == connectOffset) ? liningInset : liningSpace;
                    StringBuilder insetBuilder = new StringBuilder();
                    for (int j = 0; j < inset; j++) {
                        insetBuilder.append(insetString);
                    }
                    itemPrefix += insetBuilder.toString();
                }
                writeln(out, prefix + itemPrefix + line);
            }
        }
        
        List<TreeNode> childNodes = node.getChildren();
        childNodes.removeAll(Collections.singleton(null));
        int childNodeCount = childNodes.size();
        for (int i = 0; i < childNodeCount; i++) {
            TreeNode childNode = childNodes.get(i);
            boolean childIsLast = (i == childNodeCount - 1);
            String lining = type == NODE_LAST ? liningSpace : liningGeneral;
            String subPrefix = type == NODE_ROOT ? prefix : prefix + lining;
            int subInset = Math.max(0, inset - 1);
            printSub(childNode, out, subPrefix, childIsLast ? NODE_LAST : NODE_GENERAL, subInset);
        }
    }
    
    public static Builder createBuilder() {
        return new Builder();
    }
    
    public static class Builder {

        private boolean displayRoot = true;
        private boolean align = false;
        
        private String[] lines = (
            UnicodeMode.isUnicodeDefault() ?
            LINE_STRINGS_UNICODE :
            LINE_STRINGS_ASCII
        ).clone();

        public Builder displayRoot(boolean displayRoot) {
            this.displayRoot = displayRoot;
            return this;
        }

        public Builder align(boolean align) {
            this.align = align;
            return this;
        }
        
        public Builder ascii() {
            this.lines = LINE_STRINGS_ASCII.clone();
            return this;
        }
        
        public Builder unicode() {
            this.lines = LINE_STRINGS_UNICODE.clone();
            return this;
        }

        public Builder lining(String space, String general, String node, String lastNode, String inset) {
            this.lines = new String[] {space, general, node, lastNode, inset};
            return this;
        }

        public Builder liningSpace(String liningSpace) {
            this.lines[0] = liningSpace;
            return this;
        }

        public Builder liningGeneral(String liningGeneral) {
            this.lines[1] = liningGeneral;
            return this;
        }

        public Builder liningNode(String liningNode) {
            this.lines[2] = liningNode;
            return this;
        }

        public Builder liningLastNode(String liningLastNode) {
            this.lines[3] = liningLastNode;
            return this;
        }

        public Builder liningInset(String liningInset) {
            this.lines[4] = liningInset;
            return this;
        }

        public ListingTreePrinter build() {
            return new ListingTreePrinter(
                lines[0], lines[1], lines[2], lines[3], lines[4],
                displayRoot, align
            );
        }
        
    }
    
}

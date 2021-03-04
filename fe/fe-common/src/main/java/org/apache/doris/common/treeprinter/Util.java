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

public final class Util {

    private Util() {
        // utility class
    }

    public static int[] getContentDimension(String content) {
        int longsetLineLength = 0;
        String[] lines = content.split("\n");
        for (String line: lines) {
            int lineLength = line.length();
            if (lineLength > longsetLineLength) {
                longsetLineLength = lineLength;
            }
        }
        return new int[] {longsetLineLength, lines.length};
    }
    
    public static int getDepth(TreeNode treeNode) {
        List<TreeNode> levelNodes = new ArrayList<TreeNode>();
        levelNodes.add(treeNode);
        int depth = 0;
        while (true) {
            List<TreeNode> newLevelNodes = new ArrayList<TreeNode>();
            for (TreeNode levelNode: levelNodes) {
                for (TreeNode childNode: levelNode.getChildren()) {
                    if (childNode != null) {
                        newLevelNodes.add(childNode);
                    }
                }
            }
            if (newLevelNodes.isEmpty()) {
                break;
            }
            levelNodes = newLevelNodes;
            depth++;
        }
        return depth;
    }

    public static String repeat(char character, int repeats) {
        StringBuilder resultBuilder = new StringBuilder();
        repeat(resultBuilder, character, repeats);
        return resultBuilder.toString();
    }
    
    public static void repeat(StringBuilder stringBuilder, char character, int repeats) {
        for (int i = 0; i < repeats; i ++) {
            stringBuilder.append(character);
        }
    }

}

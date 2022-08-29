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

package org.apache.doris.common.profile;

import hu.webarticum.treeprinter.BorderTreeNodeDecorator;
import hu.webarticum.treeprinter.SimpleTreeNode;
import hu.webarticum.treeprinter.TraditionalTreePrinter;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ProfileTreePrinter {

    public enum PrintLevel {
        FRAGMENT, INSTANCE
    }

    // Fragment tree only print the entire query plan tree with node name
    // and some other brief info.
    public static String printFragmentTree(ProfileTreeNode root) {
        SimpleTreeNode rootNode = buildNode(root, PrintLevel.FRAGMENT);
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        new TraditionalTreePrinter().print(new BorderTreeNodeDecorator(rootNode), sb);
        return sb.toString();
    }

    // Instance tree will print the details of the tree of a single instance
    public static String printInstanceTree(ProfileTreeNode root) {
        SimpleTreeNode rootNode = buildNode(root, PrintLevel.INSTANCE);
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        new TraditionalTreePrinter().print(new BorderTreeNodeDecorator(rootNode), sb);
        return sb.toString();
    }

    private static SimpleTreeNode buildNode(ProfileTreeNode profileNode, PrintLevel level) {
        SimpleTreeNode node = new SimpleTreeNode(profileNode.debugString(0, level));
        for (ProfileTreeNode child : profileNode.getChildren()) {
            node.addChild(buildNode(child, level));
        }
        return node;
    }

    public static JSONObject printFragmentTreeInJson(ProfileTreeNode root, ProfileTreePrinter.PrintLevel level) {
        JSONObject object = new JSONObject();
        JSONArray jsonNodes = new JSONArray();
        JSONArray edges = new JSONArray();
        object.put("nodes", jsonNodes);
        object.put("edges", edges);
        buildNodeInJson(root, level, "", "", jsonNodes, edges);
        return object;
    }

    private static void buildNodeInJson(ProfileTreeNode profileNode, PrintLevel level, String sourceNodeId,
            String targetNodeId, JSONArray jsonNodes, JSONArray edges) {
        boolean isFrist = false;
        if (StringUtils.isBlank(sourceNodeId)) {
            isFrist = true;
            targetNodeId = "1";
        }
        jsonNodes.add(profileNode.debugStringInJson(level, targetNodeId));
        int i = 0;
        for (ProfileTreeNode child : profileNode.getChildren()) {
            buildNodeInJson(child, level, targetNodeId, targetNodeId + i++, jsonNodes, edges);
        }
        if (!isFrist) {
            JSONObject edge = new JSONObject();
            edge.put("id", "e" + targetNodeId);
            edge.put("source", sourceNodeId);
            edge.put("target", targetNodeId);
            edges.add(edge);
        }
    }
}

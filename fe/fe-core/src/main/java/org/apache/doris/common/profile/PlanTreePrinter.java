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

public class PlanTreePrinter {

    private static final String DELIMITER = "||";

    public static String printPlanExplanation(PlanTreeNode root) {
        SimpleTreeNode rootNode = buildNode(root);
        StringBuilder sb = new StringBuilder();
        new TraditionalTreePrinter().print(new BorderTreeNodeDecorator(rootNode), sb);
        return sb.toString();
    }

    private static SimpleTreeNode buildNode(PlanTreeNode planNode) {
        SimpleTreeNode node = new SimpleTreeNode(planNode.getExplainStr());
        for (PlanTreeNode child : planNode.getChildren()) {
            node.addChild(buildNode(child));
        }
        return node;
    }

    public static String printPlanTree(PlanTreeNode root) {
        return buildTree(root, "");
    }

    private static String buildTree(PlanTreeNode planNode, String prefix) {
        StringBuilder builder = new StringBuilder();
        builder.append(prefix).append(planNode.getIds()).append(":")
                .append(planNode.getExplainStr().replaceAll("\n", DELIMITER)).append("\n");
        String childPrefix = prefix + "--";
        planNode.getChildren().forEach(
                child -> {
                    builder.append(buildTree(child, childPrefix));
                }
        );
        return builder.toString();
    }
}

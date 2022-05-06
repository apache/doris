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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.Objects;

/**
 * Pattern node used in pattern matching.
 */
public class Pattern extends TreeNode<Pattern> {
    public static final Pattern PATTERN_MULTI_LEAF_INSTANCE = new Pattern(NodeType.PATTERN_MULTI_LEAF);
    public static final Pattern PATTERN_LEAF_INSTANCE = new Pattern(NodeType.PATTERN_LEAF);

    private final NodeType nodeType;

    /**
     * Constructor for Pattern.
     *
     * @param nodeType node type to matching
     * @param children sub pattern
     */
    public Pattern(NodeType nodeType, Pattern... children) {
        super(NodeType.PATTERN);
        this.nodeType = nodeType;
        for (Pattern child : children) {
            addChild(child);
        }
    }

    /**
     * get current type in Pattern.
     *
     * @return node type in pattern
     */
    public NodeType getNodeType() {
        return nodeType;
    }

    /**
     * Return ture if current Pattern match Plan in params.
     *
     * @param plan wait to match
     * @return ture if current Pattern match Plan in params
     */
    public boolean matchRoot(Plan<?> plan) {
        if (plan == null) {
            return false;
        }

        if (plan.getChildren().size() < this.getChildren().size() && children.contains(PATTERN_MULTI_LEAF_INSTANCE)) {
            return false;
        }

        if (nodeType == NodeType.PATTERN_MULTI_LEAF || nodeType == NodeType.PATTERN_LEAF) {
            return true;
        }

        return getNodeType().equals(plan.getType());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pattern pattern = (Pattern) o;
        return nodeType == pattern.nodeType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeType);
    }
}

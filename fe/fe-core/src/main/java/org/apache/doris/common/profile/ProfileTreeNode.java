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

import org.apache.doris.common.TreeNode;

import com.google.common.base.Strings;

public class ProfileTreeNode extends TreeNode<ProfileTreeNode> {

    protected String name;
    protected String id;
    protected CounterNode counterNode;
    protected String activeTime;
    protected String nonChild;

    protected String fragmentId = "";
    protected String instanceId = "";

    // This is used to record the max activeTime of all instances in a fragment.
    // Usually recorded on the Sender node.
    protected String maxInstanceActiveTime = "";

    protected ProfileTreeNode parentNode;

    protected ProfileTreeNode(String name, String id) {
        this.name = name;
        this.id = id;
    }

    public void setParentNode(ProfileTreeNode parentNode) {
        this.parentNode = parentNode;
    }

    public ProfileTreeNode getParentNode() {
        return parentNode;
    }

    public void setCounterNode(CounterNode counterNode) {
        this.counterNode = counterNode;
    }

    public CounterNode getCounterNode() {
        return counterNode;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setActiveTime(String activeTime) {
        this.activeTime = activeTime;
    }

    public String getActiveTime() {
        return activeTime;
    }

    public void setNonChild(String nonChild) {
        this.nonChild = nonChild;
    }

    public String getNonChild() {
        return nonChild;
    }

    public String getIdentity() {
        if (id.equals(ProfileTreeBuilder.UNKNOWN_ID)) {
            return "[" + name + "]";
        }
        return "[" + id + ": " + name + "]";
    }

    public void setFragmentAndInstanceId(String fragmentId, String instanceId) {
        this.fragmentId = fragmentId;
        this.instanceId = instanceId;
    }

    public void setMaxInstanceActiveTime(String maxInstanceActiveTime) {
        this.maxInstanceActiveTime = maxInstanceActiveTime;
    }

    public String getMaxInstanceActiveTime() {
        return maxInstanceActiveTime;
    }

    public String debugTree(int indent, ProfileTreePrinter.PrintLevel level) {
        StringBuilder sb = new StringBuilder(printIndent(indent));
        sb.append(debugString(indent, level));
        if (!getChildren().isEmpty()) {
            int childSize = getChildren().size();
            for (int i = 0; i < childSize; i++) {
                ProfileTreeNode node = getChild(i);
                sb.append("\n").append(node.debugTree(indent + 4, level));
            }
        }
        return sb.toString();
    }

    public String debugString(int indent, ProfileTreePrinter.PrintLevel level) {
        String indentStr = printIndent(indent);
        StringBuilder sb = new StringBuilder();
        sb.append(indentStr).append(getIdentity()).append("\n");
        if (level == ProfileTreePrinter.PrintLevel.FRAGMENT) {
            sb.append(indentStr).append("Fragment: ").append(fragmentId).append("\n");
            if (!Strings.isNullOrEmpty(maxInstanceActiveTime)) {
                sb.append(indentStr).append("MaxActiveTime: ").append(maxInstanceActiveTime).append("\n");
            }
        }
        if (level == ProfileTreePrinter.PrintLevel.INSTANCE) {
            sb.append("(Active: ").append(activeTime).append(", ");
            sb.append("non-child: ").append(nonChild).append(")").append("\n");
            // print counters
            sb.append(counterNode.toTree(indent + 1));
        }
        return sb.toString();
    }

    private String printIndent(int indent) {
        String res = "";
        for (int i = 0; i < indent; i++) {
            res += " ";
        }
        return res;
    }
}

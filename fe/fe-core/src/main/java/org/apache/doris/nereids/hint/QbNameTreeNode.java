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

package org.apache.doris.nereids.hint;

import org.apache.doris.nereids.util.TreeStringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * QbNameTreeNode
 */
public class QbNameTreeNode {
    private Optional<String> qbName;
    private List<QbNameTreeNode> children = new ArrayList<>();

    private QbNameTreeNode(Optional<String> qbName) {
        this.qbName = qbName;
    }

    /**
     * addChildQbName
     */
    public QbNameTreeNode addChildQbName(Optional<String> qbName) {
        QbNameTreeNode child = findQbNameNode(this, qbName);
        if (child == null) {
            child = new QbNameTreeNode(qbName);
            children.add(child);
        }
        return child;
    }

    public String treeString() {
        return TreeStringUtils.treeString(this,
                plan -> plan.toString(),
                plan -> ((QbNameTreeNode) plan).getTempChildren(),
                plan -> new ArrayList<>(),
                plan -> false);
    }

    public int getChildCount() {
        return children.size();
    }

    public List<QbNameTreeNode> getChildSubList(int startIndex, int endIndex) {
        return children.subList(startIndex, endIndex);
    }

    List<Object> getTempChildren() {
        children.sort(new QbNameTreeNodeComparator());
        List<Object> tempChild = new ArrayList<>(children.size());
        tempChild.addAll(children);
        return tempChild;
    }

    public String toString() {
        return qbName.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QbNameTreeNode that = (QbNameTreeNode) o;
        return qbName.equals(that.qbName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qbName);
    }

    /**
     * findQbNameNode
     */
    public static QbNameTreeNode findQbNameNode(QbNameTreeNode root, Optional<String> qbName) {
        QbNameTreeNode node;
        for (QbNameTreeNode child : root.children) {
            node = findQbNameNode(child, qbName);
            if (node != null) {
                return node;
            }
        }
        if (root.qbName.equals(qbName)) {
            return root;
        } else {
            return null;
        }
    }

    public static QbNameTreeNode createQbNameNode(Optional<String> qbName) {
        return new QbNameTreeNode(qbName);
    }

    public static void collectAllNodes(QbNameTreeNode root, List<QbNameTreeNode> result) {
        result.add(root);
        for (QbNameTreeNode child : root.children) {
            collectAllNodes(child, result);
        }
    }

    /**
     * QbNameTreeNodeComparator
     */
    public static class QbNameTreeNodeComparator implements Comparator<QbNameTreeNode> {
        @Override
        public int compare(QbNameTreeNode o1, QbNameTreeNode o2) {
            return o1.qbName.get().compareTo(o2.qbName.get());
        }
    }
}

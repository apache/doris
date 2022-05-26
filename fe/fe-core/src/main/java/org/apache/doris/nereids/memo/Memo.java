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

package org.apache.doris.nereids.memo;

import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Representation for memo in cascades optimizer.
 */
public class Memo<NODE_TYPE extends TreeNode> {
    private final List<Group> groups = Lists.newArrayList();
    // we could not use Set, because Set has no get method.
    private final Map<GroupExpression, GroupExpression> groupExpressions = Maps.newHashMap();
    private Group root;

    public void initialize(NODE_TYPE node) {
        root = copyIn(node, null, false).getParent();
    }

    public Group getRoot() {
        return root;
    }

    /**
     * Add node to Memo.
     *
     * @param node {@link Plan} or {@link Expression} to be added
     * @param target target group to add node. null to generate new Group
     * @param rewrite whether to rewrite the node to the target group
     * @return Reference of node in Memo
     */
    public GroupExpression copyIn(NODE_TYPE node, Group target, boolean rewrite) {
        List<Group> childrenGroups = Lists.newArrayList();
        for (Object object : node.children()) {
            NODE_TYPE child = (NODE_TYPE) object;
            childrenGroups.add(copyIn(child, null, rewrite).getParent());
        }
        if (node.getGroupExpression() != null && groupExpressions.containsKey(node.getGroupExpression())) {
            return node.getGroupExpression();
        }
        GroupExpression newGroupExpression = new GroupExpression(node.getOperator());
        newGroupExpression.setChildren(childrenGroups);
        return insertOrRewriteGroupExpression(newGroupExpression, target, rewrite);
        // TODO: need to derive logical property if generate new group. current we ont copy logical plan into
    }

    private GroupExpression insertOrRewriteGroupExpression(
            GroupExpression groupExpression, Group target, boolean rewrite) {
        GroupExpression existedGroupExpression = groupExpressions.get(groupExpression);
        if (existedGroupExpression != null) {
            if (target != null && !target.getGroupId().equals(existedGroupExpression.getParent().getGroupId())) {
                // TODO: merge group
            }
            return existedGroupExpression;
        }
        if (target != null) {
            if (rewrite) {
                GroupExpression oldExpression = target.rewriteLogicalExpression(groupExpression);
                groupExpressions.remove(oldExpression);
            } else {
                target.addGroupExpression(groupExpression);
            }
        } else {
            Group group = new Group(groupExpression);
            Preconditions.checkArgument(!groups.contains(group), "new group with already exist output");
            groups.add(group);
        }
        groupExpressions.put(groupExpression, groupExpression);
        return groupExpression;
    }

}

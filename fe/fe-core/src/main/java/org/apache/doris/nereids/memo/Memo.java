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

import org.apache.doris.nereids.properties.LogicalProperties;
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
public class Memo {
    private final List<Group> groups = Lists.newArrayList();
    // we could not use Set, because Set has no get method.
    private final Map<GroupExpression, GroupExpression> groupExpressions = Maps.newHashMap();
    private Group root;

    public void initialize(Plan node) {
        root = copyIn(node, null, false).getParent();
    }

    public Group getRoot() {
        return root;
    }

    /**
     * Add node to Memo.
     * TODO: add ut later
     *
     * @param node {@link Plan} or {@link Expression} to be added
     * @param target target group to add node. null to generate new Group
     * @param rewrite whether to rewrite the node to the target group
     * @return Reference of node in Memo
     */
    public GroupExpression copyIn(Plan node, Group target, boolean rewrite) {
        Preconditions.checkArgument(!rewrite || target != null);
        List<Group> childrenGroups = Lists.newArrayList();
        for (Plan child : node.children()) {
            childrenGroups.add(copyIn(child, null, rewrite).getParent());
        }
        if (node.getGroupExpression().isPresent() && groupExpressions.containsKey(node.getGroupExpression().get())) {
            return node.getGroupExpression().get();
        }
        GroupExpression newGroupExpression = new GroupExpression(node.getOperator());
        newGroupExpression.setChildren(childrenGroups);
        return insertOrRewriteGroupExpression(newGroupExpression, target, rewrite, node.getLogicalProperties());
        // TODO: need to derive logical property if generate new group. currently we not copy logical plan into
    }

    /**
     * Insert or rewrite groupExpression to target group.
     * If group expression is already in memo and target group is not null, we merge two groups.
     * If target is null, generate new group.
     * If rewrite is true, rewrite the groupExpression to target group.
     *
     * @param groupExpression groupExpression to insert
     * @param target target group to insert or rewrite groupExpression
     * @param rewrite whether to rewrite the groupExpression to target group
     * @return existing groupExpression in memo or newly generated groupExpression
     */
    private GroupExpression insertOrRewriteGroupExpression(GroupExpression groupExpression, Group target,
                                                           boolean rewrite, LogicalProperties logicalProperties) {
        GroupExpression existedGroupExpression = groupExpressions.get(groupExpression);
        if (existedGroupExpression != null) {
            if (target != null && !target.getGroupId().equals(existedGroupExpression.getParent().getGroupId())) {
                mergeGroup(target, existedGroupExpression.getParent());
            }
            return existedGroupExpression;
        }
        if (target != null) {
            if (rewrite) {
                GroupExpression oldExpression = target.rewriteLogicalExpression(groupExpression, logicalProperties);
                groupExpressions.remove(oldExpression);
            } else {
                target.addGroupExpression(groupExpression);
            }
        } else {
            Group group = new Group(groupExpression, logicalProperties);
            Preconditions.checkArgument(!groups.contains(group), "new group with already exist output");
            groups.add(group);
        }
        groupExpressions.put(groupExpression, groupExpression);
        return groupExpression;
    }

    /**
     * Merge two groups.
     * 1. find all group expression which has source as child
     * 2. replace its child with destination
     * 3. remove redundant group expression after replace child
     * 4. move all group expression in source to destination
     *
     * @param source source group
     * @param destination destination group
     */
    private void mergeGroup(Group source, Group destination) {
        if (source.equals(destination)) {
            return;
        }
        List<GroupExpression> needReplaceChild = Lists.newArrayList();
        for (GroupExpression groupExpression : groupExpressions.values()) {
            if (groupExpression.children().contains(source)) {
                if (groupExpression.getParent().equals(destination)) {
                    // cycle, we should not merge
                    return;
                }
                needReplaceChild.add(groupExpression);
            }
        }
        for (GroupExpression groupExpression : needReplaceChild) {
            groupExpressions.remove(groupExpression);
            List<Group> children = groupExpression.children();
            // TODO: use a better way to replace child, avoid traversing all groupExpression
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i).equals(source)) {
                    children.set(i, destination);
                }
            }
            if (groupExpressions.containsKey(groupExpression)) {
                // TODO: need to merge group recursively
                groupExpression.getParent().removeGroupExpression(groupExpression);
            } else {
                groupExpressions.put(groupExpression, groupExpression);
            }
        }
        for (GroupExpression groupExpression : source.getLogicalExpressions()) {
            source.removeGroupExpression(groupExpression);
            destination.addGroupExpression(groupExpression);
        }
        for (GroupExpression groupExpression : source.getPhysicalExpressions()) {
            source.removeGroupExpression(groupExpression);
            destination.addGroupExpression(groupExpression);
        }
        groups.remove(source);
    }
}

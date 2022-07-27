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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Representation for memo in cascades optimizer.
 */
public class Memo {
    // generate group id in memo is better for test, since we can reproduce exactly same Memo.
    private final IdGenerator<GroupId> groupIdGenerator = GroupId.createGenerator();
    private final List<Group> groups = Lists.newArrayList();
    // we could not use Set, because Set does not have get method.
    private final Map<GroupExpression, GroupExpression> groupExpressions = Maps.newHashMap();
    private Group root;

    public Memo(Plan plan) {
        root = copyIn(plan, null, false).second.getOwnerGroup();
    }

    public Group getRoot() {
        return root;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public Map<GroupExpression, GroupExpression> getGroupExpressions() {
        return groupExpressions;
    }

    /**
     * Add plan to Memo.
     * TODO: add ut later
     *
     * @param node {@link Plan} or {@link Expression} to be added
     * @param target target group to add node. null to generate new Group
     * @param rewrite whether to rewrite the node to the target group
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    public Pair<Boolean, GroupExpression> copyIn(Plan node, @Nullable Group target, boolean rewrite) {
        Optional<GroupExpression> groupExpr = node.getGroupExpression();
        if (!rewrite && groupExpr.isPresent() && groupExpressions.containsKey(groupExpr.get())) {
            return new Pair(false, groupExpr.get());
        }
        List<Group> childrenGroups = Lists.newArrayList();
        for (int i = 0; i < node.children().size(); i++) {
            Plan child = node.children().get(i);
            if (child instanceof GroupPlan) {
                childrenGroups.add(((GroupPlan) child).getGroup());
            } else if (child.getGroupExpression().isPresent()) {
                childrenGroups.add(child.getGroupExpression().get().getOwnerGroup());
            } else {
                childrenGroups.add(copyIn(child, null, rewrite).second.getOwnerGroup());
            }
        }
        node = replaceChildrenToGroupPlan(node, childrenGroups);
        GroupExpression newGroupExpression = new GroupExpression(node);
        newGroupExpression.setChildren(childrenGroups);
        return insertOrRewriteGroupExpression(newGroupExpression, target, rewrite,
                node.getLogicalProperties());
        // TODO: need to derive logical property if generate new group. currently we not copy logical plan into
    }

    public Plan copyOut() {
        return groupToTreeNode(root);
    }

    /**
     * Utility function to create a new {@link PlannerContext} with this Memo.
     */
    public PlannerContext newPlannerContext(ConnectContext connectContext) {
        return new PlannerContext(this, connectContext);
    }

    private Plan groupToTreeNode(Group group) {
        GroupExpression logicalExpression = group.getLogicalExpression();
        List<Plan> childrenNode = Lists.newArrayList();
        for (Group child : logicalExpression.children()) {
            childrenNode.add(groupToTreeNode(child));
        }
        Plan result = logicalExpression.getPlan();
        if (result.children().size() == 0) {
            return result;
        }
        return result.withChildren(childrenNode);
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
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    private Pair<Boolean, GroupExpression> insertOrRewriteGroupExpression(GroupExpression groupExpression, Group target,
            boolean rewrite, LogicalProperties logicalProperties) {
        GroupExpression existedGroupExpression = groupExpressions.get(groupExpression);
        if (existedGroupExpression != null) {
            Group mergedGroup = existedGroupExpression.getOwnerGroup();
            if (target != null && !target.getGroupId().equals(existedGroupExpression.getOwnerGroup().getGroupId())) {
                mergedGroup = mergeGroup(target, existedGroupExpression.getOwnerGroup());
            }
            if (rewrite) {
                mergedGroup.setLogicalProperties(logicalProperties);
            }
            return new Pair(false, existedGroupExpression);
        }
        if (target != null) {
            if (rewrite) {
                GroupExpression oldExpression = target.rewriteLogicalExpression(groupExpression, logicalProperties);
                groupExpressions.remove(oldExpression);
            } else {
                target.addGroupExpression(groupExpression);
            }
        } else {
            Group group = new Group(groupIdGenerator.getNextId(), groupExpression, logicalProperties);
            Preconditions.checkArgument(!groups.contains(group), "new group with already exist output");
            groups.add(group);
        }
        groupExpressions.put(groupExpression, groupExpression);
        return new Pair(true, groupExpression);
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
     * @return merged group
     */
    private Group mergeGroup(Group source, Group destination) {
        if (source.equals(destination)) {
            return source;
        }
        List<GroupExpression> needReplaceChild = Lists.newArrayList();
        groupExpressions.values().forEach(groupExpression -> {
            if (groupExpression.children().contains(source)) {
                if (groupExpression.getOwnerGroup().equals(destination)) {
                    // cycle, we should not merge
                    return;
                }
                needReplaceChild.add(groupExpression);
            }
        });
        for (GroupExpression groupExpression : needReplaceChild) {
            groupExpressions.remove(groupExpression);
            List<Group> children = groupExpression.children();
            // TODO: use a better way to replace child, avoid traversing all groupExpression
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i).equals(source)) {
                    children.set(i, destination);
                }
            }
            GroupExpression that = groupExpressions.get(groupExpression);
            if (that != null && that.getOwnerGroup() != null
                    && !that.getOwnerGroup().equals(groupExpression.getOwnerGroup())) {
                // remove groupExpression from its owner group to avoid adding it to that.getOwnerGroup()
                // that.getOwnerGroup() already has this groupExpression.
                Group ownerGroup = groupExpression.getOwnerGroup();
                groupExpression.getOwnerGroup().removeGroupExpression(groupExpression);
                mergeGroup(ownerGroup, that.getOwnerGroup());
            } else {
                groupExpressions.put(groupExpression, groupExpression);
            }
        }
        if (!source.equals(destination)) {
            source.moveLogicalExpressionOwnership(destination);
            source.movePhysicalExpressionOwnership(destination);
            groups.remove(source);
        }
        return destination;
    }

    /**
     * Add enforcer expression into the target group.
     */
    public void addEnforcerPlan(GroupExpression groupExpression, Group group) {
        groupExpression.setOwnerGroup(group);
    }

    private Plan replaceChildrenToGroupPlan(Plan plan, List<Group> childrenGroups) {
        List<Plan> groupPlanChildren = childrenGroups.stream()
                .map(group -> new GroupPlan(group))
                .collect(ImmutableList.toImmutableList());
        LogicalProperties logicalProperties = plan.getLogicalProperties();
        return plan.withChildren(groupPlanChildren)
            .withLogicalProperties(Optional.of(logicalProperties));
    }
}

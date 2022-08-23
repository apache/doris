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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;

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
            return Pair.of(false, groupExpr.get());
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
        if (rewrite) {
            return rewriteGroupExpression(newGroupExpression, target, node.getLogicalProperties());
        } else {
            return insertGroupExpression(newGroupExpression, target, node.getLogicalProperties());
        }
        // TODO: need to derive logical property if generate new group. currently we not copy logical plan into
    }

    public Plan copyOut() {
        return copyOut(root);
    }

    /**
     * copyOut the group.
     * @param group the group what want to copyOut
     * @return plan
     */
    public Plan copyOut(Group group) {
        GroupExpression logicalExpression = group.getLogicalExpression();
        List<Plan> childrenNode = Lists.newArrayList();
        for (Group child : logicalExpression.children()) {
            childrenNode.add(copyOut(child));
        }
        Plan result = logicalExpression.getPlan();
        if (result.children().size() == 0) {
            return result;
        }
        return result.withChildren(childrenNode);
    }

    /**
     * Utility function to create a new {@link CascadesContext} with this Memo.
     */
    public CascadesContext newCascadesContext(StatementContext statementContext) {
        return new CascadesContext(this, statementContext);
    }

    /**
     * Insert groupExpression to target group.
     * If group expression is already in memo and target group is not null, we merge two groups.
     * If target is null, generate new group.
     * If target is not null, add group expression to target group
     *
     * @param groupExpression groupExpression to insert
     * @param target target group to insert groupExpression
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    private Pair<Boolean, GroupExpression> insertGroupExpression(
            GroupExpression groupExpression, Group target, LogicalProperties logicalProperties) {
        GroupExpression existedGroupExpression = groupExpressions.get(groupExpression);
        if (existedGroupExpression != null) {
            if (target != null && !target.getGroupId().equals(existedGroupExpression.getOwnerGroup().getGroupId())) {
                mergeGroup(existedGroupExpression.getOwnerGroup(), target);
            }
            return Pair.of(false, existedGroupExpression);
        }
        if (target != null) {
            target.addGroupExpression(groupExpression);
        } else {
            Group group = new Group(groupIdGenerator.getNextId(), groupExpression, logicalProperties);
            groups.add(group);
        }
        groupExpressions.put(groupExpression, groupExpression);
        return Pair.of(true, groupExpression);
    }

    /**
     * Rewrite groupExpression to target group.
     * If group expression is already in memo, we replace logical properties regardless the target group present or not
     *     for replace UnboundLogicalProperties to LogicalProperties
     * If target is null, generate new group.
     * If target is not null, rewrite the groupExpression to target group.
     *
     * @param groupExpression groupExpression to rewrite old one
     * @param target target group to rewrite groupExpression
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    private Pair<Boolean, GroupExpression> rewriteGroupExpression(
            GroupExpression groupExpression, Group target, LogicalProperties logicalProperties) {
        boolean newGroupExpressionGenerated = true;
        GroupExpression existedGroupExpression = groupExpressions.get(groupExpression);
        /*
         * here we need to handle one situation that original target is not the same with
         * existedGroupExpression.getOwnerGroup(). In this case, if we change target to
         * existedGroupExpression.getOwnerGroup(), we could not rewrite plan as we expected and the plan
         * will not be changed anymore.
         * Think below example:
         * We have a plan like this:
         * Original (Group 2 is root):
         * Group2: Project(outside)
         * Group1: |---Project(inside)
         * Group0:     |---UnboundRelation
         *
         * and we want to rewrite group 2 by Project(inside, GroupPlan(group 0))
         *
         * After rewriting we should get (Group 2 is root):
         * Group2: Project(inside)
         * Group0: |---UnboundRelation
         *
         * Group1: Project(inside)
         *
         * After rewriting, Group 1's GroupExpression is not in GroupExpressionsMap anymore and Group 1 is unreachable.
         * Merge Group 1 into Group 2 is better, but in consideration of there is others way to let a Group take into
         * unreachable. There's no need to complicate to add a merge step. Instead, we need to have a clear step to
         * remove unreachable groups and GroupExpressions after rewrite.
         * TODO: add a clear groups function to memo.
         */
        if (existedGroupExpression != null
                && (target == null || target.equals(existedGroupExpression.getOwnerGroup()))) {
            target = existedGroupExpression.getOwnerGroup();
            newGroupExpressionGenerated = false;
        }
        if (target != null) {
            GroupExpression oldExpression = target.rewriteLogicalExpression(groupExpression, logicalProperties);
            groupExpressions.remove(oldExpression);
        } else {
            Group group = new Group(groupIdGenerator.getNextId(), groupExpression, logicalProperties);
            groups.add(group);
        }
        groupExpressions.put(groupExpression, groupExpression);
        return Pair.of(newGroupExpressionGenerated, groupExpression);
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
                .map(GroupPlan::new)
                .collect(ImmutableList.toImmutableList());
        LogicalProperties logicalProperties = plan.getLogicalProperties();
        return plan.withChildren(groupPlanChildren)
            .withLogicalProperties(Optional.of(logicalProperties));
    }
}

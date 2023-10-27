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
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.event.GroupMergeEvent;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestPropertyDeriver;
import org.apache.doris.nereids.properties.RequirePropertiesSupplier;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Representation for memo in cascades optimizer.
 */
public class Memo {
    public static final Logger LOG = LogManager.getLogger(Memo.class);
    // generate group id in memo is better for test, since we can reproduce exactly same Memo.
    private static final EventProducer GROUP_MERGE_TRACER = new EventProducer(GroupMergeEvent.class,
            EventChannel.getDefaultChannel().addConsumers(new LogConsumer(GroupMergeEvent.class, EventChannel.LOG)));
    private static long stateId = 0;
    private final IdGenerator<GroupId> groupIdGenerator = GroupId.createGenerator();
    private final Map<GroupId, Group> groups = Maps.newLinkedHashMap();
    // we could not use Set, because Set does not have get method.
    private final Map<GroupExpression, GroupExpression> groupExpressions = Maps.newHashMap();
    private Group root;

    // FOR TEST ONLY
    public Memo() {
        root = null;
    }

    public Memo(Plan plan) {
        root = init(plan);
    }

    public static long getStateId() {
        return stateId;
    }

    public Group getRoot() {
        return root;
    }

    /**
     * This function used to update the root group when DPHyp change the root Group
     * Note it only used in DPHyp
     */
    public void setRoot(Group root) {
        this.root = root;
    }

    public List<Group> getGroups() {
        return ImmutableList.copyOf(groups.values());
    }

    public Group getGroup(GroupId groupId) {
        return groups.get(groupId);
    }

    public Map<GroupExpression, GroupExpression> getGroupExpressions() {
        return groupExpressions;
    }

    public int getGroupExpressionsSize() {
        return groupExpressions.size();
    }

    private Plan skipProject(Plan plan, Group targetGroup) {
        // Some top project can't be eliminated
        if (plan instanceof LogicalProject) {
            LogicalProject<?> logicalProject = (LogicalProject<?>) plan;
            if (targetGroup != root) {
                if (logicalProject.getOutputSet().equals(logicalProject.child().getOutputSet())) {
                    return skipProject(logicalProject.child(), targetGroup);
                }
            } else {
                if (logicalProject.getOutput().equals(logicalProject.child().getOutput())) {
                    return skipProject(logicalProject.child(), targetGroup);
                }
            }
        }
        return plan;
    }

    private Plan skipProjectGetChild(Plan plan) {
        if (plan instanceof LogicalProject) {
            LogicalProject<?> logicalProject = (LogicalProject<?>) plan;
            Plan child = logicalProject.child();
            if (logicalProject.getOutputSet().equals(child.getOutputSet())) {
                return skipProjectGetChild(child);
            }
        }
        return plan;
    }

    public int countMaxContinuousJoin() {
        return countGroupJoin(root).second;
    }

    /**
     * return the max continuous join operator
     */

    public Pair<Integer, Integer> countGroupJoin(Group group) {
        GroupExpression logicalExpr = group.getLogicalExpression();
        List<Pair<Integer, Integer>> children = new ArrayList<>();
        for (Group child : logicalExpr.children()) {
            children.add(countGroupJoin(child));
        }

        if (group.isProjectGroup()) {
            return children.get(0);
        }

        int maxJoinCount = 0;
        int continuousJoinCount = 0;
        for (Pair<Integer, Integer> child : children) {
            maxJoinCount = Math.max(maxJoinCount, child.second);
        }
        if (group.getLogicalExpression().getPlan() instanceof LogicalJoin) {
            for (Pair<Integer, Integer> child : children) {
                continuousJoinCount += child.first;
            }
            continuousJoinCount += 1;
        } else if (group.isProjectGroup()) {
            return children.get(0);
        }
        return Pair.of(continuousJoinCount, Math.max(continuousJoinCount, maxJoinCount));
    }

    /**
     * Add plan to Memo.
     */
    public CopyInResult copyIn(Plan plan, @Nullable Group target, boolean rewrite, HashMap<Long, Group> planTable) {
        CopyInResult result;
        if (rewrite) {
            result = doRewrite(plan, target);
        } else {
            result = doCopyIn(skipProject(plan, target), target, planTable);
        }
        maybeAddStateId(result);
        return result;
    }

    /**
     * Add plan to Memo.
     *
     * @param plan {@link Plan} or {@link Expression} to be added
     * @param target target group to add node. null to generate new Group
     * @param rewrite whether to rewrite the node to the target group
     * @return CopyInResult, in which the generateNewExpression is true if a newly generated
     *                       groupExpression added into memo, and the correspondingExpression
     *                       is the corresponding group expression of the plan
     */
    public CopyInResult copyIn(Plan plan, @Nullable Group target, boolean rewrite) {
        CopyInResult result;
        if (rewrite) {
            result = doRewrite(plan, target);
        } else {
            result = doCopyIn(skipProject(plan, target), target, null);
        }
        maybeAddStateId(result);
        return result;
    }

    private void maybeAddStateId(CopyInResult result) {
        if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().isEnableNereidsTrace()
                && result.generateNewExpression) {
            stateId++;
        }
    }

    public List<Plan> copyOutAll() {
        return copyOutAll(root);
    }

    private List<Plan> copyOutAll(Group group) {
        List<GroupExpression> logicalExpressions = group.getLogicalExpressions();
        return logicalExpressions.stream()
                .flatMap(groupExpr -> copyOutAll(groupExpr).stream())
                .collect(Collectors.toList());
    }

    private List<Plan> copyOutAll(GroupExpression logicalExpression) {
        if (logicalExpression.arity() == 0) {
            return Lists.newArrayList(logicalExpression.getPlan().withChildren(ImmutableList.of()));
        } else if (logicalExpression.arity() == 1) {
            List<Plan> multiChild = copyOutAll(logicalExpression.child(0));
            return multiChild.stream()
                    .map(children -> logicalExpression.getPlan().withChildren(children))
                    .collect(Collectors.toList());
        } else if (logicalExpression.arity() == 2) {
            int leftCount = logicalExpression.child(0).getLogicalExpressions().size();
            int rightCount = logicalExpression.child(1).getLogicalExpressions().size();
            int count = leftCount * rightCount;

            List<Plan> leftChildren = copyOutAll(logicalExpression.child(0));
            List<Plan> rightChildren = copyOutAll(logicalExpression.child(1));

            List<Plan> result = new ArrayList<>(count);
            for (Plan leftChild : leftChildren) {
                for (Plan rightChild : rightChildren) {
                    result.add(logicalExpression.getPlan().withChildren(leftChild, rightChild));
                }
            }
            return result;
        } else {
            throw new RuntimeException("arity > 2");
        }
    }

    public Plan copyOut() {
        return copyOut(root, false);
    }

    /**
     * copyOut the group.
     * @param group the group what want to copyOut
     * @param includeGroupExpression whether include group expression in the plan
     * @return plan
     */
    public Plan copyOut(Group group, boolean includeGroupExpression) {
        GroupExpression logicalExpression = group.getLogicalExpression();
        return copyOut(logicalExpression, includeGroupExpression);
    }

    /**
     * copyOut the logicalExpression.
     * @param logicalExpression the logicalExpression what want to copyOut
     * @param includeGroupExpression whether include group expression in the plan
     * @return plan
     */
    public Plan copyOut(GroupExpression logicalExpression, boolean includeGroupExpression) {
        List<Plan> children = Lists.newArrayList();
        for (Group child : logicalExpression.children()) {
            children.add(copyOut(child, includeGroupExpression));
        }
        Plan planWithChildren = logicalExpression.getPlan().withChildren(children);

        Optional<GroupExpression> groupExpression = includeGroupExpression
                ? Optional.of(logicalExpression)
                : Optional.empty();

        return planWithChildren.withGroupExpression(groupExpression);
    }

    /**
     * init memo by a first plan.
     * @param plan first plan
     * @return plan's corresponding group
     */
    private Group init(Plan plan) {
        Preconditions.checkArgument(!(plan instanceof GroupPlan), "Cannot init memo by a GroupPlan");

        // initialize children recursively
        List<Group> childrenGroups = new ArrayList<>(plan.arity());
        for (Plan child : plan.children()) {
            childrenGroups.add(init(child));
        }

        plan = replaceChildrenToGroupPlan(plan, childrenGroups);
        GroupExpression newGroupExpression = new GroupExpression(plan, childrenGroups);
        Group group = new Group(groupIdGenerator.getNextId(), newGroupExpression, plan.getLogicalProperties());

        groups.put(group.getGroupId(), group);
        if (groupExpressions.containsKey(newGroupExpression)) {
            throw new IllegalStateException("groupExpression already exists in memo, maybe a bug");
        }
        groupExpressions.put(newGroupExpression, newGroupExpression);
        return group;
    }

    /**
     * add or replace the plan into the target group.
     * <p>
     * the result truth table:
     * <pre>
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case                                  | is generated new group expression | corresponding group expression |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 1:                               |                                   |                                |
     * | if plan is GroupPlan                  |              false                |    existed group expression    |
     * | or plan has groupExpression           |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 2:                               |                                   |                                |
     * | if targetGroup is null                |              true                 |      new group expression      |
     * | and same group expression not exist   |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 3:                               |                                   |                                |
     * | if targetGroup is not null            |              true                 |      new group expression      |
     * | and same group expression not exist   |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 4:                               |                                   |                                |
     * | if targetGroup is not null and not    |              true                 |      new group expression      |
     * | equal to the existed group            |                                   |                                |
     * | expression's owner group              |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 5:                               |                                   |                                |
     * | if targetGroup is null or equal to    |              false                |    existed group expression    |
     * | the existed group expression's owner  |                                   |                                |
     * | group                                 |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * </pre>
     *
     * @param plan the plan which want to rewrite or added
     * @param targetGroup target group to replace plan. null to generate new Group. It should be the ancestors
     *                    of the plan's group, or equals to the plan's group, we do not check this constraint
     *                    completely because of performance.
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    private CopyInResult doRewrite(Plan plan, @Nullable Group targetGroup) {
        Preconditions.checkArgument(plan != null, "plan can not be null");
        Preconditions.checkArgument(plan instanceof LogicalPlan, "only logical plan can be rewrite");

        // case 1: fast check the plan whether exist in the memo
        if (plan instanceof GroupPlan || plan.getGroupExpression().isPresent()) {
            return rewriteByExistedPlan(targetGroup, plan);
        }

        List<Group> childrenGroups = rewriteChildrenPlansToGroups(plan, targetGroup);
        plan = replaceChildrenToGroupPlan(plan, childrenGroups);

        // try to create a new group expression
        GroupExpression newGroupExpression = new GroupExpression(plan, childrenGroups);

        // slow check the groupExpression/plan whether exists in the memo
        GroupExpression existedExpression = groupExpressions.get(newGroupExpression);
        if (existedExpression == null) {
            // case 2 or case 3
            return rewriteByNewGroupExpression(targetGroup, plan, newGroupExpression);
        } else {
            // case 4 or case 5
            return rewriteByExistedGroupExpression(targetGroup, plan, existedExpression, newGroupExpression);
        }
    }

    /**
     * add the plan into the target group
     * @param plan the plan which want added
     * @param targetGroup target group to add plan. null to generate new Group. It should be the ancestors
     *                    of the plan's group, or equals to the plan's group, we do not check this constraint
     *                    completely because of performance.
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    private CopyInResult doCopyIn(Plan plan, @Nullable Group targetGroup, @Nullable HashMap<Long, Group> planTable) {
        Preconditions.checkArgument(!(plan instanceof GroupPlan), "plan can not be GroupPlan");
        // check logicalproperties, must same output in a Group.
        if (targetGroup != null && !plan.getLogicalProperties().equals(targetGroup.getLogicalProperties())) {
            LOG.info("Insert a plan into targetGroup but differ in logicalproperties."
                            + "\nPlan logicalproperties: {}\n targetGroup logicalproperties: {}",
                    plan.getLogicalProperties(), targetGroup.getLogicalProperties());
            throw new IllegalStateException("Insert a plan into targetGroup but differ in logicalproperties");
        }
        Optional<GroupExpression> groupExpr = plan.getGroupExpression();
        if (groupExpr.isPresent()) {
            Preconditions.checkState(groupExpressions.containsKey(groupExpr.get()));
            return CopyInResult.of(false, groupExpr.get());
        }
        List<Group> childrenGroups = Lists.newArrayList();
        for (int i = 0; i < plan.children().size(); i++) {
            // skip useless project.
            Plan child = skipProjectGetChild(plan.child(i));
            if (child instanceof GroupPlan) {
                childrenGroups.add(((GroupPlan) child).getGroup());
            } else if (child.getGroupExpression().isPresent()) {
                childrenGroups.add(child.getGroupExpression().get().getOwnerGroup());
            } else {
                childrenGroups.add(doCopyIn(child, null, planTable).correspondingExpression.getOwnerGroup());
            }
        }
        plan = replaceChildrenToGroupPlan(plan, childrenGroups);
        GroupExpression newGroupExpression = new GroupExpression(plan, childrenGroups);
        return insertGroupExpression(newGroupExpression, targetGroup, plan.getLogicalProperties(), planTable);
        // TODO: need to derive logical property if generate new group. currently we not copy logical plan into
    }

    private List<Group> rewriteChildrenPlansToGroups(Plan plan, Group targetGroup) {
        List<Group> childrenGroups = Lists.newArrayList();
        for (int i = 0; i < plan.children().size(); i++) {
            Plan child = plan.children().get(i);
            if (child instanceof GroupPlan) {
                GroupPlan childGroupPlan = (GroupPlan) child;
                validateRewriteChildGroup(childGroupPlan.getGroup(), targetGroup);
                childrenGroups.add(childGroupPlan.getGroup());
            } else if (child.getGroupExpression().isPresent()) {
                Group childGroup = child.getGroupExpression().get().getOwnerGroup();
                validateRewriteChildGroup(childGroup, targetGroup);
                childrenGroups.add(childGroup);
            } else {
                childrenGroups.add(doRewrite(child, null).correspondingExpression.getOwnerGroup());
            }
        }
        return childrenGroups;
    }

    private void validateRewriteChildGroup(Group childGroup, Group targetGroup) {
        /*
         * 'A => B(A)' is invalid equivalent transform because of dead loop.
         * see 'MemoTest.a2ba()'
         */
        if (childGroup == targetGroup) {
            throw new IllegalStateException("Can not add plan which is ancestor of the target plan");
        }
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
    private CopyInResult insertGroupExpression(GroupExpression groupExpression, Group target,
            LogicalProperties logicalProperties, HashMap<Long, Group> planTable) {
        GroupExpression existedGroupExpression = groupExpressions.get(groupExpression);
        if (existedGroupExpression != null) {
            if (target != null && !target.getGroupId().equals(existedGroupExpression.getOwnerGroup().getGroupId())) {
                mergeGroup(target, existedGroupExpression.getOwnerGroup(), planTable);
            }
            // When we create a GroupExpression, we will add it into ParentExpression of childGroup.
            // But if it already exists, we should remove it from ParentExpression of childGroup.
            groupExpression.children().forEach(childGroup -> childGroup.removeParentExpression(groupExpression));
            return CopyInResult.of(false, existedGroupExpression);
        }
        if (target != null) {
            target.addGroupExpression(groupExpression);
        } else {
            Group group = new Group(groupIdGenerator.getNextId(), groupExpression, logicalProperties);
            groups.put(group.getGroupId(), group);
        }
        groupExpressions.put(groupExpression, groupExpression);
        return CopyInResult.of(true, groupExpression);
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
    public void mergeGroup(Group source, Group destination, HashMap<Long, Group> planTable) {
        if (source.equals(destination)) {
            return;
        }
        List<GroupExpression> needReplaceChild = Lists.newArrayList();
        for (GroupExpression parent : source.getParentGroupExpressions()) {
            if (parent.getOwnerGroup().equals(destination)) {
                // cycle, we should not merge
                return;
            }
            Group parentOwnerGroup = parent.getOwnerGroup();
            HashSet<GroupExpression> enforcers = new HashSet<>(parentOwnerGroup.getEnforcers());
            if (enforcers.contains(parent)) {
                continue;
            }
            needReplaceChild.add(parent);
        }
        GROUP_MERGE_TRACER.log(GroupMergeEvent.of(source, destination, needReplaceChild));

        for (GroupExpression reinsertGroupExpr : needReplaceChild) {
            // After change GroupExpression children, hashcode will change, so need to reinsert into map.
            groupExpressions.remove(reinsertGroupExpr);
            reinsertGroupExpr.replaceChild(source, destination);

            GroupExpression existGroupExpr = groupExpressions.get(reinsertGroupExpr);
            if (existGroupExpr != null) {
                Preconditions.checkState(existGroupExpr.getOwnerGroup() != null);
                // remove reinsertGroupExpr from its owner group to avoid adding it to existGroupExpr.getOwnerGroup()
                // existGroupExpr.getOwnerGroup() already has this reinsertGroupExpr.
                reinsertGroupExpr.setUnused(true);
                if (existGroupExpr.getOwnerGroup().equals(reinsertGroupExpr.getOwnerGroup())) {
                    // reinsertGroupExpr & existGroupExpr are in same Group, so merge them.
                    if (reinsertGroupExpr.getPlan() instanceof PhysicalPlan) {
                        reinsertGroupExpr.getOwnerGroup().replaceBestPlanGroupExpr(reinsertGroupExpr, existGroupExpr);
                    }
                    // existingGroupExpression merge the state of reinsertGroupExpr
                    reinsertGroupExpr.mergeTo(existGroupExpr);
                } else {
                    // reinsertGroupExpr & existGroupExpr aren't in same group, need to merge their OwnerGroup.
                    mergeGroup(reinsertGroupExpr.getOwnerGroup(), existGroupExpr.getOwnerGroup(), planTable);
                }
            } else {
                groupExpressions.put(reinsertGroupExpr, reinsertGroupExpr);
            }
        }
        // replace source with destination in groups of planTable
        if (planTable != null) {
            planTable.forEach((bitset, group) -> {
                if (group.equals(source)) {
                    planTable.put(bitset, destination);
                }
            });
        }

        source.mergeTo(destination);
        if (source == root) {
            root = destination;
        }
        groups.remove(source.getGroupId());
    }

    private CopyInResult rewriteByExistedPlan(Group targetGroup, Plan existedPlan) {
        GroupExpression existedLogicalExpression = existedPlan instanceof GroupPlan
                ? ((GroupPlan) existedPlan).getGroup().getLogicalExpression() // get first logicalGroupExpression
                : existedPlan.getGroupExpression().get();
        if (targetGroup != null) {
            Group existedGroup = existedLogicalExpression.getOwnerGroup();
            // clear targetGroup, from exist group move all logical groupExpression
            // and logicalProperties to target group
            eliminateFromGroupAndMoveToTargetGroup(existedGroup, targetGroup, existedPlan.getLogicalProperties());
        }
        return CopyInResult.of(false, existedLogicalExpression);
    }

    public Group newGroup(LogicalProperties logicalProperties) {
        Group group = new Group(groupIdGenerator.getNextId(), logicalProperties);
        groups.put(group.getGroupId(), group);
        return group;
    }

    // This function is used to copy new group expression
    // It's used in DPHyp after construct new group expression
    public Group copyInGroupExpression(GroupExpression newGroupExpression) {
        Group newGroup = new Group(groupIdGenerator.getNextId(), newGroupExpression,
                newGroupExpression.getPlan().getLogicalProperties());
        groups.put(newGroup.getGroupId(), newGroup);
        groupExpressions.put(newGroupExpression, newGroupExpression);
        return newGroup;
    }

    private CopyInResult rewriteByNewGroupExpression(Group targetGroup, Plan newPlan,
            GroupExpression newGroupExpression) {
        if (targetGroup == null) {
            // case 2:
            // if not exist target group and not exist the same group expression,
            // then create new group with the newGroupExpression
            Group newGroup = new Group(groupIdGenerator.getNextId(), newGroupExpression,
                    newPlan.getLogicalProperties());
            groups.put(newGroup.getGroupId(), newGroup);
            groupExpressions.put(newGroupExpression, newGroupExpression);
        } else {
            // case 3:
            // if exist the target group, clear all origin group expressions in the
            // existedExpression's owner group and reset logical properties, the
            // newGroupExpression is the init logical group expression.
            reInitGroup(targetGroup, newGroupExpression, newPlan.getLogicalProperties());

            // note: put newGroupExpression must behind recycle existedExpression(reInitGroup method),
            //       because existedExpression maybe equal to the newGroupExpression and recycle
            //       existedExpression will recycle newGroupExpression
            groupExpressions.put(newGroupExpression, newGroupExpression);
        }
        return CopyInResult.of(true, newGroupExpression);
    }

    private CopyInResult rewriteByExistedGroupExpression(Group targetGroup, Plan transformedPlan,
            GroupExpression existedExpression, GroupExpression newExpression) {
        if (targetGroup != null && !targetGroup.equals(existedExpression.getOwnerGroup())) {
            // case 4:
            existedExpression.propagateApplied(newExpression);
            moveParentExpressionsReference(existedExpression.getOwnerGroup(), targetGroup);
            recycleGroup(existedExpression.getOwnerGroup());
            reInitGroup(targetGroup, newExpression, transformedPlan.getLogicalProperties());

            // note: put newGroupExpression must behind recycle existedExpression(reInitGroup method),
            //       because existedExpression maybe equal to the newGroupExpression and recycle
            //       existedExpression will recycle newGroupExpression
            groupExpressions.put(newExpression, newExpression);
            return CopyInResult.of(true, newExpression);
        } else {
            // case 5:
            // if targetGroup is null or targetGroup equal to the existedExpression's ownerGroup,
            // then recycle the temporary new group expression
            // No ownerGroup, don't need ownerGroup.removeChild()
            recycleExpression(newExpression);
            return CopyInResult.of(false, existedExpression);
        }
    }

    /**
     * eliminate fromGroup, clear targetGroup, then move the logical group expressions in the fromGroup to the toGroup.
     * <p>
     * the scenario is:
     * <pre>
     *  Group 1(project, the targetGroup)                  Group 1(logicalOlapScan, the targetGroup)
     *               |                             =>
     *  Group 0(logicalOlapScan, the fromGroup)
     * </pre>
     * we should recycle the group 0, and recycle all group expressions in group 1, then move the logicalOlapScan to
     * the group 1, and reset logical properties of the group 1.
     */
    private void eliminateFromGroupAndMoveToTargetGroup(Group fromGroup, Group targetGroup,
            LogicalProperties logicalProperties) {
        if (fromGroup == targetGroup) {
            return;
        }
        // simple check targetGroup is the ancestors of the fromGroup, not check completely because of performance
        if (fromGroup == root) {
            throw new IllegalStateException(
                    "TargetGroup should be ancestors of fromGroup, but fromGroup is root. Maybe a bug");
        }

        List<GroupExpression> logicalExpressions = fromGroup.clearLogicalExpressions();
        recycleGroup(fromGroup);

        recycleLogicalAndPhysicalExpressions(targetGroup);

        for (GroupExpression logicalExpression : logicalExpressions) {
            targetGroup.addLogicalExpression(logicalExpression);
        }
        targetGroup.setLogicalProperties(logicalProperties);
    }

    private void reInitGroup(Group group, GroupExpression initLogicalExpression, LogicalProperties logicalProperties) {
        recycleLogicalAndPhysicalExpressions(group);

        group.setLogicalProperties(logicalProperties);
        group.addLogicalExpression(initLogicalExpression);
    }

    private Plan replaceChildrenToGroupPlan(Plan plan, List<Group> childrenGroups) {
        if (childrenGroups.isEmpty()) {
            return plan;
        }
        List<Plan> groupPlanChildren = childrenGroups.stream()
                .map(GroupPlan::new)
                .collect(ImmutableList.toImmutableList());
        return plan.withChildren(groupPlanChildren);
    }

    /*
     * the scenarios that 'parentGroupExpression == toGroup': eliminate the root group.
     * the fromGroup is group 1, the toGroup is group 2, we can not replace group2's
     * groupExpressions reference the child group which is group 2 (reference itself).
     *
     *   A(group 2)            B(group 2)
     *   |                     |
     *   B(group 1)      =>    C(group 0)
     *   |
     *   C(group 0)
     *
     *
     * note: the method don't save group and groupExpression to the memo, so you need
     *       save group and groupExpression to the memo at other place.
     */
    private void moveParentExpressionsReference(Group fromGroup, Group toGroup) {
        for (GroupExpression parentGroupExpression : fromGroup.getParentGroupExpressions()) {
            if (parentGroupExpression.getOwnerGroup() != toGroup) {
                parentGroupExpression.replaceChild(fromGroup, toGroup);
            }
        }
    }

    /**
     * Notice: this func don't replace { Parent GroupExpressions -> this Group }.
     */
    private void recycleGroup(Group group) {
        // recycle in memo.
        if (groups.get(group.getGroupId()) == group) {
            groups.remove(group.getGroupId());
        }

        // recycle children GroupExpression
        recycleLogicalAndPhysicalExpressions(group);
    }

    private void recycleLogicalAndPhysicalExpressions(Group group) {
        group.getLogicalExpressions().forEach(this::recycleExpression);
        group.clearLogicalExpressions();

        group.getPhysicalExpressions().forEach(this::recycleExpression);
        group.clearPhysicalExpressions();
    }

    /**
     * Notice: this func don't clear { OwnerGroup() -> this GroupExpression }.
     */
    private void recycleExpression(GroupExpression groupExpression) {
        // recycle in memo.
        if (groupExpressions.get(groupExpression) == groupExpression) {
            groupExpressions.remove(groupExpression);
        }

        // recycle parentGroupExpr in childGroup
        groupExpression.children().forEach(childGroup -> {
            // if not any groupExpression reference child group, then recycle the child group
            if (childGroup.removeParentExpression(groupExpression) == 0) {
                recycleGroup(childGroup);
            }
        });

        groupExpression.setOwnerGroup(null);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("root:").append(getRoot()).append("\n");
        for (Group group : groups.values()) {
            builder.append("\n\n").append(group).append("\n");
        }
        return builder.toString();
    }

    /**
     * rank all plan and select n-th plan, we write the algorithm according paper:
     * * Counting,Enumerating, and Sampling of Execution Plans in a Cost-Based Query Optimizer
     * Specifically each physical plan in memo is assigned a unique ID in rank(). And then we sort the
     * plan according their cost and choose the n-th plan. Note we don't generate any physical plan in rank
     * function.
     * <p>
     * In unrank() function, we will extract the actual physical function according the unique ID
     */
    public Pair<Long, Double> rank(long n) {
        double threshold = 0.000000001;
        Preconditions.checkArgument(n > 0, "the n %d must be greater than 0 in nthPlan", n);
        List<Pair<Long, Cost>> plans = rankGroup(root, PhysicalProperties.GATHER);
        plans = plans.stream()
                .filter(p -> Double.isFinite(p.second.getValue()))
                .collect(Collectors.toList());
        // This is big heap, it always pops the element with larger cost or larger id.
        PriorityQueue<Pair<Long, Cost>> pq = new PriorityQueue<>((l, r) ->
                Math.abs(l.second.getValue() - r.second.getValue()) < threshold
                        ? -Long.compare(l.first, r.first)
                        : -Double.compare(l.second.getValue(), r.second.getValue()));
        for (Pair<Long, Cost> p : plans) {
            pq.add(p);
            if (pq.size() > n) {
                pq.poll();
            }
        }
        Preconditions.checkArgument(pq.peek() != null, "rank error because there is no valid plan");
        return Pair.of(pq.peek().first, pq.peek().second.getValue());
    }

    /**
     * return number of plan that can be ranked
     */
    public int getRankSize() {
        List<Pair<Long, Cost>> plans = rankGroup(root, PhysicalProperties.GATHER);
        plans = plans.stream().filter(
                        p -> !p.second.equals(Double.NaN)
                                && !p.second.equals(Double.POSITIVE_INFINITY)
                                && !p.second.equals(Double.NEGATIVE_INFINITY))
                .collect(Collectors.toList());
        return plans.size();
    }

    private List<Pair<Long, Cost>> rankGroup(Group group, PhysicalProperties prop) {
        List<Pair<Long, Cost>> res = new ArrayList<>();
        int prefix = 0;
        List<GroupExpression> validGroupExprList = extractGroupExpressionSatisfyProp(group, prop);
        for (GroupExpression groupExpression : validGroupExprList) {
            for (Pair<Long, Cost> idCostPair : rankGroupExpression(groupExpression, prop)) {
                res.add(Pair.of(idCostPair.first + prefix, idCostPair.second));
            }
            prefix = res.size();
            // avoid ranking all plans
            if (res.size() > 1e2) {
                break;
            }
        }
        return res;
    }

    private List<Pair<Long, Cost>> rankGroupExpression(GroupExpression groupExpression,
            PhysicalProperties prop) {
        if (!groupExpression.getLowestCostTable().containsKey(prop)) {
            return new ArrayList<>();
        }
        List<Pair<Long, Cost>> res = new ArrayList<>();
        if (groupExpression.getPlan() instanceof LeafPlan) {
            res.add(Pair.of(0L, groupExpression.getCostValueByProperties(prop)));
            return res;
        }

        List<List<PhysicalProperties>> inputPropertiesList = extractInputProperties(groupExpression, prop);
        for (List<PhysicalProperties> inputProperties : inputPropertiesList) {
            int prefix = res.size();
            List<List<Pair<Long, Cost>>> children = new ArrayList<>();
            for (int i = 0; i < inputProperties.size(); i++) {
                // To avoid reach a circle, we don't allow ranking the same group with the same physical properties.
                Preconditions.checkArgument(!groupExpression.child(i).equals(groupExpression.getOwnerGroup())
                        || !prop.equals(inputProperties.get(i)));
                List<Pair<Long, Cost>> idCostPair
                        = rankGroup(groupExpression.child(i), inputProperties.get(i));
                children.add(idCostPair);
            }

            List<Pair<Long, List<Integer>>> childrenId = new ArrayList<>();
            permute(children, 0, childrenId, new ArrayList<>());
            Cost cost = CostCalculator.calculateCost(groupExpression, inputProperties);
            for (Pair<Long, List<Integer>> c : childrenId) {
                Cost totalCost = cost;
                for (int i = 0; i < children.size(); i++) {
                    totalCost = CostCalculator.addChildCost(groupExpression.getPlan(),
                            totalCost,
                            children.get(i).get(c.second.get(i)).second,
                            i);
                }
                if (res.isEmpty()) {
                    Preconditions.checkArgument(
                            Math.abs(totalCost.getValue() - groupExpression.getCostByProperties(prop)) < 0.0001,
                            "Please check operator %s, expected cost %s but found %s",
                            groupExpression.getPlan().shapeInfo(), totalCost.getValue(),
                            groupExpression.getCostByProperties(prop));
                }
                res.add(Pair.of(prefix + c.first, totalCost));
            }
        }

        return res;
    }

    /**
     * we permute all children, e.g.,
     * for children [1, 2] [1, 2, 3]
     * we can get: 0: [1,1] 1:[1, 2] 2:[1, 3] 3:[2, 1] 4:[2, 2] 5:[2, 3]
     */
    private void permute(List<List<Pair<Long, Cost>>> children, int index,
            List<Pair<Long, List<Integer>>> result, List<Integer> current) {
        if (index == children.size()) {
            result.add(Pair.of(getUniqueId(children, current), current));
            return;
        }
        for (int i = 0; i < children.get(index).size(); i++) {
            List<Integer> next = new ArrayList<>(current);
            next.add(i);
            permute(children, index + 1, result, next);
        }
    }

    /**
     * This method is used to calculate the unique ID for one combination,
     * The current is used to represent the index of the child in lists e.g.,
     * for children [1], [1, 2], The possible indices and IDs are:
     * [0, 0]: 0*1 + 0*1*2
     * [0, 1]: 0*1 + 1*1*2
     */
    private static long getUniqueId(List<List<Pair<Long, Cost>>> lists, List<Integer> current) {
        long id = 0;
        long factor = 1;
        for (int i = 0; i < lists.size(); i++) {
            id += factor * current.get(i);
            factor *= lists.get(i).size();
        }
        return id;
    }

    private List<GroupExpression> extractGroupExpressionSatisfyProp(Group group, PhysicalProperties prop) {
        GroupExpression bestExpr = group.getLowestCostPlan(prop).get().second;
        List<GroupExpression> exprs = Lists.newArrayList(bestExpr);
        Set<GroupExpression> hasVisited = new HashSet<>();
        hasVisited.add(bestExpr);
        Stream.concat(group.getPhysicalExpressions().stream(), group.getEnforcers().stream())
                .forEach(groupExpression -> {
                    if (!groupExpression.getInputPropertiesListOrEmpty(prop).isEmpty()
                            && !groupExpression.equals(bestExpr) && !hasVisited.contains(groupExpression)) {
                        hasVisited.add(groupExpression);
                        exprs.add(groupExpression);
                    }
                });
        return exprs;
    }

    // ----------------------------------------------------------------
    // extract input properties for a given group expression and required output properties
    // There are three cases:
    // 1. If group expression is enforcer, return the input properties of the best expression
    // 2. If group expression require any, return any input properties
    // 3. Otherwise, return all input properties that satisfies the required output properties
    private List<List<PhysicalProperties>> extractInputProperties(GroupExpression groupExpression,
            PhysicalProperties prop) {
        List<List<PhysicalProperties>> res = new ArrayList<>();
        res.add(groupExpression.getInputPropertiesList(prop));

        // return optimized input for enforcer
        if (groupExpression.getOwnerGroup().getEnforcers().contains(groupExpression)) {
            return res;
        }

        // return any if exits except RequirePropertiesSupplier and SetOperators
        // Because PropRegulator could change their input properties
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(prop);
        List<List<PhysicalProperties>> requestList = requestPropertyDeriver
                .getRequestChildrenPropertyList(groupExpression);
        Optional<List<PhysicalProperties>> any = requestList.stream()
                .filter(e -> e.stream().allMatch(PhysicalProperties.ANY::equals))
                .findAny();
        if (any.isPresent()
                && !(groupExpression.getPlan() instanceof RequirePropertiesSupplier)
                && !(groupExpression.getPlan() instanceof SetOperation)) {
            res.clear();
            res.add(any.get());
            return res;
        }

        // return all optimized inputs
        Set<List<PhysicalProperties>> inputProps = groupExpression.getLowestCostTable().keySet().stream()
                .filter(physicalProperties -> physicalProperties.satisfy(prop))
                .map(groupExpression::getInputPropertiesList)
                .collect(Collectors.toSet());
        res.addAll(inputProps);
        return res;
    }

    private int getGroupSize(Group group, PhysicalProperties prop,
            Map<GroupExpression, List<List<Integer>>> exprSizeCache) {
        List<GroupExpression> validGroupExprs = extractGroupExpressionSatisfyProp(group, prop);
        int groupCount = 0;
        for (GroupExpression groupExpression : validGroupExprs) {
            int exprCount = getExprSize(groupExpression, prop, exprSizeCache);
            groupCount += exprCount;
            if (groupCount > 1e2) {
                break;
            }
        }
        return groupCount;
    }

    // return size for each input properties
    private int getExprSize(GroupExpression groupExpression, PhysicalProperties properties,
            Map<GroupExpression, List<List<Integer>>> exprChildSizeCache) {
        List<List<Integer>> exprCount = new ArrayList<>();
        if (!groupExpression.getLowestCostTable().containsKey(properties)) {
            exprCount.add(Lists.newArrayList(0));
        } else if (groupExpression.getPlan() instanceof LeafPlan) {
            exprCount.add(Lists.newArrayList(1));
        } else {
            List<List<PhysicalProperties>> inputPropertiesList = extractInputProperties(groupExpression, properties);
            for (List<PhysicalProperties> inputProperties : inputPropertiesList) {
                List<Integer> groupExprSize = new ArrayList<>();
                for (int i = 0; i < inputProperties.size(); i++) {
                    groupExprSize.add(
                            getGroupSize(groupExpression.child(i), inputProperties.get(i), exprChildSizeCache));
                }
                exprCount.add(groupExprSize);
            }
        }
        exprChildSizeCache.put(groupExpression, exprCount);
        return exprCount.stream()
                .mapToInt(s -> s.stream().reduce(1, (a, b) -> a * b))
                .sum();
    }

    private PhysicalPlan unrankGroup(Group group, PhysicalProperties prop, long rank,
            Map<GroupExpression, List<List<Integer>>> exprSizeCache) {
        int prefix = 0;
        for (GroupExpression groupExpression : extractGroupExpressionSatisfyProp(group, prop)) {
            int exprCount = exprSizeCache.get(groupExpression).stream()
                    .mapToInt(s -> s.stream().reduce(1, (a, b) -> a * b))
                    .sum();
            // rank is start from 0
            if (exprCount != 0 && rank + 1 - prefix <= exprCount) {
                return unrankGroupExpression(groupExpression, prop, rank - prefix,
                        exprSizeCache);
            }
            prefix += exprCount;
        }
        throw new RuntimeException("the group has no plan for prop %s in rank job");
    }

    private PhysicalPlan unrankGroupExpression(GroupExpression groupExpression, PhysicalProperties prop, long rank,
            Map<GroupExpression, List<List<Integer>>> exprSizeCache) {
        if (groupExpression.getPlan() instanceof LeafPlan) {
            Preconditions.checkArgument(rank == 0,
                    "leaf plan's %s rank must be 0 but is %d", groupExpression, rank);
            return ((PhysicalPlan) groupExpression.getPlan()).withPhysicalPropertiesAndStats(
                    groupExpression.getOutputProperties(prop),
                    groupExpression.getOwnerGroup().getStatistics());
        }

        List<List<PhysicalProperties>> inputPropertiesList = extractInputProperties(groupExpression, prop);
        for (int i = 0; i < inputPropertiesList.size(); i++) {
            List<PhysicalProperties> properties = inputPropertiesList.get(i);
            List<Integer> childrenSize = exprSizeCache.get(groupExpression).get(i);
            int count = childrenSize.stream().reduce(1, (a, b) -> a * b);
            if (rank >= count) {
                rank -= count;
                continue;
            }
            List<Long> childrenRanks = extractChildRanks(rank, childrenSize);
            List<Plan> childrenPlan = new ArrayList<>();
            for (int j = 0; j < properties.size(); j++) {
                Plan plan = unrankGroup(groupExpression.child(j), properties.get(j),
                        childrenRanks.get(j), exprSizeCache);
                Preconditions.checkArgument(plan != null, "rank group get null");
                childrenPlan.add(plan);
            }

            Plan plan = groupExpression.getPlan().withChildren(childrenPlan);
            return ((PhysicalPlan) plan).withPhysicalPropertiesAndStats(
                    groupExpression.getOutputProperties(prop),
                    groupExpression.getOwnerGroup().getStatistics());
        }
        throw new RuntimeException("the groupExpr has no plan for prop in rank job");
    }

    /**
     * This method is used to decode ID for each child, which is the opposite of getUniqueID method, e.g.,
     * 0: [0%1, 0%(1*2)]
     * 1: [1%1, 1%(1*2)]
     * 2: [2%1, 2%(1*2)]
     */
    private List<Long> extractChildRanks(long rank, List<Integer> childrenSize) {
        Preconditions.checkArgument(!childrenSize.isEmpty(), "children should not empty in extractChildRanks");
        List<Long> indices = new ArrayList<>();
        for (int i = 0; i < childrenSize.size(); i++) {
            int factor = childrenSize.get(i);
            indices.add(rank % factor);
            rank = rank / factor;
        }
        return indices;
    }

    public PhysicalPlan unrank(long id) {
        Map<GroupExpression, List<List<Integer>>> exprSizeCache = new HashMap<>();
        getGroupSize(getRoot(), PhysicalProperties.GATHER, exprSizeCache);
        return unrankGroup(getRoot(), PhysicalProperties.GATHER, id, exprSizeCache);
    }
}

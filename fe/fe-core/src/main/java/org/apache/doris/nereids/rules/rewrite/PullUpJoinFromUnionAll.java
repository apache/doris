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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pull up join from union all rule.
 */
public class PullUpJoinFromUnionAll extends OneRewriteRuleFactory {
    private static final Set<Class<? extends LogicalPlan>> SUPPORTED_PLAN_TYPE = ImmutableSet.of(
            LogicalFilter.class,
            LogicalJoin.class,
            LogicalProject.class,
            LogicalCatalogRelation.class
    );

    private static class PullUpContext {
        public static final String unifiedOutputAlias = "PULL_UP_UNIFIED_OUTPUT_ALIAS";
        public final Map<String, List<LogicalCatalogRelation>> pullUpCandidatesMaps = Maps.newHashMap();
        public final Map<LogicalCatalogRelation, LogicalJoin> tableToJoinRootMap = Maps.newHashMap();
        public final Map<LogicalCatalogRelation, LogicalAggregate> tableToAggrRootMap = Maps.newHashMap();
        public final Map<NamedExpression, SlotReference> origChild0ToNewUnionOutputMap = Maps.newHashMap();
        public final List<LogicalAggregate> aggrChildList = Lists.newArrayList();
        public final List<LogicalJoin> joinChildList = Lists.newArrayList();
        public final List<SlotReference> replaceColumns = Lists.newArrayList();
        public final Map<LogicalCatalogRelation, Slot> pullUpTableToPkSlotMap = Maps.newHashMap();
        public int replacedColumnIndex = -1;
        public LogicalCatalogRelation pullUpTable;

        // the slot will replace the original pk in group by and select list
        public SlotReference replaceColumn;
        public boolean needAddReplaceColumn = false;

        public PullUpContext() {}

        public void setReplacedColumn(SlotReference slot) {
            this.replaceColumn = slot;
        }

        public void setPullUpTable(LogicalCatalogRelation table) {
            this.pullUpTable = table;
        }

        public void setNeedAddReplaceColumn(boolean needAdd) {
            this.needAddReplaceColumn = needAdd;
        }
    }

    @Override
    public Rule build() {
        return logicalUnion()
                        .when(union -> union.getQualifier() != Qualifier.DISTINCT)
                        .then(union -> {
                            PullUpContext context = new PullUpContext();
                            if (!checkUnionPattern(union, context)
                                    || !checkJoinCondition(context)
                                    || !checkGroupByKeys(context)) {
                                return null;
                            }
                            // only support single table pull up currently
                            if (context.pullUpCandidatesMaps.entrySet().size() != 1) {
                                return null;
                            }

                            List<LogicalCatalogRelation> pullUpTableList = context.pullUpCandidatesMaps
                                    .entrySet().iterator().next().getValue();
                            if (pullUpTableList.size() != union.children().size()
                                    || context.replaceColumns.size() != union.children().size()
                                    || !checkNoFilterOnPullUpTable(pullUpTableList, context)) {
                                return null;
                            }
                            // make new union node
                            LogicalUnion newUnionNode = makeNewUnionNode(union, pullUpTableList, context);
                            // make new join node
                            LogicalJoin newJoin = makeNewJoin(newUnionNode, pullUpTableList.get(0), context);
                            // add project on pull up table with origin union output
                            List<NamedExpression> newProjectOutputs = makeNewProjectOutputs(union, newJoin, context);

                            return new LogicalProject(newProjectOutputs, newJoin);
                        }).toRule(RuleType.PULL_UP_JOIN_FROM_UNIONALL);
    }

    private boolean checkUnionPattern(LogicalUnion union, PullUpContext context) {
        int tableListNumber = -1;
        for (Plan child : union.children()) {
            if (!(child instanceof LogicalProject
                    && child.child(0) instanceof LogicalAggregate
                    && child.child(0).child(0) instanceof LogicalProject
                    && child.child(0).child(0).child(0) instanceof LogicalJoin)) {
                return false;
            }
            LogicalAggregate aggrRoot = (LogicalAggregate) child.child(0);
            if (!checkAggrRoot(aggrRoot)) {
                return false;
            }
            context.aggrChildList.add(aggrRoot);
            LogicalJoin joinRoot = (LogicalJoin) aggrRoot.child().child(0);
            // check join under union is spj
            if (!checkJoinRoot(joinRoot)) {
                return false;
            }
            context.joinChildList.add(joinRoot);

            List<LogicalCatalogRelation> tableList = getTableListUnderJoin(joinRoot);
            // add into table -> joinRoot map
            for (LogicalCatalogRelation table : tableList) {
                context.tableToJoinRootMap.put(table, joinRoot);
                context.tableToAggrRootMap.put(table, aggrRoot);
            }
            if (tableListNumber == -1) {
                tableListNumber = tableList.size();
            } else {
                // check all union children have the same number of tables
                if (tableListNumber != tableList.size()) {
                    return false;
                }
            }

            for (LogicalCatalogRelation table : tableList) {
                // key: qualified table name
                // value: table list in all union children
                String qName = makeQualifiedName(table);
                if (context.pullUpCandidatesMaps.get(qName) == null) {
                    List<LogicalCatalogRelation> newList = new ArrayList<>();
                    newList.add(table);
                    context.pullUpCandidatesMaps.put(qName, newList);
                } else {
                    context.pullUpCandidatesMaps.get(qName).add(table);
                }
            }
        }
        int expectedNumber = union.children().size();
        List<String> toBeRemoved = new ArrayList<>();
        // check the pull up table candidate exists in all union children
        for (Map.Entry<String, List<LogicalCatalogRelation>> e : context.pullUpCandidatesMaps.entrySet()) {
            if (e.getValue().size() != expectedNumber) {
                toBeRemoved.add(e.getKey());
            }
        }
        for (String key : toBeRemoved) {
            context.pullUpCandidatesMaps.remove(key);
        }
        return !context.pullUpCandidatesMaps.isEmpty();
    }

    private boolean checkJoinCondition(PullUpContext context) {
        List<String> toBeRemoved = new ArrayList<>();
        for (Map.Entry<String, List<LogicalCatalogRelation>> e : context.pullUpCandidatesMaps.entrySet()) {
            List<LogicalCatalogRelation> tableList = e.getValue();
            boolean allFound = true;
            for (LogicalCatalogRelation table : tableList) {
                LogicalJoin joinRoot = context.tableToJoinRootMap.get(table);
                if (joinRoot == null) {
                    return false;
                } else if (!checkJoinConditionOnPk(joinRoot, table, context)) {
                    allFound = false;
                    break;
                }
            }
            if (!allFound) {
                toBeRemoved.add(e.getKey());
            }
        }
        for (String table : toBeRemoved) {
            context.pullUpCandidatesMaps.remove(table);
        }

        if (context.pullUpCandidatesMaps.isEmpty()) {
            return false;
        }
        return true;
    }

    private boolean checkGroupByKeys(PullUpContext context) {
        List<String> toBeRemoved = new ArrayList<>();
        for (Map.Entry<String, List<LogicalCatalogRelation>> e : context.pullUpCandidatesMaps.entrySet()) {
            List<LogicalCatalogRelation> tableList = e.getValue();
            boolean allFound = true;
            for (LogicalCatalogRelation table : tableList) {
                LogicalAggregate aggrRoot = context.tableToAggrRootMap.get(table);
                if (aggrRoot == null) {
                    return false;
                } else if (!checkAggrKeyOnUkOrPk(aggrRoot, table)) {
                    allFound = false;
                    break;
                }
            }
            if (!allFound) {
                toBeRemoved.add(e.getKey());
            }
        }
        for (String table : toBeRemoved) {
            context.pullUpCandidatesMaps.remove(table);
        }

        if (context.pullUpCandidatesMaps.isEmpty()) {
            return false;
        }
        return true;
    }

    private boolean checkNoFilterOnPullUpTable(List<LogicalCatalogRelation> pullUpTableList, PullUpContext context) {
        for (LogicalCatalogRelation table : pullUpTableList) {
            LogicalJoin joinRoot = context.tableToJoinRootMap.get(table);
            if (joinRoot == null) {
                return false;
            } else {
                List<LogicalFilter> filterList = new ArrayList<>();
                filterList.addAll((Collection<? extends LogicalFilter>)
                        joinRoot.collect(LogicalFilter.class::isInstance));
                for (LogicalFilter filter : filterList) {
                    if (filter.child().equals(context.pullUpTable)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean checkAggrKeyOnUkOrPk(LogicalAggregate aggregate, LogicalCatalogRelation table) {
        List<Expression> groupByKeys = aggregate.getGroupByExpressions();
        boolean isAllSlotReference = groupByKeys.stream().allMatch(e -> e instanceof SlotReference);
        if (!isAllSlotReference) {
            return false;
        } else {
            Set<String> ukInfo = getUkInfoFromConstraint(table);
            Set<String> pkInfo = getPkInfoFromConstraint(table);
            if (ukInfo == null || pkInfo == null || ukInfo.size() != 1 || pkInfo.size() != 1) {
                return false;
            } else {
                String ukName = ukInfo.iterator().next();
                String pkName = pkInfo.iterator().next();
                for (Object expr : aggregate.getGroupByExpressions()) {
                    SlotReference slot = (SlotReference) expr;
                    if (table.getOutputExprIds().contains(slot.getExprId())
                            && (slot.getName().equals(ukName) || slot.getName().equals(pkName))) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    private boolean checkJoinConditionOnPk(LogicalJoin joinRoot, LogicalCatalogRelation table, PullUpContext context) {
        Set<String> pkInfos = getPkInfoFromConstraint(table);
        if (pkInfos == null || pkInfos.size() != 1) {
            return false;
        }
        String pkSlot = pkInfos.iterator().next();
        List<LogicalJoin> joinList = new ArrayList<>();
        joinList.addAll((Collection<? extends LogicalJoin>) joinRoot.collect(LogicalJoin.class::isInstance));
        boolean found = false;
        for (LogicalJoin join : joinList) {
            List<Expression> conditions = join.getHashJoinConjuncts();
            List<LogicalCatalogRelation> basicTableList = new ArrayList<>();
            basicTableList.addAll((Collection<? extends LogicalCatalogRelation>) join
                    .collect(LogicalCatalogRelation.class::isInstance));
            for (Expression equalTo : conditions) {
                if (equalTo instanceof EqualTo
                        && ((EqualTo) equalTo).left() instanceof SlotReference
                        && ((EqualTo) equalTo).right() instanceof SlotReference) {
                    SlotReference leftSlot = (SlotReference) ((EqualTo) equalTo).left();
                    SlotReference rightSlot = (SlotReference) ((EqualTo) equalTo).right();
                    if (table.getOutputExprIds().contains(leftSlot.getExprId())
                            && pkSlot.equals(leftSlot.getName())) {
                        // pk-fk join condition, check other side's join key is on fk
                        LogicalCatalogRelation rightTable = findTableFromSlot(rightSlot, basicTableList);
                        if (rightTable != null && getFkInfoFromConstraint(rightTable) != null) {
                            ForeignKeyConstraint fkInfo = getFkInfoFromConstraint(rightTable);
                            if (fkInfo.getReferencedTable().getId() == table.getTable().getId()) {
                                for (Map.Entry<String, String> entry : fkInfo.getForeignToReference().entrySet()) {
                                    if (entry.getValue().equals(pkSlot) && entry.getKey().equals(rightSlot.getName())) {
                                        found = true;
                                        context.replaceColumns.add(rightSlot);
                                        context.pullUpTableToPkSlotMap.put(table, leftSlot);
                                        break;
                                    }
                                }
                            }
                        }
                    } else if (table.getOutputExprIds().contains(rightSlot.getExprId())
                            && pkSlot.equals(rightSlot.getName())) {
                        // pk-fk join condition, check other side's join key is on fk
                        LogicalCatalogRelation leftTable = findTableFromSlot(leftSlot, basicTableList);
                        if (leftTable != null && getFkInfoFromConstraint(leftTable) != null) {
                            ForeignKeyConstraint fkInfo = getFkInfoFromConstraint(leftTable);
                            if (fkInfo.getReferencedTable().getId() == table.getTable().getId()) {
                                for (Map.Entry<String, String> entry : fkInfo.getForeignToReference().entrySet()) {
                                    if (entry.getValue().equals(pkSlot) && entry.getKey().equals(leftSlot.getName())) {
                                        found = true;
                                        context.replaceColumns.add(leftSlot);
                                        context.pullUpTableToPkSlotMap.put(table, rightSlot);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (found) {
                        break;
                    }
                }
            }
            if (found) {
                break;
            }
        }
        return found;
    }

    private LogicalCatalogRelation findTableFromSlot(SlotReference targetSlot,
            List<LogicalCatalogRelation> tableList) {
        for (LogicalCatalogRelation table : tableList) {
            if (table.getOutputExprIds().contains(targetSlot.getExprId())) {
                return table;
            }
        }
        return null;
    }

    private ForeignKeyConstraint getFkInfoFromConstraint(LogicalCatalogRelation table) {
        Set<ForeignKeyConstraint> foreignKeyConstraints = table.getTable().getForeignKeyConstraints();
        if (foreignKeyConstraints.isEmpty()) {
            return null;
        }
        return foreignKeyConstraints.stream().iterator().next();
    }

    private Set<String> getPkInfoFromConstraint(LogicalCatalogRelation table) {
        Set<PrimaryKeyConstraint> primaryKeyConstraints = table.getTable().getPrimaryKeyConstraints();
        if (primaryKeyConstraints.isEmpty()) {
            return null;
        }
        return primaryKeyConstraints.stream().iterator().next().getPrimaryKeyNames();
    }

    private Set<String> getUkInfoFromConstraint(LogicalCatalogRelation table) {
        Set<UniqueConstraint> uniqueConstraints = table.getTable().getUniqueConstraints();
        if (uniqueConstraints.isEmpty()) {
            return null;
        }
        return uniqueConstraints.stream().iterator().next().getUniqueColumnNames();
    }

    private boolean checkJoinRoot(LogicalJoin joinRoot) {
        List<LogicalPlan> joinChildrenPlans = Lists.newArrayList();
        joinChildrenPlans.addAll((Collection<? extends LogicalPlan>) joinRoot
                .collect(LogicalPlan.class::isInstance));
        boolean planTypeMatch = joinChildrenPlans.stream()
                .allMatch(p -> SUPPORTED_PLAN_TYPE.stream().anyMatch(c -> c.isInstance(p)));
        if (!planTypeMatch) {
            return false;
        }

        List<LogicalJoin> allJoinNodes = Lists.newArrayList();
        allJoinNodes.addAll((Collection<? extends LogicalJoin>) joinRoot.collect(LogicalJoin.class::isInstance));
        boolean joinTypeMatch = allJoinNodes.stream().allMatch(e -> e.getJoinType() == JoinType.INNER_JOIN);
        boolean joinConditionMatch = allJoinNodes.stream()
                .allMatch(e -> !e.getHashJoinConjuncts().isEmpty() && e.getOtherJoinConjuncts().isEmpty());
        if (!joinTypeMatch || !joinConditionMatch) {
            return false;
        }

        return true;
    }

    private boolean checkAggrRoot(LogicalAggregate aggrRoot) {
        for (Object expr : aggrRoot.getGroupByExpressions()) {
            if (!(expr instanceof NamedExpression)) {
                return false;
            }
        }
        return true;
    }

    private List<LogicalCatalogRelation> getTableListUnderJoin(LogicalJoin joinRoot) {
        List<LogicalCatalogRelation> tableLists = new ArrayList<>();
        tableLists.addAll((Collection<? extends LogicalCatalogRelation>) joinRoot
                .collect(LogicalCatalogRelation.class::isInstance));
        return tableLists;
    }

    private String makeQualifiedName(LogicalCatalogRelation table) {
        String dbName = table.getTable().getDatabase().getFullName();
        String tableName = table.getTable().getName();
        return dbName + ":" + tableName;
    }

    private Plan doPullUpJoinFromUnionAll(Plan unionChildPlan, PullUpContext context) {
        return PullUpRewriter.INSTANCE.rewrite(unionChildPlan, context);
    }

    private List<Plan> doWrapReplaceColumnForUnionChildren(List<Plan> unionChildren, PullUpContext context) {
        List<Plan> newUnionChildren = new ArrayList<>();
        for (int i = 0; i < unionChildren.size(); i++) {
            // has been checked before
            LogicalProject oldProject = (LogicalProject) unionChildren.get(i);
            List<NamedExpression> newNamedExpressionList = new ArrayList<>();
            for (int j = 0; j < oldProject.getProjects().size(); j++) {
                Object child = oldProject.getProjects().get(j);
                if (context.replaceColumns.contains(child)) {
                    Alias newExpr = new Alias((Expression) child, context.unifiedOutputAlias);
                    newNamedExpressionList.add(newExpr);
                    context.replacedColumnIndex = j;
                } else {
                    newNamedExpressionList.add((NamedExpression) child);
                }
            }
            LogicalProject newProject = new LogicalProject(newNamedExpressionList, (LogicalPlan) oldProject.child());
            newUnionChildren.add(newProject);
        }
        return newUnionChildren;
    }

    private List<NamedExpression> makeNewProjectOutputs(LogicalUnion origUnion,
            LogicalJoin newJoin, PullUpContext context) {
        List<NamedExpression> newProjectOutputs = new ArrayList<>();
        List<Slot> origUnionSlots = origUnion.getOutput();
        List<NamedExpression> origUnionChildOutput = ((LogicalProject) origUnion.child(0)).getOutputs();
        for (int i = 0; i < origUnionChildOutput.size(); i++) {
            NamedExpression unionOutputExpr = origUnionChildOutput.get(i);
            if (unionOutputExpr instanceof Alias) {
                if (!(unionOutputExpr.child(0) instanceof Literal)) {
                    unionOutputExpr = (Slot) unionOutputExpr.child(0);
                }
            }
            boolean found = false;
            Slot matchedJoinSlot = null;
            for (Slot joinOutput : newJoin.getOutput()) {
                Slot slot = joinOutput;
                if (context.origChild0ToNewUnionOutputMap.get(slot) != null) {
                    slot = context.origChild0ToNewUnionOutputMap.get(slot);
                }
                if (slot.equals(unionOutputExpr) || slot.getExprId() == unionOutputExpr.getExprId()) {
                    matchedJoinSlot = joinOutput;
                    found = true;
                    break;
                }
            }
            if (found) {
                ExprId exprId = origUnionSlots.get(i).getExprId();
                Alias aliasExpr = new Alias(exprId, matchedJoinSlot, matchedJoinSlot.toSql());
                newProjectOutputs.add(aliasExpr);
            }
        }
        return newProjectOutputs;
    }

    private LogicalJoin makeNewJoin(LogicalUnion newUnionNode,
            LogicalCatalogRelation pullUpTable, PullUpContext context) {
        List<Expression> newHashJoinConjuncts = new ArrayList<>();
        Slot unionSideExpr = newUnionNode.getOutput().get(context.replacedColumnIndex);

        Slot pullUpSidePkSlot = context.pullUpTableToPkSlotMap.get(pullUpTable);
        if (pullUpSidePkSlot == null) {
            return null;
        }
        EqualTo pullUpJoinCondition = new EqualTo(unionSideExpr, pullUpSidePkSlot);
        newHashJoinConjuncts.add(pullUpJoinCondition);

        // new a join with the newUnion and the pulled up table
        return new LogicalJoin<>(
                JoinType.INNER_JOIN,
                newHashJoinConjuncts,
                ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                newUnionNode,
                pullUpTable, null);
    }

    private LogicalUnion makeNewUnionNode(LogicalUnion origUnion,
            List<LogicalCatalogRelation> pullUpTableList, PullUpContext context) {
        List<Plan> newUnionChildren = new ArrayList<>();
        for (int i = 0; i < origUnion.children().size(); i++) {
            Plan unionChild = origUnion.child(i);
            context.setPullUpTable(pullUpTableList.get(i));
            context.setReplacedColumn(context.replaceColumns.get(i));
            Plan newChild = doPullUpJoinFromUnionAll(unionChild, context);
            newUnionChildren.add(newChild);
        }

        // wrap the replaced column with a shared alias which is exposed to outside
        List<Plan> formalizedNewUnionChildren = doWrapReplaceColumnForUnionChildren(newUnionChildren, context);
        List<List<SlotReference>> childrenOutputs = formalizedNewUnionChildren.stream()
                .map(j -> j.getOutput().stream()
                        .map(SlotReference.class::cast)
                        .collect(ImmutableList.toImmutableList()))
                .collect(ImmutableList.toImmutableList());

        LogicalUnion newUnionNode = new LogicalUnion(Qualifier.ALL, formalizedNewUnionChildren);
        newUnionNode = (LogicalUnion) newUnionNode.withChildrenAndTheirOutputs(
                formalizedNewUnionChildren, childrenOutputs);
        List<NamedExpression> newOutputs = newUnionNode.buildNewOutputs();
        newUnionNode = newUnionNode.withNewOutputs(newOutputs);

        // set up origin child 0 output to new union output mapping
        List<SlotReference> origChild0Output = childrenOutputs.get(0);
        for (int i = 0; i < origChild0Output.size(); i++) {
            SlotReference slot = origChild0Output.get(i);
            NamedExpression newExpr = newOutputs.get(i);
            context.origChild0ToNewUnionOutputMap.put(newExpr, slot);
        }

        return newUnionNode;
    }

    private static class PullUpRewriter extends DefaultPlanRewriter<PullUpContext> implements CustomRewriter {
        public static final PullUpRewriter INSTANCE = new PullUpRewriter();

        @Override
        public Plan rewriteRoot(Plan plan, JobContext context) {
            return null;
        }

        public Plan rewrite(Plan plan, PullUpContext context) {
            return plan.accept(this, context);
        }

        @Override
        public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, PullUpContext context) {
            Plan input = agg.child().accept(this, context);

            LogicalCatalogRelation pullTable = context.pullUpTable;
            SlotReference replaceColumn = context.replaceColumn;

            // eliminate group by keys
            List<Expression> groupByExprList = new ArrayList<>();
            for (Expression expr : agg.getGroupByExpressions()) {
                // expr has been checked before
                if (!pullTable.getOutputExprIds().contains(((NamedExpression) expr).getExprId())) {
                    groupByExprList.add(expr);
                }
            }
            // add replaced group by key
            groupByExprList.add(replaceColumn);

            // eliminate outputs keys
            List<NamedExpression> outputExprList = new ArrayList<>();
            for (NamedExpression expr : agg.getOutputExpressions()) {
                if (!pullTable.getOutputExprIds().contains(expr.getExprId())) {
                    outputExprList.add(expr);
                }
            }
            // add replaced group by key
            outputExprList.add(replaceColumn);

            return new LogicalAggregate<>(groupByExprList, outputExprList, input);
        }

        public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, PullUpContext context) {
            Plan leftChild = join.child(0).accept(this, context);
            Plan rightChild = join.child(1).accept(this, context);
            LogicalCatalogRelation pullUpTable = context.pullUpTable;

            // no filter on pull up table, which has been checked before
            if (leftChild instanceof LogicalCatalogRelation
                    && leftChild.equals(pullUpTable)) {
                context.setNeedAddReplaceColumn(true);
                return rightChild;
            } else if (rightChild instanceof LogicalCatalogRelation
                    && rightChild.equals(pullUpTable)) {
                context.setNeedAddReplaceColumn(true);
                return leftChild;
            } else if (leftChild instanceof LogicalProject
                    && leftChild.child(0) instanceof LogicalCatalogRelation
                    && leftChild.child(0).equals(pullUpTable)) {
                context.setNeedAddReplaceColumn(true);
                return rightChild;
            } else if (rightChild instanceof LogicalProject
                    && rightChild.child(0) instanceof LogicalCatalogRelation
                    && rightChild.child(0).equals(pullUpTable)) {
                context.setNeedAddReplaceColumn(true);
                return leftChild;
            } else {
                return new LogicalJoin(JoinType.INNER_JOIN,
                        join.getHashJoinConjuncts(),
                        join.getOtherJoinConjuncts(),
                        new DistributeHint(DistributeType.NONE),
                        Optional.empty(),
                        leftChild, rightChild, null);
            }
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, PullUpContext context) {
            Plan input = project.child().accept(this, context);
            List<NamedExpression> outputs = input.getOutput().stream()
                    .map(e -> (NamedExpression) e).collect(Collectors.toList());
            for (NamedExpression expr : project.getProjects()) {
                // handle alias
                if (expr instanceof Alias && expr.child(0) instanceof Literal) {
                    outputs.add(expr);
                }
            }
            return new LogicalProject<>(outputs, input);
        }

        @Override
        public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, PullUpContext context) {
            Plan input = filter.child().accept(this, context);
            return new LogicalFilter<>(filter.getConjuncts(), input);
        }
    }
}

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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Push down MATCH expressions from join/filter predicates as virtual columns on OlapScan.
 *
 * When MATCH appears in a predicate that cannot be pushed below a join (e.g., OR with
 * join-dependent conditions like EXISTS mark or outer join null checks), this rule:
 * 1. Extracts the MATCH expression from the predicate
 * 2. Traces the alias slot back through the Project to find the original column expression
 * 3. Creates a virtual column on the OlapScan with the MATCH on the original expression
 * 4. Replaces the MATCH in the predicate with the virtual column's boolean slot
 *
 * Before:
 *   Filter(fn MATCH_ANY 'hello' OR l.col IS NOT NULL)
 *     └── Join[LEFT_OUTER]
 *           └── Project[objectId, CAST(col) as fn]
 *                 └── OlapScan[table]
 *           └── ...
 *
 * After:
 *   Filter(__match_vc OR l.col IS NOT NULL)
 *     └── Join[LEFT_OUTER]
 *           └── Project[objectId, fn, __match_vc]
 *                 └── OlapScan[table, virtualColumns=[(CAST(col) MATCH_ANY 'hello')]]
 *           └── ...
 *
 * NOTE: Currently only handles MATCH on the left side of a join. If the MATCH references
 * a column from the right side (e.g., RIGHT JOIN with Project on the right), this rule
 * will not trigger. This is acceptable for the primary use case (CTE + LEFT JOIN with
 * the main table on the left side).
 */
public class PushDownMatchPredicateAsVirtualColumn implements RewriteRuleFactory {

    private boolean canPushDown(LogicalOlapScan scan) {
        return MatchPushDownHelper.canPushDownMatch(scan);
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // Pattern 1: Filter -> Join -> Project -> OlapScan
                logicalFilter(logicalJoin(
                        logicalProject(logicalOlapScan().when(this::canPushDown)), any()))
                        .when(filter -> hasMatchInSet(filter.getConjuncts()))
                        .then(this::handleFilterProjectScan)
                        .toRule(RuleType.PUSH_DOWN_MATCH_PREDICATE_AS_VIRTUAL_COLUMN),

                // Pattern 2: Filter -> Join -> Project -> Filter -> OlapScan
                logicalFilter(logicalJoin(
                        logicalProject(logicalFilter(logicalOlapScan().when(this::canPushDown))), any()))
                        .when(filter -> hasMatchInSet(filter.getConjuncts()))
                        .then(this::handleFilterProjectFilterScan)
                        .toRule(RuleType.PUSH_DOWN_MATCH_PREDICATE_AS_VIRTUAL_COLUMN),

                // Pattern 3: Join(otherPredicates has MATCH) -> Project -> OlapScan
                logicalJoin(
                        logicalProject(logicalOlapScan().when(this::canPushDown)), any())
                        .when(join -> hasMatchInList(join.getOtherJoinConjuncts()))
                        .then(this::handleJoinProjectScan)
                        .toRule(RuleType.PUSH_DOWN_MATCH_PREDICATE_AS_VIRTUAL_COLUMN),

                // Pattern 4: Join(otherPredicates has MATCH) -> Project -> Filter -> OlapScan
                logicalJoin(
                        logicalProject(logicalFilter(logicalOlapScan().when(this::canPushDown))), any())
                        .when(join -> hasMatchInList(join.getOtherJoinConjuncts()))
                        .then(this::handleJoinProjectFilterScan)
                        .toRule(RuleType.PUSH_DOWN_MATCH_PREDICATE_AS_VIRTUAL_COLUMN)
        );
    }

    private Plan handleFilterProjectScan(LogicalFilter<LogicalJoin<LogicalProject<LogicalOlapScan>, Plan>> filter) {
        LogicalJoin<LogicalProject<LogicalOlapScan>, Plan> join = filter.child();
        LogicalProject<LogicalOlapScan> project = join.left();
        LogicalOlapScan scan = project.child();
        return doHandleFilter(filter, join, project, scan, newScan -> newScan);
    }

    private Plan handleFilterProjectFilterScan(
            LogicalFilter<LogicalJoin<LogicalProject<LogicalFilter<LogicalOlapScan>>, Plan>> filter) {
        LogicalJoin<LogicalProject<LogicalFilter<LogicalOlapScan>>, Plan> join = filter.child();
        LogicalProject<LogicalFilter<LogicalOlapScan>> project = join.left();
        LogicalFilter<LogicalOlapScan> scanFilter = project.child();
        LogicalOlapScan scan = scanFilter.child();
        return doHandleFilter(filter, join, project, scan,
                newScan -> scanFilter.withChildren(ImmutableList.of(newScan)));
    }

    private Plan handleJoinProjectScan(LogicalJoin<LogicalProject<LogicalOlapScan>, Plan> join) {
        LogicalProject<LogicalOlapScan> project = join.left();
        LogicalOlapScan scan = project.child();
        return doHandleJoin(join, project, scan, newScan -> newScan);
    }

    private Plan handleJoinProjectFilterScan(
            LogicalJoin<LogicalProject<LogicalFilter<LogicalOlapScan>>, Plan> join) {
        LogicalProject<LogicalFilter<LogicalOlapScan>> project = join.left();
        LogicalFilter<LogicalOlapScan> scanFilter = project.child();
        LogicalOlapScan scan = scanFilter.child();
        return doHandleJoin(join, project, scan,
                newScan -> scanFilter.withChildren(ImmutableList.of(newScan)));
    }

    private interface ScanRebuilder {
        Plan rebuild(LogicalOlapScan newScan);
    }

    private Plan doHandleFilter(LogicalFilter<?> filter, LogicalJoin<?, ?> join,
            LogicalProject<?> project, LogicalOlapScan scan, ScanRebuilder rebuilder) {
        Set<Slot> leftOutputSlots = ImmutableSet.copyOf(project.getOutput());
        List<Expression> predicateList = new ArrayList<>(filter.getConjuncts());
        PushDownResult result = buildVirtualColumnsFromList(predicateList, project, scan, leftOutputSlots);
        if (result == null) {
            return null;
        }

        LogicalProject<?> newProject = (LogicalProject<?>) project.withProjectsAndChild(
                result.newProjections, rebuilder.rebuild(result.newScan));
        Plan newJoin = join.withChildren(newProject, join.right());
        return filter.withConjunctsAndChild(ImmutableSet.copyOf(result.newPredicateList), newJoin);
    }

    private Plan doHandleJoin(LogicalJoin<?, ?> join, LogicalProject<?> project,
            LogicalOlapScan scan, ScanRebuilder rebuilder) {
        Set<Slot> leftOutputSlots = ImmutableSet.copyOf(project.getOutput());
        List<Expression> otherConjuncts = join.getOtherJoinConjuncts();
        PushDownResult result = buildVirtualColumnsFromList(otherConjuncts, project, scan, leftOutputSlots);
        if (result == null) {
            return null;
        }

        LogicalProject<?> newProject = (LogicalProject<?>) project.withProjectsAndChild(
                result.newProjections, rebuilder.rebuild(result.newScan));
        return join.withJoinConjuncts(join.getHashJoinConjuncts(),
                result.newPredicateList, join.getJoinReorderContext())
                .withChildren(newProject, join.right());
    }

    private boolean hasMatchInSet(Set<Expression> conjuncts) {
        return conjuncts.stream().anyMatch(this::containsMatch);
    }

    private boolean hasMatchInList(List<Expression> exprs) {
        return exprs.stream().anyMatch(this::containsMatch);
    }

    private boolean containsMatch(Expression expr) {
        if (expr instanceof Match) {
            return true;
        }
        for (Expression child : expr.children()) {
            if (containsMatch(child)) {
                return true;
            }
        }
        return false;
    }

    private PushDownResult buildVirtualColumnsFromList(List<Expression> predicates,
            LogicalProject<?> project, LogicalOlapScan scan, Set<Slot> leftOutputSlots) {
        Map<Match, Alias> matchToVirtualColumn = new HashMap<>();
        Map<Match, Slot> matchToVirtualSlot = new HashMap<>();

        for (Expression predicate : predicates) {
            collectMatchesNeedingPushDown(predicate, project, leftOutputSlots,
                    matchToVirtualColumn, matchToVirtualSlot);
        }

        if (matchToVirtualColumn.isEmpty()) {
            return null;
        }

        LogicalOlapScan newScan = scan.appendVirtualColumns(
                new ArrayList<>(matchToVirtualColumn.values()));

        List<NamedExpression> newProjections = new ArrayList<>(project.getProjects());
        for (Alias vcAlias : matchToVirtualColumn.values()) {
            newProjections.add(vcAlias.toSlot());
        }

        List<Expression> newPredicateList = new ArrayList<>();
        for (Expression predicate : predicates) {
            newPredicateList.add(replaceMatch(predicate, matchToVirtualSlot));
        }

        return new PushDownResult(newScan, newProjections, newPredicateList);
    }

    private void collectMatchesNeedingPushDown(Expression expr,
            LogicalProject<?> project, Set<Slot> leftOutputSlots,
            Map<Match, Alias> matchToVirtualColumn, Map<Match, Slot> matchToVirtualSlot) {
        if (expr instanceof Match) {
            Match match = (Match) expr;
            Set<Slot> inputSlots = match.left().getInputSlots();
            List<SlotReference> matchSlots = inputSlots.stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .collect(Collectors.toList());
            if (matchSlots.isEmpty()) {
                return;
            }

            // All slots must come from the left side of the join
            if (!matchSlots.stream().allMatch(leftOutputSlots::contains)) {
                return;
            }

            // If all slots have metadata, no need to push down
            boolean allHaveMetadata = matchSlots.stream()
                    .allMatch(s -> s.getOriginalColumn().isPresent() && s.getOriginalTable().isPresent());
            if (allHaveMetadata) {
                return;
            }

            // Use the first slot to trace back through the project
            SlotReference matchSlot = matchSlots.get(0);
            Expression sourceExpr = findSourceExpression(matchSlot, project);
            if (sourceExpr == null) {
                return;
            }

            Match newMatch = (Match) match.withChildren(
                    ImmutableList.of(sourceExpr, match.right()));
            Alias vcAlias = new Alias(newMatch);
            Slot vcSlot = vcAlias.toSlot();

            matchToVirtualColumn.put(match, vcAlias);
            matchToVirtualSlot.put(match, vcSlot);
            return;
        }

        for (Expression child : expr.children()) {
            collectMatchesNeedingPushDown(child, project, leftOutputSlots,
                    matchToVirtualColumn, matchToVirtualSlot);
        }
    }

    private Expression findSourceExpression(SlotReference slot, LogicalProject<?> project) {
        for (NamedExpression ne : project.getProjects()) {
            if (ne.getExprId().equals(slot.getExprId())) {
                if (ne instanceof Alias) {
                    return ((Alias) ne).child();
                } else if (ne instanceof SlotReference) {
                    return ne;
                }
            }
        }
        return null;
    }

    private Expression replaceMatch(Expression expr, Map<Match, Slot> matchToSlot) {
        if (expr instanceof Match && matchToSlot.containsKey(expr)) {
            return matchToSlot.get(expr);
        }

        boolean changed = false;
        List<Expression> newChildren = new ArrayList<>();
        for (Expression child : expr.children()) {
            Expression newChild = replaceMatch(child, matchToSlot);
            if (newChild != child) {
                changed = true;
            }
            newChildren.add(newChild);
        }

        if (!changed) {
            return expr;
        }
        return expr.withChildren(newChildren);
    }

    private static class PushDownResult {
        final LogicalOlapScan newScan;
        final List<NamedExpression> newProjections;
        final List<Expression> newPredicateList;

        PushDownResult(LogicalOlapScan newScan, List<NamedExpression> newProjections,
                List<Expression> newPredicateList) {
            this.newScan = newScan;
            this.newProjections = newProjections;
            this.newPredicateList = newPredicateList;
        }
    }
}

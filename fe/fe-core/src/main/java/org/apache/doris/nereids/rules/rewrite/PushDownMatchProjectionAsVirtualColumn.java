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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Materialize MATCH and SEARCH expressions as virtual columns on OlapScan.
 * Projections and residual Filter/Join conditions consume the resulting booleans,
 * while BE evaluates the search expressions using inverted indexes during scan.
 *
 * Example transformation:
 * Before:
 * Project[a, b, col MATCH_ANY 'hello']
 * └── OlapScan[table]
 *
 * After:
 * Project[a, b, virtual_slot_ref]
 * └── OlapScan[table, virtual_columns=[(col MATCH_ANY 'hello') as alias]]
 */
public class PushDownMatchProjectionAsVirtualColumn implements RewriteRuleFactory {

    private boolean canPushDown(LogicalOlapScan scan) {
        boolean dupTblOrMOW = scan.getTable().getKeysType() == KeysType.DUP_KEYS
                || (scan.getTable().getTableProperty() != null
                    && scan.getTable().getTableProperty().getEnableUniqueKeyMergeOnWrite());
        return dupTblOrMOW;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // Pattern 1: Project -> OlapScan
                logicalProject(logicalOlapScan().when(this::canPushDown))
                        .then(project -> {
                            LogicalOlapScan scan = project.child();
                            return pushDown(project, scan, newScan -> newScan);
                        }).toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN),
                // Pattern 2: Project -> Filter -> OlapScan
                logicalProject(logicalFilter(logicalOlapScan().when(this::canPushDown)))
                        .then(project -> {
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return pushDown(project, scan,
                                    newScan -> filter.withChildren(newScan));
                        }).toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN),
                logicalJoin().then(this::pushDownJoin)
                        .toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN),
                logicalFilter().when(filter -> !(filter.child() instanceof LogicalOlapScan))
                        .then(this::pushDownResidual)
                        .toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN),
                logicalProject().when(project -> !(project.child() instanceof LogicalOlapScan))
                        .then(this::pushDownResidual)
                        .toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN)
        );
    }

    /**
     * Extract MATCH projections and push them as virtual columns on the scan.
     * @param childRebuilder rebuilds the project's child tree with the new scan
     */
    private LogicalProject<?> pushDown(
            LogicalProject<?> project, LogicalOlapScan scan,
            Function<LogicalOlapScan, ? extends Plan> childRebuilder) {
        List<NamedExpression> projections = project.getProjects();
        List<NamedExpression> virtualColumns = new ArrayList<>();
        Map<Expression, Expression> replaceMap = new HashMap<>();

        for (NamedExpression projection : projections) {
            Expression matchExpr = unwrapMatch(projection);
            if (matchExpr != null && !replaceMap.containsKey(matchExpr)) {
                Alias alias = new Alias(matchExpr);
                replaceMap.put(matchExpr, alias.toSlot());
                virtualColumns.add(alias);
            }
        }

        if (virtualColumns.isEmpty()) {
            return null;
        }

        ImmutableList.Builder<NamedExpression> newProjections = ImmutableList.builder();
        for (NamedExpression projection : projections) {
            Expression matchExpr = unwrapMatch(projection);
            if (matchExpr != null && replaceMap.containsKey(matchExpr)) {
                Expression slot = replaceMap.get(matchExpr);
                if (projection instanceof Alias) {
                    newProjections.add(new Alias(((Alias) projection).getExprId(),
                            slot, ((Alias) projection).getName()));
                } else {
                    newProjections.add((NamedExpression) slot);
                }
            } else {
                newProjections.add(projection);
            }
        }

        LogicalOlapScan newScan = scan.appendVirtualColumns(virtualColumns);
        return (LogicalProject<?>) project.withProjectsAndChild(
                newProjections.build(), childRebuilder.apply(newScan));
    }

    private boolean isIndexSearch(Expression expression) {
        return expression instanceof Match || expression instanceof SearchExpression;
    }

    private Plan pushDownResidual(Plan plan) {
        Plan child = plan.child(0);
        Map<Expression, Expression> replacements = new LinkedHashMap<>();
        for (Expression expression : plan.getExpressions()) {
            for (Expression search : expression.<Expression>collect(e -> isIndexSearch((Expression) e))) {
                if (replacements.containsKey(search)) {
                    continue;
                }
                Pair<Plan, Slot> result = materialize(search, child);
                if (result != null) {
                    child = result.first;
                    replacements.put(search, result.second);
                }
            }
        }
        if (replacements.isEmpty()) {
            return null;
        }
        if (plan instanceof LogicalFilter) {
            Set<Expression> conjuncts = new LinkedHashSet<>();
            for (Expression expression : ((LogicalFilter<?>) plan).getConjuncts()) {
                conjuncts.add(ExpressionUtils.replace(expression, replacements));
            }
            // Hide additional scan values from the original filter's consumers.
            return new LogicalProject<>(ImmutableList.copyOf(plan.getOutput()),
                    new LogicalFilter<>(conjuncts, child));
        }
        LogicalProject<?> project = (LogicalProject<?>) plan;
        List<NamedExpression> projects = new ArrayList<>();
        for (NamedExpression expression : project.getProjects()) {
            projects.add((NamedExpression) ExpressionUtils.replace(expression, replacements));
        }
        return project.withProjectsAndChild(projects, child);
    }

    private Plan pushDownJoin(LogicalJoin<?, ?> join) {
        List<Plan> children = new ArrayList<>(join.children());
        Map<Expression, Expression> replacements = new LinkedHashMap<>();
        for (Expression expression : join.getExpressions()) {
            for (Expression search : expression.<Expression>collect(e -> isIndexSearch((Expression) e))) {
                if (replacements.containsKey(search)) {
                    continue;
                }
                for (int side = 0; side < children.size(); side++) {
                    // Join conditions consume child values before this join's NULL extension.
                    // This also handles WHERE predicates moved into an inner join by rewriting.
                    Pair<Plan, Slot> result = materialize(search, children.get(side));
                    if (result != null) {
                        children.set(side, result.first);
                        replacements.put(search, result.second);
                        break;
                    }
                }
            }
        }
        if (replacements.isEmpty()) {
            return null;
        }
        Plan rewritten = join.withConjunctsChildren(
                ExpressionUtils.replace(join.getHashJoinConjuncts(), replacements),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), replacements),
                ExpressionUtils.replace(join.getMarkJoinConjuncts(), replacements),
                children.get(0), children.get(1), join.getJoinReorderContext());
        return new LogicalProject<>(ImmutableList.copyOf(join.getOutput()), rewritten);
    }

    private Pair<Plan, Slot> materialize(Expression expression, Plan plan) {
        Set<Slot> inputs = expression.getInputSlots();
        if (inputs.isEmpty() || !plan.getOutputSet().containsAll(inputs)) {
            return null;
        }
        if (plan instanceof LogicalOlapScan) {
            LogicalOlapScan scan = (LogicalOlapScan) plan;
            if (!canPushDown(scan)) {
                return null;
            }
            for (NamedExpression column : scan.getVirtualColumns()) {
                if (column instanceof Alias && ((Alias) column).child().equals(expression)) {
                    return Pair.of(scan, column.toSlot());
                }
            }
            Alias alias = new Alias(expression);
            return Pair.of(scan.appendVirtualColumns(ImmutableList.of(alias)), alias.toSlot());
        }
        if (plan instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) plan;
            if (project.containsNoneMovableFunction()) {
                return null;
            }
            Expression rewritten = ExpressionUtils.replace(expression, project.getAliasToProducer());
            Pair<Plan, Slot> result = materialize(rewritten, project.child());
            if (result == null) {
                return null;
            }
            List<NamedExpression> projects = new ArrayList<>(project.getProjects());
            projects.add(result.second);
            return Pair.of(project.withProjectsAndChild(projects, result.first), result.second);
        }
        if (plan instanceof LogicalFilter) {
            Pair<Plan, Slot> result = materialize(expression, plan.child(0));
            return result == null ? null : Pair.of(plan.withChildren(result.first), result.second);
        }
        if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            for (int side = 0; side < 2; side++) {
                if (!join.child(side).getOutputSet().containsAll(inputs)) {
                    continue;
                }
                boolean nullExtended = side == 0 ? join.getJoinType().isLeftSideNullable()
                        : join.getJoinType().isRightSideNullable();
                // SEARCH has DSL-level existence and negation semantics. Do not assume
                // every DSL node is NULL-propagating across an outer join.
                if (nullExtended && expression instanceof SearchExpression) {
                    return null;
                }
                Pair<Plan, Slot> result = materialize(expression, join.child(side));
                if (result == null) {
                    return null;
                }
                List<Plan> children = new ArrayList<>(join.children());
                children.set(side, result.first);
                Plan newJoin = join.withChildren(children);
                for (Slot output : newJoin.getOutput()) {
                    if (output.getExprId().equals(result.second.getExprId())) {
                        return Pair.of(newJoin, output);
                    }
                }
                return null;
            }
        }
        return null;
    }

    /**
     * Unwrap a Match expression from a projection.
     * Returns the Match expression if the projection is a Match directly or an Alias wrapping a Match.
     * Returns null otherwise.
     */
    private Expression unwrapMatch(NamedExpression projection) {
        if (projection instanceof Alias && isIndexSearch(((Alias) projection).child())) {
            return ((Alias) projection).child();
        }
        return null;
    }
}

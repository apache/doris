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
import org.apache.doris.nereids.CascadesContext;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Materialize index searches (MATCH and SEARCH expressions) as virtual columns on OlapScan, so that BE evaluates
 * them with inverted indexes during the scan and the consumer reads the resulting boolean.
 *
 * <p>Consumers are projections, residual filters (a filter directly on a scan is evaluated by the scan itself)
 * and join conditions. All of them go through the same steps: collect every index search in the consumer's
 * expressions, however deeply nested, {@link #materialize} it in the child that produces its inputs, and
 * replace it with the virtual column slot.
 *
 * <p>{@link #materialize} is the only place that decides where an index search may be computed. It descends
 * through Project, Filter and Join down to one OlapScan that outputs all inputs, and refuses to cross the
 * NULL-extended side of an outer join unless the expression is NULL for NULL input. A MATCH that cannot be
 * materialized stays where it is; a SEARCH that is left outside a scan is rejected by CheckAfterRewrite.
 *
 * <p>The rule type keeps its original name because it is referenced by disable_nereids_rules.
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
public class PushDownIndexSearchAsVirtualColumn implements RewriteRuleFactory {

    private boolean canPushDown(LogicalOlapScan scan) {
        boolean dupTblOrMOW = scan.getTable().getKeysType() == KeysType.DUP_KEYS
                || (scan.getTable().getTableProperty() != null
                    && scan.getTable().getTableProperty().getEnableUniqueKeyMergeOnWrite());
        return dupTblOrMOW;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalProject().thenApply(ctx -> pushDownFromProject(ctx.root, ctx.cascadesContext))
                        .toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN),
                // A filter directly on the scan is pushed into the scan and evaluated there.
                logicalFilter().when(filter -> !(filter.child() instanceof LogicalOlapScan))
                        .thenApply(ctx -> pushDownFromFilter(ctx.root, ctx.cascadesContext))
                        .toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN),
                logicalJoin().thenApply(ctx -> pushDownFromJoin(ctx.root, ctx.cascadesContext))
                        .toRule(RuleType.PUSH_DOWN_MATCH_PROJECTION_AS_VIRTUAL_COLUMN)
        );
    }

    private boolean isIndexSearch(Expression expression) {
        return expression instanceof Match || expression instanceof SearchExpression;
    }

    /**
     * Materialize every index search of the consumer's expressions in one of the children, which are updated in
     * place. Returns the replacement of each materialized search.
     */
    private Map<Expression, Expression> materializeAll(List<? extends Expression> expressions,
            List<Plan> children, CascadesContext context) {
        Map<Expression, Expression> replacements = new LinkedHashMap<>();
        for (Expression expression : expressions) {
            for (Expression search : expression.<Expression>collect(e -> isIndexSearch((Expression) e))) {
                if (replacements.containsKey(search)) {
                    continue;
                }
                for (int i = 0; i < children.size(); i++) {
                    Pair<Plan, Slot> result = materialize(search, children.get(i), context);
                    if (result != null) {
                        children.set(i, result.first);
                        replacements.put(search, result.second);
                        break;
                    }
                }
            }
        }
        return replacements;
    }

    private Plan pushDownFromProject(LogicalProject<Plan> project, CascadesContext context) {
        List<Plan> children = new ArrayList<>(project.children());
        Map<Expression, Expression> replacements = materializeAll(project.getProjects(), children, context);
        if (replacements.isEmpty()) {
            return null;
        }
        List<NamedExpression> projects = new ArrayList<>();
        for (NamedExpression expression : project.getProjects()) {
            projects.add((NamedExpression) ExpressionUtils.replace(expression, replacements));
        }
        return project.withProjectsAndChild(projects, children.get(0));
    }

    private Plan pushDownFromFilter(LogicalFilter<Plan> filter, CascadesContext context) {
        List<Plan> children = new ArrayList<>(filter.children());
        Map<Expression, Expression> replacements = materializeAll(filter.getExpressions(), children, context);
        if (replacements.isEmpty()) {
            return null;
        }
        // Hide additional scan values from the original filter's consumers.
        return new LogicalProject<>(ImmutableList.copyOf(filter.getOutput()),
                new LogicalFilter<>(ExpressionUtils.replace(filter.getConjuncts(), replacements),
                        children.get(0)));
    }

    private Plan pushDownFromJoin(LogicalJoin<Plan, Plan> join, CascadesContext context) {
        // Join conditions consume child values before this join's NULL extension, so they are materialized
        // directly in the children. This also handles WHERE predicates moved into an inner join by rewriting.
        List<Plan> children = new ArrayList<>(join.children());
        Map<Expression, Expression> replacements = materializeAll(join.getExpressions(), children, context);
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

    /**
     * Compute the index search in the OlapScan below the plan that produces all its inputs, reusing an equal
     * virtual column of that scan. Returns the rewritten plan and the slot, part of its output, that carries the
     * value; or null when the search cannot be computed below the plan.
     */
    private Pair<Plan, Slot> materialize(Expression expression, Plan plan, CascadesContext context) {
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
            // Like PushDownFilterThroughProject: inlining a volatile producer would evaluate it a second time.
            Map<Slot, Expression> aliasToProducer = project.getAliasToProducer();
            if (inputs.stream().map(aliasToProducer::get)
                    .anyMatch(producer -> producer != null && producer.containsVolatileExpression())) {
                return null;
            }
            Expression rewritten = ExpressionUtils.replace(expression, aliasToProducer);
            Pair<Plan, Slot> result = materialize(rewritten, project.child(), context);
            if (result == null) {
                return null;
            }
            List<NamedExpression> projects = new ArrayList<>(project.getProjects());
            // A reused virtual column may already pass through this project.
            if (!project.getOutputSet().contains(result.second)) {
                projects.add(result.second);
            }
            return Pair.of(project.withProjectsAndChild(projects, result.first), result.second);
        }
        if (plan instanceof LogicalFilter) {
            Pair<Plan, Slot> result = materialize(expression, plan.child(0), context);
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
                // The join turns a value computed below it into NULL for NULL-extended rows, so only
                // an expression that is itself NULL for NULL input may be computed there. SEARCH has
                // DSL-level existence and negation semantics and is not NULL-propagating.
                if (nullExtended && !ExpressionUtils.isNullPropagating(expression, context)) {
                    return null;
                }
                Pair<Plan, Slot> result = materialize(expression, join.child(side), context);
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
}

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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionMatchingContext;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sleep;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**SimplifyConditionalFunction*/
public class SimplifyConditionalFunction implements ExpressionPatternRuleFactory {
    public static SimplifyConditionalFunction INSTANCE = new SimplifyConditionalFunction();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Coalesce.class).thenApply(SimplifyConditionalFunction::rewriteCoalesce)
                        .toRule(ExpressionRuleType.SIMPLIFY_CONDITIONAL_FUNCTION),
                matchesType(Nvl.class).thenApply(SimplifyConditionalFunction::rewriteNvl)
                        .toRule(ExpressionRuleType.SIMPLIFY_CONDITIONAL_FUNCTION),
                matchesType(NullIf.class).thenApply(SimplifyConditionalFunction::rewriteNullIf)
                        .toRule(ExpressionRuleType.SIMPLIFY_CONDITIONAL_FUNCTION),
                matchesType(If.class).thenApply(SimplifyConditionalFunction::rewriteIf)
                        .toRule(ExpressionRuleType.SIMPLIFY_CONDITIONAL_FUNCTION)
        );
    }

    /*
     * coalesce(null, ..., null, expr, null) => expr
     * coalesce(a, null, b, null) => coalesce(a, b)
     * coalesce(a, b_not_nullable, c) => coalesce(a, b_not_nullable)
     * coalesce(expr_not_nullable, ...) => expr_not_nullable
     * coalesce(null, null) => null
     * coalesce(expr) => expr
     * */
    private static Expression rewriteCoalesce(ExpressionMatchingContext<Coalesce> ctx) {
        Coalesce coalesce = ctx.expr;
        ImmutableList.Builder<Expression> childBuilder = ImmutableList.builder();
        for (int i = 0; i < coalesce.arity(); i++) {
            Expression child = coalesce.children().get(i);
            if (child instanceof NullLiteral) {
                continue;
            }
            childBuilder.add(child);
            if (!child.nullable()) {
                break;
            }
        }
        List<Expression> newChildren = childBuilder.build();
        if (newChildren.isEmpty()) {
            return TypeCoercionUtils.ensureSameResultType(
                    coalesce, new NullLiteral(coalesce.getDataType()), ctx.rewriteContext
            );
        } else if (newChildren.size() == 1) {
            return TypeCoercionUtils.ensureSameResultType(
                    coalesce, newChildren.get(0), ctx.rewriteContext
            );
        } else {
            if (1 == newChildren.size()) {
                return TypeCoercionUtils.ensureSameResultType(coalesce, newChildren.get(0), ctx.rewriteContext);
            } else {
                return TypeCoercionUtils.ensureSameResultType(
                        coalesce, coalesce.withChildren(newChildren), ctx.rewriteContext
                );
            }
        }
    }

    /*
    * nvl(null,R) => R
    * nvl(L(not-nullable ),R) => L
    * nvl(L,null) => L
    * */
    private static Expression rewriteNvl(ExpressionMatchingContext<Nvl> ctx) {
        Nvl nvl = ctx.expr;
        if (nvl.child(0) instanceof NullLiteral) {
            return TypeCoercionUtils.ensureSameResultType(nvl, nvl.child(1), ctx.rewriteContext);
        }
        if (!nvl.child(0).nullable() || nvl.child(1) instanceof NullLiteral) {
            return TypeCoercionUtils.ensureSameResultType(nvl, nvl.child(0), ctx.rewriteContext);
        }
        return nvl;
    }

    /*
    * nullif(null, R) => Null
    * nullif(L, null) => Null
    * nullif(null, null) => Null
     */
    private static Expression rewriteNullIf(ExpressionMatchingContext<NullIf> ctx) {
        NullIf nullIf = ctx.expr;
        if (nullIf.child(0) instanceof NullLiteral && nullIf.child(1) instanceof NullLiteral) {
            return TypeCoercionUtils.ensureSameResultType(nullIf, nullIf.child(0), ctx.rewriteContext);
        } else if (nullIf.child(0) instanceof NullLiteral || nullIf.child(1) instanceof NullLiteral) {
            return TypeCoercionUtils.ensureSameResultType(
                    nullIf, new Nullable(nullIf.child(0)), ctx.rewriteContext
            );
        } else {
            return nullIf;
        }
    }

    /*
     * if(cond, x, x) => x
     * Both branches are structurally identical, so the branch value is returned regardless of
     * the condition. Removing the condition is only sound when it cannot change observable
     * behavior, so the rewrite fires only when:
     *   1. then and else are structurally equal;
     *   2. the condition is deterministic (no rand()/now()/unique functions) so dropping its
     *      evaluation cannot remove an observable side effect;
     *   3. neither the condition NOR the branch itself contains a function whose evaluation is
     *      observable even when deterministic and error-free, i.e. NoneMovableFunction
     *      (contractually "should not prune", e.g. assert_true) or sleep() (a deterministic
     *      ScalarFunction whose whole point is the blocking side effect — the BE also refuses to
     *      fold it, see FoldConstantRuleOnBE). The branch must be checked too: BE's
     *      VectorizedFnCall::_do_execute evaluates the then- and else-argument columns
     *      unconditionally before FunctionIf selects between them, so if(cond, sleep(1), sleep(1))
     *      already runs sleep() twice per block; collapsing it to a single sleep(1) would halve
     *      that observable side effect even though the two branches are structurally identical;
     *   4. every subtree of the condition that may throw is also evaluated UNCONDITIONALLY by the
     *      surviving branch, so removing the condition cannot suppress a runtime error the original
     *      expression would have raised (e.g. Case3's ROUND(cost/denom,8) appears both in the
     *      condition and unconditionally inside CEIL(...) in the branch).
     * Nullability is preserved automatically: If.nullable() = then.nullable() || else.nullable(),
     * which equals then.nullable() when the branches are identical.
     */
    private static Expression rewriteIf(ExpressionMatchingContext<If> ctx) {
        If ifExpr = ctx.expr;
        Expression condition = ifExpr.child(0);
        Expression thenBranch = ifExpr.child(1);
        Expression elseBranch = ifExpr.child(2);
        if (!thenBranch.equals(elseBranch)) {
            return ifExpr;
        }
        if (condition.containsNondeterministic()) {
            return ifExpr;
        }
        // Functions that stay observable even when deterministic and error-free: dropping the
        // condition's evaluation would still change behavior (an assert_true never checked, one
        // fewer sleep). containsNondeterministic() does not cover them, so guard explicitly.
        // The branch itself needs the same guard: BE evaluates then/else unconditionally before
        // selecting, so collapsing if(cond, sleep(1), sleep(1)) to one sleep(1) would halve the
        // number of times it actually runs.
        if (condition.containsType(NoneMovableFunction.class, Sleep.class)
                || thenBranch.containsType(NoneMovableFunction.class, Sleep.class)) {
            return ifExpr;
        }
        ImmutableList.Builder<Expression> throwingSubtrees = ImmutableList.builder();
        collectThrowingSubtrees(condition, throwingSubtrees);
        for (Expression throwingSubtree : throwingSubtrees.build()) {
            if (!occursUnconditionally(thenBranch, throwingSubtree)) {
                return ifExpr;
            }
        }
        return TypeCoercionUtils.ensureSameResultType(ifExpr, thenBranch, ctx.rewriteContext);
    }

    // A node whose OWN evaluation cannot throw. Anything not listed is conservatively treated as
    // potentially throwing; this is a sound over-approximation (an unknown node is assumed unsafe),
    // avoiding an unmaintainable blacklist of every throwing expression class.
    private static boolean cannotThrowAtNode(Expression expr) {
        return expr instanceof Literal
                || expr instanceof Slot
                || expr instanceof ComparisonPredicate
                || expr instanceof CompoundPredicate
                || expr instanceof Not
                || expr instanceof IsNull
                || expr instanceof InPredicate;
    }

    // Operators that evaluate some of their children conditionally (short-circuit / guarded).
    // When scanning a branch for unconditionally-evaluated subtrees we must not descend past these.
    private static boolean isLazyBoundary(Expression expr) {
        return expr instanceof If
                || expr instanceof CaseWhen
                || expr instanceof Coalesce
                || expr instanceof Nvl
                || expr instanceof NullIf
                || expr instanceof CompoundPredicate
                || expr instanceof Lambda;
    }

    // Collect the maximal subtrees of the condition that may throw: descend through no-throw nodes,
    // and when a node that may throw is reached, record that whole subtree (its children are subsumed).
    private static void collectThrowingSubtrees(Expression expr, ImmutableList.Builder<Expression> out) {
        if (!cannotThrowAtNode(expr)) {
            out.add(expr);
            return;
        }
        for (Expression child : expr.children()) {
            collectThrowingSubtrees(child, out);
        }
    }

    // True if target occurs as an unconditionally-evaluated subtree of branch, i.e. reachable from
    // the branch root without crossing a lazy/guarded boundary.
    private static boolean occursUnconditionally(Expression branch, Expression target) {
        if (branch.equals(target)) {
            return true;
        }
        if (isLazyBoundary(branch)) {
            return false;
        }
        for (Expression child : branch.children()) {
            if (occursUnconditionally(child, target)) {
                return true;
            }
        }
        return false;
    }
}

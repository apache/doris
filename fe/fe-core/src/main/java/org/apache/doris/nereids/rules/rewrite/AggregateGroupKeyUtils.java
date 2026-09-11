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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TryCast;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Shared safety proofs for removing aggregate group keys. */
final class AggregateGroupKeyUtils {
    private static final ImmutableSet<Class<? extends Expression>> TOTAL_INTEGRAL_ARITHMETIC
            = ImmutableSet.of(Add.class, Subtract.class, Multiply.class);

    private AggregateGroupKeyUtils() {
    }

    static List<Expression> simplifyGroupBy(List<Expression> groupByExpressions) {
        List<Expression> distinctGroupBy = ImmutableList.copyOf(new LinkedHashSet<>(groupByExpressions));
        Map<Slot, Expression> determinants = collectDeterminants(distinctGroupBy);
        ImmutableList.Builder<Expression> simplified = ImmutableList.builder();
        boolean removedExpression = false;
        for (Expression expression : distinctGroupBy) {
            Optional<Slot> determinantSlot = extractInjectiveBaseSlot(expression);
            Expression retainedDeterminant = determinantSlot
                    .map(determinants::get)
                    .orElse(null);
            if (retainedDeterminant != null && !retainedDeterminant.equals(expression)) {
                removedExpression = true;
                continue;
            }
            Optional<Slot> dependentSlot = extractTotalDependentSlot(expression);
            if (dependentSlot.isPresent() && determinants.containsKey(dependentSlot.get())) {
                removedExpression = true;
                continue;
            }
            simplified.add(expression);
        }
        if (!removedExpression && distinctGroupBy.size() == groupByExpressions.size()) {
            return null;
        }
        return simplified.build();
    }

    /**
     * Return whether an expression made redundant by a child-plan functional dependency can be
     * removed without suppressing NULL or error behavior. Project-produced slots are resolved to
     * their definitions first. Any ambiguous or incomplete lineage fails closed.
     */
    static boolean canSafelyEliminateByFunctionalDependency(Expression expression, Plan child) {
        return resolveAndCheck(expression, child, new HashSet<>());
    }

    private static boolean resolveAndCheck(Expression expression, Plan child, Set<ExprId> resolvingSlots) {
        Optional<ResolvedExpression> resolved = resolveExpression(expression, child, resolvingSlots);
        if (!resolved.isPresent()) {
            return false;
        }
        Expression resolvedExpression = resolved.get().expression;
        if (resolvedExpression instanceof Slot) {
            return true;
        }

        Optional<Slot> dependencySlot = resolvedExpression instanceof Cast
                ? traceNonFailingCastToSlot(resolvedExpression)
                : extractTotalDependentSlot(resolvedExpression);
        return dependencySlot.isPresent()
                && resolveAndCheck(dependencySlot.get(), resolved.get().child, resolvingSlots);
    }

    private static Optional<ResolvedExpression> resolveExpression(
            Expression expression, Plan child, Set<ExprId> resolvingSlots) {
        while (expression instanceof Alias) {
            expression = expression.child(0);
        }
        if (!(expression instanceof Slot)) {
            return Optional.of(new ResolvedExpression(expression, child));
        }

        Slot slot = (Slot) expression;
        if (!resolvingSlots.add(slot.getExprId())) {
            return Optional.empty();
        }
        return resolveSlotInPlan(slot, child, resolvingSlots);
    }

    private static Optional<ResolvedExpression> resolveSlotInPlan(
            Slot slot, Plan plan, Set<ExprId> resolvingSlots) {
        if (plan instanceof LogicalProject) {
            LogicalProject<? extends Plan> project = (LogicalProject<? extends Plan>) plan;
            List<NamedExpression> definitions = new ArrayList<>();
            for (NamedExpression projected : project.getProjects()) {
                if (projected.toSlot().equals(slot)) {
                    definitions.add(projected);
                }
            }
            if (definitions.size() != 1) {
                return Optional.empty();
            }

            Expression definition = definitions.get(0);
            if (definition instanceof Alias) {
                definition = definition.child(0);
            }
            if (definition instanceof Slot && definition.equals(slot)) {
                return resolveSlotInPlan(slot, project.child(), resolvingSlots);
            }
            return resolveExpression(definition, project.child(), resolvingSlots);
        }

        List<Plan> producingChildren = new ArrayList<>();
        for (Plan planChild : plan.children()) {
            if (planChild.getOutput().contains(slot)) {
                producingChildren.add(planChild);
            }
        }
        if (producingChildren.size() == 1) {
            return resolveSlotInPlan(slot, producingChildren.get(0), resolvingSlots);
        }
        if (!producingChildren.isEmpty()) {
            return Optional.empty();
        }
        if (plan instanceof LogicalCatalogRelation && plan.getOutput().contains(slot)) {
            return Optional.of(new ResolvedExpression(slot, plan));
        }
        return Optional.empty();
    }

    private static Map<Slot, Expression> collectDeterminants(List<Expression> groupByExpressions) {
        Map<Slot, Expression> determinants = new LinkedHashMap<>();
        // Keep only an existing bare slot as a determinant. Although an injective cast preserves
        // grouping equivalence, the aggregate cannot generally reconstruct the original slot from
        // the cast result when a removed group-key expression is still referenced by the output.
        for (Expression expression : groupByExpressions) {
            if (expression instanceof Slot) {
                determinants.put((Slot) expression, expression);
            }
        }
        return determinants;
    }

    private static Optional<Slot> extractTotalDependentSlot(Expression expression) {
        if (!(expression instanceof BinaryArithmetic)
                || !TOTAL_INTEGRAL_ARITHMETIC.contains(expression.getClass())) {
            return Optional.empty();
        }
        // Integral arithmetic is total in Doris: overflow uses the integral result semantics.
        // Decimal arithmetic can raise overflow, and division/date/time arithmetic can also
        // produce NULL or errors, so removing those expressions is not semantics-safe.
        if (!expression.getDataType().isIntegralType()
                || !expression.child(0).getDataType().isIntegralType()
                || !expression.child(1).getDataType().isIntegralType()) {
            return Optional.empty();
        }

        Expression slotExpression;
        Literal literal;
        if (expression.child(0) instanceof Literal && !(expression.child(1) instanceof Literal)) {
            literal = (Literal) expression.child(0);
            slotExpression = expression.child(1);
        } else if (expression.child(1) instanceof Literal && !(expression.child(0) instanceof Literal)) {
            literal = (Literal) expression.child(1);
            slotExpression = expression.child(0);
        } else {
            return Optional.empty();
        }
        if (literal.isNullLiteral()) {
            return Optional.empty();
        }
        return traceNonFailingCastToSlot(slotExpression);
    }

    private static Optional<Slot> traceNonFailingCastToSlot(Expression expression) {
        Expression current = expression;
        while (current instanceof Cast) {
            Cast cast = (Cast) current;
            // TRY_CAST turns conversion failures into a deterministic NULL. An ordinary CAST may
            // instead raise under session or system strictness, so only a proven lossless cast can
            // be removed without suppressing that behavior.
            if (!(cast instanceof TryCast)
                    && !isProvenInjectiveCast(cast.child().getDataType(), cast.getDataType())) {
                return Optional.empty();
            }
            current = cast.child();
        }
        return current instanceof Slot ? Optional.of((Slot) current) : Optional.empty();
    }

    private static Optional<Slot> extractInjectiveBaseSlot(Expression expression) {
        Expression current = expression;
        while (current instanceof Cast) {
            Cast cast = (Cast) current;
            if (!isProvenInjectiveCast(cast.child().getDataType(), cast.getDataType())) {
                return Optional.empty();
            }
            current = cast.child();
        }
        return current instanceof Slot ? Optional.of((Slot) current) : Optional.empty();
    }

    static boolean isProvenInjectiveCast(DataType sourceType, DataType targetType) {
        if (sourceType.equals(targetType)) {
            return true;
        }
        // DataType.isInjectiveCastTo intentionally has broad compatibility answers for character
        // and complex types. Those answers do not prove that serialized SQL values remain unique.
        if (sourceType.isStringLikeType() || targetType.isStringLikeType()
                || sourceType.isComplexType() || targetType.isComplexType()) {
            return false;
        }
        boolean auditedScalarFamily = (sourceType.isNumericType() && targetType.isNumericType())
                || (sourceType.isDateLikeType() && targetType.isDateLikeType())
                || (sourceType.isTimeType() && targetType.isTimeType());
        return auditedScalarFamily && sourceType.isInjectiveCastTo(targetType);
    }

    private static class ResolvedExpression {
        final Expression expression;
        final Plan child;

        ResolvedExpression(Expression expression, Plan child) {
            this.expression = expression;
            this.child = child;
        }
    }
}

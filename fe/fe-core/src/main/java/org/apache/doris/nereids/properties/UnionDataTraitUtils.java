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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NumericLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Union;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Shared equal-set derivation for logical and physical union plans. */
public final class UnionDataTraitUtils {

    private UnionDataTraitUtils() {
    }

    /** Compute output equal pairs that hold for every row source of a union. */
    public static void computeEqualSet(Union union, Plan unionPlan, DataTrait.Builder builder) {
        List<Slot> outputs = unionPlan.getOutput();
        List<Plan> children = unionPlan.children();
        List<List<SlotReference>> childrenOutputs = union.getRegularChildrenOutputs();
        List<List<NamedExpression>> constantRows = union.getConstantExprsList();
        if (outputs.size() < 2 || (children.isEmpty() && constantRows.isEmpty())) {
            return;
        }
        if (children.size() != childrenOutputs.size()) {
            return;
        }
        // An incomplete or over-complete mapping cannot prove anything about union ordinals.
        for (List<SlotReference> childOutputs : childrenOutputs) {
            if (childOutputs.size() != outputs.size()) {
                return;
            }
        }

        List<List<Integer>> equalGroups = children.isEmpty()
                ? oneGroupForAllOutputs(outputs.size())
                : intersectChildEqualGroups(children, childrenOutputs, outputs.size());

        if (!constantRows.isEmpty() && !equalGroups.isEmpty()) {
            Optional<ExpressionRewriteContext> context = createRewriteContext(unionPlan);
            for (List<NamedExpression> row : constantRows) {
                equalGroups = refineByConstantRow(equalGroups, row, context, outputs.size());
                if (equalGroups.isEmpty()) {
                    return;
                }
            }
        }

        for (List<Integer> equalGroup : equalGroups) {
            int first = equalGroup.get(0);
            for (int i = 1; i < equalGroup.size(); i++) {
                builder.addEqualPair(outputs.get(first), outputs.get(equalGroup.get(i)));
            }
        }
    }

    private static List<List<Integer>> intersectChildEqualGroups(List<Plan> children,
            List<List<SlotReference>> childrenOutputs, int outputSize) {
        List<List<Integer>> classIdsByChild = new ArrayList<>(children.size());
        for (int childIndex = 0; childIndex < children.size(); childIndex++) {
            classIdsByChild.add(equalClassIds(children.get(childIndex), childrenOutputs.get(childIndex)));
        }

        Map<List<Integer>, List<Integer>> ordinalsBySignature = new LinkedHashMap<>();
        for (int outputIndex = 0; outputIndex < outputSize; outputIndex++) {
            List<Integer> signature = new ArrayList<>(children.size());
            for (List<Integer> childClassIds : classIdsByChild) {
                signature.add(childClassIds.get(outputIndex));
            }
            ordinalsBySignature.computeIfAbsent(signature, key -> new ArrayList<>()).add(outputIndex);
        }
        return onlyNonTrivialGroups(ordinalsBySignature.values());
    }

    private static List<Integer> equalClassIds(Plan child, List<SlotReference> childOutputs) {
        DataTrait childTrait = child.getLogicalProperties().getTrait();
        Map<Slot, Integer> classIdBySlot = new HashMap<>();
        int nextClassId = 0;
        for (Set<Slot> equalSet : childTrait.calAllEqualSet()) {
            for (Slot slot : equalSet) {
                classIdBySlot.put(slot, nextClassId);
            }
            nextClassId++;
        }

        List<Integer> classIds = new ArrayList<>(childOutputs.size());
        for (Slot childOutput : childOutputs) {
            Integer classId = classIdBySlot.get(childOutput);
            if (classId == null) {
                classId = nextClassId++;
                classIdBySlot.put(childOutput, classId);
            }
            classIds.add(classId);
        }
        return classIds;
    }

    private static List<List<Integer>> refineByConstantRow(List<List<Integer>> equalGroups,
            List<NamedExpression> row, Optional<ExpressionRewriteContext> context, int outputSize) {
        if (row.size() != outputSize) {
            return new ArrayList<>();
        }
        List<Optional<Literal>> literals = new ArrayList<>(outputSize);
        for (NamedExpression expression : row) {
            literals.add(foldConstant(unwrapAlias(expression), context));
        }

        List<List<Integer>> refinedGroups = new ArrayList<>();
        for (List<Integer> equalGroup : equalGroups) {
            Map<ConstantValueKey, List<Integer>> ordinalsByValue = new LinkedHashMap<>();
            for (int outputIndex : equalGroup) {
                Optional<ConstantValueKey> key = literals.get(outputIndex).flatMap(
                        UnionDataTraitUtils::constantValueKey);
                key.ifPresent(valueKey -> ordinalsByValue
                        .computeIfAbsent(valueKey, ignored -> new ArrayList<>()).add(outputIndex));
            }
            for (List<Integer> sameValueOrdinals : ordinalsByValue.values()) {
                if (sameValueOrdinals.size() <= 1) {
                    continue;
                }
                int first = sameValueOrdinals.get(0);
                boolean allProvenEqual = true;
                for (int i = 1; i < sameValueOrdinals.size(); i++) {
                    if (!isEqualInConstantRow(row, first, sameValueOrdinals.get(i), context)) {
                        allProvenEqual = false;
                        break;
                    }
                }
                if (allProvenEqual) {
                    refinedGroups.add(sameValueOrdinals);
                }
            }
        }
        return refinedGroups;
    }

    private static boolean isEqualInConstantRow(List<NamedExpression> row, int left, int right,
            Optional<ExpressionRewriteContext> context) {
        try {
            Expression leftExpression = unwrapAlias(row.get(left));
            Expression rightExpression = unwrapAlias(row.get(right));
            Expression equality = TypeCoercionUtils.processComparisonPredicate(
                    new EqualTo(leftExpression, rightExpression));
            Optional<Literal> result = ExpressionUtils.checkConstantExpr(equality, context);
            // NULL = NULL is UNKNOWN, so only a folded TRUE is a proof of equality.
            return result.isPresent() && BooleanLiteral.TRUE.equals(result.get());
        } catch (RuntimeException e) {
            // Unsupported coercion or an expression that cannot be folded is not proof.
            return false;
        }
    }

    private static Optional<Literal> foldConstant(Expression expression,
            Optional<ExpressionRewriteContext> context) {
        try {
            return ExpressionUtils.checkConstantExpr(expression, context);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Optional<ConstantValueKey> constantValueKey(Literal literal) {
        if (literal.isNullLiteral()) {
            return Optional.empty();
        }
        try {
            if (literal instanceof NumericLiteral) {
                BigDecimal value = ((NumericLiteral) literal).getBigDecimalValue().stripTrailingZeros();
                return Optional.of(new ConstantValueKey(NumericLiteral.class, value));
            } else if (literal instanceof StringLikeLiteral) {
                return Optional.of(new ConstantValueKey(StringLikeLiteral.class, literal.getStringValue()));
            } else if (literal instanceof DateLiteral) {
                return Optional.of(new ConstantValueKey(DateLiteral.class, literal.getStringValue()));
            }
            return Optional.of(new ConstantValueKey(literal.getClass(), literal));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Expression unwrapAlias(NamedExpression expression) {
        Expression unwrapped = expression;
        while (unwrapped instanceof Alias) {
            unwrapped = unwrapped.child(0);
        }
        return unwrapped;
    }

    private static Optional<ExpressionRewriteContext> createRewriteContext(Plan plan) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null || connectContext.getStatementContext() == null) {
            return Optional.empty();
        }
        return Optional.of(new ExpressionRewriteContext(plan, CascadesContext.initContext(
                connectContext.getStatementContext(), plan, PhysicalProperties.ANY)));
    }

    private static List<List<Integer>> oneGroupForAllOutputs(int outputSize) {
        List<Integer> allOutputs = new ArrayList<>(outputSize);
        for (int outputIndex = 0; outputIndex < outputSize; outputIndex++) {
            allOutputs.add(outputIndex);
        }
        List<List<Integer>> groups = new ArrayList<>(1);
        groups.add(allOutputs);
        return groups;
    }

    private static List<List<Integer>> onlyNonTrivialGroups(Iterable<List<Integer>> groups) {
        List<List<Integer>> nonTrivialGroups = new ArrayList<>();
        for (List<Integer> group : groups) {
            if (group.size() > 1) {
                nonTrivialGroups.add(group);
            }
        }
        return nonTrivialGroups;
    }

    private static final class ConstantValueKey {
        private final Class<?> family;
        private final Object value;

        private ConstantValueKey(Class<?> family, Object value) {
            this.family = family;
            this.value = value;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof ConstantValueKey)) {
                return false;
            }
            ConstantValueKey that = (ConstantValueKey) object;
            return family.equals(that.family) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(family, value);
        }
    }
}

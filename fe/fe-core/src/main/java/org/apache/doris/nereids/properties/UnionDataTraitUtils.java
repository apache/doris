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

/**
 * Derives equal sets shared by logical and physical union plans.
 *
 * <p>Two union output slots are equal only when the corresponding values are equal in every regular
 * child and every constant row. Regular children contribute equality information through their data
 * traits, while constant rows contribute equality information through constant folding and SQL
 * comparison semantics.
 */
public final class UnionDataTraitUtils {

    /** Utility class; it must not be instantiated. */
    private UnionDataTraitUtils() {
    }

    /**
     * Computes union output equalities that hold for every row source and adds them to {@code builder}.
     *
     * <p>An output ordinal identifies the same union column across {@code outputs}, every entry in
     * {@code regularChildrenOutputs}, and every constant row. For regular children, this method keeps
     * only groups of ordinals whose mapped child slots belong to the same equality class in every
     * child. It then refines those groups with every constant row. A constant row retains a pair only
     * when folding their expressions and evaluating their SQL equality produces {@code TRUE}.
     *
     * <p>The union is assumed to satisfy the structural invariants established during analysis: each
     * regular child has one output mapping and every regular or constant input has the union output
     * width. This method does not validate those invariants. It leaves existing entries in
     * {@code builder} unchanged and only adds equal pairs proven by all union inputs.
     *
     * @param union union metadata that supplies regular-child output mappings and constant rows
     * @param unionPlan concrete logical or physical union plan that supplies children and output slots
     * @param builder destination to which proven equal pairs between union output slots are added
     */
    public static void computeEqualSet(Union union, Plan unionPlan, DataTrait.Builder builder) {
        List<Slot> outputs = unionPlan.getOutput();
        List<Plan> children = unionPlan.children();
        List<List<SlotReference>> childrenOutputs = union.getRegularChildrenOutputs();
        List<List<NamedExpression>> constantRows = union.getConstantExprsList();
        if (outputs.size() < 2 || (children.isEmpty() && constantRows.isEmpty())) {
            return;
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

    /**
     * Intersects the equality partitions of all regular union children by union output ordinal.
     *
     * <p>For each output ordinal, this method builds a signature containing that ordinal's equality
     * class ID in every child. Two output ordinals have the same signature exactly when their mapped
     * child slots are equal in every regular child. Singleton signature groups are omitted because
     * they do not describe an equality between different union outputs.
     *
     * @param children regular union children; child {@code i} corresponds to mapping {@code i}
     * @param childrenOutputs mapped child slots indexed first by child and then by union output ordinal
     * @param outputSize number of union output ordinals represented by every child mapping
     * @return groups of at least two output ordinals that are equal in every regular child
     */
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

    /**
     * Encodes one child's mapped output slots as equality-class IDs.
     *
     * <p>Slots in the same child data-trait equal set receive the same ID. A mapped slot not present in
     * any equal set receives its own ID, so it cannot accidentally compare equal to a different slot.
     * Repeated occurrences of the same mapped slot reuse the same ID.
     *
     * @param child child plan whose logical data trait defines slot equalities
     * @param childOutputs child slots in union output-ordinal order
     * @return class IDs in union output-ordinal order; equal IDs denote equal mapped child slots
     */
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

    /**
     * Refines candidate output equality groups using one constant row.
     *
     * <p>Each expression is first folded to a literal when possible. Candidate ordinals are bucketed by
     * a normalized {@link ConstantValueKey} to avoid comparing values that clearly differ. Every
     * multi-ordinal bucket is then verified with SQL equality against its first ordinal. An ordinal
     * whose expression is NULL, cannot be folded, cannot be normalized, or cannot be proven equal is
     * omitted from the returned groups.
     *
     * @param equalGroups candidate output-ordinal groups proven equal by inputs processed so far
     * @param row constant expressions in union output-ordinal order
     * @param context optional rewrite context used while folding constants and comparisons
     * @param outputSize number of union outputs, used to size the per-ordinal literal list
     * @return non-singleton subgroups whose expressions are also proven equal in this constant row
     */
    private static List<List<Integer>> refineByConstantRow(List<List<Integer>> equalGroups,
            List<NamedExpression> row, Optional<ExpressionRewriteContext> context, int outputSize) {
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

    /**
     * Tests whether two ordinals in a constant row are provably equal under SQL comparison semantics.
     *
     * <p>The expressions are unwrapped, coerced as operands of {@link EqualTo}, and constant-folded.
     * Only the literal result {@code TRUE} proves equality; {@code FALSE}, SQL {@code NULL}/unknown,
     * an unavailable fold result, and unsupported coercion or folding all return {@code false}.
     *
     * @param row constant expressions in union output-ordinal order
     * @param left ordinal of the left expression to compare
     * @param right ordinal of the right expression to compare
     * @param context optional rewrite context used for constant evaluation
     * @return {@code true} only if the coerced equality folds to {@link BooleanLiteral#TRUE}
     */
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

    /**
     * Attempts to fold an expression to a literal without allowing a fold failure to publish a trait.
     *
     * @param expression expression to evaluate as a constant
     * @param context optional rewrite context used by constant evaluation
     * @return the folded literal, or an empty optional when the expression cannot be folded safely
     */
    private static Optional<Literal> foldConstant(Expression expression,
            Optional<ExpressionRewriteContext> context) {
        try {
            return ExpressionUtils.checkConstantExpr(expression, context);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * Builds a normalized key used to bucket literals that may be equal.
     *
     * <p>All numeric literals share a numeric family and use a scale-insensitive decimal value.
     * String-like and date literals are grouped within their respective families by string value.
     * Other literals retain their concrete class and literal object. NULL has no key because SQL NULL
     * never proves equality, and any failure while extracting a value yields an empty optional. Key
     * equality is only a prefilter; {@link #isEqualInConstantRow(List, int, int, Optional)} performs the
     * final SQL-semantic proof.
     *
     * @param literal folded literal to normalize
     * @return a normalized non-NULL value key, or an empty optional if no safe key can be produced
     */
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

    /**
     * Removes all outer alias layers from a named expression.
     *
     * @param expression named expression whose underlying value expression is needed
     * @return the first expression below all consecutive outer {@link Alias} nodes
     */
    private static Expression unwrapAlias(NamedExpression expression) {
        Expression unwrapped = expression;
        while (unwrapped instanceof Alias) {
            unwrapped = unwrapped.child(0);
        }
        return unwrapped;
    }

    /**
     * Creates the rewrite context needed for context-dependent constant folding when one is available.
     *
     * <p>Trait derivation can run without a thread-local connection or statement context. In that case
     * callers receive an empty optional and constant evaluation decides whether it can proceed without
     * the context.
     *
     * @param plan union plan used as the root of the temporary cascades and expression rewrite contexts
     * @return a rewrite context for the current statement, or an empty optional when none is available
     */
    private static Optional<ExpressionRewriteContext> createRewriteContext(Plan plan) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null || connectContext.getStatementContext() == null) {
            return Optional.empty();
        }
        return Optional.of(new ExpressionRewriteContext(plan, CascadesContext.initContext(
                connectContext.getStatementContext(), plan, PhysicalProperties.ANY)));
    }

    /**
     * Creates the initial candidate group used when a union has only constant rows.
     *
     * <p>With no regular child, no child trait can rule out equality, so all output ordinals begin in
     * one candidate group. Each constant row subsequently splits or removes members from this group.
     *
     * @param outputSize number of union output slots
     * @return one group containing every ordinal from zero (inclusive) to {@code outputSize} (exclusive)
     */
    private static List<List<Integer>> oneGroupForAllOutputs(int outputSize) {
        List<Integer> allOutputs = new ArrayList<>(outputSize);
        for (int outputIndex = 0; outputIndex < outputSize; outputIndex++) {
            allOutputs.add(outputIndex);
        }
        List<List<Integer>> groups = new ArrayList<>(1);
        groups.add(allOutputs);
        return groups;
    }

    /**
     * Removes singleton and empty ordinal groups that cannot express equality between distinct outputs.
     *
     * @param groups candidate ordinal groups in the order they should be considered
     * @return a new outer list containing the original group objects whose size is greater than one
     */
    private static List<List<Integer>> onlyNonTrivialGroups(Iterable<List<Integer>> groups) {
        List<List<Integer>> nonTrivialGroups = new ArrayList<>();
        for (List<Integer> group : groups) {
            if (group.size() > 1) {
                nonTrivialGroups.add(group);
            }
        }
        return nonTrivialGroups;
    }

    /**
     * Normalized identity used to pre-group folded constants before evaluating SQL equality.
     *
     * <p>The key deliberately combines a literal family with a canonical value. The family prevents
     * unrelated literal categories from sharing a bucket, while allowing representations within a
     * supported category, such as different numeric literal classes, to meet in the same bucket. A
     * matching key is only a cheap candidate signal; it is never used by itself as proof of equality.
     */
    private static final class ConstantValueKey {
        /** Literal category used to keep values from unrelated SQL type families in separate buckets. */
        private final Class<?> family;

        /** Canonical value compared within {@link #family}, such as a scale-normalized decimal. */
        private final Object value;

        /**
         * Creates a key from a literal family and its canonical non-NULL value.
         *
         * @param family normalized literal category used as the first part of key identity
         * @param value canonical value within {@code family}, used as the second part of key identity
         */
        private ConstantValueKey(Class<?> family, Object value) {
            this.family = family;
            this.value = value;
        }

        /**
         * Compares both normalized components of this key with another object.
         *
         * @param object object to compare with this key
         * @return {@code true} when {@code object} is a key with the same family and canonical value
         */
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

        /**
         * Computes a hash from the same family and canonical value used by {@link #equals(Object)}.
         *
         * @return hash code for this normalized constant key
         */
        @Override
        public int hashCode() {
            return Objects.hash(family, value);
        }
    }
}

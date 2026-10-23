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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;

import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Canonical description and fingerprint of the equality conditions of a join, used as the key of
 * manually injected join expansion entries ({@code HBO SET EXPANSION}).
 *
 * <p>The join type and the residual (non equality) conditions take part in the key next to the hash
 * (equality) conjuncts, because the injected factor replaces the output rows of the whole join: an
 * inner join which matches nothing and a left outer join of the same columns produce very different
 * row counts, and a selective residual predicate changes the output by orders of magnitude. The
 * shape of both sub trees is still excluded, so one injected entry stays reusable by every query
 * which joins the same columns with the same join type and the same residual conditions.
 *
 * <p>The canonical form is {@code JE{<join type>;EqualTo(...);...}} with the pairs and the residual
 * conditions sorted; a pair is rendered by {@link GroupStructInfo#normalizeExpression(Expression)}
 * which already sorts the two operands of an equality (equality is commutative), so {@code a = b}
 * and {@code b = a} collapse. A residual condition is rendered with its literal in both modes,
 * because it decides how many partner rows a row finds ({@code b > c + 1} is not {@code b > c + 2}).
 */
public class HboJoinConditions {
    private static final String PREFIX = "JE{";
    private static final String SEP = ";";

    private HboJoinConditions() {
    }

    /** True when the injected expansion of this join type is applicable at all. */
    public static boolean isExpansionApplicable(JoinType joinType) {
        // semi / anti / mark style joins can never produce more rows than their left input, and
        // asof joins match at most one row per left row, so expansion can not happen there
        return joinType == JoinType.INNER_JOIN
                || joinType == JoinType.LEFT_OUTER_JOIN
                || joinType == JoinType.RIGHT_OUTER_JOIN
                || joinType == JoinType.FULL_OUTER_JOIN;
    }

    /**
     * Canonical string of the conditions of a join, empty when the join has no expansion key at all.
     *
     * <p>The key describes the <b>equality</b> conditions only: the factor is the fan out of the equi
     * join, and the non equality conditions are applied by the estimator on top of it (see
     * {@code JoinEstimation}), so a join with a residual predicate uses the factor of its equality pair
     * and the residual keeps its own selectivity. The operands have to name the columns of the tables
     * themselves: an operand which is not a direct slot of a catalog table (a computed column, an
     * aggregate output, a cast) cannot name its table, so such a join has no key. A null safe equality
     * ({@code <=>}) is a hash conjunct as well and takes part like a plain equality.
     */
    public static Optional<String> canonicalOf(Join join) {
        return keyParts(join).map(parts -> PREFIX + String.join(SEP, parts) + "}");
    }

    /**
     * The key of the expansion entry of a join: the sha256 of its structural identity, empty when the
     * join has no expansion key (see {@link #canonicalOf}). The canonical string is the readable form of
     * the same parts: it is printed by EXPLAIN and stored as the struct of the entry, and the key is not
     * derived from the text.
     */
    public static Optional<String> fingerprintOf(Join join) {
        return keyParts(join).map(parts -> {
            ShapeKeyWriter key = new ShapeKeyWriter();
            key.tag(0);
            for (String part : parts) {
                key.text(part);
            }
            return Hashing.sha256().hashBytes(key.toByteArray()).toString();
        });
    }

    /**
     * The identity of a join expansion: its type, the equality pairs, the residual conditions and the
     * columns of the tables the join reads. The identity is the same while a group is estimated (its
     * children are group references there) and while the chosen plan is printed, because the operands are
     * read from the slots themselves.
     */
    private static Optional<List<String>> keyParts(Join join) {
        Set<String> operands = new TreeSet<>();
        List<String> forward = new ArrayList<>();
        List<String> backward = new ArrayList<>();
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            if (!(conjunct instanceof EqualTo) && !(conjunct instanceof NullSafeEqual)) {
                return Optional.empty();
            }
            Optional<String> left = operandIdentity(conjunct.child(0));
            Optional<String> right = operandIdentity(conjunct.child(1));
            if (!left.isPresent() || !right.isPresent()) {
                return Optional.empty();
            }
            operands.add(left.get());
            operands.add(right.get());
            String name = conjunct instanceof NullSafeEqual ? "NullSafeEqual" : "EqualTo";
            if (isCommutative(join.getJoinType())) {
                // an inner join has no preserved side: order the pair by identity, which is canonical in
                // both orientations, and choose one global orientation of the whole set below
                String first = left.get().compareTo(right.get()) <= 0 ? left.get() : right.get();
                String second = first.equals(left.get()) ? right.get() : left.get();
                String pair = name + "(col(" + first + "),col(" + second + "))";
                forward.add(pair);
                backward.add(pair);
                continue;
            }
            Integer leftSide = sideOf(join, conjunct.child(0));
            Integer rightSide = sideOf(join, conjunct.child(1));
            // an outer join preserves one side: the operand of the left child is written first. A pair
            // which cannot name a side (both operands from one side), or whose two operands are the same
            // column, reads the same whichever occurrence is the preserved one, so the join has no key.
            if (leftSide == null || rightSide == null || leftSide.equals(rightSide)
                    || left.get().equals(right.get())) {
                return Optional.empty();
            }
            forward.add(leftSide == 0
                    ? name + "(col(" + left.get() + "),col(" + right.get() + "))"
                    : name + "(col(" + right.get() + "),col(" + left.get() + "))");
            backward.add(leftSide == 0
                    ? name + "(col(" + right.get() + "),col(" + left.get() + "))"
                    : name + "(col(" + left.get() + "),col(" + right.get() + "))");
        }
        if (forward.isEmpty()) {
            return Optional.empty();
        }
        forward.sort(String::compareTo);
        backward.sort(String::compareTo);
        // An inner join is commutative, so one global orientation of its two sides is chosen: the pairs are
        // ordered by side all together, and the lexicographically smaller orientation is the key. Ordering
        // each pair on its own would lose the incidence of a self join (which operands share a column),
        // and flipping each pair on its own would lose commutativity. An outer join preserves a side, so
        // its orientation is fixed (the operand of the left child first).
        List<String> pairs = isCommutative(join.getJoinType())
                && String.join(SEP, backward).compareTo(String.join(SEP, forward)) < 0
                ? backward : forward;
        if (pairs.isEmpty()) {
            return Optional.empty();
        }
        List<String> parts = new ArrayList<>(2 + pairs.size());
        parts.add(String.valueOf(join.getJoinType()));
        parts.addAll(pairs);
        parts.add("operands:[" + String.join(",", operands) + "]");
        return Optional.of(parts);
    }

    /** Whether the operands of an equality are interchangeable without changing the row count. */
    private static boolean isCommutative(JoinType joinType) {
        return joinType == JoinType.INNER_JOIN || joinType == JoinType.CROSS_JOIN;
    }

    /** The side (0 or 1) of the join a slot is read from, null when it cannot be told. */
    private static Integer sideOf(Join join, Expression operand) {
        if (!(operand instanceof SlotReference)) {
            return null;
        }
        ExprId exprId = ((SlotReference) operand).getExprId();
        Plan joinPlan = (Plan) join;
        for (int side = 0; side < joinPlan.arity(); side++) {
            for (Slot slot : joinPlan.child(side).getOutput()) {
                if (slot.getExprId().equals(exprId)) {
                    return side;
                }
            }
        }
        return null;
    }

    /**
     * The stable identity of the column an operand reads: the table it belongs to and the name of the
     * column. The slot carries both, so the identity does not depend on the alias a query gave the
     * relation, on the shape of its qualifier or on where the join is described.
     */
    // The characters which delimit an operand in the pair text: a quoted column name may contain them,
    // and would otherwise forge the boundary between two operands (`EqualTo(col(a),col(b))`).
    private static String escapeOperandComponent(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            switch (c) {
                case '%':
                    escaped.append("%25");
                    break;
                case ',':
                    escaped.append("%2C");
                    break;
                case '(':
                    escaped.append("%28");
                    break;
                case ')':
                    escaped.append("%29");
                    break;
                default:
                    escaped.append(c);
            }
        }
        return escaped.toString();
    }

    private static Optional<String> operandIdentity(Expression operand) {
        if (!(operand instanceof SlotReference)) {
            return Optional.empty();
        }
        SlotReference slot = (SlotReference) operand;
        if (!slot.getOriginalTable().isPresent() || !slot.getOriginalColumn().isPresent()) {
            return Optional.empty();
        }
        return Optional.of(escapeOperandComponent(
                HboScanDescriptor.qualifiedNameOf(slot.getOriginalTable().get()))
                + "." + escapeOperandComponent(slot.getOriginalColumn().get().getName()));
    }
}

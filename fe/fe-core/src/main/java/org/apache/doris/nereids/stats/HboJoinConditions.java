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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
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
     * Canonical string of the equality conditions, empty when the join has no equality condition
     * (e.g. a cross join). A null safe equality ({@code <=>}) is a hash conjunct as well, so it
     * takes part in the canonical like a plain equality.
     */
    public static Optional<String> canonicalOf(Join join) {
        List<String> pairs = new ArrayList<>();
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            if (conjunct instanceof EqualTo || conjunct instanceof NullSafeEqual) {
                pairs.add(GroupStructInfo.normalizeExpression(conjunct));
            }
        }
        if (pairs.isEmpty()) {
            return Optional.empty();
        }
        pairs.sort(String::compareTo);
        List<String> residuals = new ArrayList<>();
        for (Expression conjunct : join.getOtherJoinConjuncts()) {
            // a residual predicate decides how many partner rows a row finds (`b > u.b + 1` is not
            // `b > u.b + 2`), so its literals are part of the key and are never folded
            residuals.add(GroupStructInfo.normalizeExpression(conjunct,
                    GroupStructInfo.LiteralMode.WITH_LITERAL));
        }
        residuals.sort(String::compareTo);
        StringBuilder canonical = new StringBuilder(PREFIX).append(join.getJoinType());
        pairs.forEach(pair -> canonical.append(SEP).append(pair));
        residuals.forEach(residual -> canonical.append(SEP).append(residual));
        if (join.getJoinType() != JoinType.INNER_JOIN) {
            // `A LEFT JOIN B` and `B LEFT JOIN A` keep the same join type and the same (sorted) equality
            // pairs, but they preserve different rows and expand a different side, so the side each
            // relation is on is part of the key of an outer join. An inner join is commutative and keeps
            // the side insensitive form.
            // a join node is a plan: the algebra interface only adds the join conditions
            Plan joinPlan = (Plan) join;
            canonical.append(SEP).append("sides:[").append(relationsOf(joinPlan.child(0))).append('|')
                    .append(relationsOf(joinPlan.child(1))).append(']');
        }
        return Optional.of(canonical.append("}").toString());
    }

    /**
     * The relations a side of a join reads, as the qualifiers of its output slots (sorted and
     * de-duplicated). Empty when the side projects nothing, which cannot happen for a join input.
     */
    private static String relationsOf(Plan plan) {
        Set<String> qualifiers = new TreeSet<>();
        for (Slot slot : plan.getOutput()) {
            if (slot instanceof SlotReference) {
                qualifiers.add(String.join(".", ((SlotReference) slot).getQualifier()));
            }
        }
        return String.join(",", qualifiers);
    }

    /** sha256 of the canonical string, empty when there is no equality condition. */
    public static Optional<String> fingerprintOf(Join join) {
        return canonicalOf(join).map(canonical -> Hashing.sha256()
                .hashString(canonical, StandardCharsets.UTF_8).toString());
    }
}

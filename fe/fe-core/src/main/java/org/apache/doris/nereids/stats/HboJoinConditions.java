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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;

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
        // the operands of the join name the alias a query gave a relation (`t AS x, u AS y` against
        // `u AS x, t AS y` render the same `x.a = y.a`), and the alias is bound to a table per plan: the
        // table each relation names is part of the key. An inner join is commutative, so the bindings of
        // both sides are one set for it; an outer join preserves a side, so each side keeps its own.
        // a join node is a plan: the algebra interface only adds the join conditions
        Plan joinPlan = (Plan) join;
        if (join.getJoinType() == JoinType.INNER_JOIN) {
            canonical.append(SEP).append("bindings:[").append(String.join(",",
                    bindingsOf(joinPlan))).append(']');
        } else {
            canonical.append(SEP).append("sides:[").append(relationsOf(joinPlan.child(0))).append('|')
                    .append(relationsOf(joinPlan.child(1))).append(']');
        }
        return Optional.of(canonical.append("}").toString());
    }

    /**
     * The relations of one side of a join: the qualifiers its output carries and, for every relation
     * below it, the table that relation's alias is bound to. The operands of the join name the alias the
     * query gave a relation, and the same operands are produced by the other binding of the aliases
     * (`t AS x, u AS y` against `u AS x, t AS y`), which keeps a different number of rows.
     */
    private static String relationsOf(Plan plan) {
        Set<String> qualifiers = new TreeSet<>();
        for (Slot slot : plan.getOutput()) {
            if (slot instanceof SlotReference) {
                qualifiers.add(String.join(".", ((SlotReference) slot).getQualifier()));
            }
        }
        return String.join(",", qualifiers) + ';' + String.join(",", bindingsOf(plan));
    }

    /** The alias a query gave a relation, empty when it has none. */
    private static String aliasOf(CatalogRelation relation) {
        if (relation instanceof LogicalCatalogRelation) {
            return ((LogicalCatalogRelation) relation).getTableAlias();
        }
        if (relation instanceof PhysicalCatalogRelation) {
            return ((PhysicalCatalogRelation) relation).getTableAlias();
        }
        return String.join(".", relation.getQualifier());
    }

    /** The table each relation of a plan is bound to, as the alias the plan names it by. */
    private static Set<String> bindingsOf(Plan plan) {
        Set<String> bindings = new TreeSet<>();
        List<Plan> relations = plan.collectToList(planNode -> planNode instanceof CatalogRelation);
        for (Plan node : relations) {
            CatalogRelation relation = (CatalogRelation) node;
            TableIf table = relation.getTable();
            String identity = table instanceof OlapTable
                    ? String.valueOf(((OlapTable) table).getId())
                    : HboScanDescriptor.qualifiedNameOf(table);
            bindings.add(aliasOf(relation) + '=' + identity);
        }
        return bindings;
    }

    /** sha256 of the canonical string, empty when there is no equality condition. */
    public static Optional<String> fingerprintOf(Join join) {
        return canonicalOf(join).map(canonical -> Hashing.sha256()
                .hashString(canonical, StandardCharsets.UTF_8).toString());
    }
}

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
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Join;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Canonical description and fingerprint of the equality conditions of a join, used as the key of
 * manually injected join expansion entries ({@code HBO SET EXPANSION}).
 *
 * <p>Only the hash (equality) conjuncts participate: the expansion behaviour of a join is a
 * property of the joined column pairs, so the key deliberately excludes the join type, the shape
 * of both sub trees and every non equality condition. That makes one injected entry reusable by
 * every query joining the same columns.
 *
 * <p>The canonical form is {@code JE{EqualTo(...);EqualTo(...)}} with the pairs sorted; a pair is
 * rendered by {@link GroupStructInfo#normalizeExpression(Expression)} which already sorts the two
 * operands of an equality (equality is commutative), so {@code a = b} and {@code b = a} collapse.
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
     * (e.g. a cross join).
     */
    public static Optional<String> canonicalOf(Join join) {
        List<String> pairs = new ArrayList<>();
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            if (conjunct instanceof EqualTo) {
                pairs.add(GroupStructInfo.normalizeExpression(conjunct));
            }
        }
        if (pairs.isEmpty()) {
            return Optional.empty();
        }
        pairs.sort(String::compareTo);
        return Optional.of(PREFIX + String.join(SEP, pairs) + "}");
    }

    /** sha256 of the canonical string, empty when there is no equality condition. */
    public static Optional<String> fingerprintOf(Join join) {
        return canonicalOf(join).map(canonical -> Hashing.sha256()
                .hashString(canonical, StandardCharsets.UTF_8).toString());
    }
}

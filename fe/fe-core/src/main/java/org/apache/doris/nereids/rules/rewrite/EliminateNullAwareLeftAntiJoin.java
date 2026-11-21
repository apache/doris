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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import java.util.stream.Stream;

/**
 * Convert null-aware anti joins when all join conditions are non-null. Uniform child properties can
 * prove equality keys non-null even when their slots remain declared nullable.
 */
public class EliminateNullAwareLeftAntiJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return nullAwareLeftAntiLogicalJoin().then(antiJoin -> {
            if (Stream.concat(antiJoin.getOtherJoinConjuncts().stream(),
                            antiJoin.getMarkJoinConjuncts().stream()).anyMatch(Expression::nullable)) {
                return null;
            }
            boolean hashKeysNotNull = antiJoin.getHashJoinConjuncts().stream().noneMatch(Expression::nullable);
            if (!hashKeysNotNull && !antiJoin.isMarkJoin()) {
                hashKeysNotNull = antiJoin.getHashJoinConjuncts().stream()
                        .allMatch(conjunct -> isNonNullHashConjunct(conjunct, antiJoin));
            }
            if (hashKeysNotNull) {
                return antiJoin.withJoinTypeAndContext(JoinType.LEFT_ANTI_JOIN, antiJoin.getJoinReorderContext());
            } else {
                return null;
            }
        }).toRule(RuleType.ELIMINATE_NULL_AWARE_LEFT_ANTI_JOIN);
    }

    private boolean isNonNullHashConjunct(Expression conjunct, LogicalJoin<?, ?> join) {
        if (!conjunct.nullable()) {
            return true;
        }
        if (!(conjunct instanceof EqualTo)
                || !(conjunct.child(0) instanceof Slot) || !(conjunct.child(1) instanceof Slot)) {
            return false;
        }
        Slot first = (Slot) conjunct.child(0);
        Slot second = (Slot) conjunct.child(1);
        return (isNonNullInChild(first, join.left()) && isNonNullInChild(second, join.right()))
                || (isNonNullInChild(first, join.right()) && isNonNullInChild(second, join.left()));
    }

    private boolean isNonNullInChild(Slot slot, Plan child) {
        for (Slot output : child.getOutput()) {
            if (output.equals(slot)) {
                // Use the child's output nullability: an outer join can null-extend a schema non-null slot.
                return !output.nullable() || child.getLogicalProperties().getTrait().isUniformAndNotNull(output);
            }
        }
        return false;
    }
}

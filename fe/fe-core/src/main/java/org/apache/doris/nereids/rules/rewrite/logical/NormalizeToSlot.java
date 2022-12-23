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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

/** NormalizeToSlot */
public interface NormalizeToSlot {

    /** NormalizeSlotContext */
    class NormalizeToSlotContext {
        private final Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap;
        private final Plan currentPlan;

        public NormalizeToSlotContext(
                Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap, Plan currentPlan) {
            this.normalizeToSlotMap = normalizeToSlotMap;
            this.currentPlan = currentPlan;
        }

        /** buildContext */
        public static NormalizeToSlotContext buildContext(
                Set<Alias> existsAliases, Set<? extends Expression> sourceExpressions, Plan currentPlan) {
            Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap = Maps.newLinkedHashMap();

            Map<Expression, Alias> existsAliasMap = Maps.newLinkedHashMap();
            for (Alias existsAlias : existsAliases) {
                existsAliasMap.put(existsAlias.child(), existsAlias);
            }

            for (Expression expression : sourceExpressions) {
                if (normalizeToSlotMap.containsKey(expression)) {
                    continue;
                }
                NormalizeToSlotTriplet normalizeToSlotTriplet =
                        NormalizeToSlotTriplet.toTriplet(expression, existsAliasMap.get(expression));
                normalizeToSlotMap.put(expression, normalizeToSlotTriplet);
            }
            return new NormalizeToSlotContext(normalizeToSlotMap, currentPlan);
        }

        /** normalizeToUseSlotRef, no custom normalize */
        public <E extends Expression> List<E> normalizeToUseSlotRef(
                List<E> expressions, boolean isOutput) {
            return normalizeToUseSlotRef(expressions, (context, expr) -> expr, isOutput);
        }

        /** normalizeToUseSlotRef */
        public <E extends Expression> List<E> normalizeToUseSlotRef(List<E> expressions,
                BiFunction<NormalizeToSlotContext, Expression, Expression> customNormalize,
                boolean isOutput) {
            return expressions.stream()
                    .map(expr -> (E) expr.rewriteDownShortCircuit(child -> {
                        Expression newChild = customNormalize.apply(this, child);
                        if (newChild != null && newChild != child) {
                            return newChild;
                        }
                        if (child instanceof ScalarFunction && !isOutput) {
                            return getSlotFromChildOutputsWhichEqualName(currentPlan, child);
                        }
                        if (child instanceof ScalarFunction && isOutput
                                && collectSlotFromChildOutputsWhichEqualName(currentPlan, child).isPresent()) {
                            NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(child);
                            return normalizeToSlotTriplet == null ? child : normalizeToSlotTriplet.originExpr;
                        }
                        NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(child);
                        return normalizeToSlotTriplet == null ? child : normalizeToSlotTriplet.remainExpr;
                    })).collect(ImmutableList.toImmutableList());
        }

        private Expression getSlotFromChildOutputsWhichEqualName(Plan repeat, Expression child) {
            NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(child);
            Optional<Slot> slot = collectSlotFromChildOutputsWhichEqualName(repeat, child);
            if (slot.isPresent()) {
                return slot.get();
            }
            return normalizeToSlotTriplet == null ? child : normalizeToSlotTriplet.remainExpr;
        }

        private Optional<Slot> collectSlotFromChildOutputsWhichEqualName(
                Plan repeat, Expression child) {
            NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(child);
            String asName = normalizeToSlotTriplet == null
                    ? child.toSql() : normalizeToSlotTriplet.remainExpr.getName();
            return repeat.child(0).getOutput().stream()
                    .filter(s -> s.getName().equals(asName))
                    .findFirst();
        }

        /**
         * generate bottom projections with groupByExpressions.
         * eg:
         * groupByExpressions: k1#0, k2#1 + 1;
         * bottom: k1#0, (k2#1 + 1) AS (k2 + 1)#2;
         */
        public Set<NamedExpression> pushDownToNamedExpression(
                Collection<? extends Expression> needToPushExpressions, Plan current) {
            return needToPushExpressions.stream()
                    .filter(e -> filterScalarFunWithSameAliasNameInChildOutput(e, current))
                    .map(expr -> {
                        NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(expr);
                        return normalizeToSlotTriplet == null
                                ? (NamedExpression) expr
                                : normalizeToSlotTriplet.pushedExpr;
                    }).collect(ImmutableSet.toImmutableSet());
        }

        private boolean filterScalarFunWithSameAliasNameInChildOutput(
                Expression expression, Plan current) {
            if (expression instanceof ScalarFunction) {
                Optional<Slot> slot = collectSlotFromChildOutputsWhichEqualName(current, expression);
                return !slot.isPresent();
            }
            return true;
        }
    }

    /** NormalizeToSlotTriplet */
    class NormalizeToSlotTriplet {
        // which expression need to normalized to slot?
        // e.g. `a + 1`
        public final Expression originExpr;
        // the slot already normalized.
        // e.g. new Alias(`a + 1`).toSlot()
        public final Slot remainExpr;
        // the output expression need to push down to the bottom project.
        // e.g. new Alias(`a + 1`)
        public final NamedExpression pushedExpr;

        public NormalizeToSlotTriplet(Expression originExpr, Slot remainExpr, NamedExpression pushedExpr) {
            this.originExpr = originExpr;
            this.remainExpr = remainExpr;
            this.pushedExpr = pushedExpr;
        }

        /** toTriplet */
        public static NormalizeToSlotTriplet toTriplet(Expression expression, @Nullable Alias existsAlias) {
            if (existsAlias != null) {
                return new NormalizeToSlotTriplet(expression, existsAlias.toSlot(), existsAlias);
            }

            if (expression instanceof NamedExpression) {
                NamedExpression namedExpression = (NamedExpression) expression;
                NormalizeToSlotTriplet normalizeToSlotTriplet =
                        new NormalizeToSlotTriplet(expression, namedExpression.toSlot(), namedExpression);
                return normalizeToSlotTriplet;
            }

            Alias alias = new Alias(expression, expression.toSql());
            return new NormalizeToSlotTriplet(expression, alias.toSlot(), alias);
        }
    }
}

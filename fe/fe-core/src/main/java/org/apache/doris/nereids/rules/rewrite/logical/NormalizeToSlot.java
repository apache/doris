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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/** NormalizeToSlot */
public interface NormalizeToSlot {

    /** NormalizeSlotContext */
    class NormalizeToSlotContext {
        private final Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap;

        public NormalizeToSlotContext(Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap) {
            this.normalizeToSlotMap = normalizeToSlotMap;
        }

        /** buildContext */
        public static NormalizeToSlotContext buildContext(
                Set<Alias> existsAliases, Set<? extends Expression> sourceExpressions) {
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
            return new NormalizeToSlotContext(normalizeToSlotMap);
        }

        public <E extends Expression> E normalizeToUseSlotRef(E expression) {
            return normalizeToUseSlotRef(ImmutableList.of(expression)).get(0);
        }

        /** normalizeToUseSlotRef, no custom normalize */
        public <E extends Expression> List<E> normalizeToUseSlotRef(List<E> expressions) {
            return normalizeToUseSlotRef(expressions, (context, expr) -> expr);
        }

        /** normalizeToUseSlotRef */
        public <E extends Expression> List<E> normalizeToUseSlotRef(List<E> expressions,
                BiFunction<NormalizeToSlotContext, Expression, Expression> customNormalize) {
            return expressions.stream()
                    .map(expr -> (E) expr.rewriteDownShortCircuit(child -> {
                        Expression newChild = customNormalize.apply(this, child);
                        if (newChild != null && newChild != child) {
                            return newChild;
                        }
                        NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(child);
                        return normalizeToSlotTriplet == null ? child : normalizeToSlotTriplet.remainExpr;
                    })).collect(ImmutableList.toImmutableList());
        }

        public <E extends Expression> E normalizeToUseSlotRefUp(E expression, Predicate skip) {
            return (E) expression.rewriteDownShortCircuitUp(child -> {
                NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(child);
                return normalizeToSlotTriplet == null ? child : normalizeToSlotTriplet.remainExpr;
            }, skip);
        }

        /**
         * rewrite subtrees whose root matches predicate border
         * when we traverse to the node satisfies border predicate, aboveBorder becomes false
         */
        public <E extends Expression> E normalizeToUseSlotRefDown(E expression, Predicate border, boolean aboveBorder) {
            return (E) expression.rewriteDownShortCircuitDown(child -> {
                NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(child);
                return normalizeToSlotTriplet == null ? child : normalizeToSlotTriplet.remainExpr;
            }, border, aboveBorder);
        }

        /**
         * generate bottom projections with groupByExpressions.
         * eg:
         * groupByExpressions: k1#0, k2#1 + 1;
         * bottom: k1#0, (k2#1 + 1) AS (k2 + 1)#2;
         */
        public Set<NamedExpression> pushDownToNamedExpression(Collection<? extends Expression> needToPushExpressions) {
            return needToPushExpressions.stream()
                    .map(expr -> {
                        NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(expr);
                        return normalizeToSlotTriplet == null
                                ? (NamedExpression) expr
                                : normalizeToSlotTriplet.pushedExpr;
                    }).collect(ImmutableSet.toImmutableSet());
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

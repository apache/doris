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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** NormalizeToSlot */
public interface NormalizeToSlot {

    /** NormalizeSlotContext */
    class NormalizeToSlotContext {
        private final Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap;

        public NormalizeToSlotContext(Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap) {
            this.normalizeToSlotMap = normalizeToSlotMap;
        }

        /**
         * build normalization context by follow step.
         *   1. collect all exists alias by input parameters existsAliases build a reverted map: expr -> alias
         *   2. for all input source expressions, use existsAliasMap to construct triple:
         *     origin expr, pushed expr and alias to replace origin expr,
         *     see more detail in {@link NormalizeToSlotTriplet}
         *   3. construct a map: original expr -> triple constructed by step 2
         */
        public static NormalizeToSlotContext buildContext(
                Set<Alias> existsAliases, Collection<? extends Expression> sourceExpressions) {
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

        /**
         * normalizeToUseSlotRef, no custom normalize.
         * This function use a lambda that always return original expression as customNormalize
         * So always use normalizeToSlotMap to process normalization when we call this function
         */
        public <E extends Expression> List<E> normalizeToUseSlotRef(Collection<E> expressions) {
            return normalizeToUseSlotRef(expressions, (context, expr) -> expr);
        }

        /**
         * normalizeToUseSlotRef.
         * try to use customNormalize do normalization first. if customNormalize cannot handle current expression,
         * use normalizeToSlotMap to get the default replaced expression.
         */
        public <E extends Expression> List<E> normalizeToUseSlotRef(Collection<E> expressions,
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

        public <E extends Expression> List<E> normalizeToUseSlotRefWithoutWindowFunction(
                Collection<E> expressions) {
            return expressions.stream()
                    .map(e -> (E) e.accept(NormalizeWithoutWindowFunction.INSTANCE, normalizeToSlotMap))
                    .collect(Collectors.toList());
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

    /**
     * replace any expression except window function.
     * because the window function could be same with aggregate function and should never be replaced.
     */
    class NormalizeWithoutWindowFunction
            extends DefaultExpressionRewriter<Map<Expression, NormalizeToSlotTriplet>> {

        public static final NormalizeWithoutWindowFunction INSTANCE = new NormalizeWithoutWindowFunction();

        private NormalizeWithoutWindowFunction() {
        }

        @Override
        public Expression visit(Expression expr, Map<Expression, NormalizeToSlotTriplet> replaceMap) {
            if (replaceMap.containsKey(expr)) {
                return replaceMap.get(expr).remainExpr;
            }
            return super.visit(expr, replaceMap);
        }

        @Override
        public Expression visitWindow(WindowExpression windowExpression,
                Map<Expression, NormalizeToSlotTriplet> replaceMap) {
            if (replaceMap.containsKey(windowExpression)) {
                return replaceMap.get(windowExpression).remainExpr;
            }
            List<Expression> newChildren = new ArrayList<>();
            Expression function = super.visit(windowExpression.getFunction(), replaceMap);
            newChildren.add(function);
            boolean hasNewChildren = function != windowExpression.getFunction();
            for (Expression partitionKey : windowExpression.getPartitionKeys()) {
                Expression newChild = partitionKey.accept(this, replaceMap);
                if (newChild != partitionKey) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }
            for (Expression orderKey : windowExpression.getOrderKeys()) {
                Expression newChild = orderKey.accept(this, replaceMap);
                if (newChild != orderKey) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }
            return hasNewChildren ? windowExpression.withChildren(newChildren) : windowExpression;
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

        /**
         * construct triplet by three conditions.
         * 1. already has exists alias: use this alias as pushed expr
         * 2. expression is {@link NamedExpression}, use itself as pushed expr
         * 3. other expression, construct a new Alias contains current expression as pushed expr
         */
        public static NormalizeToSlotTriplet toTriplet(Expression expression, @Nullable Alias existsAlias) {
            if (existsAlias != null) {
                return new NormalizeToSlotTriplet(expression, existsAlias.toSlot(), existsAlias);
            }

            if (expression instanceof NamedExpression) {
                NamedExpression namedExpression = (NamedExpression) expression;
                return new NormalizeToSlotTriplet(expression, namedExpression.toSlot(), namedExpression);
            }

            Alias alias = new Alias(expression, expression.toSql());
            return new NormalizeToSlotTriplet(expression, alias.toSlot(), alias);
        }
    }
}

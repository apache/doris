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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

/**
 * NormalizeToSlot
 */
public interface NormalizeToSlot {

    /**
     * NormalizeSlotContext
     */
    class NormalizeToSlotContext {
        private final Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap;

        public NormalizeToSlotContext(Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap) {
            this.normalizeToSlotMap = normalizeToSlotMap;
        }

        public Map<Expression, NormalizeToSlotTriplet> getNormalizeToSlotMap() {
            return normalizeToSlotMap;
        }

        public NormalizeToSlotContext mergeContext(NormalizeToSlotContext context) {
            Map<Expression, NormalizeToSlotTriplet> newMap = Maps.newHashMap();
            newMap.putAll(this.normalizeToSlotMap);
            newMap.putAll(context.getNormalizeToSlotMap());
            return new NormalizeToSlotContext(newMap);
        }

        /**
         * build normalization context by follow step.
         * 1. collect all exists alias by input parameters existsAliases build a reverted map: expr -> alias
         * 2. for all input source expressions, use existsAliasMap to construct triple:
         * origin expr, pushed expr and alias to replace origin expr,
         * see more detail in {@link NormalizeToSlotTriplet}
         * 3. construct a map: original expr -> triple constructed by step 2
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
                Alias alias = null;
                // consider projects: c1, c1 as a1. we should push down both of them,
                // so we could not replace c1 with c1 as a1.
                // use null as alias for SlotReference to avoid replace it by another alias of it.
                if (!(expression instanceof SlotReference)) {
                    alias = existsAliasMap.get(expression);
                }
                NormalizeToSlotTriplet normalizeToSlotTriplet = NormalizeToSlotTriplet.toTriplet(expression, alias);
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
            ImmutableList.Builder<E> result = ImmutableList.builderWithExpectedSize(expressions.size());
            for (E expr : expressions) {
                Expression rewriteExpr = expr.rewriteDownShortCircuit(child -> {
                    Expression newChild = customNormalize.apply(this, child);
                    if (newChild != null && newChild != child) {
                        return newChild;
                    }
                    NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(child);
                    return normalizeToSlotTriplet == null ? child : normalizeToSlotTriplet.remainExpr;
                });
                result.add((E) rewriteExpr);
            }
            return result.build();
        }

        public <E extends Expression> List<E> normalizeToUseSlotRefWithoutWindowFunction(
                Collection<E> expressions) {
            ImmutableList.Builder<E> normalized = ImmutableList.builderWithExpectedSize(expressions.size());
            for (E expression : expressions) {
                normalized.add((E) expression.accept(NormalizeWithoutWindowFunction.INSTANCE, normalizeToSlotMap));
            }
            return normalized.build();
        }

        /**
         * generate bottom projections with groupByExpressions.
         * eg:
         * groupByExpressions: k1#0, k2#1 + 1;
         * bottom: k1#0, (k2#1 + 1) AS (k2 + 1)#2;
         */
        public Set<NamedExpression> pushDownToNamedExpression(Collection<? extends Expression> needToPushExpressions) {
            ImmutableSet.Builder<NamedExpression> result
                    = ImmutableSet.builderWithExpectedSize(needToPushExpressions.size());
            for (Expression expr : needToPushExpressions) {
                NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(expr);
                result.add(normalizeToSlotTriplet == null
                        ? (NamedExpression) expr
                        : normalizeToSlotTriplet.pushedExpr);
            }
            return result.build();
        }

        public NamedExpression pushDownToNamedExpression(Expression expr) {
            NormalizeToSlotTriplet normalizeToSlotTriplet = normalizeToSlotMap.get(expr);
            return normalizeToSlotTriplet == null ? (NamedExpression) expr : normalizeToSlotTriplet.pushedExpr;
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
            NormalizeToSlotTriplet triplet = replaceMap.get(expr);
            if (triplet != null) {
                return triplet.remainExpr;
            }
            return super.visit(expr, replaceMap);
        }

        @Override
        public Expression visitWindow(WindowExpression windowExpression,
                Map<Expression, NormalizeToSlotTriplet> replaceMap) {
            NormalizeToSlotTriplet triplet = replaceMap.get(windowExpression);
            if (triplet != null) {
                return triplet.remainExpr;
            }
            ImmutableList.Builder<Expression> newChildren =
                    ImmutableList.builderWithExpectedSize(windowExpression.arity());
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
            if (!hasNewChildren) {
                return windowExpression;
            }
            if (windowExpression.getWindowFrame().isPresent()) {
                newChildren.add(windowExpression.getWindowFrame().get());
            }
            return windowExpression.withChildren(newChildren.build());
        }
    }

    /**
     * NormalizeToSlotTriplet
     */
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

            Alias alias = new Alias(expression);
            return new NormalizeToSlotTriplet(expression, alias.toSlot(), alias);
        }
    }
}

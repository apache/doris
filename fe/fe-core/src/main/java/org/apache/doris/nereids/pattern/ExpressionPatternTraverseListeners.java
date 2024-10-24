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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.rules.expression.ExpressionMatchingContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionTraverseListener;
import org.apache.doris.nereids.rules.expression.ExpressionTraverseListenerMapping;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/** ExpressionPatternTraverseListeners */
public class ExpressionPatternTraverseListeners
        extends TypeMappings<Expression, ExpressionTraverseListenerMapping> {
    public ExpressionPatternTraverseListeners(
            List<ExpressionTraverseListenerMapping> typeMappings) {
        super(typeMappings);
    }

    @Override
    protected Set<Class<? extends Expression>> getChildrenClasses(Class<? extends Expression> clazz) {
        return org.apache.doris.nereids.pattern.GeneratedExpressionRelations.CHILDREN_CLASS_MAP.get(clazz);
    }

    /** matchesAndCombineListener */
    public @Nullable CombinedListener matchesAndCombineListeners(
            Expression expr, ExpressionRewriteContext context, Expression parent) {
        List<ExpressionTraverseListenerMapping> listenerSingleMappings = singleMappings.get(expr.getClass());
        ExpressionMatchingContext<Expression> matchingContext
                = new ExpressionMatchingContext<>(expr, parent, context);
        switch (listenerSingleMappings.size()) {
            case 0: {
                ImmutableList.Builder<ExpressionTraverseListener<Expression>> matchedListeners
                        = ImmutableList.builder();
                for (ExpressionTraverseListenerMapping multiMapping : multiMappings) {
                    if (multiMapping.matchesTypeAndPredicates(matchingContext)) {
                        matchedListeners.add(multiMapping.listener);
                    }
                }
                return CombinedListener.tryCombine(matchedListeners.build(), matchingContext);
            }
            case 1: {
                ExpressionTraverseListenerMapping listenerMapping = listenerSingleMappings.get(0);
                if (listenerMapping.matchesPredicates(matchingContext)) {
                    return CombinedListener.tryCombine(ImmutableList.of(listenerMapping.listener), matchingContext);
                }
                return null;
            }
            default: {
                ImmutableList.Builder<ExpressionTraverseListener<Expression>> matchedListeners
                        = ImmutableList.builder();
                for (ExpressionTraverseListenerMapping singleMapping : listenerSingleMappings) {
                    if (singleMapping.matchesPredicates(matchingContext)) {
                        matchedListeners.add(singleMapping.listener);
                    }
                }
                return CombinedListener.tryCombine(matchedListeners.build(), matchingContext);
            }
        }
    }

    /** CombinedListener */
    public static class CombinedListener {
        private final ExpressionMatchingContext<Expression> context;
        private final List<ExpressionTraverseListener<Expression>> listeners;

        /** CombinedListener */
        public CombinedListener(ExpressionMatchingContext<Expression> context,
                List<ExpressionTraverseListener<Expression>> listeners) {
            this.context = context;
            this.listeners = listeners;
        }

        public static @Nullable CombinedListener tryCombine(
                List<ExpressionTraverseListener<Expression>> listenerMappings,
                ExpressionMatchingContext<Expression> context) {
            return listenerMappings.isEmpty() ? null : new CombinedListener(context, listenerMappings);
        }

        public void onEnter() {
            for (ExpressionTraverseListener<Expression> listener : listeners) {
                listener.onEnter(context);
            }
        }

        public void onExit(Expression rewritten) {
            for (ExpressionTraverseListener<Expression> listener : listeners) {
                listener.onExit(context, rewritten);
            }
        }
    }
}

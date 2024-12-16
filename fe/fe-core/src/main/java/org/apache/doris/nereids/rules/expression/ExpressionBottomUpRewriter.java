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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.pattern.ExpressionPatternRules;
import org.apache.doris.nereids.pattern.ExpressionPatternTraverseListeners;
import org.apache.doris.nereids.pattern.ExpressionPatternTraverseListeners.CombinedListener;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/** ExpressionBottomUpRewriter */
public class ExpressionBottomUpRewriter implements ExpressionRewriteRule<ExpressionRewriteContext> {
    public static final String BATCH_ID_KEY = "batch_id";
    private static final Logger LOG = LogManager.getLogger(ExpressionBottomUpRewriter.class);
    private static final AtomicInteger rewriteBatchId = new AtomicInteger();
    private final ExpressionPatternRules rules;
    private final ExpressionPatternTraverseListeners listeners;

    public ExpressionBottomUpRewriter(ExpressionPatternRules rules, ExpressionPatternTraverseListeners listeners) {
        this.rules = rules;
        this.listeners = listeners;
    }

    // entrance
    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        int currentBatch = rewriteBatchId.incrementAndGet();
        return rewriteBottomUp(expr, ctx, currentBatch, null, rules, listeners);
    }

    private static Expression rewriteBottomUp(
            Expression expression, ExpressionRewriteContext context, int currentBatch, @Nullable Expression parent,
            ExpressionPatternRules rules, ExpressionPatternTraverseListeners listeners) {

        Optional<Integer> rewriteState = expression.getMutableState(BATCH_ID_KEY);
        if (!rewriteState.isPresent() || rewriteState.get() != currentBatch) {
            CombinedListener listener = null;
            boolean hasChildren = expression.arity() > 0;
            if (hasChildren) {
                listener = listeners.matchesAndCombineListeners(expression, context, parent);
                if (listener != null) {
                    listener.onEnter();
                }
            }

            Expression afterRewrite = expression;
            try {
                Expression beforeRewrite;
                afterRewrite = rewriteChildren(expression, context, currentBatch, rules, listeners);
                // use rewriteTimes to avoid dead loop
                int rewriteTimes = 0;
                boolean changed;
                do {
                    beforeRewrite = afterRewrite;

                    // rewrite this
                    Optional<Expression> applied = rules.matchesAndApply(beforeRewrite, context, parent);

                    changed = applied.isPresent();
                    if (changed) {
                        afterRewrite = applied.get();
                        // ensure children are rewritten
                        afterRewrite = rewriteChildren(afterRewrite, context, currentBatch, rules, listeners);
                    }
                    rewriteTimes++;
                } while (changed && rewriteTimes < 100);

                // set rewritten
                afterRewrite.setMutableState(BATCH_ID_KEY, currentBatch);
            } finally {
                if (hasChildren && listener != null) {
                    listener.onExit(afterRewrite);
                }
            }

            return afterRewrite;
        }

        // already rewritten
        return expression;
    }

    private static Expression rewriteChildren(Expression parent, ExpressionRewriteContext context, int currentBatch,
            ExpressionPatternRules rules, ExpressionPatternTraverseListeners listeners) {
        boolean changed = false;
        ImmutableList.Builder<Expression> newChildren = ImmutableList.builderWithExpectedSize(parent.arity());
        for (Expression child : parent.children()) {
            Expression newChild = rewriteBottomUp(child, context, currentBatch, parent, rules, listeners);
            changed |= !child.equals(newChild);
            newChildren.add(newChild);
        }

        Expression result = parent;
        if (changed) {
            result = parent.withChildren(newChildren.build());
        }
        if (changed && context.cascadesContext.isEnableExprTrace()) {
            LOG.info("WithChildren: \nbefore: " + parent + "\nafter: " + result);
        }
        return result;
    }
}

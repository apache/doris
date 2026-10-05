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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Evaluate an expression after replacing an exact set of slots with typed SQL NULLs. */
public final class NullInputEvaluator {

    /** The only results callers may use to prove behavior for NULL-extended inputs. */
    public enum Result {
        NULL,
        FALSE,
        TRUE,
        OTHER_NON_NULL,
        UNKNOWN
    }

    private NullInputEvaluator() {
    }

    /**
     * Replace each supplied slot with a NULL of that slot's data type and constant-fold the
     * resulting expression. Only a fully folded literal is classified. An incomplete fold or any
     * exception is UNKNOWN so optimization callers fail closed.
     */
    public static Result evaluate(Expression expression, Set<? extends Slot> nullSlots,
            ExpressionRewriteContext context) {
        return evaluateInternal(expression, nullSlots, context, false);
    }

    /**
     * Evaluate using FE constant-folding rules only. Optimizer safety checks use this entry point
     * so proving NULL behavior never issues a BE-folding RPC.
     */
    public static Result evaluateOnFE(Expression expression, Set<? extends Slot> nullSlots,
            ExpressionRewriteContext context) {
        return evaluateInternal(expression, nullSlots, context, true);
    }

    private static Result evaluateInternal(Expression expression, Set<? extends Slot> nullSlots,
            ExpressionRewriteContext context, boolean feOnly) {
        try {
            Map<Expression, Expression> replacements = new HashMap<>();
            for (Slot slot : nullSlots) {
                replacements.put(slot, new NullLiteral(slot.getDataType()));
            }
            Expression nullInput = ExpressionUtils.replace(expression, replacements);
            Expression folded = feOnly
                    ? FoldConstantRule.evaluateOnFE(nullInput, context)
                    : FoldConstantRule.evaluate(nullInput, context);
            if (!(folded instanceof Literal)) {
                return Result.UNKNOWN;
            }
            if (folded.isNullLiteral()) {
                return Result.NULL;
            }
            if (BooleanLiteral.FALSE.equals(folded)) {
                return Result.FALSE;
            }
            if (BooleanLiteral.TRUE.equals(folded)) {
                return Result.TRUE;
            }
            return Result.OTHER_NON_NULL;
        } catch (Exception e) {
            return Result.UNKNOWN;
        }
    }
}

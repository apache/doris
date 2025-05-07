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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionMatchingContext;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**SimplifyConditionalFunction*/
public class SimplifyConditionalFunction implements ExpressionPatternRuleFactory {
    public static SimplifyConditionalFunction INSTANCE = new SimplifyConditionalFunction();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Coalesce.class).thenApply(SimplifyConditionalFunction::rewriteCoalesce)
                        .toRule(ExpressionRuleType.SIMPLIFY_CONDITIONAL_FUNCTION),
                matchesType(Nvl.class).thenApply(SimplifyConditionalFunction::rewriteNvl)
                        .toRule(ExpressionRuleType.SIMPLIFY_CONDITIONAL_FUNCTION),
                matchesType(NullIf.class).thenApply(SimplifyConditionalFunction::rewriteNullIf)
                        .toRule(ExpressionRuleType.SIMPLIFY_CONDITIONAL_FUNCTION)
        );
    }

    /*
     * coalesce(null,null,expr,...) => coalesce(expr,...)
     * coalesce(expr1(not null able ), expr2, ...., expr_n) => expr1
     * coalesce(null,null) => null
     * coalesce(expr1) => expr1
     * */
    private static Expression rewriteCoalesce(ExpressionMatchingContext<Coalesce> ctx) {
        Coalesce coalesce = ctx.expr;
        if (1 == coalesce.arity()) {
            return TypeCoercionUtils.ensureSameResultType(coalesce, coalesce.child(0), ctx.rewriteContext);
        }
        if (!(coalesce.child(0) instanceof NullLiteral) && coalesce.child(0).nullable()) {
            return TypeCoercionUtils.ensureSameResultType(coalesce, coalesce, ctx.rewriteContext);
        }
        ImmutableList.Builder<Expression> childBuilder = ImmutableList.builder();
        for (int i = 0; i < coalesce.arity(); i++) {
            Expression child = coalesce.children().get(i);
            if (child instanceof NullLiteral) {
                continue;
            }
            if (!child.nullable()) {
                return TypeCoercionUtils.ensureSameResultType(coalesce, child, ctx.rewriteContext);
            } else {
                for (int j = i; j < coalesce.arity(); j++) {
                    childBuilder.add(coalesce.children().get(j));
                }
                break;
            }
        }
        List<Expression> newChildren = childBuilder.build();
        if (newChildren.isEmpty()) {
            return TypeCoercionUtils.ensureSameResultType(
                    coalesce, new NullLiteral(coalesce.getDataType()), ctx.rewriteContext
            );
        } else {
            return TypeCoercionUtils.ensureSameResultType(
                    coalesce, coalesce.withChildren(newChildren), ctx.rewriteContext
            );
        }
    }

    /*
    * nvl(null,R) => R
    * nvl(L(not-nullable ),R) => L
    * */
    private static Expression rewriteNvl(ExpressionMatchingContext<Nvl> ctx) {
        Nvl nvl = ctx.expr;
        if (nvl.child(0) instanceof NullLiteral) {
            return TypeCoercionUtils.ensureSameResultType(nvl, nvl.child(1), ctx.rewriteContext);
        }
        if (!nvl.child(0).nullable()) {
            return TypeCoercionUtils.ensureSameResultType(nvl, nvl.child(0), ctx.rewriteContext);
        }
        return nvl;
    }

    /*
    * nullif(null, R) => Null
    * nullif(L, null) => Null
     */
    private static Expression rewriteNullIf(ExpressionMatchingContext<NullIf> ctx) {
        NullIf nullIf = ctx.expr;
        if (nullIf.child(0) instanceof NullLiteral || nullIf.child(1) instanceof NullLiteral) {
            return TypeCoercionUtils.ensureSameResultType(
                    nullIf, new Nullable(nullIf.child(0)), ctx.rewriteContext
            );
        } else {
            return nullIf;
        }
    }
}

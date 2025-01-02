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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**SimplifyConditionalFunction*/
public class SimplifyConditionalFunction implements ExpressionPatternRuleFactory {
    public static SimplifyConditionalFunction INSTANCE = new SimplifyConditionalFunction();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Coalesce.class).then(SimplifyConditionalFunction::rewriteCoalesce),
                matchesType(Nvl.class).then(SimplifyConditionalFunction::rewriteNvl),
                matchesType(NullIf.class).then(SimplifyConditionalFunction::rewriteNullIf)
        );
    }

    /*
     * coalesce(null,null,expr,...) => coalesce(expr,...)
     * coalesce(expr1(not null able ), expr2, ...., expr_n) => expr1
     * coalesce(null,null) => null
     * coalesce(expr1) => expr1
     * */
    private static Expression rewriteCoalesce(Coalesce expression) {
        if (1 == expression.arity()) {
            return expression.child(0);
        }
        if (!(expression.child(0) instanceof NullLiteral) && expression.child(0).nullable()) {
            return expression;
        }
        ImmutableList.Builder<Expression> childBuilder = ImmutableList.builder();
        for (int i = 0; i < expression.arity(); i++) {
            Expression child = expression.children().get(i);
            if (child instanceof NullLiteral) {
                continue;
            }
            if (!child.nullable()) {
                return child;
            } else {
                for (int j = i; j < expression.arity(); j++) {
                    childBuilder.add(expression.children().get(j));
                }
                break;
            }
        }
        List<Expression> newChildren = childBuilder.build();
        if (newChildren.isEmpty()) {
            return new NullLiteral(expression.getDataType());
        } else {
            return expression.withChildren(newChildren);
        }
    }

    /*
    * nvl(null,R) => R
    * nvl(L(not-nullable ),R) => L
    * */
    private static Expression rewriteNvl(Nvl nvl) {
        if (nvl.child(0) instanceof NullLiteral) {
            return nvl.child(1);
        }
        if (!nvl.child(0).nullable()) {
            return nvl.child(0);
        }
        return nvl;
    }

    /*
    * nullif(null, R) => Null
    * nullif(L, null) => Null
     */
    private static Expression rewriteNullIf(NullIf nullIf) {
        if (nullIf.child(0) instanceof NullLiteral || nullIf.child(1) instanceof NullLiteral) {
            return new Nullable(nullIf.child(0));
        } else {
            return nullIf;
        }
    }
}

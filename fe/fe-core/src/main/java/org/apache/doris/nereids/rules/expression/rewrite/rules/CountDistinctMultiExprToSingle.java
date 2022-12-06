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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/** CountDistinctMultiExprToSingle */
public class CountDistinctMultiExprToSingle extends AbstractExpressionRewriteRule {
    public static final CountDistinctMultiExprToSingle INSTANCE = new CountDistinctMultiExprToSingle();

    @Override
    public Expression visitCount(Count count, ExpressionRewriteContext context) {
        int arity = count.arity();
        if (count.isDistinct() && arity > 1) {
            Set<Expression> arguments = ImmutableSet.copyOf(count.getArguments());
            if (arguments.size() == 1) {
                return new Count(true, arguments.iterator().next());
            }

            Expression countExpr = count.getArgument(arguments.size() - 1);
            for (int i = arguments.size() - 2; i >= 0; --i) {
                Expression argument = count.getArgument(i);
                countExpr = new If(new IsNull(argument), NullLiteral.INSTANCE, countExpr);
            }
            return new Count(true, countExpr);
        }
        return count;
    }
}

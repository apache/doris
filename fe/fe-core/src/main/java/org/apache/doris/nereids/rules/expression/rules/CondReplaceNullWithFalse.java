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
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;

import java.util.List;

/**
 * replace null with false for condition expression.
 *    a) if(null and a > 1, ...) => if(false and a > 1, ...)
 *    b) case when null and a > 1 then ... => case when false and a > 1 then ...
 *    c) null or (null and a > 1) or not(null) => false or (false and a > 1) or not(null)
 */
public class CondReplaceNullWithFalse extends ConditionRewrite {

    public static final CondReplaceNullWithFalse INSTANCE = new CondReplaceNullWithFalse();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return buildCondRules(ExpressionRuleType.COND_REPLACE_NULL_WITH_FALSE);
    }

    @Override
    protected boolean needRewrite(Expression expression, boolean isInsideCondition) {
        if (!super.needRewrite(expression, isInsideCondition)) {
            return false;
        }

        return expression.containsType(NullLiteral.class);
    }

    @Override
    public Expression visitNullLiteral(NullLiteral nullLiteral, Boolean isInsideCondition) {
        if (isInsideCondition
                && (nullLiteral.getDataType().isBooleanType() || nullLiteral.getDataType().isNullType())) {
            return BooleanLiteral.FALSE;
        }
        return nullLiteral;
    }

}

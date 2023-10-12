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

import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Cast(AggState(tinyint), AggState(int)) to AggState(cast(tinyint,int))
 */
public class AggStateCastPushDownRule extends AbstractExpressionRewriteRule {

    public static AggStateCastPushDownRule INSTANCE = new AggStateCastPushDownRule();

    private Expression unwarpToCombinator(Expression expr, List<DataType> targetTypes) {
        if (expr instanceof StateCombinator) {
            List<Expression> aggStateChilds = expr.children();
            Preconditions.checkArgument(aggStateChilds.size() == targetTypes.size(),
                    "aggStateChilds(%s) not matched with targetTypes(%s).", aggStateChilds.size(), targetTypes.size());
            List<Expression> newAggStateChilds = new ArrayList<>();
            for (int i = 0; i < aggStateChilds.size(); i++) {
                newAggStateChilds.add(new Cast(aggStateChilds.get(i), targetTypes.get(i)));
            }
            return expr.withChildren(newAggStateChilds);
        }
        if (expr instanceof Alias) {
            Expression result = unwarpToCombinator(expr.child(0), targetTypes);
            if (result != null) {
                return expr.withChildren(result);
            }
        }
        return null;
    }

    @Override
    public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
        DataType originalType = cast.child().getDataType();
        DataType targetType = cast.getDataType();
        if (targetType.equals(originalType) || !originalType.isAggStateType() || !targetType.isAggStateType()) {
            return cast;
        }
        Expression result = unwarpToCombinator(cast.child(), ((AggStateType) targetType).getSubTypes());
        return result == null ? cast : result;
    }
}

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
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapToString;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * support convert bitmap to varchar
 */
public class CastBitmapToVarcharRewrite implements ExpressionPatternRuleFactory {

    public static CastBitmapToVarcharRewrite INSTANCE = new CastBitmapToVarcharRewrite();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Cast.class).then(CastBitmapToVarcharRewrite::rewrite)
                        .toRule(ExpressionRuleType.CAST_BITMAP_TO_VARCHAR)
        );
    }

    private static Expression rewrite(Cast cast) {
        Expression expr = cast.child();
        if (expr.getDataType() instanceof BitmapType && cast.getDataType() instanceof VarcharType) {
            return new BitmapToString(expr);
        }
        return cast;
    }
}

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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * LikeToEqualRewrite
 */
public class LikeToEqualRewrite implements ExpressionPatternRuleFactory {
    public static LikeToEqualRewrite INSTANCE = new LikeToEqualRewrite();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Like.class).then(LikeToEqualRewrite::rewriteLikeToEqual)
        );
    }

    private static Expression rewriteLikeToEqual(Like like) {
        Expression left = like.child(0);
        Expression right = like.child(1);
        if (!(right instanceof VarcharLiteral)) {
            return like;
        }
        String str = ((VarcharLiteral) right).value;
        StringBuilder sb = new StringBuilder();
        int len = str.length();
        char escapeChar = '\\';
        for (int i = 0; i < len;) {
            char c = str.charAt(i);
            if (c == escapeChar && (i + 1) < len
                    && (str.charAt(i + 1) == '%' || str.charAt(i + 1) == '_' || str.charAt(i + 1) == escapeChar)) {
                sb.append(str.charAt(i + 1));
                i += 2;
            } else {
                if (c == '%' || c == '_') {
                    return like;
                }
                sb.append(c);
                i++;
            }
        }
        return new EqualTo(left, new VarcharLiteral(sb.toString()));
    }
}

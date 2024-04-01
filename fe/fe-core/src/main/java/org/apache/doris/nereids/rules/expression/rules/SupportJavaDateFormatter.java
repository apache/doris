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
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/** SupportJavaDateFormatter2 */
public class SupportJavaDateFormatter implements ExpressionPatternRuleFactory {
    public static final SupportJavaDateFormatter INSTANCE = new SupportJavaDateFormatter();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(DateFormat.class).then(SupportJavaDateFormatter::rewriteDateFormat),
                matchesType(FromUnixtime.class).then(SupportJavaDateFormatter::rewriteFromUnixtime),
                matchesType(UnixTimestamp.class).then(SupportJavaDateFormatter::rewriteUnixTimestamp)
        );
    }

    public static Expression rewriteDateFormat(DateFormat dateFormat) {
        if (dateFormat.arity() > 1) {
            return translateJavaFormatter(dateFormat, 1);
        }
        return dateFormat;
    }

    public static Expression rewriteFromUnixtime(FromUnixtime fromUnixtime) {
        if (fromUnixtime.arity() > 1) {
            return translateJavaFormatter(fromUnixtime, 1);
        }
        return fromUnixtime;
    }

    public static Expression rewriteUnixTimestamp(UnixTimestamp unixTimestamp) {
        if (unixTimestamp.arity() > 1) {
            return translateJavaFormatter(unixTimestamp, 1);
        }
        return unixTimestamp;
    }

    private static Expression translateJavaFormatter(Expression function, int formatterIndex) {
        Expression formatterExpr = function.getArgument(formatterIndex);
        Expression newFormatterExpr = translateJavaFormatter(formatterExpr);
        if (newFormatterExpr != formatterExpr) {
            List<Expression> newArguments = Lists.newArrayList(function.getArguments());
            newArguments.set(formatterIndex, newFormatterExpr);
            return function.withChildren(newArguments);
        }
        return function;
    }

    private static Expression translateJavaFormatter(Expression formatterExpr) {
        if (formatterExpr.isLiteral() && formatterExpr.getDataType().isStringLikeType()) {
            Literal literal = (Literal) formatterExpr;
            String originFormatter = literal.getStringValue();
            if (originFormatter.equals("yyyyMMdd")) {
                return new VarcharLiteral("%Y%m%d");
            } else if (originFormatter.equals("yyyy-MM-dd")) {
                return new VarcharLiteral("%Y-%m-%d");
            } else if (originFormatter.equals("yyyy-MM-dd HH:mm:ss")) {
                return new VarcharLiteral("%Y-%m-%d %H:%i:%s");
            }
        }
        return formatterExpr;
    }
}

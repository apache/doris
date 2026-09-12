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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.TryCast;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Hour;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourFromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Microsecond;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MicrosecondFromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Minute;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinuteFromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Second;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondFromUnixtime;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Simplify time extraction functions with from_unixtime():
 * - hour(from_unixtime(ts)) -> hour_from_unixtime(ts)
 * - minute(from_unixtime(ts)) -> minute_from_unixtime(ts)
 * - second(from_unixtime(ts)) -> second_from_unixtime(ts)
 * - microsecond(from_unixtime(ts)) -> microsecond_from_unixtime(ts)
 */
public class SimplifyTimeFieldFromUnixtime implements ExpressionPatternRuleFactory {
    public static final SimplifyTimeFieldFromUnixtime INSTANCE = new SimplifyTimeFieldFromUnixtime();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Hour.class)
                        .thenApply(SimplifyTimeFieldFromUnixtime::rewriteHour)
                        .toRule(ExpressionRuleType.SIMPLIFY_DATETIME_FUNCTION),
                matchesType(Minute.class)
                        .thenApply(SimplifyTimeFieldFromUnixtime::rewriteMinute)
                        .toRule(ExpressionRuleType.SIMPLIFY_DATETIME_FUNCTION),
                matchesType(Second.class)
                        .thenApply(SimplifyTimeFieldFromUnixtime::rewriteSecond)
                        .toRule(ExpressionRuleType.SIMPLIFY_DATETIME_FUNCTION),
                matchesType(Microsecond.class)
                        .thenApply(SimplifyTimeFieldFromUnixtime::rewriteMicrosecond)
                        .toRule(ExpressionRuleType.SIMPLIFY_DATETIME_FUNCTION)
        );
    }

    private static Expression rewriteHour(ExpressionMatchingContext<Hour> ctx) {
        Hour hour = ctx.expr;
        Expression nestedChild = removeLosslessDateTimeCast(hour.child());
        if (!(nestedChild instanceof FromUnixtime && nestedChild.arity() == 1)) {
            return hour;
        }

        FromUnixtime fromUnixtime = (FromUnixtime) nestedChild;
        Expression tsArg = fromUnixtime.child(0);
        if (!tsArg.getDataType().isNumericType() && !tsArg.isNullLiteral()) {
            return hour;
        }
        Expression bigintArg = TypeCoercionUtils.castIfNotSameType(tsArg, BigIntType.INSTANCE);
        Expression rewritten = new HourFromUnixtime(bigintArg);
        return TypeCoercionUtils.ensureSameResultType(hour, rewritten, ctx.rewriteContext);
    }

    private static Expression rewriteMinute(ExpressionMatchingContext<Minute> ctx) {
        Minute minute = ctx.expr;
        Expression nestedChild = removeLosslessDateTimeCast(minute.child());
        if (!(nestedChild instanceof FromUnixtime && nestedChild.arity() == 1)) {
            return minute;
        }

        FromUnixtime fromUnixtime = (FromUnixtime) nestedChild;
        Expression tsArg = fromUnixtime.child(0);
        if (!tsArg.getDataType().isNumericType() && !tsArg.isNullLiteral()) {
            return minute;
        }
        Expression bigintArg = TypeCoercionUtils.castIfNotSameType(tsArg, BigIntType.INSTANCE);
        Expression rewritten = new MinuteFromUnixtime(bigintArg);
        return TypeCoercionUtils.ensureSameResultType(minute, rewritten, ctx.rewriteContext);
    }

    private static Expression rewriteSecond(ExpressionMatchingContext<Second> ctx) {
        Second second = ctx.expr;
        Expression nestedChild = removeLosslessDateTimeCast(second.child());
        if (!(nestedChild instanceof FromUnixtime && nestedChild.arity() == 1)) {
            return second;
        }

        FromUnixtime fromUnixtime = (FromUnixtime) nestedChild;
        Expression tsArg = fromUnixtime.child(0);
        if (!tsArg.getDataType().isNumericType() && !tsArg.isNullLiteral()) {
            return second;
        }
        Expression bigintArg = TypeCoercionUtils.castIfNotSameType(tsArg, BigIntType.INSTANCE);
        Expression rewritten = new SecondFromUnixtime(bigintArg);
        return TypeCoercionUtils.ensureSameResultType(second, rewritten, ctx.rewriteContext);
    }

    private static Expression rewriteMicrosecond(ExpressionMatchingContext<Microsecond> ctx) {
        Microsecond microsecond = ctx.expr;
        Expression nestedChild = removeLosslessDateTimeCast(microsecond.child());
        if (!(nestedChild instanceof FromUnixtime && nestedChild.arity() == 1)) {
            return microsecond;
        }

        FromUnixtime fromUnixtime = (FromUnixtime) nestedChild;
        Expression tsArg = fromUnixtime.child(0);
        if (!tsArg.getDataType().isNumericType() && !tsArg.isNullLiteral()) {
            return microsecond;
        }
        Expression decimalArg = TypeCoercionUtils.castIfNotSameType(tsArg,
                                        DecimalV3Type.createDecimalV3Type(18, 6));
        Expression rewritten = new MicrosecondFromUnixtime(decimalArg);
        return TypeCoercionUtils.ensureSameResultType(microsecond, rewritten, ctx.rewriteContext);
    }

    private static Expression removeLosslessDateTimeCast(Expression expr) {
        if (!(expr instanceof Cast) || expr instanceof TryCast) {
            return expr;
        }
        Cast cast = (Cast) expr;
        if (cast.isStrict() || !(cast.child() instanceof FromUnixtime) || cast.child().arity() != 1) {
            return expr;
        }

        FromUnixtime fromUnixtime = (FromUnixtime) cast.child();
        DataType argumentType = fromUnixtime.getSignature().getArgType(0);
        int sourceScale;
        if (argumentType.isBigIntType()) {
            sourceScale = 0;
        } else if (argumentType instanceof DecimalV3Type) {
            sourceScale = ((DecimalV3Type) argumentType).getScale();
        } else {
            return expr;
        }

        DataType targetType = cast.getDataType();
        if (targetType.isDateTimeType()) {
            return sourceScale == 0 ? cast.child() : expr;
        }
        if (targetType instanceof DateTimeV2Type
                && ((DateTimeV2Type) targetType).getScale() >= sourceScale) {
            return cast.child();
        }
        return expr;
    }
}

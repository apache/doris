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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;

import java.time.LocalDateTime;

/**
 * date v2 literal for nereids
 */
public class DateV2Literal extends DateLiteral {

    public DateV2Literal(String s) throws AnalysisException {
        super(DateV2Type.INSTANCE, s);
    }

    public DateV2Literal(long year, long month, long day) {
        super(DateV2Type.INSTANCE, year, month, day);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DateLiteral(year, month, day, Type.DATEV2);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDateV2Literal(this, context);
    }

    public Expression plusDays(long days) {
        return fromJavaDateType(toJavaDateType().plusDays(days));
    }

    public Expression plusMonths(long months) {
        return fromJavaDateType(toJavaDateType().plusMonths(months));
    }

    public Expression plusWeeks(long weeks) {
        return fromJavaDateType(toJavaDateType().plusWeeks(weeks));
    }

    public Expression plusYears(long years) {
        return fromJavaDateType(toJavaDateType().plusYears(years));
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        return isDateOutOfRange(dateTime)
                ? new NullLiteral(DateV2Type.INSTANCE)
                : new DateV2Literal(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth());
    }

    /**
     * 2020-01-01
     * @return 2020-01-01 00:00:00
     */
    public DateTimeV2Literal toBeginOfTheDay() {
        return toBeginOfTheDay(DateTimeV2Type.SYSTEM_DEFAULT);
    }

    /**
     * 2020-01-01
     * @return 2020-01-01 00:00:00
     */
    public DateTimeV2Literal toBeginOfTheDay(DateTimeV2Type dateType) {
        return new DateTimeV2Literal(dateType, year, month, day, 0, 0, 0, 000000);
    }

    /**
     * 2020-01-01
     * @return 2020-01-01 23:59:59
     */
    public DateTimeV2Literal toEndOfTheDay() {
        return toEndOfTheDay(DateTimeV2Type.SYSTEM_DEFAULT);
    }

    /**
     * 2020-01-01
     * @return 2020-01-01 23:59:59.9[scale]
     */
    public DateTimeV2Literal toEndOfTheDay(DateTimeV2Type dateType) {
        long microSecond = 0;
        // eg. scale == 4 -> 999900
        for (int i = 0; i < 6; ++i) {
            microSecond *= 10;
            if (i < dateType.getScale()) {
                microSecond += 9;
            }
        }
        return new DateTimeV2Literal(dateType, year, month, day, 23, 59, 59, microSecond);
    }

    /**
     * 2020-01-01
     * @return 2020-01-02 0:0:0
     */
    public DateTimeV2Literal toBeginOfTomorrow() {
        Expression tomorrow = plusDays(1);
        if (tomorrow instanceof DateV2Literal) {
            return ((DateV2Literal) tomorrow).toBeginOfTheDay();
        } else {
            return toEndOfTheDay();
        }
    }
}

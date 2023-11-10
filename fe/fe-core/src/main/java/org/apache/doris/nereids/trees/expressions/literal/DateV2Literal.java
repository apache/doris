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
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.nereids.util.StandardDateFormat;

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
        return fromJavaDateType(DateUtils.getTime(StandardDateFormat.DATE_FORMATTER, getStringValue()).plusDays(days));
    }

    public Expression plusMonths(long months) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_FORMATTER, getStringValue()).plusMonths(months));
    }

    public Expression plusWeeks(long weeks) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_FORMATTER, getStringValue()).plusWeeks(weeks));
    }

    public Expression plusYears(long years) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_FORMATTER, getStringValue()).plusYears(years));
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        return isDateOutOfRange(dateTime)
                ? new NullLiteral(DateV2Type.INSTANCE)
                : new DateV2Literal(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth());
    }
}

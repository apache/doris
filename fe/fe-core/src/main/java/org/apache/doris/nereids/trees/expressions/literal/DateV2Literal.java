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
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.util.DateUtils;

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

    public DateV2Literal plusDays(int days) {
        LocalDateTime dateTime = DateUtils.getTime(DATE_FORMATTER, getStringValue()).plusDays(days);
        return new DateV2Literal(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth());
    }

    public DateV2Literal plusMonths(int months) {
        LocalDateTime dateTime = DateUtils.getTime(DATE_FORMATTER, getStringValue()).plusMonths(months);
        return new DateV2Literal(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth());
    }

    public DateV2Literal plusYears(int years) {
        LocalDateTime dateTime = DateUtils.getTime(DATE_FORMATTER, getStringValue()).plusYears(years);
        return new DateV2Literal(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth());
    }
}

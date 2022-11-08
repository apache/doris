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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.util.DateUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;

/**
 * Date literal in Nereids.
 */
public class DateLiteral extends Literal {

    private static final Logger LOG = LogManager.getLogger(DateLiteral.class);

    private static final int DATEKEY_LENGTH = 8;

    private static DateTimeFormatter DATE_FORMATTER = null;
    private static DateTimeFormatter DATE_FORMATTER_TWO_DIGIT = null;
    private static DateTimeFormatter DATEKEY_FORMATTER = null;

    protected long year;
    protected long month;
    protected long day;

    static {
        try {
            DATE_FORMATTER = DateUtils.formatBuilder("%Y-%m-%d").toFormatter();
            DATEKEY_FORMATTER = DateUtils.formatBuilder("%Y%m%d").toFormatter();
            DATE_FORMATTER_TWO_DIGIT = DateUtils.formatBuilder("%y-%m-%d").toFormatter();
        } catch (AnalysisException e) {
            LOG.error("invalid date format", e);
            System.exit(-1);
        }
    }

    public DateLiteral(String s) throws AnalysisException {
        super(DataType.fromCatalogType(ScalarType.createDateType()));
        init(s);
    }

    public DateLiteral(DataType type) throws AnalysisException {
        super(type);
    }

    /**
     * C'tor for date type.
     */
    public DateLiteral(long year, long month, long day) {
        super(DateType.INSTANCE);
        this.year = year;
        this.month = month;
        this.day = day;
    }

    /**
     * C'tor for type conversion.
     */
    public DateLiteral(DateLiteral other, DataType type) {
        super(type);
        this.year = other.year;
        this.month = other.month;
        this.day = other.day;
    }

    private void init(String s) throws AnalysisException {
        try {
            LocalDateTime dateTime;
            if (s.split("-")[0].length() == 2) {
                dateTime = DATE_FORMATTER_TWO_DIGIT.parseLocalDateTime(s);
            } else if (s.length() == DATEKEY_LENGTH && !s.contains("-")) {
                dateTime = DATEKEY_FORMATTER.parseLocalDateTime(s);
            } else {
                dateTime = DATE_FORMATTER.parseLocalDateTime(s);
            }
            year = dateTime.getYear();
            month = dateTime.getMonthOfYear();
            day = dateTime.getDayOfMonth();
        } catch (Exception ex) {
            throw new AnalysisException("date literal [" + s + "] is invalid");
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDateLiteral(this, context);
    }

    @Override
    public Long getValue() {
        return (year * 10000 + month * 100 + day) * 1000000L;
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public String toString() {
        return String.format("%04d-%02d-%02d", year, month, day);
    }

    @Override
    public String getStringValue() {
        return String.format("%04d-%02d-%02d", year, month, day);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DateLiteral(year, month, day);
    }

    public long getYear() {
        return year;
    }

    public long getMonth() {
        return month;
    }

    public long getDay() {
        return day;
    }

    public DateLiteral plusDays(int days) {
        LocalDateTime dateTime = LocalDateTime.parse(getStringValue(), DATE_FORMATTER).plusDays(days);
        return new DateLiteral(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth());
    }
}



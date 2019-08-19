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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TDateLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

public class DateLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(DateLiteral.class);
    //private Date date;
    int hour;
    int minute;
    int second;
    int year;
    int month;
    int day;
    int microsecond;

    private DateLiteral() {
        super();
    }

    public DateLiteral(Type type, boolean isMax) throws AnalysisException{
        super();
        this.type = type;
        if (type == Type.DATE) {
            if (isMax) {
                init("1900-01-01", Type.DATE);
            } else {
                init("9999-12-31", Type.DATE);
            }
        } else {
            if (isMax) {
                init("1900-01-01 00:00:00", Type.DATETIME);
            } else {
                init("9999-12-31 23:59:59", Type.DATETIME);
            }
        }
        analysisDone();
    }

    public DateLiteral(String s, Type type) throws AnalysisException{
        super();
        init(s, type);
        analysisDone();
    }

    protected DateLiteral(DateLiteral other) {
        super(other);
        hour = other.hour;
        minute = other.minute;
        second = other.second;
        year = other.year;
        month = other.month;
        day = other.day;
        microsecond = other.microsecond;
        //date = other.date;
    }

    @Override
    public Expr clone() {
        return new DateLiteral(this);
    }

    public static DateLiteral createMinValue(Type type) throws AnalysisException{
        DateLiteral dateLiteral = new DateLiteral(type, false);
        return dateLiteral;
    }

    //private void init(String s, Type type) throws AnalysisException {
    private void init(String s, Type type) throws AnalysisException {
        try {
            Preconditions.checkArgument(type.isDateType());
            DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
            LocalDateTime datetime = builder.toFormatter().parseLocalDateTime(s);

            year = datetime.getYear();
            month = datetime.getMonthOfYear();
            day = datetime.getDayOfMonth();

            hour = datetime.getHourOfDay();
            minute = datetime.getMinuteOfHour();
            second = datetime.getSecondOfMinute();
            this.type = type;
        } catch (Exception ex) {
            throw new AnalysisException(ex.getMessage());
        }

        /*
        date = TimeUtils.parseDate(s, type);
        if (type.isScalarType(PrimitiveType.DATE)) {
            if (date.compareTo(TimeUtils.MAX_DATE) > 0 || date.compareTo(TimeUtils.MIN_DATE) < 0) {
                throw new AnalysisException("Date type column should range from [" + TimeUtils.MIN_DATE + "] to ["
                        + TimeUtils.MAX_DATE + "]");
            }
        } else {
            if (date.compareTo(TimeUtils.MAX_DATETIME) > 0 || date.compareTo(TimeUtils.MIN_DATETIME) < 0) {
                throw new AnalysisException("Datetime type column should range from [" + TimeUtils.MIN_DATETIME
                        + "] to [" + TimeUtils.MAX_DATETIME + "]");
            }
        }
        this.type = type;
        */
    }

    @Override
    public boolean isMinValue() {
        switch (type.getPrimitiveType()) {
            case DATE:
                return this.getStringValue().compareTo("1900-01-01") == 0;
            case DATETIME:
                return this.getStringValue().compareTo("1900-01-01 00:00:00") == 0;
            default:
                return false;
        }
    }

    @Override
    public Object getRealValue() {
        return getStringValue();
        //return TimeUtils.dateTransform(date.getTime(), type);
    }

    // Date column and Datetime column's hash value is not same.
    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        //String value = TimeUtils.format(date, type);
        String value = getStringValue();
        ByteBuffer buffer;
        try {
            buffer = ByteBuffer.wrap(value.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }

        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        // date time will not overflow when doing addition and subtraction
        return Long.signum(getLongValue() - expr.getLongValue());
    }

    @Override
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public String getStringValue() {
        //return TimeUtils.format(date, type);
        if (type == Type.DATE) {
            return year + "-" + month + "-" + day;
        } else {
            return year + "-" + month + "-" + day +
                    " " + hour + ":" + minute + ":" + second;
        }
    }

    @Override
    public long getLongValue() {
        return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
    }

    @Override
    public double getDoubleValue() {
        return getLongValue();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.DATE_LITERAL;
        msg.date_literal = new TDateLiteral(getStringValue());
    }
    /*
    public Date getValue() {
        return date;
    }
    */

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isDateType()) {
            return this;
        } else if (targetType.isStringType()) {
            return new StringLiteral(getStringValue()); 
        }
        Preconditions.checkState(false);
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        long ymd = ((year * 13 + month) << 5) | day;
        long hms = (hour << 12) | (minute << 6) | second;
        long tmp = ((ymd << 17) | hms) << 24 + microsecond;
        long packed_datetime =  tmp;
        out.writeLong(packed_datetime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        //date = new Date(in.readLong());
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_59) {
            long packed_time = in.readLong();
            microsecond = (int) (packed_time % (1L << 24));
            int ymdhms = (int) (packed_time >> 24);
            int ymd = ymdhms >> 17;
            int hms = ymdhms % (1 << 17);

            day = ymd % (1 << 5);
            int ym = ymd >> 5;
            month = ym % 13;
            year = ym / 13;
            year %= 10000;
            second = hms % (1 << 6);
            minute = (hms >> 6) % (1 << 6);
            hour = (hms >> 12);
            this.type = Type.DATETIME;
        } else {
            Date date = new Date(in.readLong());
            String date_str = TimeUtils.format(date, Type.DATETIME);
            try {
                init(date_str, Type.DATETIME);
            } catch (AnalysisException ex) {
                throw new IOException(ex.getMessage());
            }
        }
    }

    public static DateLiteral read(DataInput in) throws IOException {
        DateLiteral literal = new DateLiteral();
        literal.readFields(in);
        return literal;
    }
}

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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;

public class DateLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(DateLiteral.class);

    private static final DateLiteral MIN_DATE = new DateLiteral(1900, 1, 1);
    private static final DateLiteral MAX_DATE = new DateLiteral(9999, 12, 31);
    private static final DateLiteral MIN_DATETIME = new DateLiteral(1900, 1, 1, 0, 0, 0);
    private static final DateLiteral MAX_DATETIME = new DateLiteral(9999, 12, 31, 23, 59, 59);
    public static final DateLiteral UNIX_EPOCH_TIME = new DateLiteral(1970, 01, 01, 00, 00, 00);
    
    private static DateTimeFormatter DATE_TIME_FORMATTER = null;
    private static DateTimeFormatter DATE_FORMATTER = null;
    static {
        try {
            DATE_TIME_FORMATTER = formatBuilder("%Y-%m-%d %H:%i:%s").toFormatter();
            DATE_FORMATTER = formatBuilder("%Y-%m-%d").toFormatter();
        } catch (AnalysisException e) {
            LOG.error("invalid date format", e);
            System.exit(-1);
        }
    }

    //Regex used to determine if the TIME field exists int date_format
    private static final Pattern HAS_TIME_PART = Pattern.compile("^.*[HhIiklrSsT]+.*$");
    //Date Literal persist type in meta
    private enum  DateLiteralType {
        DATETIME(0),
        DATE(1);

        private final int value;
        private DateLiteralType(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }    

    private DateLiteral() {
        super();
    }

    public DateLiteral(Type type, boolean isMax) throws AnalysisException {
        super();
        this.type = type;
        if (type == Type.DATE) {
            if (isMax) {
                copy(MAX_DATE);
            } else {
                copy(MIN_DATE);
            }
        } else {
            if (isMax) {
                copy(MAX_DATETIME);
            } else {
                copy(MIN_DATETIME);
            }
        }
        analysisDone();
    }

    public DateLiteral(String s, Type type) throws AnalysisException {
        super();
        init(s, type);
        analysisDone();
    }

    public DateLiteral(long unixTimestamp, TimeZone timeZone, Type type) {
        DateTime dt = new DateTime(unixTimestamp, DateTimeZone.forTimeZone(timeZone));
        year = dt.getYear();
        month = dt.getMonthOfYear();
        day = dt.getDayOfMonth();
        hour = dt.getHourOfDay();
        minute = dt.getMinuteOfHour();
        second = dt.getSecondOfMinute();
        if (type == Type.DATE) {
            hour = 0;
            minute = 0;
            second = 0;
            this.type = Type.DATE;
        } else {
            this.type = Type.DATETIME;
        }            
    }

    public DateLiteral(long year, long month, long day) {
        this.hour = 0;
        this.minute = 0;
        this.second = 0;
        this.year = year;
        this.month = month;
        this.day = day;
        this.type = Type.DATE;
    }

    public DateLiteral(long year, long month, long day, long hour, long minute, long second) {
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.year = year;
        this.month = month;
        this.day = day;
        this.type = Type.DATETIME;
    }

    public DateLiteral(DateLiteral other) {
        super(other);
        hour = other.hour;
        minute = other.minute;
        second = other.second;
        year = other.year;
        month = other.month;
        day = other.day;
        microsecond = other.microsecond;
        type = other.type;
    }

    public static DateLiteral createMinValue(Type type) throws AnalysisException {
        return new DateLiteral(type, false);
    }

    private void init(String s, Type type) throws AnalysisException {
        try {
            Preconditions.checkArgument(type.isDateType());
            LocalDateTime dateTime;
            if (type == Type.DATE) {
                dateTime = DATE_FORMATTER.parseLocalDateTime(s);
            } else {
                dateTime = DATE_TIME_FORMATTER.parseLocalDateTime(s);
            }
            year = dateTime.getYear();
            month = dateTime.getMonthOfYear();
            day = dateTime.getDayOfMonth();
            hour = dateTime.getHourOfDay();
            minute = dateTime.getMinuteOfHour();
            second = dateTime.getSecondOfMinute();
            this.type = type;
        } catch (Exception ex) {
            throw new AnalysisException("date literal [" + s + "] is valid");
        }
    }

    private void copy(DateLiteral other) {
        hour = other.hour;
        minute = other.minute;
        second = other.second;
        year = other.year;
        month = other.month;
        day = other.day;
        microsecond = other.microsecond;
        type = other.type;
    }

    @Override
    public Expr clone() {
        return new DateLiteral(this);
    }

    @Override
    public boolean isMinValue() {
        switch (type.getPrimitiveType()) {
            case DATE:
                return this.getStringValue().compareTo(MIN_DATE.getStringValue()) == 0;
            case DATETIME:
                return this.getStringValue().compareTo(MIN_DATETIME.getStringValue()) == 0;
            default:
                return false;
        }
    }

    @Override
    public Object getRealValue() {
        if (type == Type.DATE) {
            return year * 16 * 32L + month * 32 + day;
        } else if (type == Type.DATETIME) {
            return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
        } else {
            Preconditions.checkState(false, "invalid date type: " + type);
            return -1L;
        }
    }

    // Date column and Datetime column's hash value is not same.
    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
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
        if (type == Type.DATE) {
            return String.format("%04d-%02d-%02d", year, month, day);
        } else {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
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

    public void castToDate() {
        this.type = Type.DATE;
        hour = 0;
        minute = 0;
        second = 0;
    }

    private long makePackedDatetime() {
        long ymd = ((year * 13 + month) << 5) | day;
        long hms = (hour << 12) | (minute << 6) | second;
        long packed_datetime = ((ymd << 17) | hms) << 24 + microsecond;
        return packed_datetime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        //set flag bit in meta, 0 is DATETIME and 1 is DATE
        if (this.type == Type.DATETIME) {
            out.writeShort(DateLiteralType.DATETIME.value());
        } else if (this.type == Type.DATE) {
            out.writeShort(DateLiteralType.DATE.value());
        } else {
            throw new IOException("Error date literal type : " + type);
        }
        out.writeLong(makePackedDatetime());
    }

    private void fromPackedDatetime(long packed_time) {
        microsecond = (packed_time % (1L << 24));
        long ymdhms = (packed_time >> 24);
        long ymd = ymdhms >> 17;
        long hms = ymdhms % (1 << 17);

        day = ymd % (1 << 5);
        long ym = ymd >> 5;
        month = ym % 13;
        year = ym / 13;
        year %= 10000;
        second = hms % (1 << 6);
        minute = (hms >> 6) % (1 << 6);
        hour = (hms >> 12);
        // set default date literal type to DATETIME
        // date literal read from meta will set type by flag bit;
        this.type = Type.DATETIME;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_60) {
            short date_literal_type = in.readShort();
            fromPackedDatetime(in.readLong());
            if (date_literal_type == DateLiteralType.DATETIME.value()) {
                this.type = Type.DATETIME;
            } else if (date_literal_type == DateLiteralType.DATE.value()) {
                this.type = Type.DATE;
            } else {
                throw new IOException("Error date literal type : " + type);
            }
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

    public long unixTimestamp(TimeZone timeZone) {
        DateTime dt = new DateTime((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second,
                DateTimeZone.forTimeZone(timeZone));
        return dt.getMillis();
    }

    public static DateLiteral dateParser(String date, String pattern) throws AnalysisException {
        LocalDateTime dateTime = formatBuilder(pattern).toFormatter().parseLocalDateTime(date);
        DateLiteral dateLiteral = new DateLiteral(
                dateTime.getYear(),
                dateTime.getMonthOfYear(),
                dateTime.getDayOfMonth(),
                dateTime.getHourOfDay(),
                dateTime.getMinuteOfHour(),
                dateTime.getSecondOfMinute());
        if(HAS_TIME_PART.matcher(pattern).matches()) {
            dateLiteral.setType(Type.DATETIME);
        } else {
            dateLiteral.setType(Type.DATE);
        }
        return dateLiteral;
    }

    //Return the date stored in the dateliteral as pattern format.
    //eg : "%Y-%m-%d" or "%Y-%m-%d %H:%i:%s"
    public String dateFormat(String pattern) throws AnalysisException {
        if (type == Type.DATE) {
            return DATE_FORMATTER.parseLocalDateTime(getStringValue())
                    .toString(formatBuilder(pattern).toFormatter());
        } else {
            return DATE_TIME_FORMATTER.parseLocalDateTime(getStringValue())
                    .toString(formatBuilder(pattern).toFormatter());
        }
    }

    private static DateTimeFormatterBuilder formatBuilder(String pattern) throws AnalysisException {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char character = pattern.charAt(i);
            if (escaped) {
                switch (character) {
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                        builder.appendDayOfWeekShortText();
                        break;
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                        builder.appendMonthOfYearShortText();
                        break;
                    case 'c': // %c Month, numeric (0..12)
                        builder.appendMonthOfYear(1);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        builder.appendDayOfMonth(2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        builder.appendDayOfMonth(1);
                        break;
                    case 'H': // %H Hour (00..23)
                        builder.appendHourOfDay(2);
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        builder.appendClockhourOfHalfday(2);
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        builder.appendMinuteOfHour(2);
                        break;
                    case 'j': // %j Day of year (001..366)
                        builder.appendDayOfYear(3);
                        break;
                    case 'k': // %k Hour (0..23)
                        builder.appendHourOfDay(1);
                        break;
                    case 'l': // %l Hour (1..12)
                        builder.appendClockhourOfHalfday(1);
                        break;
                    case 'M': // %M Month name (January..December)
                        builder.appendMonthOfYearText();
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        builder.appendMonthOfYear(2);
                        break;
                    case 'p': // %p AM or PM
                        builder.appendHalfdayOfDayText();
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        builder.appendClockhourOfHalfday(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2)
                                .appendLiteral(' ')
                                .appendHalfdayOfDayText();
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        builder.appendSecondOfMinute(2);
                        break;
                    case 'T': // %T Time, 24-hour (hh:mm:ss)
                        builder.appendHourOfDay(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2);
                        break;
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        builder.appendWeekOfWeekyear(2);
                        break;
                    case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
                        builder.appendWeekyear(4, 4);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        builder.appendDayOfWeekText();
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        builder.appendYear(4, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendTwoDigitYear(2020);
                        break;
                    case 'f': // %f Microseconds (000000..999999)
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                    case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                        throw new AnalysisException(String.format("%%%s not supported in date format string", character));
                    case '%': // %% A literal "%" character
                        builder.appendLiteral('%');
                        break;
                    default: // %<x> The literal character represented by <x>
                        builder.appendLiteral(character);
                        break;
                }
                escaped = false;
            } else if (character == '%') {
                escaped = true;
            } else {
                builder.appendLiteral(character);
            }
        }
        return builder;
    }

    public DateLiteral plusDays(int day) throws AnalysisException {
        LocalDateTime dateTime;
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        if (type == Type.DATE) {
            dateTime = DATE_FORMATTER.parseLocalDateTime(getStringValue()).plusDays(day);                                        
        } else {
            dateTime = DATE_TIME_FORMATTER.parseLocalDateTime(getStringValue()).plusDays(day);
        }
        DateLiteral dateLiteral = new DateLiteral(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(),
                dateTime.getHourOfDay(), dateTime.getMinuteOfHour(), dateTime.getSecondOfMinute());                                
        dateLiteral.setType(type);
        return dateLiteral;
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

    public long getHour() {
        return hour;
    }

    public long getMinute() {
        return minute;
    }

    public long getSecond() {
        return second;
    }

    private long year;
    private long month;
    private long day;
    private long hour;
    private long minute;
    private long second;
    private long microsecond;
}

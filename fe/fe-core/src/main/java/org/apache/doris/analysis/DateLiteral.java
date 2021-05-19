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
import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TDateLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
import java.time.Year;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.regex.Pattern;

public class DateLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(DateLiteral.class);

    private static final DateLiteral MIN_DATE = new DateLiteral(0000, 1, 1);
    private static final DateLiteral MAX_DATE = new DateLiteral(9999, 12, 31);
    private static final DateLiteral MIN_DATETIME = new DateLiteral(0000, 1, 1, 0, 0, 0);
    private static final DateLiteral MAX_DATETIME = new DateLiteral(9999, 12, 31, 23, 59, 59);
    public static final DateLiteral UNIX_EPOCH_TIME = new DateLiteral(1970, 01, 01, 00, 00, 00);

    private static final int DATEKEY_LENGTH = 8;
    private static final int MAX_MICROSECOND = 999999;
    private static final int DATETIME_TO_MINUTE_STRING_LENGTH = 16;
    private static final int DATETIME_TO_HOUR_STRING_LENGTH = 13;

    private static DateTimeFormatter DATE_TIME_FORMATTER = null;
    private static DateTimeFormatter DATE_TIME_FORMATTER_TO_HOUR = null;
    private static DateTimeFormatter DATE_TIME_FORMATTER_TO_MINUTE = null;
    private static DateTimeFormatter DATE_FORMATTER = null;
    /* 
     * Dates containing two-digit year values are ambiguous because the century is unknown. 
     * MySQL interprets two-digit year values using these rules:
     * Year values in the range 70-99 are converted to 1970-1999.
     * Year values in the range 00-69 are converted to 2000-2069.
     * */
    private static DateTimeFormatter DATE_TIME_FORMATTER_TWO_DIGIT = null;
    private static DateTimeFormatter DATE_FORMATTER_TWO_DIGIT = null;
    /*
     *  The datekey type is widely used in data warehouses
     *  For example, 20121229 means '2012-12-29'
     *  and data in the form of 'yyyymmdd' is generally called the datekey type.
     */
    private static DateTimeFormatter DATEKEY_FORMATTER = null;

    private static Map<String, Integer> MONTH_NAME_DICT = Maps.newHashMap();
    private static Map<String, Integer> MONTH_ABBR_NAME_DICT = Maps.newHashMap();
    private static Map<String, Integer> WEEK_DAY_NAME_DICT = Maps.newHashMap();
    private static Map<String, Integer> WEEK_DAY_ABBR_NAME_DICT = Maps.newHashMap();
    private static List<Integer> DAYS_IN_MONTH = Lists.newArrayList(0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);

    static {
        try {
            DATE_TIME_FORMATTER = formatBuilder("%Y-%m-%d %H:%i:%s").toFormatter();
            DATE_TIME_FORMATTER_TO_HOUR = formatBuilder("%Y-%m-%d %H").toFormatter();
            DATE_TIME_FORMATTER_TO_MINUTE = formatBuilder("%Y-%m-%d %H:%i").toFormatter();
            DATE_FORMATTER = formatBuilder("%Y-%m-%d").toFormatter();
            DATEKEY_FORMATTER = formatBuilder("%Y%m%d").toFormatter();
            DATE_TIME_FORMATTER_TWO_DIGIT = formatBuilder("%y-%m-%d %H:%i:%s").toFormatter();
            DATE_FORMATTER_TWO_DIGIT = formatBuilder("%y-%m-%d").toFormatter();
        } catch (AnalysisException e) {
            LOG.error("invalid date format", e);
            System.exit(-1);
        }

        MONTH_NAME_DICT.put("january", 1);
        MONTH_NAME_DICT.put("february", 2);
        MONTH_NAME_DICT.put("march", 3);
        MONTH_NAME_DICT.put("april", 4);
        MONTH_NAME_DICT.put("may", 5);
        MONTH_NAME_DICT.put("june", 6);
        MONTH_NAME_DICT.put("july", 7);
        MONTH_NAME_DICT.put("august", 8);
        MONTH_NAME_DICT.put("september", 9);
        MONTH_NAME_DICT.put("october", 10);
        MONTH_NAME_DICT.put("november", 11);
        MONTH_NAME_DICT.put("december", 12);

        MONTH_ABBR_NAME_DICT.put("jan", 1);
        MONTH_ABBR_NAME_DICT.put("feb", 2);
        MONTH_ABBR_NAME_DICT.put("mar", 3);
        MONTH_ABBR_NAME_DICT.put("apr", 4);
        MONTH_ABBR_NAME_DICT.put("may", 5);
        MONTH_ABBR_NAME_DICT.put("jun", 6);
        MONTH_ABBR_NAME_DICT.put("jul", 7);
        MONTH_ABBR_NAME_DICT.put("aug", 8);
        MONTH_ABBR_NAME_DICT.put("sep", 9);
        MONTH_ABBR_NAME_DICT.put("oct", 10);
        MONTH_ABBR_NAME_DICT.put("nov", 11);
        MONTH_ABBR_NAME_DICT.put("dec", 12);

        WEEK_DAY_NAME_DICT.put("monday", 0);
        WEEK_DAY_NAME_DICT.put("tuesday", 1);
        WEEK_DAY_NAME_DICT.put("wednesday", 2);
        WEEK_DAY_NAME_DICT.put("thursday", 3);
        WEEK_DAY_NAME_DICT.put("friday", 4);
        WEEK_DAY_NAME_DICT.put("saturday", 5);
        WEEK_DAY_NAME_DICT.put("sunday", 6);

        MONTH_ABBR_NAME_DICT.put("mon", 0);
        MONTH_ABBR_NAME_DICT.put("tue", 1);
        MONTH_ABBR_NAME_DICT.put("wed", 2);
        MONTH_ABBR_NAME_DICT.put("thu", 3);
        MONTH_ABBR_NAME_DICT.put("fri", 4);
        MONTH_ABBR_NAME_DICT.put("sat", 5);
        MONTH_ABBR_NAME_DICT.put("sun", 6);
    }

    //Regex used to determine if the TIME field exists int date_format
    private static final Pattern HAS_TIME_PART = Pattern.compile("^.*[HhIiklrSsTp]+.*$");
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

    public DateLiteral() {
        super();
    }

    public DateLiteral(Type type, boolean isMax) throws AnalysisException {
        super();
        this.type = type;
        if (type.equals(Type.DATE)) {
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
        if (type.equals(Type.DATE)) {
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

    public DateLiteral(LocalDateTime dateTime, Type type) {
        this.year = dateTime.getYear();
        this.month = dateTime.getMonthOfYear();
        this.day = dateTime.getDayOfMonth();
        this.hour = dateTime.getHourOfDay();
        this.minute = dateTime.getMinuteOfHour();
        this.second = dateTime.getSecondOfMinute();
        this.type = type;                                                            
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
            if (type.equals(Type.DATE)) {
                if (s.split("-")[0].length() == 2) {
                    dateTime = DATE_FORMATTER_TWO_DIGIT.parseLocalDateTime(s);
                } else if(s.length() == DATEKEY_LENGTH) {
                    dateTime = DATEKEY_FORMATTER.parseLocalDateTime(s);
                } else {
                    dateTime = DATE_FORMATTER.parseLocalDateTime(s);
                }
            } else {
                if (s.split("-")[0].length() == 2) {
                    dateTime = DATE_TIME_FORMATTER_TWO_DIGIT.parseLocalDateTime(s);
                } else {
                    // parse format '%Y-%m-%d %H:%i' and '%Y-%m-%d %H'
                    if (s.length() == DATETIME_TO_MINUTE_STRING_LENGTH) {
                        dateTime = DATE_TIME_FORMATTER_TO_MINUTE.parseLocalDateTime(s);
                    } else if (s.length() == DATETIME_TO_HOUR_STRING_LENGTH) {
                        dateTime = DATE_TIME_FORMATTER_TO_HOUR.parseLocalDateTime(s);
                    } else {
                        dateTime = DATE_TIME_FORMATTER.parseLocalDateTime(s);
                    }
                }
            }

            year = dateTime.getYear();
            month = dateTime.getMonthOfYear();
            day = dateTime.getDayOfMonth();
            hour = dateTime.getHourOfDay();
            minute = dateTime.getMinuteOfHour();
            second = dateTime.getSecondOfMinute();
            this.type = type;
        } catch (Exception ex) {
            throw new AnalysisException("date literal [" + s + "] is invalid");
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
        if (type.equals(Type.DATE)) {
            return year * 16 * 32L + month * 32 + day;
        } else if (type.equals(Type.DATETIME)) {
            return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
        } else {
            Preconditions.checkState(false, "invalid date type: " + type);
            return -1L;
        }
    }

    // Date column and Datetime column's hash value is not same.
    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        String value = convertToString(type);
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
        return convertToString(type.getPrimitiveType());
    }

    private String convertToString(PrimitiveType type) {
        if (type == PrimitiveType.DATE) {
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
            if (type.equals(targetType)) {
                return this;
            }
            if (targetType.equals(Type.DATE)) {
                return new DateLiteral(this.year, this.month, this.day);
            } else if (targetType.equals(Type.DATETIME)) {
                return new DateLiteral(this.year, this.month, this.day, this.hour, this.minute, this.second);
            } else {
                throw new AnalysisException("Error date literal type : " + type);
            }
        } else if (targetType.isStringType()) {
            return new StringLiteral(getStringValue());
        } else if (Type.isImplicitlyCastable(this.type, targetType, true)) {
            return new CastExpr(targetType, this);
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
        if (this.type.equals(Type.DATETIME)) {
            out.writeShort(DateLiteralType.DATETIME.value());
        } else if (this.type.equals(Type.DATE)) {
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
        DateTimeFormatter formatter = formatBuilder(pattern).toFormatter();
        LocalDateTime dateTime = formatter.parseLocalDateTime(date);
        DateLiteral dateLiteral = new DateLiteral(
                dateTime.getYear(),
                dateTime.getMonthOfYear(),
                dateTime.getDayOfMonth(),
                dateTime.getHourOfDay(),
                dateTime.getMinuteOfHour(),
                dateTime.getSecondOfMinute());
        if (HAS_TIME_PART.matcher(pattern).matches()) {
            dateLiteral.setType(Type.DATETIME);
        } else {
            dateLiteral.setType(Type.DATE);
        }
        return dateLiteral;
    }

    public static boolean hasTimePart(String format) {
        return HAS_TIME_PART.matcher(format).matches();
    }

    //Return the date stored in the dateliteral as pattern format.
    //eg : "%Y-%m-%d" or "%Y-%m-%d %H:%i:%s"
    public String dateFormat(String pattern) throws AnalysisException {
        if (type.equals(Type.DATE)) {
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

    public LocalDateTime getTimeFormatter() throws AnalysisException {
        if (type.equals(Type.DATE)) {
            return DATE_FORMATTER.parseLocalDateTime(getStringValue());                        
        } else if (type.equals(Type.DATETIME)) {
            return DATE_TIME_FORMATTER.parseLocalDateTime(getStringValue());
        } else {
            throw new AnalysisException("Not support date literal type");
        }        
    }

    public DateLiteral plusYears(int year) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusYears(year), type);
    }

    public DateLiteral plusMonths(int month) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusMonths(month), type);
    }

    public DateLiteral plusDays(int day) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusDays(day), type);
    }

    public DateLiteral plusHours(int hour) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusHours(hour), type);
    }

    public DateLiteral plusMinutes(int minute) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusMinutes(minute), type);
    }

    public DateLiteral plusSeconds(int second) throws AnalysisException {
        return new DateLiteral(getTimeFormatter().plusSeconds(second), type);
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


    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(unixTimestamp(TimeZone.getDefault()));
    }

    // parse the date string value in 'value' by 'format' pattern.
    // return the next position to parse if hasSubVal is true.
    // throw InvalidFormatException if encounter errors.
    // this method is exaclty same as from_date_format_str() in be/src/runtime/datetime_value.cpp
    // change this method should also change that.
    public int fromDateFormatStr(String format, String value, boolean hasSubVal) throws InvalidFormatException {
        int fp = 0; // pointer to the current format string
        int fend = format.length(); // end of format string
        int vp = 0; // pointer to the date string value
        int vend = value.length(); // end of date string value

        boolean datePartUsed = false;
        boolean timePartUsed = false;

        int dayPart = 0;
        long weekday = -1;
        long yearday = -1;
        long weekNum = -1;

        boolean strictWeekNumber = false;
        boolean sundayFirst = false;
        boolean strictWeekNumberYearType = false;
        long strictWeekNumberYear = -1;
        boolean usaTime = false;

        char f;
        while (fp < fend && vp < vend) {
            // Skip space character
            while (vp < vend && Character.isSpaceChar(value.charAt(vp))) {
                vp++;
            }
            if (vp >= vend) {
                break;
            }

            // Check switch
            f = format.charAt(fp);
            if (f == '%' && fp + 1 < fend) {
                int tmp = 0;
                long intValue = 0;
                fp++;
                f = format.charAt(fp);
                fp++;
                switch (f) {
                    // Year
                    case 'y':
                        // Year, numeric (two digits)
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        intValue += intValue >= 70 ? 1900 : 2000;
                        this.year = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'Y':
                        // Year, numeric, four digits
                        tmp = vp + Math.min(4, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        if (tmp - vp <= 2) {
                            intValue += intValue >= 70 ? 1900 : 2000;
                        }
                        this.year = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    // Month
                    case 'm':
                    case 'c':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.month = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'M': {
                        int nextPos = findWord(value, vp);
                        intValue = checkWord(MONTH_NAME_DICT, value.substring(vp, nextPos));
                        this.month = intValue;
                        vp = nextPos;
                        break;
                    }
                    case 'b': {
                        int nextPos = findWord(value, vp);
                        intValue = checkWord(MONTH_ABBR_NAME_DICT, value.substring(vp, nextPos));
                        this.month = intValue;
                        vp = nextPos;
                        break;
                    }
                    // Day
                    case 'd':
                    case 'e':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.day = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'D':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.day = intValue;
                        vp = tmp + Math.min(2, vend - tmp);
                        datePartUsed = true;
                        break;
                    // Hour
                    case 'h':
                    case 'I':
                    case 'l':
                        usaTime = true;
                        // Fall through
                    case 'k':
                    case 'H':
                        tmp = findNumber(value, vp, 2);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.hour = intValue;
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    // Minute
                    case 'i':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.minute = intValue;
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    // Second
                    case 's':
                    case 'S':
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        this.second = intValue;
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    // Micro second
                    case 'f':
                        // micro second is not supported, so just eat it and go one.
                        tmp = vp + Math.min(6, vend - vp);
                        vp = tmp;
                        break;
                    // AM/PM
                    case 'p':
                        if ((vend - vp) < 2 || Character.toUpperCase(value.charAt(vp + 1)) != 'M' || !usaTime) {
                            throw new InvalidFormatException("Invalid %p format");
                        }
                        if (Character.toUpperCase(value.charAt(vp)) == 'P') {
                            // PM
                            dayPart = 12;
                        }
                        timePartUsed = true;
                        vp += 2;
                        break;
                    // Weekday
                    case 'W': {
                        int nextPos = findWord(value, vp);
                        intValue = checkWord(WEEK_DAY_NAME_DICT, value.substring(vp, nextPos));
                        intValue++;
                        weekday = intValue;
                        datePartUsed = true;
                        break;
                    }
                    case 'a': {
                        int nextPos = findWord(value, vp);
                        intValue = checkWord(WEEK_DAY_NAME_DICT, value.substring(vp, nextPos));
                        intValue++;
                        weekday = intValue;
                        datePartUsed = true;
                        break;
                    }
                    case 'w':
                        tmp = vp + Math.min(1, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        if (intValue >= 7) {
                            throw new InvalidFormatException("invalid day of week: " + intValue);
                        }
                        if (intValue == 0) {
                            intValue = 7;
                        }
                        weekday = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'j':
                        tmp = vp + Math.min(3, vend - vp);
                        intValue = strToLong(value.substring(vp, tmp));
                        yearday = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'u':
                    case 'v':
                    case 'U':
                    case 'V':
                        sundayFirst = (format.charAt(fp - 1) == 'U' || format.charAt(fp - 1) == 'V');
                        // Used to check if there is %x or %X
                        strictWeekNumber = (format.charAt(fp - 1) == 'V' || format.charAt(fp - 1) == 'v');
                        tmp = vp + Math.min(2, vend - vp);
                        intValue = Long.valueOf(value.substring(vp, tmp));
                        weekNum = intValue;
                        if (weekNum > 53 || (strictWeekNumber && weekNum == 0)) {
                            throw new InvalidFormatException("invalid num of week: " + weekNum);
                        }
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    // strict week number, must be used with %V or %v
                    case 'x':
                    case 'X':
                        strictWeekNumberYearType = (format.charAt(fp - 1) == 'X');
                        tmp = vp + Math.min(4, vend - vp);
                        intValue = Long.valueOf(value.substring(vp, tmp));
                        strictWeekNumberYear = intValue;
                        vp = tmp;
                        datePartUsed = true;
                        break;
                    case 'r':
                        tmp = fromDateFormatStr("%I:%i:%S %p", value.substring(vp, vend), true);
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    case 'T':
                        tmp = fromDateFormatStr("%H:%i:%S", value.substring(vp, vend), true);
                        vp = tmp;
                        timePartUsed = true;
                        break;
                    case '.':
                        while (vp < vend && Character.toString(value.charAt(vp)).matches("\\p{Punct}")) {
                            vp++;
                        }
                        break;
                    case '@':
                        while (vp < vend && Character.isLetter(value.charAt(vp))) {
                            vp++;
                        }
                        break;
                    case '#':
                        while (vp < vend && Character.isDigit(value.charAt(vp))) {
                            vp++;
                        }
                        break;
                    case '%': // %%, escape the %
                        if ('%' != value.charAt(vp)) {
                            throw new InvalidFormatException("invalid char after %: " + value.charAt(vp));
                        }
                        vp++;
                        break;
                    default:
                        throw new InvalidFormatException("Invalid format pattern: " + f);
                }
            } else if (format.charAt(fp) != ' ') {
                if (format.charAt(fp) != value.charAt(vp)) {
                    throw new InvalidFormatException("Invalid char: " + value.charAt(vp) + ", expected: " + format.charAt(fp));
                }
                fp++;
                vp++;
            } else {
                fp++;
            }
        }

        // continue to iterate pattern if has
        // to find out if it has time part.
        while (fp < fend) {
            f = format.charAt(fp);
            if (f == '%' && fp + 1 < fend) {
                fp++;
                f = format.charAt(fp);
                fp++;
                switch (f) {
                    case 'H':
                    case 'h':
                    case 'I':
                    case 'i':
                    case 'k':
                    case 'l':
                    case 'r':
                    case 's':
                    case 'S':
                    case 'p':
                    case 'T':
                        timePartUsed = true;
                        break;
                    default:
                        break;
                }
            } else {
                fp++;
            }
        }

        if (usaTime) {
            if (this.hour > 12 || this.hour < 1) {
                throw new InvalidFormatException("Invalid hour: " + hour);
            }
            this.hour = (this.hour % 12) + dayPart;
        }

        if (hasSubVal) {
            return vp;
        }

        // Year day
        if (yearday > 0) {
            long days = calcDaynr(this.year, 1, 1) + yearday - 1;
            getDateFromDaynr(days);
        }

        // weekday
        if (weekNum >= 0 && weekday > 0) {
            // Check
            if ((strictWeekNumber && (strictWeekNumberYear < 0
                    || strictWeekNumberYearType != sundayFirst))
                    || (!strictWeekNumber && strictWeekNumberYear >= 0)) {
                throw new InvalidFormatException("invalid week number");
            }
            long days = calcDaynr(strictWeekNumber ? strictWeekNumberYear : this.year, 1, 1);

            long weekday_b = calcWeekday(days, sundayFirst);

            if (sundayFirst) {
                days += ((weekday_b == 0) ? 0 : 7) - weekday_b + (weekNum - 1) * 7 + weekday % 7;
            } else {
                days += ((weekday_b <= 3) ? 0 : 7) - weekday_b + (weekNum - 1) * 7 + weekday - 1;
            }
            getDateFromDaynr(days);
        }

        // Compute timestamp type
        if (datePartUsed) {
            if (timePartUsed) {
                this.type = Type.DATETIME;
            } else {
                this.type = Type.DATE;
            }
        }

        if (checkRange() || checkDate()) {
            throw new InvalidFormatException("Invalid format");
        }
        return 0;
    }

    private boolean checkRange() {
        return year > MAX_DATETIME.year || month > MAX_DATETIME.month || day > MAX_DATETIME.day
                || hour > MAX_DATETIME.hour || minute > MAX_DATETIME.minute || second > MAX_DATETIME.second
                || microsecond > MAX_MICROSECOND;
    }
    private boolean checkDate() {
        if (month != 0 && day > DAYS_IN_MONTH.get((int)month)){
            if (month == 2 && day == 29 && Year.isLeap(year)) {
                return false;
            }
            return true;
        }
        return false;
    }

    private long strToLong(String l) throws InvalidFormatException {
        try {
            long y = Long.valueOf(l);
            if (y < 0) {
                throw new InvalidFormatException("Invalid format: negative number.");
            }
            return y;
        } catch (NumberFormatException e) {
            throw new InvalidFormatException(e.getMessage());
        }
    }

    // calculate the number of days from year 0000-00-00 to year-month-day
    private long calcDaynr(long year, long month, long day) {
        long delsum = 0;
        long y = year;

        if (year == 0 && month == 0) {
            return 0;
        }

        /* Cast to int to be able to handle month == 0 */
        delsum = 365 * y + 31 * (month - 1) + day;
        if (month <= 2) {
            // No leap year
            y--;
        } else {
            // This is great!!!
            // 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
            // 0, 0, 3, 3, 4, 4, 5, 5, 5,  6,  7,  8
            delsum -= (month * 4 + 23) / 10;
        }
        // Every 400 year has 97 leap year, 100, 200, 300 are not leap year.
        return delsum + y / 4 - y / 100 + y / 400;
    }

    private long calcWeekday(long dayNr, boolean isSundayFirstDay) {
        return (dayNr + 5L + (isSundayFirstDay ? 1L : 0L)) % 7;
    }

    private void getDateFromDaynr(long daynr) throws InvalidFormatException {
        if (daynr <= 0 || daynr > 3652424) {
            throw new InvalidFormatException("Invalid days to year: " + daynr);
        }
        this.year = daynr / 365;
        long daysBeforeYear = 0;
        while (daynr < (daysBeforeYear = calcDaynr(this.year, 1, 1))) {
            this.year--;
        }
        long daysOfYear = daynr - daysBeforeYear + 1;
        int leapDay = 0;
        if (Year.isLeap(this.year)) {
            if (daysOfYear > 31 + 28) {
                daysOfYear--;
                if (daysOfYear == 31 + 28) {
                    leapDay = 1;
                }
            }
        }
        this.month = 1;
        while (daysOfYear > DAYS_IN_MONTH.get((int) this.month)) {
            daysOfYear -= DAYS_IN_MONTH.get((int) this.month);
            this.month++;
        }
        this.day = daysOfYear + leapDay;
    }

    // find a word start from 'start' from value.
    private int findWord(String value, int start) {
        int p = start;
        while (p < value.length() && Character.isLetter(value.charAt(p))) {
            p++;
        }
        return p;
    }

    // find a number start from 'start' from value.
    private int findNumber(String value, int start, int maxLen) {
        int p = start;
        int left = maxLen;
        while (p < value.length() && Character.isDigit(value.charAt(p)) && left > 0) {
            p++;
            left--;
        }
        return p;
    }

    // check if the given value exist in dict, return dict value.
    private int checkWord(Map<String, Integer> dict, String value) throws InvalidFormatException {
        Integer i = dict.get(value.toLowerCase());
        if (i != null) {
            return i;
        }
        throw new InvalidFormatException("'" + value + "' is invalid");
    }
}

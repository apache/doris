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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
    
    private static DateTimeFormatter DATE_TIME_FORMATTER = null;
    private static DateTimeFormatter DATE_FORMATTER = null;
    /* 
     * Dates containing two-digit year values are ambiguous because the century is unknown. 
     * MySQL interprets two-digit year values using these rules:
     * Year values in the range 70-99 are converted to 1970-1999.
     * Year values in the range 00-69 are converted to 2000-2069.
     * */
    private static DateTimeFormatter DATE_TIME_FORMATTER_TWO_DIGIT = null;
    private static DateTimeFormatter DATE_FORMATTER_TWO_DIGIT = null;

    private static Map<String, Integer> MONTH_NAME_DICT = Maps.newHashMap();
    private static Map<String, Integer> MONTH_ABBR_NAME_DICT = Maps.newHashMap();
    private static Map<String, Integer> WEEK_DAY_NAME_DICT = Maps.newHashMap();
    private static Map<String, Integer> WEEK_DAY_ABBR_NAME_DICT = Maps.newHashMap();
    private static List<Integer> DAYS_IN_MONTH = Lists.newArrayList(0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);

    static {
        try {
            DATE_TIME_FORMATTER = formatBuilder("%Y-%m-%d %H:%i:%s").toFormatter();
            DATE_FORMATTER = formatBuilder("%Y-%m-%d").toFormatter();
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

    private DateLiteral() {
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
                } else {
                    dateTime = DATE_FORMATTER.parseLocalDateTime(s);
                }
            } else {
                if (s.split("-")[0].length() == 2) {
                    dateTime = DATE_TIME_FORMATTER_TWO_DIGIT.parseLocalDateTime(s);
                } else {
                    dateTime = DATE_TIME_FORMATTER.parseLocalDateTime(s);
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


    public boolean from_date_format_str(String format, String value) {
        int ptr = 0;
        int end = format.length();
        int val = 0;
        int val_end = value.length();

        boolean date_part_used = false;
        boolean time_part_used = false;
        boolean frac_part_used = false;

        int day_part = 0;
        long weekday = -1;
        long yearday = -1;
        long week_num = -1;

        boolean strict_week_number = false;
        boolean sunday_first = false;
        boolean strict_week_number_year_type = false;
        long strict_week_number_year = -1;
        boolean usa_time = false;

        char f;
        while (ptr < end && val < val_end) {
            // Skip space character
            while (val < val_end && value.charAt(val) == ' ') {
                val++;
            }
            if (val >= val_end) {
                break;
            }

            // Check switch
            f = format.charAt(ptr);
            if (f == '%' && ptr + 1 < end) {
                int tmp = 0;
                long int_value = 0;
                ptr++;
                f = format.charAt(ptr);
                ptr++;
                switch (f) {
                    // Year
                    case 'y':
                        // Year, numeric (two digits)
                        tmp = val + Math.min(2, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        int_value += int_value >= 70 ? 1900 : 2000;
                        this.year = int_value;
                        val = tmp;
                        date_part_used = true;
                        break;
                    case 'Y':
                        // Year, numeric, four digits
                        tmp = val + Math.min(4, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        if (tmp - val <= 2) {
                            int_value += int_value >= 70 ? 1900 : 2000;
                        }
                        this.year = int_value;
                        val = tmp;
                        date_part_used = true;
                        break;
                    // Month
                    case 'm':
                    case 'c':
                        tmp = val + Math.min(2, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        this.month = int_value;
                        val = tmp;
                        date_part_used = true;
                        break;
                    case 'M': {
                        int nextPos = findWord(value, val);
                        int_value = checkWord(MONTH_NAME_DICT, value.substring(val, nextPos));
                        if (int_value < 0) {
                            return false;
                        }
                        this.month = int_value;
                        val = nextPos;
                        break;
                    }
                    case 'b': {
                        int nextPos = findWord(value, val);
                        int_value = checkWord(MONTH_ABBR_NAME_DICT, value.substring(val, nextPos));
                        if (int_value < 0) {
                            return false;
                        }
                        this.month = int_value;
                        val = nextPos;
                        break;
                    }
                    // Day
                    case 'd':
                    case 'e':
                        tmp = val + Math.min(2, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        this.day = int_value;
                        val = tmp;
                        date_part_used = true;
                        break;
                    case 'D':
                        tmp = val + Math.min(2, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        this.day = int_value;
                        val = tmp + Math.min(2, val_end - tmp);
                        date_part_used = true;
                        break;
                    // Hour
                    case 'h':
                    case 'I':
                    case 'l':
                        usa_time = true;
                        // Fall through
                    case 'k':
                    case 'H':
                        tmp = val + Math.min(2, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        this.hour = int_value;
                        val = tmp;
                        time_part_used = true;
                        break;
                    // Minute
                    case 'i':
                        tmp = val + Math.min(2, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        this.minute = int_value;
                        val = tmp;
                        time_part_used = true;
                        break;
                    // Second
                    case 's':
                    case 'S':
                        tmp = val + Math.min(2, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        this.second = int_value;
                        val = tmp;
                        time_part_used = true;
                        break;
                    // Micro second
                    case 'f':
                        // micro second is not supported, so just eat it and go one.
                        tmp = val + Math.min(6, val_end - val);
                        val = tmp;
                        break;
                    // AM/PM
                    case 'p':
                        if ((val_end - val) < 2 || Character.toUpperCase(value.charAt(val + 1)) != 'M' || !usa_time) {
                            return false;
                        }
                        if (Character.toUpperCase(value.charAt(val)) == 'P') {
                            // PM
                            day_part = 12;
                        }
                        time_part_used = true;
                        val += 2;
                        break;

                    // Weekday
                    case 'W': {
                        int nextPos = findWord(value, val);
                        int_value = checkWord(WEEK_DAY_NAME_DICT, value.substring(val, nextPos));
                        if (int_value < 0) {
                            return false;
                        }
                        int_value++;
                        weekday = int_value;
                        date_part_used = true;
                        break;
                    }
                    case 'a': {
                        int nextPos = findWord(value, val);
                        int_value = checkWord(WEEK_DAY_NAME_DICT, value.substring(val, nextPos));
                        if (int_value < 0) {
                            return false;
                        }
                        int_value++;
                        weekday = int_value;
                        date_part_used = true;
                        break;
                    }

                    case 'w':
                        tmp = val + Math.min(1, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        if (int_value >= 7) {
                            return false;
                        }
                        if (int_value == 0) {
                            int_value = 7;
                        }
                        weekday = int_value;
                        val = tmp;
                        date_part_used = true;
                        break;
                    case 'j':
                        tmp = val + Math.min(3, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        yearday = int_value;
                        val = tmp;
                        date_part_used = true;
                        break;

                    case 'u':
                    case 'v':
                    case 'U':
                    case 'V':
                        sunday_first = (format.charAt(ptr - 1) == 'U' || format.charAt(ptr - 1) == 'V');
                        // Used to check if there is %x or %X
                        strict_week_number = (format.charAt(ptr - 1) == 'V' || format.charAt(ptr - 1) == 'v');
                        tmp = val + Math.min(2, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        week_num = int_value;
                        if (week_num > 53 || (strict_week_number && week_num == 0)) {
                            return false;
                        }
                        val = tmp;
                        date_part_used = true;
                        break;
                    // strict week number, must be used with %V or %v
                    case 'x':
                    case 'X':
                        strict_week_number_year_type = (format.charAt(ptr - 1) == 'X');
                        tmp = val + Math.min(4, val_end - val);
                        int_value = Long.valueOf(value.substring(val, tmp));
                        strict_week_number_year = int_value;
                        val = tmp;
                        date_part_used = true;
                        break;
                    /*
                    case 'r':
                        if (from_date_format_str("%I:%i:%S %p", 11, val, val_end - val, & tmp)){
                        return false;
                    }
                    val = tmp;
                    time_part_used = true;
                    break;
                    case 'T':
                        if (from_date_format_str("%H:%i:%S", 8, val, val_end - val, & tmp)){
                        return false;
                    }
                    time_part_used = true;
                    val = tmp;
                    break;
                    */
                    case '.':
                        while (val < val_end && Character.toString(value.charAt(val)).matches("\\p{Punct}")) {
                            val++;
                        }
                        break;
                    case '@':
                        while (val < val_end && Character.isLetter(value.charAt(val))) {
                            val++;
                        }
                        break;
                    case '#':
                        while (val < val_end && Character.isDigit(value.charAt(val))) {
                            val++;
                        }
                        break;
                    case '%': // %%, escape the %
                        if ('%' != value.charAt(val)) {
                            return false;
                        }
                        val++;
                        break;
                    default:
                        return false;
                }
            } else if (format.charAt(ptr) != ' ') {
                if (format.charAt(ptr) != value.charAt(val)) {
                    return false;
                }
                ptr++;
                val++;
            } else {
                ptr++;
            }
        }

        if (usa_time) {
            if (this.hour > 12 || this.hour < 1) {
                return false;
            }
            this.hour = (this.hour % 12) + day_part;
        }
        /*
        if (sub_val_end) {
        *sub_val_end = val;
            return 0;
        }
        */
        // Year day

        if (yearday > 0) {
            long days = calc_daynr(this.year, 1, 1) + yearday - 1;
            if (!get_date_from_daynr(days)) {
                return false;
            }
        }
        // weekday
        if (week_num >= 0 && weekday > 0) {
            // Check
            if ((strict_week_number && (strict_week_number_year < 0
                    || strict_week_number_year_type != sunday_first))
                    || (!strict_week_number && strict_week_number_year >= 0)) {
                return false;
            }
            long days = calc_daynr(strict_week_number ? strict_week_number_year : this.year, 1, 1);

            long weekday_b = calc_weekday(days, sunday_first);

            if (sunday_first) {
                days += ((weekday_b == 0) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday % 7;
            } else {
                days += ((weekday_b <= 3) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday - 1;
            }
            if (!get_date_from_daynr(days)) {
                return false;
            }
        }

        // Compute timestamp type

        if (date_part_used) {
            if (time_part_used) {
                this.type = Type.DATETIME;
            } else {
                this.type = Type.DATE;
            }
        }
        return true;
    }

    private long calc_daynr(long year, long month, long day) {
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

    private long calc_weekday(long day_nr, boolean is_sunday_first_day) {
        return (day_nr + 5L + (is_sunday_first_day ? 1L : 0L)) % 7;
    }

    private boolean get_date_from_daynr(long daynr) {
        if (daynr <= 0 || daynr > 3652424) {
            return false;
        }
        this.year = daynr / 365;
        long days_befor_year = 0;
        while (daynr < (days_befor_year = calc_daynr(this.year, 1, 1))) {
            this.year--;
        }
        long days_of_year = daynr - days_befor_year + 1;
        int leap_day = 0;
        if (Year.isLeap(this.year)) {
            if (days_of_year > 31 + 28) {
                days_of_year--;
                if (days_of_year == 31 + 28) {
                    leap_day = 1;
                }
            }
        }
        this.month = 1;
        while (days_of_year > DAYS_IN_MONTH.get((int) this.month)) {
            days_of_year -= DAYS_IN_MONTH.get((int) this.month);
            this.month++;
        }
        this.day = days_of_year + leap_day;
        return true;
    }

    private int findWord(String value, int start) {
        int p = start;
        while (p < value.length() && Character.isLetter(value.charAt(p))) {
            p++;
        }
        return p;
    }

    private int checkWord(Map<String, Integer> dict, String value) {
        Integer i = dict.get(value.toLowerCase());
        if (i != null) {
            return i;
        }
        return -1;
    }

    public static void main(String[] args) throws AnalysisException {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            DateLiteral dateLiteral = new DateLiteral();
            dateLiteral.from_date_format_str("%Y-%m-%d", "2020-08-02");
        }
        System.out.println("cost: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            DateLiteral dateLiteral = DateLiteral.dateParser("2020-08-02", "%Y-%m-%d");
        }
        System.out.println("cost: " + (System.currentTimeMillis() - start));
    }
}

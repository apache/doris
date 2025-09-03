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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * StringLikeLiteral.
 */
public abstract class StringLikeLiteral extends Literal implements ComparableLiteral {
    public static final int CHINESE_CHAR_BYTE_LENGTH = 4;
    public static final String toDateStrictRegex
            // <date> ::= (<year> "-" <month1> "-" <day1>) | (<year> <month2> <day2>)
            = "((?:(?<year1>\\d{2}|\\d{4})-(?<month1>\\d{1,2})-(?<date1>\\d{1,2})"
            + "|(?<year2>\\d{2}|\\d{4})(?<month2>\\d{2})(?<date2>\\d{2}))"
            + "(?:[T ]"
            // <time> ::= <hour1> (":" <minute1> (":" <second1> <fraction>?)?)?
            // | <hour2> (<minute2> (<second2> <fraction>?)?)?
            + "(?:(?<hour1>\\d{1,2})(?::(?<minute1>\\d{1,2})(?::(?<second1>\\d{1,2})(?<fraction1>\\.\\d*)?)?)?"
            + "|(?<hour2>\\d{2})(?:(?<minute2>\\d{2})(?:(?<second2>\\d{2})(?<fraction2>\\.\\d*)?)?)?)"
            // <offset> ::= (( "+" | "-" ) <hour-offset> [ ":"? <minute-offset> ]) | (<tz-name>)
            + "(?:\\s*(?<tz>[+-]\\d{1,2}(?::?(?:00|30|45))?"
            + "|(?i)[A-Za-z]+\\S*))?"
            + ")?)"
            + "|"
            // <digit>{14} <fraction>? <whitespace>* <offset>?
            + "((?<timestamp>\\d{14})(?<fraction3>\\.\\d*)?\\s*"
            + "(?:\\s*(?<tz1>[+-]\\d{1,2}(?::?(?:00|30|45))?|(?i)([A-Za-z]+\\S*)))?)";
    public static final String toDateUnStrictRegex
            = "^\\s*((?<year>\\d{2}|\\d{4})[^a-zA-Z\\d](?<month>\\d{1,2})[^a-zA-Z\\d](?<date>\\d{1,2}))"
            + "(?:[ T]"
            + "(?<hour>\\d{1,2})[^a-zA-Z\\d](?<minute>\\d{1,2})[^a-zA-Z\\d](?<"
            + "second>\\d{1,2})(?<fraction>\\.\\d*)?"
            + "(?:\\s*(?<tz>[+-]\\d{1,2}(?::?(?:00|30|45))?"
            + "|(?i)([A-Za-z]+\\S*)))?"
            + ")?\\s*$";
    public static final String toDoubleRegex
            = "^\\s*[+-]?(?:(?:[0-9]+|[0-9]+\\.[0-9]*|[0-9]*\\.[0-9]+)(?:[eE][+-]?[0-9]+)?|(?"
            + "i)(?:INF|INFINITY)|(?i)NAN)\\s*$";
    public static final String toDecimalRegex
            = "^\\s*[+-]?((\\d+\\.\\d+)|(\\d+)|(\\d+\\.)|(\\.\\d+))(?:([eE])[+-]?\\d+)?\\s*$";
    public static final String toIntStrict = "\\s*[+-]?\\d+\\s*";
    public static final String toIntUnStrict = "\\s*[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)\\s*";
    public static final Pattern dateStrictPattern = Pattern.compile(toDateStrictRegex);
    public static final Pattern dateUnStrictPattern = Pattern.compile(toDateUnStrictRegex);
    public static final Pattern doublePattern = Pattern.compile(toDoubleRegex);
    public static final Pattern decimalPattern = Pattern.compile(toDecimalRegex);
    public static final Pattern intStrictPattern = Pattern.compile(toIntStrict);
    public static final Pattern intUnStrictPattern = Pattern.compile(toIntUnStrict);

    public final String value;

    public StringLikeLiteral(String value, DataType dataType) {
        super(dataType);
        this.value = value;
    }

    public String getStringValue() {
        return value;
    }

    @Override
    public double getDouble() {
        return getDouble(value);
    }

    /**
     * get double value
     */
    public static double getDouble(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        long v = 0;
        int pos = 0;
        int len = Math.min(bytes.length, 7);
        while (pos < len) {
            v += Byte.toUnsignedLong(bytes[pos]) << ((6 - pos) * 8);
            pos++;
        }
        return (double) v;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.StringLiteral(value);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        boolean strictCast = SessionVariable.enableStrictCast();
        if (targetType.isIntegralType()) {
            return castToIntegral(targetType, strictCast);
        }
        if (targetType.isDateType() || targetType.isDateV2Type()) {
            Expression expression = castToDateTime(DateTimeV2Type.MAX, strictCast, false);
            DateTimeV2Literal datetime = (DateTimeV2Literal) expression;
            if (targetType.isDateType()) {
                return new DateLiteral(datetime.year, datetime.month, datetime.day);
            } else {
                return new DateV2Literal(datetime.year, datetime.month, datetime.day);
            }
        } else if (targetType.isDateTimeType()) {
            Expression expression = castToDateTime(DateTimeV2Type.MAX, strictCast, true);
            DateTimeV2Literal datetime = (DateTimeV2Literal) expression;
            return new DateTimeLiteral((DateTimeType) targetType, datetime.year, datetime.month, datetime.day,
                    datetime.hour, datetime.minute, datetime.second, datetime.microSecond);
        } else if (targetType.isDateTimeV2Type()) {
            return castToDateTime(targetType, strictCast, true);
        } else if (targetType.isFloatType()) {
            return castToFloat();
        } else if (targetType.isDoubleType()) {
            return castToDouble();
        } else if (targetType.isDecimalV2Type()) {
            return castToDecimal(targetType);
        } else if (targetType.isDecimalV3Type()) {
            return castToDecimal(targetType);
        } else if (targetType.isBooleanType()) {
            return castToBoolean();
        } else if (targetType.isIPv4Type()) {
            return new IPv4Literal(value);
        } else if (targetType.isIPv6Type()) {
            return new IPv6Literal(value);
        } else if (targetType.isTimeType()) {
            if (this.dataType.isStringLikeType()) { // could parse in FE
                return new TimeV2Literal((TimeV2Type) targetType, value);
            }
        }
        return super.uncheckedCastTo(targetType);
    }

    protected Expression castToIntegral(DataType targetType, boolean strictCast) {
        Pattern pattern = strictCast ? intStrictPattern : intUnStrictPattern;
        if (pattern.matcher(value).matches() && !numericOverflow(value.trim(), targetType)) {
            String trimmedValue = value.trim();
            trimmedValue = trimmedValue.startsWith(".")
                    || trimmedValue.startsWith("+.") || trimmedValue.startsWith("-.")
                    ? "0" : trimmedValue.split("\\.")[0];
            if (targetType.isTinyIntType()) {
                return Literal.of(Byte.valueOf(trimmedValue));
            } else if (targetType.isSmallIntType()) {
                return Literal.of(Short.valueOf(trimmedValue));
            } else if (targetType.isIntegerType()) {
                return Literal.of(Integer.valueOf(trimmedValue));
            } else if (targetType.isBigIntType()) {
                return Literal.of(Long.valueOf(trimmedValue));
            } else if (targetType.isLargeIntType()) {
                return Literal.of(new BigDecimal(trimmedValue).toBigInteger());
            } else {
                throw new AnalysisException(String.format("Invalid target type %s", targetType));
            }
        } else {
            throw new CastException(String.format("%s can't cast to %s in strict mode.", value, targetType));
        }
    }

    protected Expression castToFloat() {
        String trimmedValue = value.trim();
        if (doublePattern.matcher(trimmedValue).matches()) {
            if (DoubleLiteral.POS_INF_NAME.contains(trimmedValue.toLowerCase())) {
                return Literal.of(Float.POSITIVE_INFINITY);
            }
            if (DoubleLiteral.NEG_INF_NAME.contains(trimmedValue.toLowerCase())) {
                return Literal.of(Float.NEGATIVE_INFINITY);
            }
            if (DoubleLiteral.NAN_NAME.contains(trimmedValue.toLowerCase())) {
                return Literal.of(Float.NaN);
            }
            return Literal.of(Float.parseFloat(value.trim()));
        }
        throw new CastException(String.format("%s can't cast to float in strict mode.", value));
    }

    protected Expression castToDouble() {
        String trimmedValue = value.trim();
        if (doublePattern.matcher(trimmedValue).matches()) {
            if (DoubleLiteral.POS_INF_NAME.contains(trimmedValue.toLowerCase())) {
                return Literal.of(Double.POSITIVE_INFINITY);
            }
            if (DoubleLiteral.NEG_INF_NAME.contains(trimmedValue.toLowerCase())) {
                return Literal.of(Double.NEGATIVE_INFINITY);
            }
            if (DoubleLiteral.NAN_NAME.contains(trimmedValue.toLowerCase())) {
                return Literal.of(Double.NaN);
            }
            return Literal.of(Double.parseDouble(value.trim()));
        }
        throw new CastException(String.format("%s can't cast to double in strict mode.", value));
    }

    protected Expression castToBoolean() {
        String trimmedValue = value.trim();
        if (trimmedValue.equals("1")
                || trimmedValue.equalsIgnoreCase("t")
                || trimmedValue.equalsIgnoreCase("yes")
                || trimmedValue.equalsIgnoreCase("on")
                || trimmedValue.equalsIgnoreCase("true")) {
            return BooleanLiteral.TRUE;
        }
        if (trimmedValue.equals("0")
                || trimmedValue.equalsIgnoreCase("f")
                || trimmedValue.equalsIgnoreCase("no")
                || trimmedValue.equalsIgnoreCase("off")
                || trimmedValue.equalsIgnoreCase("false")) {
            return BooleanLiteral.FALSE;
        }
        throw new CastException(String.format("%s can't cast to bool in strict mode.", value));
    }

    protected Expression castToDecimal(DataType targetType) {
        String trimmedValue = value.trim();
        if (decimalPattern.matcher(trimmedValue).matches()) {
            BigDecimal bigDecimal = new BigDecimal(trimmedValue);
            if (bigDecimal.compareTo(BigDecimal.ZERO) == 0) {
                return getDecimalLiteral(BigDecimal.ZERO, targetType);
            }
            return getDecimalLiteral(bigDecimal, targetType);
        }
        throw new CastException(String.format("%s can't cast to decimal in strict mode.", value));
    }

    protected Expression castToDateTime(DataType targetType, boolean strictCast, boolean isDatetime) {
        Matcher strictMatcher = dateStrictPattern.matcher(value);
        String year;
        String month;
        String date;
        String hour;
        String minute;
        String second;
        String fraction;
        String tz;
        if (strictMatcher.matches()) {
            if (strictMatcher.group("timestamp") != null) {
                String timestamp = strictMatcher.group("timestamp");
                Preconditions.checkArgument(timestamp.length() == 14);
                year = timestamp.substring(0, 4);
                month = timestamp.substring(4, 6);
                date = timestamp.substring(6, 8);
                hour = timestamp.substring(8, 10);
                minute = timestamp.substring(10, 12);
                second = timestamp.substring(12, 14);
                fraction = strictMatcher.group("fraction3");
                tz = strictMatcher.group("tz1");
            } else {
                year = strictMatcher.group("year1") == null
                        ? strictMatcher.group("year2") : strictMatcher.group("year1");
                month = strictMatcher.group("month1") == null
                        ? strictMatcher.group("month2") : strictMatcher.group("month1");
                date = strictMatcher.group("date1") == null
                        ? strictMatcher.group("date2") : strictMatcher.group("date1");
                hour = strictMatcher.group("hour1") == null
                        ? strictMatcher.group("hour2") : strictMatcher.group("hour1");
                minute = strictMatcher.group("minute1") == null
                        ? strictMatcher.group("minute2") : strictMatcher.group("minute1");
                second = strictMatcher.group("second1") == null
                        ? strictMatcher.group("second2") : strictMatcher.group("second1");
                fraction = strictMatcher.group("fraction1") == null
                        ? strictMatcher.group("fraction2") : strictMatcher.group("fraction1");
                tz = strictMatcher.group("tz");
            }
            if (tz != null && tz.equalsIgnoreCase("CST")) {
                tz = "+08:00";
            }
            DateTimeV2Literal dt = getDateTimeLiteral(year, month, date, hour, minute, second,
                    fraction, tz, targetType);
            if (isDatetime) {
                return dt;
            } else {
                return new DateTimeV2Literal(Long.parseLong(year2ToYear4(year)), Long.parseLong(month),
                        Long.parseLong(date), 0, 0, 0);
            }
        } else if (!strictCast) {
            Matcher unStrictMatcher = dateUnStrictPattern.matcher(value);
            if (unStrictMatcher.matches()) {
                year = unStrictMatcher.group("year");
                month = unStrictMatcher.group("month");
                date = unStrictMatcher.group("date");
                hour = unStrictMatcher.group("hour");
                minute = unStrictMatcher.group("minute");
                second = unStrictMatcher.group("second");
                fraction = unStrictMatcher.group("fraction");
                tz = unStrictMatcher.group("tz");
                if (tz != null && tz.equalsIgnoreCase("CST")) {
                    tz = "+08:00";
                }
                DateTimeV2Literal dt = getDateTimeLiteral(year, month, date, hour, minute, second,
                        fraction, tz, targetType);
                if (isDatetime) {
                    return dt;
                } else {
                    return new DateTimeV2Literal(Long.parseLong(year2ToYear4(year)), Long.parseLong(month),
                            Long.parseLong(date), 0, 0, 0);
                }
            }
        }
        throw new CastException(String.format("[%s] can't cast to %s.", value, targetType));
    }

    protected String year2ToYear4(String year) {
        if (year.length() == 2) {
            int year2 = Integer.parseInt(year);
            return year2 >= 70 ? String.valueOf(year2 + 1900) : String.valueOf(2000 + year2);
        }
        return year;
    }

    protected DateTimeV2Literal getDateTimeLiteral(String year, String month, String date, String hour, String minute,
            String second, String fraction, String tz, DataType targetType) {
        String year4 = year2ToYear4(year);
        tz = tz == null ? "" : tz;
        if ((tz.startsWith("+") || tz.startsWith("-")) && !tz.contains(":")) {
            if (tz.length() == 2) {
                tz = String.format("%s0%s:00", tz.charAt(0), tz.charAt(1));
            } else if (tz.length() == 3) {
                tz = String.format("%s%s:00", tz.charAt(0), tz.substring(1, 3));
            } else if (tz.length() == 4) {
                tz = String.format("%s0%s:%s", tz.charAt(0), tz.charAt(1), tz.substring(2, 4));
            } else {
                tz = String.format("%s%s:%s", tz.charAt(0), tz.substring(1, 3), tz.substring(3, 5));
            }
        }
        hour = hour == null ? "00" : hour;
        minute = minute == null ? "00" : minute;
        second = second == null ? "00" : second;
        fraction = fraction == null || fraction.equals(".") ? "" : fraction;
        if (fraction.endsWith(".")) {
            fraction = fraction.substring(0, fraction.length() - 1);
        }
        if (tz.contains(":")) {
            int hourOffset = Integer.parseInt(tz.substring(1, tz.indexOf(":")));
            int minuteOffset = Integer.parseInt(tz.substring(tz.indexOf(":") + 1));
            if (hourOffset > 14 || hourOffset == 14 && minuteOffset > 0) {
                throw new CastException("Time zone offset couldn't be larger than 14:00");
            }
        }
        String format = String.format("%s-%s-%sT%s:%s:%s%s%s", year4, month, date, hour, minute, second, fraction, tz);
        try {
            return new DateTimeV2Literal((DateTimeV2Type) targetType, format);
        } catch (AnalysisException e) {
            throw new CastException(e.getMessage(), e);
        }
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof StringLikeLiteral) {
            // compare string with utf-8 byte array, same with DM,BE,StorageEngine
            byte[] thisBytes = null;
            byte[] otherBytes = null;
            try {
                thisBytes = getStringValue().getBytes("UTF-8");
                otherBytes = ((Literal) other).getStringValue().getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new AnalysisException(e.getMessage(), e);
            }

            int minLength = Math.min(thisBytes.length, otherBytes.length);
            int i = 0;
            for (i = 0; i < minLength; i++) {
                if (Byte.toUnsignedInt(thisBytes[i]) < Byte.toUnsignedInt(otherBytes[i])) {
                    return -1;
                } else if (Byte.toUnsignedInt(thisBytes[i]) > Byte.toUnsignedInt(otherBytes[i])) {
                    return 1;
                }
            }
            if (thisBytes.length > otherBytes.length) {
                if (thisBytes[i] == 0x00) {
                    return 0;
                } else {
                    return 1;
                }
            } else if (thisBytes.length < otherBytes.length) {
                if (otherBytes[i] == 0x00) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                return 0;
            }
        }
        if (other instanceof NullLiteral) {
            return 1;
        }
        if (other instanceof MaxLiteral) {
            return -1;
        }
        throw new RuntimeException("Cannot compare two values with different data types: "
                + this + " (" + dataType + ") vs " + other + " (" + ((Literal) other).dataType + ")");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StringLikeLiteral)) {
            return false;
        }
        StringLikeLiteral that = (StringLikeLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int fastChildrenHashCode() {
        return doComputeHashCode();
    }

    @Override
    protected int computeHashCode() {
        return doComputeHashCode();
    }

    private int doComputeHashCode() {
        if (value != null && value.length() > 36) {
            return value.substring(0, 36).hashCode();
        } else {
            return Objects.hashCode(getValue());
        }
    }

    @Override
    public String toString() {
        return "'" + value + "'";
    }
}

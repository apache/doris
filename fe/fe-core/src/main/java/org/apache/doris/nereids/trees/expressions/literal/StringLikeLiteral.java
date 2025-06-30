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
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.qe.ConnectContext;

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
    public static final String toDateStrictRegex = "^(?:(?<year1>\\d{2}|\\d{4})-(?<month1>\\d{1,2})-(?<date1>\\d{1,2})"
            + "|(?<year2>\\d{2}|\\d{4})(?<month2>\\d{2})(?<date2>\\d{2}))"
            + "(?:(?:[T ])"
            + "(?:(?:(?<hour1>\\d{1,2})(?::(?<minute1>\\d{1,2})(?::(?<second1>\\d{1,2})(?<fraction1>\\.\\d*)?)?)?)"
            + "|(?:(?<hour2>\\d{2})(?:(?<minute2>\\d{2})(?:(?<second2>\\d{2})(?<fraction2>\\.\\d*)?)?)?)))?"
            + "(?:\\s*(?<tz>(?:[+-](?:\\d{1,2})(?::?(?:00|30|45))?)"
            + "|(?:(?i)(?:CST|UTC|GMT|ZULU|Z|(?:[A-Za-z_]+/[A-Za-z_]+)))))?$";
    public static final String toDateUnStrictRegex
            = "^\\s*((?<year>\\d{2,4})[^a-zA-Z\\d](?<month>\\d{1,2})[^a-zA-Z\\d](?<date>\\d{1,2}))"
            + "(?:(?:[ T])"
            + "(?:(?<hour>\\d{1,2})[^a-zA-Z\\d](?<minute>\\d{1,2})[^a-zA-Z\\d]"
            + "(?<second>\\d{1,2})(?<fraction>\\.\\d*)?))?"
            + "(?:\\s*(?<tz>[+-]\\d{1,2}(?::?(?:00|30|45))?|(?i)(?:CST|UTC|GMT|ZULU|Z|[A-Za-z_]+/[A-Za-z_]+)))?\\s*$";
    public static final String toDoubleRegex
            = "^\\s*(?:[+-]?(?:(?:[0-9]+|[0-9]+\\.[0-9]*|[0-9]*\\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
            + "|(?i)(?:INF|INFINITY)|(?i)NAN))\\s*$";
    public static final String toDecimalRegex
            = "^\\s*[+-]?((\\d+\\.\\d+)|(\\d+)|(\\d+\\.)|(\\.\\d+))(?:(e|E)[+-]?\\d+)?\\s*$";
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
        boolean strictCast = ConnectContext.get().getSessionVariable().enableStrictCast();
        if (targetType.isIntegralType()) {
            return castToIntegral(targetType, strictCast);
        }
        if (targetType.isDateType() || targetType.isDateV2Type()) {
            Expression expression = castToDateTime(DateTimeV2Type.MAX, strictCast);
            DateTimeV2Literal datetime = (DateTimeV2Literal) expression;
            if (targetType.isDateType()) {
                return new DateLiteral(datetime.year, datetime.month, datetime.day);
            } else {
                return new DateV2Literal(datetime.year, datetime.month, datetime.day);
            }
        } else if (targetType.isDateTimeV2Type()) {
            return castToDateTime(targetType, strictCast);
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
            if (isPosInf(trimmedValue)) {
                return Literal.of(Float.POSITIVE_INFINITY);
            }
            if (isNegInf(trimmedValue)) {
                return Literal.of(Float.NEGATIVE_INFINITY);
            }
            if (isNaN(trimmedValue)) {
                return Literal.of(Float.NaN);
            }
            return Literal.of(Float.parseFloat(value.trim()));
        }
        throw new CastException(String.format("%s can't cast to float in strict mode.", value));
    }

    protected Expression castToDouble() {
        String trimmedValue = value.trim();
        if (doublePattern.matcher(trimmedValue).matches()) {
            if (isPosInf(trimmedValue)) {
                return Literal.of(Double.POSITIVE_INFINITY);
            }
            if (isNegInf(trimmedValue)) {
                return Literal.of(Double.NEGATIVE_INFINITY);
            }
            if (isNaN(trimmedValue)) {
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

    protected Expression castToDateTime(DataType targetType, boolean strictCast) {
        Matcher strictMatcher = dateStrictPattern.matcher(value);
        if (strictMatcher.matches()) {
            String year = strictMatcher.group("year1") == null
                    ? strictMatcher.group("year2") : strictMatcher.group("year1");
            String month = strictMatcher.group("month1") == null
                    ? strictMatcher.group("month2") : strictMatcher.group("month1");
            String date = strictMatcher.group("date1") == null
                    ? strictMatcher.group("date2") : strictMatcher.group("date1");
            String hour = strictMatcher.group("hour1") == null
                    ? strictMatcher.group("hour2") : strictMatcher.group("hour1");
            String minute = strictMatcher.group("minute1") == null
                    ? strictMatcher.group("minute2") : strictMatcher.group("minute1");
            String second = strictMatcher.group("second1") == null
                    ? strictMatcher.group("second2") : strictMatcher.group("second1");
            String fraction = strictMatcher.group("fraction1") == null
                    ? strictMatcher.group("fraction2") : strictMatcher.group("fraction1");
            String tz = strictMatcher.group("tz");
            return getDateTimeLiteral(year, month, date, hour, minute, second, fraction, tz, targetType);
        } else if (!strictCast) {
            Matcher unStrictMatcher = dateUnStrictPattern.matcher(value);
            if (unStrictMatcher.matches()) {
                String year = unStrictMatcher.group("year");
                String month = unStrictMatcher.group("month");
                String date = unStrictMatcher.group("date");
                String hour = unStrictMatcher.group("hour");
                String minute = unStrictMatcher.group("minute");
                String second = unStrictMatcher.group("second");
                String fraction = unStrictMatcher.group("fraction");
                String tz = unStrictMatcher.group("tz");
                return getDateTimeLiteral(year, month, date, hour, minute, second, fraction, tz, targetType);
            }
        }
        throw new CastException(String.format("[%s] can't cast to %s.", value, targetType));
    }

    protected DateTimeV2Literal getDateTimeLiteral(String year, String month, String date, String hour, String minute,
            String second, String fraction, String tz, DataType targetType) {
        String year4 = year;
        if (year.length() == 2) {
            int year2 = Integer.parseInt(year);
            year4 = year2 >= 70 ? String.valueOf(year2 + 1900) : String.valueOf(2000 + year2);
        }
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
        int scale = ((DateTimeV2Type) targetType).getScale();
        fraction = fraction == null || fraction.equals(".")
                ? "" : fraction.substring(0, Math.min(fraction.length(), scale + 1));
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
    public String toString() {
        return "'" + value + "'";
    }
}

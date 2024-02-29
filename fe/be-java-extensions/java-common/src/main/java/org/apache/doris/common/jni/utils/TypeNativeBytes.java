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

package org.apache.doris.common.jni.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;

public class TypeNativeBytes {
    /**
     * Change the order of the bytes, Because JVM is Big-Endian , x86 is Little-Endian.
     */
    public static byte[] convertByteOrder(byte[] bytes) {
        int length = bytes.length;
        for (int i = 0; i < length / 2; ++i) {
            byte temp = bytes[i];
            bytes[i] = bytes[length - 1 - i];
            bytes[length - 1 - i] = temp;
        }
        return bytes;
    }

    public static byte[] getBigIntegerBytes(BigInteger v) {
        byte[] bytes = convertByteOrder(v.toByteArray());
        // here value is 16 bytes, so if result data greater than the maximum of 16
        // bytes, it will return a wrong num to backend;
        byte[] value = new byte[16];
        // check data is negative
        if (v.signum() == -1) {
            Arrays.fill(value, (byte) -1);
        }
        System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
        return value;
    }

    public static BigInteger getBigInteger(byte[] bytes) {
        // Convert the byte order back if necessary
        byte[] originalBytes = convertByteOrder(bytes);
        return new BigInteger(originalBytes);
    }

    public static byte[] getDecimalBytes(BigDecimal v, int scale, int size) {
        BigDecimal retValue = v.setScale(scale, RoundingMode.HALF_EVEN);
        BigInteger data = retValue.unscaledValue();
        byte[] bytes = convertByteOrder(data.toByteArray());
        byte[] value = new byte[size];
        if (data.signum() == -1) {
            Arrays.fill(value, (byte) -1);
        }

        System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
        return value;
    }

    public static BigDecimal getDecimal(byte[] bytes, int scale) {
        BigInteger value = new BigInteger(convertByteOrder(bytes));
        return new BigDecimal(value, scale);
    }

    public static long convertToDateTime(int year, int month, int day, int hour, int minute, int second,
            boolean isDate) {
        long time = 0;
        time = time + year;
        time = (time << 8) + month;
        time = (time << 8) + day;
        time = (time << 8) + hour;
        time = (time << 8) + minute;
        time = (time << 12) + second;
        int type = isDate ? 2 : 3;
        time = (time << 3) + type;
        //this bit is int neg = 0;
        time = (time << 1);
        return time;
    }

    public static int convertToDateV2(int year, int month, int day) {
        return (int) (day | (long) month << 5 | (long) year << 9);
    }

    public static long convertToDateTimeV2(int year, int month, int day, int hour, int minute, int second) {
        return (long) second << 20 | (long) minute << 26 | (long) hour << 32
                | (long) day << 37 | (long) month << 42 | (long) year << 46;
    }

    public static long convertToDateTimeV2(int year, int month, int day, int hour, int minute, int second,
            int microsecond) {
        return (long) microsecond | (long) second << 20 | (long) minute << 26 | (long) hour << 32
                | (long) day << 37 | (long) month << 42 | (long) year << 46;
    }

    public static LocalDate convertToJavaDateV1(long date) {
        int year = (int) (date >> 48);
        int yearMonth = (int) (date >> 40);
        int yearMonthDay = (int) (date >> 32);

        int month = (yearMonth & 0XFF);
        int day = (yearMonthDay & 0XFF);
        try {
            return LocalDate.of(year, month, day);
        } catch (DateTimeException e) {
            return null;
        }
    }

    public static Object convertToJavaDateV1(long date, Class clz) {
        int year = (int) (date >> 48);
        int yearMonth = (int) (date >> 40);
        int yearMonthDay = (int) (date >> 32);

        int month = (yearMonth & 0XFF);
        int day = (yearMonthDay & 0XFF);
        try {
            if (LocalDate.class.equals(clz)) {
                return LocalDate.of(year, month, day);
            } else if (java.util.Date.class.equals(clz)) {
                return new java.util.Date(year - 1900, month - 1, day);
            } else if (org.joda.time.LocalDate.class.equals(clz)) {
                return new org.joda.time.LocalDate(year, month, day);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static LocalDate convertToJavaDateV2(int date) {
        int year = date >> 9;
        int month = (date >> 5) & 0XF;
        int day = date & 0X1F;
        try {
            return LocalDate.of(year, month, day);
        } catch (DateTimeException e) {
            return null;
        }
    }

    public static Object convertToJavaDateV2(int date, Class clz) {
        int year = date >> 9;
        int month = (date >> 5) & 0XF;
        int day = date & 0X1F;
        try {
            if (LocalDate.class.equals(clz)) {
                return LocalDate.of(year, month, day);
            } else if (java.util.Date.class.equals(clz)) {
                return new java.util.Date(year - 1900, month - 1, day);
            } else if (org.joda.time.LocalDate.class.equals(clz)) {
                return new org.joda.time.LocalDate(year, month, day);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static LocalDateTime convertToJavaDateTimeV1(long time) {
        int year = (int) (time >> 48);
        int yearMonth = (int) (time >> 40);
        int yearMonthDay = (int) (time >> 32);

        int month = (yearMonth & 0XFF);
        int day = (yearMonthDay & 0XFF);

        int hourMinuteSecond = (int) (time % (1 << 31));
        int minuteTypeNeg = (hourMinuteSecond % (1 << 16));

        int hour = (hourMinuteSecond >> 24);
        int minute = ((hourMinuteSecond >> 16) & 0XFF);
        int second = (minuteTypeNeg >> 4);
        //here don't need those bits are type = ((minus_type_neg >> 1) & 0x7);

        try {
            return LocalDateTime.of(year, month, day, hour, minute, second);
        } catch (DateTimeException e) {
            return null;
        }
    }


    public static Object convertToJavaDateTimeV1(long time, Class clz) {
        int year = (int) (time >> 48);
        int yearMonth = (int) (time >> 40);
        int yearMonthDay = (int) (time >> 32);

        int month = (yearMonth & 0XFF);
        int day = (yearMonthDay & 0XFF);

        int hourMinuteSecond = (int) (time % (1 << 31));
        int minuteTypeNeg = (hourMinuteSecond % (1 << 16));

        int hour = (hourMinuteSecond >> 24);
        int minute = ((hourMinuteSecond >> 16) & 0XFF);
        int second = (minuteTypeNeg >> 4);
        //here don't need those bits are type = ((minus_type_neg >> 1) & 0x7);

        try {
            if (LocalDateTime.class.equals(clz)) {
                return LocalDateTime.of(year, month, day, hour, minute, second);
            } else if (org.joda.time.DateTime.class.equals(clz)) {
                return new org.joda.time.DateTime(year, month, day, hour, minute, second);
            } else if (org.joda.time.LocalDateTime.class.equals(clz)) {
                return new org.joda.time.LocalDateTime(year, month, day, hour, minute, second);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static LocalDateTime convertToJavaDateTimeV2(long time) {
        int year = (int) (time >> 46);
        int yearMonth = (int) (time >> 42);
        int yearMonthDay = (int) (time >> 37);

        int month = (yearMonth & 0XF);
        int day = (yearMonthDay & 0X1F);

        int hour = (int) ((time >> 32) & 0X1F);
        int minute = (int) ((time >> 26) & 0X3F);
        int second = (int) ((time >> 20) & 0X3F);
        int microsecond = (int) (time & 0XFFFFF);

        try {
            return LocalDateTime.of(year, month, day, hour, minute, second, microsecond * 1000);
        } catch (DateTimeException e) {
            return null;
        }
    }

    public static Object convertToJavaDateTimeV2(long time, Class clz) {
        int year = (int) (time >> 46);
        int yearMonth = (int) (time >> 42);
        int yearMonthDay = (int) (time >> 37);

        int month = (yearMonth & 0XF);
        int day = (yearMonthDay & 0X1F);

        int hour = (int) ((time >> 32) & 0X1F);
        int minute = (int) ((time >> 26) & 0X3F);
        int second = (int) ((time >> 20) & 0X3F);
        int microsecond = (int) (time & 0XFFFFF);

        try {
            if (LocalDateTime.class.equals(clz)) {
                return LocalDateTime.of(year, month, day, hour, minute, second, microsecond * 1000);
            } else if (org.joda.time.DateTime.class.equals(clz)) {
                return new org.joda.time.DateTime(year, month, day, hour, minute, second, microsecond / 1000);
            } else if (org.joda.time.LocalDateTime.class.equals(clz)) {
                return new org.joda.time.LocalDateTime(year, month, day, hour, minute, second, microsecond / 1000);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }
}

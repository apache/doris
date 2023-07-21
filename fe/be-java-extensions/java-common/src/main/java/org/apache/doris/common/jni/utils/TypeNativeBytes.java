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
        byte[] bytes = v.toByteArray();
        // If the BigInteger is not negative and the first byte is 0, remove the first byte
        if (v.signum() >= 0 && bytes[0] == 0) {
            bytes = Arrays.copyOfRange(bytes, 1, bytes.length);
        }
        // Convert the byte order if necessary
        return convertByteOrder(bytes);
    }

    public static BigInteger getBigInteger(byte[] bytes) {
        // Convert the byte order back if necessary
        byte[] originalBytes = convertByteOrder(bytes);
        // If the first byte has the sign bit set, add a 0 byte at the start
        if ((originalBytes[0] & 0x80) != 0) {
            byte[] extendedBytes = new byte[originalBytes.length + 1];
            extendedBytes[0] = 0;
            System.arraycopy(originalBytes, 0, extendedBytes, 1, originalBytes.length);
            originalBytes = extendedBytes;
        }
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

    public static LocalDate convertToJavaDate(int date) {
        int year = date >> 9;
        int month = (date >> 5) & 0XF;
        int day = date & 0X1F;
        LocalDate value;
        try {
            value = LocalDate.of(year, month, day);
        } catch (DateTimeException e) {
            value = LocalDate.MAX;
        }
        return value;
    }

    public static LocalDateTime convertToJavaDateTime(long time) {
        int year = (int) (time >> 46);
        int yearMonth = (int) (time >> 42);
        int yearMonthDay = (int) (time >> 37);

        int month = (yearMonth & 0XF);
        int day = (yearMonthDay & 0X1F);

        int hour = (int) ((time >> 32) & 0X1F);
        int minute = (int) ((time >> 26) & 0X3F);
        int second = (int) ((time >> 20) & 0X3F);
        int microsecond = (int) (time & 0XFFFFF);

        LocalDateTime value;
        try {
            value = LocalDateTime.of(year, month, day, hour, minute, second, microsecond * 1000);
        } catch (DateTimeException e) {
            value = LocalDateTime.MAX;
        }
        return value;
    }
}

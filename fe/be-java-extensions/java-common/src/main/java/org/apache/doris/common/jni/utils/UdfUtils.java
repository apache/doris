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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.common.exception.InternalException;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.vesoft.nebula.client.graph.data.DateTimeWrapper;
import com.vesoft.nebula.client.graph.data.DateWrapper;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import org.apache.log4j.Logger;
import sun.misc.Unsafe;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Set;

public class UdfUtils {
    public static final Logger LOG = Logger.getLogger(UdfUtils.class);
    public static final Unsafe UNSAFE;
    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;
    public static final long BYTE_ARRAY_OFFSET;
    public static final long INT_ARRAY_OFFSET;

    static {
        UNSAFE = (Unsafe) AccessController.doPrivileged(
                (PrivilegedAction<Object>) () -> {
                    try {
                        Field f = Unsafe.class.getDeclaredField("theUnsafe");
                        f.setAccessible(true);
                        return f.get(null);
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new Error();
                    }
                });
        BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
    }

    // Data types that are supported as return or argument types in Java UDFs.
    public enum JavaUdfDataType {
        INVALID_TYPE("INVALID_TYPE", TPrimitiveType.INVALID_TYPE, 0),
        BOOLEAN("BOOLEAN", TPrimitiveType.BOOLEAN, 1),
        TINYINT("TINYINT", TPrimitiveType.TINYINT, 1),
        SMALLINT("SMALLINT", TPrimitiveType.SMALLINT, 2),
        INT("INT", TPrimitiveType.INT, 4),
        BIGINT("BIGINT", TPrimitiveType.BIGINT, 8),
        FLOAT("FLOAT", TPrimitiveType.FLOAT, 4),
        DOUBLE("DOUBLE", TPrimitiveType.DOUBLE, 8),
        CHAR("CHAR", TPrimitiveType.CHAR, 0),
        VARCHAR("VARCHAR", TPrimitiveType.VARCHAR, 0),
        STRING("STRING", TPrimitiveType.STRING, 0),
        DATE("DATE", TPrimitiveType.DATE, 8),
        DATETIME("DATETIME", TPrimitiveType.DATETIME, 8),
        LARGEINT("LARGEINT", TPrimitiveType.LARGEINT, 16),
        DECIMALV2("DECIMALV2", TPrimitiveType.DECIMALV2, 16),
        DATEV2("DATEV2", TPrimitiveType.DATEV2, 4),
        DATETIMEV2("DATETIMEV2", TPrimitiveType.DATETIMEV2, 8),
        DECIMAL32("DECIMAL32", TPrimitiveType.DECIMAL32, 4),
        DECIMAL64("DECIMAL64", TPrimitiveType.DECIMAL64, 8),
        DECIMAL128("DECIMAL128", TPrimitiveType.DECIMAL128I, 16),
        ARRAY_TYPE("ARRAY_TYPE", TPrimitiveType.ARRAY, 0),
        MAP_TYPE("MAP_TYPE", TPrimitiveType.MAP, 0);
        private final String description;
        private final TPrimitiveType thriftType;
        private final int len;
        private int precision;
        private int scale;
        private Type itemType;
        private Type keyType;
        private Type valueType;
        JavaUdfDataType(String description, TPrimitiveType thriftType, int len) {
            this.description = description;
            this.thriftType = thriftType;
            this.len = len;
        }

        @Override
        public String toString() {
            return description;
        }

        public TPrimitiveType getPrimitiveType() {
            return thriftType;
        }

        public int getLen() {
            return len;
        }

        public static Set<JavaUdfDataType> getCandidateTypes(Class<?> c) {
            if (c == boolean.class || c == Boolean.class) {
                return Sets.newHashSet(JavaUdfDataType.BOOLEAN);
            } else if (c == byte.class || c == Byte.class) {
                return Sets.newHashSet(JavaUdfDataType.TINYINT);
            } else if (c == short.class || c == Short.class) {
                return Sets.newHashSet(JavaUdfDataType.SMALLINT);
            } else if (c == int.class || c == Integer.class) {
                return Sets.newHashSet(JavaUdfDataType.INT);
            } else if (c == long.class || c == Long.class) {
                return Sets.newHashSet(JavaUdfDataType.BIGINT);
            } else if (c == float.class || c == Float.class) {
                return Sets.newHashSet(JavaUdfDataType.FLOAT);
            } else if (c == double.class || c == Double.class) {
                return Sets.newHashSet(JavaUdfDataType.DOUBLE);
            } else if (c == char.class || c == Character.class) {
                return Sets.newHashSet(JavaUdfDataType.CHAR);
            } else if (c == String.class) {
                return Sets.newHashSet(JavaUdfDataType.STRING);
            } else if (Type.DATE_SUPPORTED_JAVA_TYPE.contains(c)) {
                return Sets.newHashSet(JavaUdfDataType.DATE, JavaUdfDataType.DATEV2);
            } else if (Type.DATETIME_SUPPORTED_JAVA_TYPE.contains(c)) {
                return Sets.newHashSet(JavaUdfDataType.DATETIME, JavaUdfDataType.DATETIMEV2);
            } else if (c == BigInteger.class) {
                return Sets.newHashSet(JavaUdfDataType.LARGEINT);
            } else if (c == BigDecimal.class) {
                return Sets.newHashSet(JavaUdfDataType.DECIMALV2, JavaUdfDataType.DECIMAL32, JavaUdfDataType.DECIMAL64,
                        JavaUdfDataType.DECIMAL128);
            } else if (c == java.util.ArrayList.class) {
                return Sets.newHashSet(JavaUdfDataType.ARRAY_TYPE);
            } else if (c == java.util.HashMap.class) {
                return Sets.newHashSet(JavaUdfDataType.MAP_TYPE);
            }
            return Sets.newHashSet(JavaUdfDataType.INVALID_TYPE);
        }

        public static boolean isSupported(Type t) {
            for (JavaUdfDataType javaType : JavaUdfDataType.values()) {
                if (javaType == JavaUdfDataType.INVALID_TYPE) {
                    continue;
                }
                if (javaType.getPrimitiveType() == t.getPrimitiveType().toThrift()) {
                    return true;
                }
            }
            return false;
        }

        public int getPrecision() {
            return precision;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public int getScale() {
            return this.thriftType == TPrimitiveType.DECIMALV2 ? 9 : scale;
        }

        public void setScale(int scale) {
            this.scale = scale;
        }

        public Type getItemType() {
            return itemType;
        }

        public void setItemType(Type type) {
            this.itemType = type;
        }

        public Type getKeyType() {
            return keyType;
        }

        public Type getValueType() {
            return valueType;
        }

        public void setKeyType(Type type) {
            this.keyType = type;
        }

        public void setValueType(Type type) {
            this.valueType = type;
        }
    }

    public static Pair<Type, Integer> fromThrift(TTypeDesc typeDesc, int nodeIdx) throws InternalException {
        TTypeNode node = typeDesc.getTypes().get(nodeIdx);
        Type type = null;
        switch (node.getType()) {
            case SCALAR: {
                Preconditions.checkState(node.isSetScalarType());
                TScalarType scalarType = node.getScalarType();
                if (scalarType.getType() == TPrimitiveType.CHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createCharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.VARCHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createVarcharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.DECIMALV2) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createDecimalType(scalarType.getPrecision(),
                            scalarType.getScale());
                } else if (scalarType.getType() == TPrimitiveType.DECIMAL32
                        || scalarType.getType() == TPrimitiveType.DECIMAL64
                        || scalarType.getType() == TPrimitiveType.DECIMAL128I) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createDecimalV3Type(scalarType.getPrecision(),
                            scalarType.getScale());
                } else {
                    type = ScalarType.createType(
                            PrimitiveType.fromThrift(scalarType.getType()));
                }
                break;
            }
            case ARRAY: {
                Preconditions.checkState(nodeIdx + 1 < typeDesc.getTypesSize());
                Pair<Type, Integer> childType = fromThrift(typeDesc, nodeIdx + 1);
                type = new ArrayType(childType.first);
                nodeIdx = childType.second;
                break;
            }
            case MAP: {
                Preconditions.checkState(nodeIdx + 2 < typeDesc.getTypesSize());
                Pair<Type, Integer> keyType = fromThrift(typeDesc, nodeIdx + 1);
                Pair<Type, Integer> valueType = fromThrift(typeDesc, keyType.second);
                type = new MapType(keyType.first, valueType.first);
                nodeIdx = valueType.second;
                break;
            }

            default:
                throw new InternalException("Return type " + node.getType() + " is not supported now!");
        }
        return Pair.of(type, nodeIdx);
    }

    public static long getAddressAtOffset(long base, int offset) {
        return base + 8L * offset;
    }

    public static void copyMemory(
            Object src, long srcOffset, Object dst, long dstOffset, long length) {
        // Check if dstOffset is before or after srcOffset to determine if we should copy
        // forward or backwards. This is necessary in case src and dst overlap.
        if (dstOffset < srcOffset) {
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
                srcOffset += size;
                dstOffset += size;
            }
        } else {
            srcOffset += length;
            dstOffset += length;
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                srcOffset -= size;
                dstOffset -= size;
                UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
            }

        }
    }

    public static URLClassLoader getClassLoader(String jarPath, ClassLoader parent)
            throws MalformedURLException, FileNotFoundException {
        File file = new File(jarPath);
        if (!file.exists()) {
            throw new FileNotFoundException("Can not find local file: " + jarPath);
        }
        URL url = file.toURI().toURL();
        return URLClassLoader.newInstance(new URL[] {url}, parent);
    }

    /**
     * Sets the return type of a Java UDF. Returns true if the return type is compatible
     * with the return type from the function definition. Throws an UdfRuntimeException
     * if the return type is not supported.
     */
    public static Pair<Boolean, JavaUdfDataType> setReturnType(Type retType, Class<?> udfReturnType)
            throws InternalException {
        if (!JavaUdfDataType.isSupported(retType)) {
            throw new InternalException("Unsupported return type: " + retType.toSql());
        }
        Set<JavaUdfDataType> javaTypes = JavaUdfDataType.getCandidateTypes(udfReturnType);
        // Check if the evaluate method return type is compatible with the return type from
        // the function definition. This happens when both of them map to the same primitive
        // type.
        Object[] res = javaTypes.stream().filter(
                t -> t.getPrimitiveType() == retType.getPrimitiveType().toThrift()).toArray();

        JavaUdfDataType result = res.length == 0 ? javaTypes.iterator().next() : (JavaUdfDataType) res[0];
        if (retType.isDecimalV3() || retType.isDatetimeV2()) {
            result.setPrecision(retType.getPrecision());
            result.setScale(((ScalarType) retType).getScalarScale());
        } else if (retType.isArrayType()) {
            ArrayType arrType = (ArrayType) retType;
            result.setItemType(arrType.getItemType());
            if (arrType.getItemType().isDatetimeV2() || arrType.getItemType().isDecimalV3()) {
                result.setPrecision(arrType.getItemType().getPrecision());
                result.setScale(((ScalarType) arrType.getItemType()).getScalarScale());
            }
        } else if (retType.isMapType()) {
            MapType mapType = (MapType) retType;
            result.setKeyType(mapType.getKeyType());
            result.setValueType(mapType.getValueType());
            if (mapType.getKeyType().isDatetimeV2() || mapType.getKeyType().isDecimalV3()) {
                result.setPrecision(mapType.getKeyType().getPrecision());
                result.setScale(((ScalarType) mapType.getKeyType()).getScalarScale());
            }
        }
        return Pair.of(res.length != 0, result);
    }

    /**
     * Sets the argument types of a Java UDF or UDAF. Returns true if the argument types specified
     * in the UDF are compatible with the argument types of the evaluate() function loaded
     * from the associated JAR file.
     */
    public static Pair<Boolean, JavaUdfDataType[]> setArgTypes(Type[] parameterTypes, Class<?>[] udfArgTypes,
            boolean isUdaf) {
        JavaUdfDataType[] inputArgTypes = new JavaUdfDataType[parameterTypes.length];
        int firstPos = isUdaf ? 1 : 0;
        for (int i = 0; i < parameterTypes.length; ++i) {
            Set<JavaUdfDataType> javaTypes = JavaUdfDataType.getCandidateTypes(udfArgTypes[i + firstPos]);
            int finalI = i;
            Object[] res = javaTypes.stream().filter(
                    t -> t.getPrimitiveType() == parameterTypes[finalI].getPrimitiveType().toThrift()).toArray();
            inputArgTypes[i] = res.length == 0 ? javaTypes.iterator().next() : (JavaUdfDataType) res[0];
            if (parameterTypes[finalI].isDecimalV3() || parameterTypes[finalI].isDatetimeV2()) {
                inputArgTypes[i].setPrecision(parameterTypes[finalI].getPrecision());
                inputArgTypes[i].setScale(((ScalarType) parameterTypes[finalI]).getScalarScale());
            } else if (parameterTypes[finalI].isArrayType()) {
                ArrayType arrType = (ArrayType) parameterTypes[finalI];
                inputArgTypes[i].setItemType(arrType.getItemType());
            } else if (parameterTypes[finalI].isMapType()) {
                MapType mapType = (MapType) parameterTypes[finalI];
                inputArgTypes[i].setKeyType(mapType.getKeyType());
                inputArgTypes[i].setValueType(mapType.getValueType());
            }
            if (res.length == 0) {
                return Pair.of(false, inputArgTypes);
            }
        }
        return Pair.of(true, inputArgTypes);
    }

    public static Object convertDateTimeV2ToJavaDateTime(long date, Class clz) {
        int year = (int) (date >> 46);
        int yearMonth = (int) (date >> 42);
        int yearMonthDay = (int) (date >> 37);

        int month = (yearMonth & 0XF);
        int day = (yearMonthDay & 0X1F);

        int hour = (int) ((date >> 32) & 0X1F);
        int minute = (int) ((date >> 26) & 0X3F);
        int second = (int) ((date >> 20) & 0X3F);
        //here don't need those bits are type = ((minus_type_neg >> 1) & 0x7);

        if (LocalDateTime.class.equals(clz)) {
            return convertToLocalDateTime(year, month, day, hour, minute, second);
        } else if (org.joda.time.DateTime.class.equals(clz)) {
            return convertToJodaDateTime(year, month, day, hour, minute, second);
        } else if (org.joda.time.LocalDateTime.class.equals(clz)) {
            return convertToJodaLocalDateTime(year, month, day, hour, minute, second);
        } else {
            return null;
        }
    }

    /**
     * input is a 64bit num from backend, and then get year, month, day, hour, minus, second by the order of bits.
     */
    public static Object convertDateTimeToJavaDateTime(long date, Class clz) {
        int year = (int) (date >> 48);
        int yearMonth = (int) (date >> 40);
        int yearMonthDay = (int) (date >> 32);

        int month = (yearMonth & 0XFF);
        int day = (yearMonthDay & 0XFF);

        int hourMinuteSecond = (int) (date % (1 << 31));
        int minuteTypeNeg = (hourMinuteSecond % (1 << 16));

        int hour = (hourMinuteSecond >> 24);
        int minute = ((hourMinuteSecond >> 16) & 0XFF);
        int second = (minuteTypeNeg >> 4);
        //here don't need those bits are type = ((minus_type_neg >> 1) & 0x7);

        if (LocalDateTime.class.equals(clz)) {
            return convertToLocalDateTime(year, month, day, hour, minute, second);
        } else if (org.joda.time.DateTime.class.equals(clz)) {
            return convertToJodaDateTime(year, month, day, hour, minute, second);
        } else if (org.joda.time.LocalDateTime.class.equals(clz)) {
            return convertToJodaLocalDateTime(year, month, day, hour, minute, second);
        } else {
            return null;
        }
    }

    public static Object convertDateV2ToJavaDate(int date, Class clz) {
        int year = date >> 9;
        int month = (date >> 5) & 0XF;
        int day = date & 0X1F;
        if (LocalDate.class.equals(clz)) {
            return convertToLocalDate(year, month, day);
        } else if (java.util.Date.class.equals(clz)) {
            return convertToJavaDate(year, month, day);
        } else if (org.joda.time.LocalDate.class.equals(clz)) {
            return convertToJodaDate(year, month, day);
        } else {
            return null;
        }
    }

    public static LocalDateTime convertToLocalDateTime(int year, int month, int day,
            int hour, int minute, int second) {
        LocalDateTime value = null;
        try {
            value = LocalDateTime.of(year, month, day, hour, minute, second);
        } catch (DateTimeException e) {
            LOG.warn("Error occurs when parsing date time value: {}", e);
        }
        return value;
    }

    public static org.joda.time.DateTime convertToJodaDateTime(int year, int month, int day,
            int hour, int minute, int second) {
        try {
            return new org.joda.time.DateTime(year, month, day, hour, minute, second);
        } catch (Exception e) {
            return null;
        }
    }

    public static org.joda.time.LocalDateTime convertToJodaLocalDateTime(int year, int month, int day,
            int hour, int minute, int second) {
        try {
            return new org.joda.time.LocalDateTime(year, month, day, hour, minute, second);
        } catch (Exception e) {
            return null;
        }
    }

    public static Object convertDateToJavaDate(long date, Class clz) {
        int year = (int) (date >> 48);
        int yearMonth = (int) (date >> 40);
        int yearMonthDay = (int) (date >> 32);

        int month = (yearMonth & 0XFF);
        int day = (yearMonthDay & 0XFF);
        if (LocalDate.class.equals(clz)) {
            return convertToLocalDate(year, month, day);
        } else if (java.util.Date.class.equals(clz)) {
            return convertToJavaDate(year, month, day);
        } else if (org.joda.time.LocalDate.class.equals(clz)) {
            return convertToJodaDate(year, month, day);
        } else {
            return null;
        }
    }

    /**
     * a 64bit num convertToDate.
     */
    public static LocalDate convertToLocalDate(int year, int month, int day) {
        LocalDate value = null;
        try {
            value = LocalDate.of(year, month, day);
        } catch (DateTimeException e) {
            LOG.warn("Error occurs when parsing date value: {}", e);
        }
        return value;
    }

    public static org.joda.time.LocalDate convertToJodaDate(int year, int month, int day) {
        try {
            return new org.joda.time.LocalDate(year, month, day);
        } catch (Exception e) {
            return null;
        }
    }

    public static java.util.Date convertToJavaDate(int year, int month, int day) {
        try {
            return new java.util.Date(year - 1900, month - 1, day);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * input is the second, minute, hours, day , month and year respectively.
     * and then combining all num to a 64bit value return to backend;
     */
    public static long convertToDateTime(Object obj, Class clz) {
        if (LocalDateTime.class.equals(clz)) {
            LocalDateTime date = (LocalDateTime) obj;
            return convertToDateTime(date.getYear(), date.getMonthValue(), date.getDayOfMonth(), date.getHour(),
                    date.getMinute(), date.getSecond(), false);
        } else if (org.joda.time.DateTime.class.equals(clz)) {
            org.joda.time.DateTime date = (org.joda.time.DateTime) obj;
            return convertToDateTime(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth(), date.getHourOfDay(),
                    date.getMinuteOfHour(), date.getSecondOfMinute(), false);
        } else if (org.joda.time.LocalDateTime.class.equals(clz)) {
            org.joda.time.LocalDateTime date = (org.joda.time.LocalDateTime) obj;
            return convertToDateTime(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth(), date.getHourOfDay(),
                    date.getMinuteOfHour(), date.getSecondOfMinute(), false);
        } else {
            return 0;
        }
    }

    public static long convertToDate(Object obj, Class clz) {
        if (LocalDate.class.equals(clz)) {
            LocalDate date = (LocalDate) obj;
            return convertToDateTime(date.getYear(), date.getMonthValue(), date.getDayOfMonth(), 0,
                    0, 0, true);
        } else if (java.util.Date.class.equals(clz)) {
            java.util.Date date = (java.util.Date) obj;
            return convertToDateTime(date.getYear() + 1900, date.getMonth(), date.getDay(), 0,
                    0, 0, true);
        } else if (org.joda.time.LocalDate.class.equals(clz)) {
            org.joda.time.LocalDate date = (org.joda.time.LocalDate) obj;
            return convertToDateTime(date.getYear(), date.getDayOfMonth(), date.getDayOfMonth(), 0,
                    0, 0, true);
        } else {
            return 0;
        }
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

    public static long convertToDateTimeV2(int year, int month, int day, int hour, int minute, int second) {
        return (long) second << 20 | (long) minute << 26 | (long) hour << 32
                | (long) day << 37 | (long) month << 42 | (long) year << 46;
    }

    public static long convertToDateTimeV2(
            int year, int month, int day, int hour, int minute, int second, int microsecond) {
        return (long) microsecond | (long) second << 20 | (long) minute << 26 | (long) hour << 32
                | (long) day << 37 | (long) month << 42 | (long) year << 46;
    }

    public static long convertToDateTimeV2(Object obj, Class clz) {
        if (LocalDateTime.class.equals(clz)) {
            LocalDateTime date = (LocalDateTime) obj;
            return convertToDateTimeV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth(), date.getHour(),
                    date.getMinute(), date.getSecond());
        } else if (org.joda.time.DateTime.class.equals(clz)) {
            org.joda.time.DateTime date = (org.joda.time.DateTime) obj;
            return convertToDateTimeV2(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth(), date.getHourOfDay(),
                    date.getMinuteOfHour(), date.getSecondOfMinute(), date.getMillisOfSecond() * 1000);
        } else if (org.joda.time.LocalDateTime.class.equals(clz)) {
            org.joda.time.LocalDateTime date = (org.joda.time.LocalDateTime) obj;
            return convertToDateTimeV2(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth(), date.getHourOfDay(),
                    date.getMinuteOfHour(), date.getSecondOfMinute(), date.getMillisOfSecond() * 1000);
        } else {
            return 0;
        }
    }

    public static int convertToDateV2(int year, int month, int day) {
        return (int) (day | (long) month << 5 | (long) year << 9);
    }

    public static int convertToDateV2(Object obj, Class clz) {
        if (LocalDate.class.equals(clz)) {
            LocalDate date = (LocalDate) obj;
            return convertToDateV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
        } else if (java.util.Date.class.equals(clz)) {
            java.util.Date date = (java.util.Date) obj;
            return convertToDateV2(date.getYear(), date.getMonth(), date.getDay());
        } else if (org.joda.time.LocalDate.class.equals(clz)) {
            org.joda.time.LocalDate date = (org.joda.time.LocalDate) obj;
            return convertToDateV2(date.getYear(), date.getDayOfMonth(), date.getDayOfMonth());
        } else {
            return 0;
        }
    }

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

    // only used by nebula-graph
    // transfer to an object that can copy to the block
    public static Object convertObject(ValueWrapper value) {
        try {
            if (value.isLong()) {
                return value.asLong();
            }
            if (value.isBoolean()) {
                return value.asBoolean();
            }
            if (value.isDouble()) {
                return value.asDouble();
            }
            if (value.isString()) {
                return value.asString();
            }
            if (value.isTime()) {
                return value.asTime().toString();
            }
            if (value.isDate()) {
                DateWrapper date = value.asDate();
                return LocalDate.of(date.getYear(), date.getMonth(), date.getDay());
            }
            if (value.isDateTime()) {
                DateTimeWrapper dateTime = value.asDateTime();
                return LocalDateTime.of(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay(),
                        dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(), dateTime.getMicrosec() * 1000);
            }
            if (value.isVertex()) {
                return value.asNode().toString();
            }
            if (value.isEdge()) {
                return value.asRelationship().toString();
            }
            if (value.isPath()) {
                return value.asPath().toString();
            }
            if (value.isList()) {
                return value.asList().toString();
            }
            if (value.isSet()) {
                return value.asSet().toString();
            }
            if (value.isMap()) {
                return value.asMap().toString();
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}

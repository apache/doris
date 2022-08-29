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

package org.apache.doris.udf;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.udf.UdfExecutor.JavaUdfDataType;

import com.google.common.base.Preconditions;
import sun.misc.Unsafe;

import java.io.File;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class UdfUtils {
    public static final Unsafe UNSAFE;
    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;
    public static final long BYTE_ARRAY_OFFSET;
    public static final char END_OF_STRING = '\0';

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
    }

    protected static Pair<Type, Integer> fromThrift(TTypeDesc typeDesc, int nodeIdx) throws InternalException {
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
                } else {
                    type = ScalarType.createType(
                            PrimitiveType.fromThrift(scalarType.getType()));
                }
                break;
            }
            default:
                throw new InternalException("Return type " + node.getType() + " is not supported now!");
        }
        return Pair.of(type, nodeIdx);
    }

    protected static long getAddressAtOffset(long base, int offset) {
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

    public static URLClassLoader getClassLoader(String jarPath, ClassLoader parent) throws MalformedURLException {
        URL url = new File(jarPath).toURI().toURL();
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
        JavaUdfDataType javaType = JavaUdfDataType.getType(udfReturnType);
        // Check if the evaluate method return type is compatible with the return type from
        // the function definition. This happens when both of them map to the same primitive
        // type.
        if (retType.getPrimitiveType().toThrift() != javaType.getPrimitiveType()) {
            return Pair.of(false, javaType);
        }
        return Pair.of(true, javaType);
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
            inputArgTypes[i] = JavaUdfDataType.getType(udfArgTypes[i + firstPos]);
            if (inputArgTypes[i].getPrimitiveType() != parameterTypes[i].getPrimitiveType().toThrift()) {
                return Pair.of(false, inputArgTypes);
            }
        }
        return Pair.of(true, inputArgTypes);
    }

    /**
     * input is a 64bit num from backend, and then get year, month, day, hour, minus, second by the order of bits.
     */
    public static LocalDateTime convertToDateTime(long date) {
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

        LocalDateTime value = LocalDateTime.of(year, month, day, hour, minute, second);
        return value;
    }

    /**
     * a 64bit num convertToDate.
     */
    public static LocalDate convertToDate(long date) {
        int year = (int) (date >> 48);
        int yearMonth = (int) (date >> 40);
        int yearMonthDay = (int) (date >> 32);

        int month = (yearMonth & 0XFF);
        int day = (yearMonthDay & 0XFF);
        LocalDate value = LocalDate.of(year, month, day);
        return value;
    }

    /**
     * input is the second, minute, hours, day , month and year respectively.
     * and then combining all num to a 64bit value return to backend;
     */
    public static long convertDateTimeToLong(int year, int month, int day, int hour, int minute, int second,
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
}

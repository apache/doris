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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.common.exception.InternalException;

import org.apache.log4j.Logger;
import sun.misc.Unsafe;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
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

        JavaUdfDataType result = new JavaUdfDataType(
                res.length == 0 ? javaTypes.iterator().next() : (JavaUdfDataType) res[0]);
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
            Type keyType = mapType.getKeyType();
            Type valuType = mapType.getValueType();
            if (keyType.isDatetimeV2() || keyType.isDecimalV3()) {
                result.setKeyScale(((ScalarType) keyType).getScalarScale());
            }
            if (valuType.isDatetimeV2() || valuType.isDecimalV3()) {
                result.setValueScale(((ScalarType) valuType).getScalarScale());
            }
        } else if (retType.isStructType()) {
            StructType structType = (StructType) retType;
            result.setFields(structType.getFields());
        }
        return Pair.of(res.length != 0, result);
    }

    /**
     * Sets the argument types of a Java UDF or UDAF. Returns true if the argument types specified
     * in the UDF are compatible with the argument types of the evaluate() function loaded
     * from the associated JAR file.
     *
     * @throws InternalException
     */
    public static Pair<Boolean, JavaUdfDataType[]> setArgTypes(Type[] parameterTypes, Class<?>[] udfArgTypes,
            boolean isUdaf) throws InternalException {
        JavaUdfDataType[] inputArgTypes = new JavaUdfDataType[parameterTypes.length];
        int firstPos = isUdaf ? 1 : 0;
        for (int i = 0; i < parameterTypes.length; ++i) {
            Set<JavaUdfDataType> javaTypes = JavaUdfDataType.getCandidateTypes(udfArgTypes[i + firstPos]);
            int finalI = i;
            Object[] res = javaTypes.stream().filter(
                    t -> t.getPrimitiveType() == parameterTypes[finalI].getPrimitiveType().toThrift()).toArray();
            inputArgTypes[i] = new JavaUdfDataType(
                    res.length == 0 ? javaTypes.iterator().next() : (JavaUdfDataType) res[0]);
            if (parameterTypes[finalI].isDecimalV3() || parameterTypes[finalI].isDatetimeV2()) {
                inputArgTypes[i].setPrecision(parameterTypes[finalI].getPrecision());
                inputArgTypes[i].setScale(((ScalarType) parameterTypes[finalI]).getScalarScale());
            } else if (parameterTypes[finalI].isArrayType()) {
                ArrayType arrType = (ArrayType) parameterTypes[finalI];
                inputArgTypes[i].setItemType(arrType.getItemType());
                if (arrType.getItemType().isDatetimeV2() || arrType.getItemType().isDecimalV3()) {
                    inputArgTypes[i].setPrecision(arrType.getItemType().getPrecision());
                    inputArgTypes[i].setScale(((ScalarType) arrType.getItemType()).getScalarScale());
                }
            } else if (parameterTypes[finalI].isMapType()) {
                MapType mapType = (MapType) parameterTypes[finalI];
                Type keyType = mapType.getKeyType();
                Type valuType = mapType.getValueType();
                inputArgTypes[i].setKeyType(mapType.getKeyType());
                inputArgTypes[i].setValueType(mapType.getValueType());
                if (keyType.isDatetimeV2() || keyType.isDecimalV3()) {
                    inputArgTypes[i].setKeyScale(((ScalarType) keyType).getScalarScale());
                }
                if (valuType.isDatetimeV2() || valuType.isDecimalV3()) {
                    inputArgTypes[i].setValueScale(((ScalarType) valuType).getScalarScale());
                }
            } else if (parameterTypes[finalI].isStructType()) {
                StructType structType = (StructType) parameterTypes[finalI];
                ArrayList<StructField> fields = structType.getFields();
                inputArgTypes[i].setFields(fields);
            }
            if (res.length == 0) {
                return Pair.of(false, inputArgTypes);
            }
        }
        return Pair.of(true, inputArgTypes);
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

    public static long convertToDateTimeV2(
            int year, int month, int day, int hour, int minute, int second, int microsecond) {
        return (long) microsecond | (long) second << 20 | (long) minute << 26 | (long) hour << 32
                | (long) day << 37 | (long) month << 42 | (long) year << 46;
    }

    public static int convertToDateV2(int year, int month, int day) {
        return (int) (day | (long) month << 5 | (long) year << 9);
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

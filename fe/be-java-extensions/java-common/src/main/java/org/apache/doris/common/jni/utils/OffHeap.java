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


import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Reference to Apache Spark with some customization.
 * Default call native method to allocate and release memory, which will be tracked by memory tracker in BE.
 * Call {@link OffHeap#setTesting()} in test scenario.
 */
public class OffHeap {
    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

    private static boolean IS_TESTING = false;

    public static final Unsafe UNSAFE;

    public static final int BOOLEAN_ARRAY_OFFSET;

    public static final int BYTE_ARRAY_OFFSET;

    public static final int SHORT_ARRAY_OFFSET;

    public static final int INT_ARRAY_OFFSET;

    public static final int LONG_ARRAY_OFFSET;

    public static final int FLOAT_ARRAY_OFFSET;

    public static final int DOUBLE_ARRAY_OFFSET;

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
        BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
        BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
        INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
        LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
        FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
        DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);
    }

    public static void setTesting() {
        IS_TESTING = true;
    }

    public static int getInt(Object object, long offset) {
        return UNSAFE.getInt(object, offset);
    }

    public static int[] getInt(Object object, long offset, int length) {
        int[] result = new int[length];
        UNSAFE.copyMemory(object, offset, result, INT_ARRAY_OFFSET, (long) length * Integer.BYTES);
        return result;
    }

    public static void putInt(Object object, long offset, int value) {
        UNSAFE.putInt(object, offset, value);
    }

    public static boolean getBoolean(Object object, long offset) {
        return UNSAFE.getBoolean(object, offset);
    }

    public static boolean[] getBoolean(Object object, long offset, int length) {
        boolean[] result = new boolean[length];
        UNSAFE.copyMemory(object, offset, result, BOOLEAN_ARRAY_OFFSET, length);
        return result;
    }

    public static void putBoolean(Object object, long offset, boolean value) {
        UNSAFE.putBoolean(object, offset, value);
    }

    public static byte getByte(Object object, long offset) {
        return UNSAFE.getByte(object, offset);
    }

    public static byte[] getByte(Object object, long offset, int length) {
        byte[] result = new byte[length];
        UNSAFE.copyMemory(object, offset, result, BYTE_ARRAY_OFFSET, length * Byte.BYTES);
        return result;
    }

    public static void putByte(Object object, long offset, byte value) {
        UNSAFE.putByte(object, offset, value);
    }

    public static short getShort(Object object, long offset) {
        return UNSAFE.getShort(object, offset);
    }

    public static short[] getShort(Object object, long offset, int length) {
        short[] result = new short[length];
        UNSAFE.copyMemory(object, offset, result, SHORT_ARRAY_OFFSET, (long) length * Short.BYTES);
        return result;
    }

    public static void putShort(Object object, long offset, short value) {
        UNSAFE.putShort(object, offset, value);
    }

    public static long getLong(Object object, long offset) {
        return UNSAFE.getLong(object, offset);
    }

    public static long[] getLong(Object object, long offset, int length) {
        long[] result = new long[length];
        UNSAFE.copyMemory(object, offset, result, LONG_ARRAY_OFFSET, (long) length * Long.BYTES);
        return result;
    }

    public static void putLong(Object object, long offset, long value) {
        UNSAFE.putLong(object, offset, value);
    }

    public static float getFloat(Object object, long offset) {
        return UNSAFE.getFloat(object, offset);
    }

    public static float[] getFloat(Object object, long offset, int length) {
        float[] result = new float[length];
        UNSAFE.copyMemory(object, offset, result, FLOAT_ARRAY_OFFSET, (long) length * Float.BYTES);
        return result;
    }

    public static void putFloat(Object object, long offset, float value) {
        UNSAFE.putFloat(object, offset, value);
    }

    public static double getDouble(Object object, long offset) {
        return UNSAFE.getDouble(object, offset);
    }

    public static double[] getDouble(Object object, long offset, int length) {
        double[] result = new double[length];
        UNSAFE.copyMemory(object, offset, result, DOUBLE_ARRAY_OFFSET, (long) length * Double.BYTES);
        return result;
    }

    public static void putDouble(Object object, long offset, double value) {
        UNSAFE.putDouble(object, offset, value);
    }

    public static void setMemory(long address, byte value, long size) {
        UNSAFE.setMemory(address, size, value);
    }

    public static long allocateMemory(long size) {
        if (IS_TESTING) {
            return UNSAFE.allocateMemory(size);
        } else {
            return JNINativeMethod.memoryTrackerMalloc(size);
        }
    }

    public static void freeMemory(long address) {
        if (IS_TESTING) {
            UNSAFE.freeMemory(address);
        } else {
            JNINativeMethod.memoryTrackerFree(address);
        }
    }

    public static long reallocateMemory(long address, long oldSize, long newSize) {
        long newMemory = allocateMemory(newSize);
        copyMemory(null, address, null, newMemory, oldSize);
        freeMemory(address);
        return newMemory;
    }

    public static void copyMemory(Object src, long srcOffset, Object dst, long dstOffset, long length) {
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
}

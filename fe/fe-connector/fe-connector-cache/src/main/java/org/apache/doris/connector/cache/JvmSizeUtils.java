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

package org.apache.doris.connector.cache;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Low-cost JVM heap layout formulas for type-specific connector cache estimators.
 *
 * <p>This class reflects class declarations once to calculate shallow sizes. It never reads fields from a runtime
 * object and never walks an object graph.
 */
public final class JvmSizeUtils {
    private static final VmLayout VM_LAYOUT = VmLayout.detect();
    private static final boolean COMPACT_STRINGS = vmBoolean("CompactStrings");

    private static final ClassValue<Long> SHALLOW_SIZES = new ClassValue<>() {
        @Override
        protected Long computeValue(Class<?> type) {
            if (type.isArray()) {
                throw new IllegalArgumentException("Array size depends on its length: " + type);
            }
            long size = VM_LAYOUT.objectHeaderBytes();
            for (Class<?> current = type; current != null; current = current.getSuperclass()) {
                for (Field field : current.getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers())) {
                        size = saturatedAdd(size, fieldSize(field.getType()));
                    }
                }
            }
            return align(size);
        }
    };
    private static final long STRING_SHALLOW_BYTES = instanceSize(String.class);
    private static final long ARRAY_LIST_SHALLOW_BYTES = instanceSize(java.util.ArrayList.class);

    private JvmSizeUtils() {
    }

    public static long instanceSize(Class<?> type) {
        return SHALLOW_SIZES.get(type);
    }

    public static long objectArraySize(int length) {
        return arraySize(length, VM_LAYOUT.referenceBytes());
    }

    public static long byteArraySize(int length) {
        return arraySize(length, Byte.BYTES);
    }

    public static long intArraySize(int length) {
        return arraySize(length, Integer.BYTES);
    }

    public static long longArraySize(int length) {
        return arraySize(length, Long.BYTES);
    }

    public static long arrayListSize(int backingArrayCapacity) {
        return saturatedAdd(ARRAY_LIST_SHALLOW_BYTES, objectArraySize(backingArrayCapacity));
    }

    /** Estimate the heap retained by a Java 17 String and its compact-string byte array. */
    public static long stringSize(String value) {
        if (value == null) {
            return 0L;
        }
        int bytesPerCharacter = COMPACT_STRINGS && isLatin1(value) ? Byte.BYTES : Character.BYTES;
        long valueBytes = saturatedMultiply(value.length(), bytesPerCharacter);
        int arrayLength = valueBytes >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) valueBytes;
        return saturatedAdd(STRING_SHALLOW_BYTES, byteArraySize(arrayLength));
    }

    public static long saturatedAdd(long left, long right) {
        if (right > 0L && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    public static long saturatedMultiply(long left, long right) {
        if (left == 0L || right == 0L) {
            return 0L;
        }
        if (left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    private static long arraySize(int length, int elementBytes) {
        long elements = saturatedMultiply(length, elementBytes);
        return align(saturatedAdd(VM_LAYOUT.arrayHeaderBytes(), elements));
    }

    private static long fieldSize(Class<?> type) {
        if (!type.isPrimitive()) {
            return VM_LAYOUT.referenceBytes();
        }
        if (type == long.class || type == double.class) {
            return Long.BYTES;
        }
        if (type == int.class || type == float.class) {
            return Integer.BYTES;
        }
        if (type == short.class || type == char.class) {
            return Short.BYTES;
        }
        return Byte.BYTES;
    }

    private static boolean isLatin1(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) > 0xff) {
                return false;
            }
        }
        return true;
    }

    private static long align(long value) {
        long remainder = value % VM_LAYOUT.objectAlignmentBytes();
        return remainder == 0L
                ? value
                : saturatedAdd(value, VM_LAYOUT.objectAlignmentBytes() - remainder);
    }

    private static boolean vmBoolean(String option) {
        return Boolean.parseBoolean(hotSpotDiagnostic().getVMOption(option).getValue());
    }

    private static int vmInt(String option) {
        return Integer.parseInt(hotSpotDiagnostic().getVMOption(option).getValue());
    }

    private static HotSpotDiagnosticMXBean hotSpotDiagnostic() {
        return ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    }

    private record VmLayout(
            int referenceBytes,
            int objectHeaderBytes,
            int arrayHeaderBytes,
            int objectAlignmentBytes) {

        private static VmLayout detect() {
            int referenceBytes = vmBoolean("UseCompressedOops") ? Integer.BYTES : Long.BYTES;
            int classPointerBytes = vmBoolean("UseCompressedClassPointers") ? Integer.BYTES : Long.BYTES;
            int alignment = vmInt("ObjectAlignmentInBytes");
            int objectHeaderBytes = Long.BYTES + classPointerBytes;
            int arrayHeaderBytes = Math.toIntExact(
                    alignWithoutLayout(objectHeaderBytes + Integer.BYTES, alignment));
            return new VmLayout(referenceBytes, objectHeaderBytes, arrayHeaderBytes, alignment);
        }

        private static long alignWithoutLayout(long value, int alignment) {
            long remainder = value % alignment;
            return remainder == 0L ? value : value + alignment - remainder;
        }
    }
}

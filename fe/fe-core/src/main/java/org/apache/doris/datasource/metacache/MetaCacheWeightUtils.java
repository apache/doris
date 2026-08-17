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

package org.apache.doris.datasource.metacache;

import org.apache.doris.datasource.NameMapping;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Overflow-safe helpers for conservative external metadata cache weights. */
public final class MetaCacheWeightUtils {
    private static final long NAME_MAPPING_BASE_BYTES = 64L;
    private static final MethodHandle STRING_VALUE_GETTER;
    private static final long STRING_VALUE_OFFSET;
    private static final int OBJECT_ALIGNMENT_BYTES;
    private static final int OBJECT_REFERENCE_BYTES;
    private static final int OBJECT_HEADER_BYTES;
    private static final int OBJECT_ARRAY_BASE_BYTES;
    private static final int BYTE_ARRAY_BASE_BYTES;
    private static final int CHAR_ARRAY_BASE_BYTES;
    private static final int INT_ARRAY_BASE_BYTES;
    private static final boolean SUPPORTED_OBJECT_LAYOUT;
    private static final long OBJECT_LAYOUT_PERCENT;

    static {
        MethodHandle stringValueGetter = null;
        long stringValueOffset = -1L;
        int referenceBytes = Long.BYTES;
        // 24B is the safe fallback for an uncompressed class pointer. Unsafe replaces these
        // values with the exact active-VM layout when access is available.
        int objectArrayBaseBytes = 24;
        int byteArrayBaseBytes = 24;
        int charArrayBaseBytes = 24;
        int intArrayBaseBytes = 24;
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            stringValueOffset = (long) unsafeClass
                    .getMethod("objectFieldOffset", Field.class)
                    .invoke(unsafe, String.class.getDeclaredField("value"));
            stringValueGetter = MethodHandles.lookup()
                    .unreflect(unsafeClass.getMethod("getObject", Object.class, long.class))
                    .bindTo(unsafe)
                    .asType(MethodType.methodType(Object.class, Object.class, long.class));
            referenceBytes = (int) unsafeClass
                    .getMethod("arrayIndexScale", Class.class)
                    .invoke(unsafe, Object[].class);
            objectArrayBaseBytes = (int) unsafeClass
                    .getMethod("arrayBaseOffset", Class.class)
                    .invoke(unsafe, Object[].class);
            byteArrayBaseBytes = (int) unsafeClass
                    .getMethod("arrayBaseOffset", Class.class)
                    .invoke(unsafe, byte[].class);
            charArrayBaseBytes = (int) unsafeClass
                    .getMethod("arrayBaseOffset", Class.class)
                    .invoke(unsafe, char[].class);
            intArrayBaseBytes = (int) unsafeClass
                    .getMethod("arrayBaseOffset", Class.class)
                    .invoke(unsafe, int[].class);
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            // A conservative UTF-16 fallback is used when the VM hides String storage.
        }
        STRING_VALUE_GETTER = stringValueGetter;
        STRING_VALUE_OFFSET = stringValueOffset;
        OBJECT_REFERENCE_BYTES = referenceBytes;
        OBJECT_ARRAY_BASE_BYTES = objectArrayBaseBytes;
        BYTE_ARRAY_BASE_BYTES = byteArrayBaseBytes;
        CHAR_ARRAY_BASE_BYTES = charArrayBaseBytes;
        INT_ARRAY_BASE_BYTES = intArrayBaseBytes;
        String alignmentOption = readVmOption("ObjectAlignmentInBytes");
        int objectAlignmentBytes = parseObjectAlignment(alignmentOption);
        OBJECT_ALIGNMENT_BYTES = objectAlignmentBytes;
        SUPPORTED_OBJECT_LAYOUT = alignmentOption != null
                && (objectAlignmentBytes == 8 || objectAlignmentBytes == 16);
        boolean compressedClassPointers = readBooleanVmOption(
                "UseCompressedClassPointers", false);
        OBJECT_HEADER_BYTES = Long.BYTES
                + (compressedClassPointers ? Integer.BYTES : Long.BYTES);
        long referencePercent = referenceBytes <= Integer.BYTES ? 100L : 145L;
        long classPointerPercent = compressedClassPointers ? 100L : 140L;
        long alignmentPercent = objectAlignmentBytes <= 8 ? 100L : 120L;
        OBJECT_LAYOUT_PERCENT = (referencePercent * classPointerPercent * alignmentPercent
                + 9_999L) / 10_000L;
    }

    private MetaCacheWeightUtils() {
    }

    public static long estimatedStringBytes(String value) {
        if (value == null) {
            return 0L;
        }
        Object storage = stringStorage(value);
        long backingArrayBytes;
        if (storage instanceof byte[]) {
            backingArrayBytes = estimatedByteArrayBytes(((byte[]) storage).length);
        } else if (storage instanceof char[]) {
            backingArrayBytes = alignedArrayBytes(
                    CHAR_ARRAY_BASE_BYTES, ((char[]) storage).length, Character.BYTES);
        } else {
            backingArrayBytes = alignedArrayBytes(
                    CHAR_ARRAY_BASE_BYTES, value.length(), Character.BYTES);
        }
        return saturatedAdd(estimatedObjectLayoutBytes(1L, 6L), backingArrayBytes);
    }

    /** Estimate retained character data without materializing a String copy. */
    public static long estimatedCharSequenceBytes(CharSequence value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof String) {
            return estimatedStringBytes((String) value);
        }
        return saturatedAdd(estimatedObjectLayoutBytes(1L, 6L),
                alignedArrayBytes(CHAR_ARRAY_BASE_BYTES, value.length(), Character.BYTES));
    }

    /** Whether calibrated formulas support the active VM object alignment. */
    public static boolean isSupportedJvmObjectLayout() {
        return SUPPORTED_OBJECT_LAYOUT;
    }

    /** Adjust a default compressed-reference object-graph constant to the active VM layout. */
    public static long estimatedObjectBytes(long compressedReferenceBytes) {
        long product = saturatedMultiply(compressedReferenceBytes, OBJECT_LAYOUT_PERCENT);
        if (product == Long.MAX_VALUE) {
            return product;
        }
        long roundedProduct = saturatedAdd(product, 99L);
        return roundedProduct == Long.MAX_VALUE ? roundedProduct : roundedProduct / 100L;
    }

    /** Returns the actual backing-array payload in O(1), or a conservative UTF-16 fallback. */
    public static long estimatedStringPayloadBytes(String value) {
        if (value == null) {
            return 0L;
        }
        Object storage = stringStorage(value);
        if (storage instanceof byte[]) {
            return alignPayload(((byte[]) storage).length);
        }
        if (storage instanceof char[]) {
            return alignPayload(saturatedMultiply(
                    ((char[]) storage).length, Character.BYTES));
        }
        return alignPayload(saturatedMultiply(value.length(), Character.BYTES));
    }

    /** Estimate a generated String whose encoded width is derived from its source components. */
    public static long estimatedGeneratedStringBytes(long characterCount, boolean latin1) {
        long payloadBytes = saturatedMultiply(characterCount, latin1 ? 1L : Character.BYTES);
        return saturatedAdd(estimatedObjectLayoutBytes(1L, 6L),
                estimatedByteArrayBytes(payloadBytes));
    }

    /** Whether this VM stores the String with one byte per character. */
    public static boolean isLatin1String(String value) {
        if (value == null || value.isEmpty()) {
            return true;
        }
        Object storage = stringStorage(value);
        return storage instanceof byte[] && ((byte[]) storage).length == value.length();
    }

    /** VM-layout size of a retained byte array, conservatively if VM introspection is hidden. */
    public static long estimatedByteArrayBytes(long length) {
        return alignedArrayBytes(BYTE_ARRAY_BASE_BYTES, length, Byte.BYTES);
    }

    /** VM-layout size of an object-reference array, conservatively if introspection is hidden. */
    public static long estimatedObjectArrayBytes(long length) {
        return alignedArrayBytes(OBJECT_ARRAY_BASE_BYTES, length, OBJECT_REFERENCE_BYTES);
    }

    /** Size of an object with a known field layout on the active VM. */
    public static long estimatedObjectLayoutBytes(long referenceFields, long primitiveBytes) {
        if (referenceFields < 0L || primitiveBytes < 0L) {
            return Long.MAX_VALUE;
        }
        long bytes = saturatedAdd(
                OBJECT_HEADER_BYTES,
                saturatedMultiply(referenceFields, OBJECT_REFERENCE_BYTES));
        return alignPayload(saturatedAdd(bytes, primitiveBytes));
    }

    /** VM-layout size of a retained int array, conservatively if introspection is hidden. */
    public static long estimatedIntArrayBytes(long length) {
        return alignedArrayBytes(INT_ARRAY_BASE_BYTES, length, Integer.BYTES);
    }

    /** Incremental VM-layout payload of an int array whose header is accounted elsewhere. */
    public static long estimatedIntArrayPayloadBytes(long length) {
        long populated = alignedArrayBytes(INT_ARRAY_BASE_BYTES, length, Integer.BYTES);
        long empty = alignPayload(INT_ARRAY_BASE_BYTES);
        return populated == Long.MAX_VALUE ? populated : populated - empty;
    }

    /** Estimate the fixed set of names retained by a cache key. */
    public static long estimatedNameMappingBytes(NameMapping nameMapping) {
        if (nameMapping == null) {
            return 0L;
        }
        long bytes = estimatedObjectBytes(NAME_MAPPING_BASE_BYTES);
        bytes = saturatedAdd(bytes, estimatedStringBytes(nameMapping.getLocalDbName()));
        bytes = saturatedAdd(bytes, estimatedStringBytes(nameMapping.getLocalTblName()));
        bytes = saturatedAdd(bytes, estimatedStringBytes(nameMapping.getRemoteDbName()));
        return saturatedAdd(bytes, estimatedStringBytes(nameMapping.getRemoteTblName()));
    }

    /**
     * Whether {@code type} itself declares exactly the expected non-static instance fields, each
     * written as {@code name:SimpleTypeName}. Estimator formulas are calibrated against pinned SDK
     * layouts; callers fail closed when a library upgrade adds, removes or retypes a field so a
     * new retained reference cannot be silently undercounted. Superclasses are pinned separately.
     */
    public static boolean hasExpectedInstanceFields(Class<?> type, String... expectedFields) {
        if (type == null) {
            return false;
        }
        Set<String> expected = new HashSet<>(Arrays.asList(expectedFields));
        Set<String> actual = new HashSet<>();
        try {
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
                    continue;
                }
                actual.add(field.getName() + ":" + field.getType().getSimpleName());
            }
        } catch (RuntimeException | LinkageError e) {
            return false;
        }
        return actual.equals(expected);
    }

    /** Same as {@link #hasExpectedInstanceFields(Class, String...)} for a class resolved by name. */
    public static boolean hasExpectedInstanceFields(
            String className, ClassLoader loader, String... expectedFields) {
        try {
            return hasExpectedInstanceFields(
                    Class.forName(className, false, loader), expectedFields);
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            return false;
        }
    }

    public static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    public static long saturatedMultiply(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    private static boolean readBooleanVmOption(String option, boolean fallback) {
        String value = readVmOption(option);
        return value == null ? fallback : Boolean.parseBoolean(value);
    }

    private static String readVmOption(String option) {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends PlatformManagedObject> beanClass =
                    (Class<? extends PlatformManagedObject>)
                            Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
            Object bean = ManagementFactory.getPlatformMXBean(beanClass);
            Method getVmOption = beanClass.getMethod("getVMOption", String.class);
            Object vmOption = getVmOption.invoke(bean, option);
            Method getValue = vmOption.getClass().getMethod("getValue");
            return (String) getValue.invoke(vmOption);
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return null;
        }
    }

    private static long alignPayload(long bytes) {
        if (bytes == Long.MAX_VALUE) {
            return bytes;
        }
        long remainder = bytes % OBJECT_ALIGNMENT_BYTES;
        return remainder == 0L ? bytes
                : saturatedAdd(bytes, OBJECT_ALIGNMENT_BYTES - remainder);
    }

    private static long alignedArrayBytes(long baseBytes, long length, long elementBytes) {
        if (length < 0L) {
            return Long.MAX_VALUE;
        }
        return alignPayload(saturatedAdd(
                baseBytes, saturatedMultiply(length, elementBytes)));
    }

    private static Object stringStorage(String value) {
        if (STRING_VALUE_GETTER != null && STRING_VALUE_OFFSET >= 0L) {
            try {
                return (Object) STRING_VALUE_GETTER.invokeExact(
                        (Object) value, STRING_VALUE_OFFSET);
            } catch (Throwable ignored) {
                // Return null so callers use the conservative UTF-16 fallback.
            }
        }
        return null;
    }

    private static int parseObjectAlignment(String value) {
        if (value == null) {
            return 16;
        }
        try {
            int alignment = Integer.parseInt(value);
            return alignment > 0 ? alignment : 16;
        } catch (NumberFormatException ignored) {
            return 16;
        }
    }
}

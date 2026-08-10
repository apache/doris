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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.RandomAccess;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bounded reflective object-graph estimator for validating type-specific cache estimators.
 *
 * <p>The expensive class discovery is cached in a {@link ClassValue}: each class hierarchy is reflected once to
 * retain its shallow size and accessible reference fields. Runtime estimation still reads each new root's actual
 * fields because two instances of the same class can retain very different lists, maps, arrays, and strings.
 * Collections and object arrays are sampled to keep this fallback bounded on large metadata values.
 *
 * <p>This is intentionally a construction-time safety net, not a Caffeine hit-path weigher. Cache values should
 * combine it with a type-specific estimate once, store the larger result, and expose that stored number to
 * Caffeine in O(1).
 */
public final class ReflectiveObjectSizeEstimator {
    private static final int DEFAULT_SAMPLE_SIZE = 5;
    private static final int DEFAULT_MAX_DEPTH = 20;

    private static final long HASH_MAP_NODE_BYTES = classSize("java.util.HashMap$Node");
    private static final long LINKED_HASH_MAP_ENTRY_BYTES = classSize("java.util.LinkedHashMap$Entry");
    private static final long TREE_MAP_ENTRY_BYTES = classSize("java.util.TreeMap$Entry");
    private static final long LINKED_LIST_NODE_BYTES = classSize("java.util.LinkedList$Node");
    private static final long CONCURRENT_HASH_MAP_NODE_BYTES = classSize("java.util.concurrent.ConcurrentHashMap$Node");

    private static final Set<Class<?>> SHALLOW_LEAF_TYPES = Set.of(
            Boolean.class,
            Byte.class,
            Character.class,
            Short.class,
            Integer.class,
            Float.class,
            Long.class,
            Double.class);

    private static final ClassValue<ClassPlan> CLASS_PLANS = new ClassValue<>() {
        @Override
        protected ClassPlan computeValue(Class<?> type) {
            List<Field> referenceFields = new ArrayList<>();
            for (Class<?> current = type; current != null; current = current.getSuperclass()) {
                for (Field field : current.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers())
                            || field.getType().isPrimitive()
                            || field.getType().isEnum()) {
                        continue;
                    }
                    try {
                        if (field.trySetAccessible()) {
                            referenceFields.add(field);
                        }
                    } catch (RuntimeException ignored) {
                        // Strongly encapsulated JDK fields are expected; container storage is modeled separately.
                    }
                }
            }
            return new ClassPlan(
                    JvmSizeUtils.instanceSize(type),
                    referenceFields.toArray(new Field[0]));
        }
    };

    private ReflectiveObjectSizeEstimator() {
    }

    public static long estimate(Object root) {
        return estimate(root, DEFAULT_SAMPLE_SIZE, DEFAULT_MAX_DEPTH);
    }

    public static long estimate(Object root, int sampleSize, int maxDepth) {
        if (sampleSize <= 0) {
            throw new IllegalArgumentException("sampleSize must be positive: " + sampleSize);
        }
        if (maxDepth < 0) {
            throw new IllegalArgumentException("maxDepth can not be negative: " + maxDepth);
        }
        return new Walker(sampleSize).estimate(root, maxDepth);
    }

    private static final class Walker {
        private final int sampleSize;
        private final Map<Object, Boolean> visited = new IdentityHashMap<>();

        private Walker(int sampleSize) {
            this.sampleSize = sampleSize;
        }

        private long estimate(Object value, int depth) {
            if (value == null || visited.put(value, Boolean.TRUE) != null) {
                return 0L;
            }

            Class<?> type = value.getClass();
            if (type.isEnum() || value instanceof Class<?>) {
                return 0L;
            }
            if (value instanceof String) {
                return JvmSizeUtils.stringSize((String) value);
            }
            if (SHALLOW_LEAF_TYPES.contains(type) || type.isHidden()) {
                return JvmSizeUtils.instanceSize(type);
            }
            if (type.isArray()) {
                return estimateArray(value, depth);
            }
            if (value instanceof ByteBuffer) {
                return estimateByteBuffer((ByteBuffer) value, depth);
            }
            if (value instanceof Optional<?>) {
                return estimateOptional((Optional<?>) value, depth);
            }
            if (value instanceof Map<?, ?>) {
                return estimateMap((Map<?, ?>) value, depth);
            }
            if (value instanceof Collection<?>) {
                return estimateCollection((Collection<?>) value, depth);
            }

            ClassPlan plan = CLASS_PLANS.get(type);
            long bytes = plan.shallowBytes;
            if (depth == 0) {
                return bytes;
            }
            for (Field field : plan.referenceFields) {
                bytes = add(bytes, estimateField(value, field, depth - 1));
            }
            return bytes;
        }

        private long estimateArray(Object value, int depth) {
            int length = Array.getLength(value);
            Class<?> componentType = value.getClass().getComponentType();
            if (componentType.isPrimitive()) {
                return JvmSizeUtils.primitiveArraySize(componentType, length);
            }

            long bytes = JvmSizeUtils.objectArraySize(length);
            if (depth == 0 || length == 0) {
                return bytes;
            }
            int samples = Math.min(length, sampleSize);
            long sampledBytes = 0L;
            for (int i = 0; i < samples; i++) {
                int index = sampleIndex(i, samples, length);
                sampledBytes = add(sampledBytes, estimate(Array.get(value, index), depth - 1));
            }
            return add(bytes, scale(sampledBytes, samples, length));
        }

        private long estimateByteBuffer(ByteBuffer value, int depth) {
            long bytes = JvmSizeUtils.instanceSize(value.getClass());
            if (depth > 0 && value.hasArray()) {
                bytes = add(bytes, estimate(value.array(), depth - 1));
            }
            return bytes;
        }

        private long estimateOptional(Optional<?> value, int depth) {
            long bytes = JvmSizeUtils.instanceSize(value.getClass());
            return depth == 0 || value.isEmpty()
                    ? bytes
                    : add(bytes, estimate(value.get(), depth - 1));
        }

        private long estimateCollection(Collection<?> values, int depth) {
            int size = values.size();
            long bytes = add(
                    JvmSizeUtils.instanceSize(values.getClass()),
                    collectionStorageBytes(values, size));
            if (depth == 0 || size == 0) {
                return bytes;
            }

            int samples = Math.min(size, sampleSize);
            long sampledBytes = 0L;
            if (values instanceof List<?> && values instanceof RandomAccess) {
                List<?> list = (List<?>) values;
                for (int i = 0; i < samples; i++) {
                    sampledBytes = add(sampledBytes,
                            estimate(list.get(sampleIndex(i, samples, size)), depth - 1));
                }
            } else {
                sampledBytes = estimateIterableSamples(values, samples, depth - 1);
            }
            return add(bytes, scale(sampledBytes, samples, size));
        }

        private long estimateMap(Map<?, ?> values, int depth) {
            int size = values.size();
            long bytes = add(
                    JvmSizeUtils.instanceSize(values.getClass()),
                    mapStorageBytes(values, size));
            if (depth == 0 || size == 0) {
                return bytes;
            }

            int samples = Math.min(size, sampleSize);
            long sampledBytes = 0L;
            Iterator<? extends Map.Entry<?, ?>> iterator = values.entrySet().iterator();
            for (int i = 0; i < samples; i++) {
                Map.Entry<?, ?> entry = iterator.next();
                sampledBytes = add(sampledBytes, estimate(entry.getKey(), depth - 1));
                sampledBytes = add(sampledBytes, estimate(entry.getValue(), depth - 1));
            }
            return add(bytes, scale(sampledBytes, samples, size));
        }

        private long estimateIterableSamples(Collection<?> values, int samples, int depth) {
            Iterator<?> iterator = values.iterator();
            long sampledBytes = 0L;
            for (int i = 0; i < samples; i++) {
                sampledBytes = add(sampledBytes, estimate(iterator.next(), depth));
            }
            return sampledBytes;
        }

        private long estimateField(Object owner, Field field, int depth) {
            try {
                return estimate(field.get(owner), depth);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Cached reference field is no longer accessible: " + field, e);
            }
        }
    }

    private static long collectionStorageBytes(Collection<?> values, int size) {
        if (size == 0) {
            return 0L;
        }
        if (values instanceof LinkedHashSet<?>) {
            return hashStorageBytes(size, LINKED_HASH_MAP_ENTRY_BYTES);
        }
        if (values instanceof HashSet<?>) {
            return hashStorageBytes(size, HASH_MAP_NODE_BYTES);
        }
        if (values instanceof TreeSet<?>) {
            return multiply(size, TREE_MAP_ENTRY_BYTES);
        }
        if (values instanceof LinkedList<?>) {
            return multiply(size, LINKED_LIST_NODE_BYTES);
        }
        return JvmSizeUtils.objectArraySize(size);
    }

    private static long mapStorageBytes(Map<?, ?> values, int size) {
        if (size == 0) {
            return 0L;
        }
        if (values instanceof LinkedHashMap<?, ?>) {
            return hashStorageBytes(size, LINKED_HASH_MAP_ENTRY_BYTES);
        }
        if (values instanceof HashMap<?, ?>) {
            return hashStorageBytes(size, HASH_MAP_NODE_BYTES);
        }
        if (values instanceof ConcurrentHashMap<?, ?>) {
            return hashStorageBytes(size, CONCURRENT_HASH_MAP_NODE_BYTES);
        }
        if (values instanceof TreeMap<?, ?>) {
            return multiply(size, TREE_MAP_ENTRY_BYTES);
        }
        return JvmSizeUtils.objectArraySize(saturatedDouble(size));
    }

    private static long hashStorageBytes(int size, long nodeBytes) {
        return add(
                JvmSizeUtils.objectArraySize(hashCapacity(size)),
                multiply(size, nodeBytes));
    }

    private static int hashCapacity(int size) {
        long needed = (size * 4L + 2L) / 3L;
        int capacity = 16;
        while (capacity < needed && capacity < 1 << 30) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static int sampleIndex(int sample, int sampleCount, int totalCount) {
        return sampleCount == 1
                ? 0
                : (int) ((long) sample * (totalCount - 1) / (sampleCount - 1));
    }

    private static long scale(long sampledBytes, int sampleCount, int totalCount) {
        if (sampleCount == totalCount) {
            return sampledBytes;
        }
        double scaled = (double) sampledBytes * totalCount / sampleCount;
        return scaled >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) scaled;
    }

    private static int saturatedDouble(int value) {
        return value > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE : value * 2;
    }

    private static long classSize(String className) {
        try {
            return JvmSizeUtils.instanceSize(Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Required JVM collection class is missing: " + className, e);
        }
    }

    private static long multiply(long left, long right) {
        return JvmSizeUtils.saturatedMultiply(left, right);
    }

    private static long add(long left, long right) {
        return JvmSizeUtils.saturatedAdd(left, right);
    }

    private static final class ClassPlan {
        private final long shallowBytes;
        private final Field[] referenceFields;

        private ClassPlan(long shallowBytes, Field[] referenceFields) {
            this.shallowBytes = shallowBytes;
            this.referenceFields = referenceFields;
        }
    }
}

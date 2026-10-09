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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.JvmSizeUtils;

import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Offline calibration checks for the cache weight formulas.
 *
 * <p>Estimates are admission weights, not exact retained sizes, so the assertions verify order-of-magnitude
 * sanity rather than byte accuracy: a populated fixture must weigh more than an empty one, the growth must not
 * be under-estimated by more than a small factor, and it must not be absurdly over-estimated. The retained-size
 * oracle walks the object graph with identity dedup and the running JVM's layout. Unlike
 * {@code ReflectiveObjectSizeEstimator} it never gives up: JDK collections and maps, whose fields are strongly
 * encapsulated without {@code --add-opens}, are walked through their public API, other inaccessible JDK state
 * counts as its shallow size, and hidden classes (lambdas) are walked like any other class.
 */
final class EstimatorCalibrationAssertions {
    // estimatedDelta / actualDelta must stay within this band.
    private static final double MIN_ESTIMATE_FACTOR = 0.34D;
    private static final double MAX_ESTIMATE_FACTOR = 12.0D;
    private static final long HASH_MAP_NODE_BYTES = hashMapNodeBytes();
    private static final Map<Class<?>, List<Field>> REFERENCE_FIELD_CACHE = new HashMap<>();

    private EstimatorCalibrationAssertions() {
    }

    static void assertConservativeDelta(String fixture, long emptyEstimate, long populatedEstimate,
            Object emptyGraph, Object populatedGraph) {
        long actualDelta = graphSize(populatedGraph) - graphSize(emptyGraph);
        long estimatedDelta = populatedEstimate - emptyEstimate;
        Assertions.assertTrue(actualDelta > 0L, fixture + " must add retained heap");
        Assertions.assertTrue(estimatedDelta > 0L, fixture + " must add estimated weight");
        Assertions.assertTrue((double) estimatedDelta >= actualDelta * MIN_ESTIMATE_FACTOR,
                fixture + " grossly under-estimates retained heap: estimated=" + estimatedDelta
                        + ", probed=" + actualDelta);
        Assertions.assertTrue((double) estimatedDelta <= actualDelta * MAX_ESTIMATE_FACTOR,
                fixture + " absurdly over-estimates retained heap: estimated=" + estimatedDelta
                        + ", probed=" + actualDelta);
    }

    /**
     * Retained size of the graph reachable from {@code graph}. Shared JVM state (classes, class loaders, threads,
     * enum constants, references) is treated as leaves so deltas between two fixtures cancel it out.
     */
    static synchronized long graphSize(Object graph) {
        if (graph == null) {
            return 0L;
        }
        IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        Deque<Object> pending = new ArrayDeque<>();
        pending.add(graph);
        seen.put(graph, Boolean.TRUE);
        long bytes = 0L;
        while (!pending.isEmpty()) {
            Object current = pending.poll();
            Class<?> type = current.getClass();
            if (type.isArray()) {
                int length = Array.getLength(current);
                Class<?> component = type.getComponentType();
                if (component.isPrimitive()) {
                    bytes += JvmSizeUtils.primitiveArraySize(component, length);
                } else {
                    bytes += JvmSizeUtils.objectArraySize(length);
                    for (int i = 0; i < length; i++) {
                        enqueue(Array.get(current, i), seen, pending);
                    }
                }
                continue;
            }
            boolean jdkType = type.getModule().isNamed();
            if (jdkType && current instanceof String) {
                bytes += JvmSizeUtils.stringSize((String) current);
                continue;
            }
            bytes += JvmSizeUtils.instanceSize(type);
            if (jdkType && current instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) current;
                bytes += JvmSizeUtils.objectArraySize(tableLength(map.size()));
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    bytes += HASH_MAP_NODE_BYTES;
                    enqueue(entry.getKey(), seen, pending);
                    enqueue(entry.getValue(), seen, pending);
                }
            } else if (jdkType && current instanceof Collection) {
                Collection<?> collection = (Collection<?>) current;
                bytes += JvmSizeUtils.objectArraySize(collection.size());
                for (Object element : collection) {
                    enqueue(element, seen, pending);
                }
            } else {
                for (Field field : referenceFields(type)) {
                    try {
                        field.setAccessible(true);
                        enqueue(field.get(current), seen, pending);
                    } catch (IllegalAccessException | RuntimeException ignored) {
                        // Inaccessible JDK state counts as its shallow size; the acceptance band absorbs it.
                    }
                }
            }
        }
        return bytes;
    }

    private static void enqueue(Object value, IdentityHashMap<Object, Boolean> seen, Deque<Object> pending) {
        if (value == null || isSharedLeaf(value)) {
            return;
        }
        if (seen.put(value, Boolean.TRUE) == null) {
            pending.add(value);
        }
    }

    private static boolean isSharedLeaf(Object value) {
        return value instanceof Class || value instanceof ClassLoader
                || value instanceof Thread || value instanceof Enum
                || value instanceof java.lang.ref.Reference;
    }

    private static List<Field> referenceFields(Class<?> type) {
        List<Field> cached = REFERENCE_FIELD_CACHE.get(type);
        if (cached != null) {
            return cached;
        }
        List<Field> fields = new ArrayList<>();
        for (Class<?> owner = type; owner != null && owner != Object.class; owner = owner.getSuperclass()) {
            for (Field field : owner.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
                    fields.add(field);
                }
            }
        }
        REFERENCE_FIELD_CACHE.put(type, fields);
        return fields;
    }

    private static int tableLength(int size) {
        int length = 1;
        while (length * 3L < size * 4L) {
            length <<= 1;
        }
        return length;
    }

    private static long hashMapNodeBytes() {
        try {
            return JvmSizeUtils.instanceSize(Class.forName("java.util.HashMap$Node"));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}

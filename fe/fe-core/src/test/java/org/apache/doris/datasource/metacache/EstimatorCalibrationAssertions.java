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

import org.junit.Assert;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Offline calibration checks for the coarse cache weight formulas.
 *
 * <p>Estimates are approximate admission weights, not exact retained sizes, so the assertions
 * verify order-of-magnitude sanity rather than byte accuracy: a populated fixture must weigh
 * more than an empty one, the growth must not be under-estimated by more than a small factor,
 * and it must not be absurdly over-estimated. The retained-size oracle is a dependency-free
 * reflective graph walk using generic layout constants; it is intentionally rough, which the
 * wide acceptance band absorbs.
 */
public final class EstimatorCalibrationAssertions {
    private static final boolean PRINT_RESULT = Boolean.getBoolean(
            "metacache.estimator.calibration.print");
    // estimatedDelta / actualDelta must stay within this band.
    private static final double MIN_ESTIMATE_FACTOR = 0.34D;
    private static final double MAX_ESTIMATE_FACTOR = 12.0D;

    private static final long OBJECT_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long ARRAY_HEADER_BYTES = 24L;
    private static final Map<Class<?>, List<Field>> FIELD_CACHE = new HashMap<>();

    private EstimatorCalibrationAssertions() {
    }

    public static void assertConservativeDelta(
            String fixture, long emptyEstimate, long populatedEstimate,
            Object emptyGraph, Object populatedGraph) {
        long actualDelta = graphSize(populatedGraph) - graphSize(emptyGraph);
        long estimatedDelta = populatedEstimate - emptyEstimate;
        if (PRINT_RESULT) {
            System.out.printf("%s: estimated=%d, probe=%d, ratio=%.3f%n",
                    fixture, estimatedDelta, actualDelta,
                    actualDelta == 0L ? Double.NaN : (double) estimatedDelta / actualDelta);
        }
        Assert.assertTrue(fixture + " must add retained heap", actualDelta > 0L);
        Assert.assertTrue(fixture + " must add estimated weight", estimatedDelta > 0L);
        Assert.assertTrue(fixture + " grossly under-estimates retained heap: estimated="
                        + estimatedDelta + ", probed=" + actualDelta,
                (double) estimatedDelta >= actualDelta * MIN_ESTIMATE_FACTOR);
        Assert.assertTrue(fixture + " absurdly over-estimates retained heap: estimated="
                        + estimatedDelta + ", probed=" + actualDelta,
                (double) estimatedDelta <= actualDelta * MAX_ESTIMATE_FACTOR);
    }

    /**
     * Rough retained size of the graph reachable from {@code graph}: reflective walk with
     * identity dedup and generic per-object layout constants. Shared JVM state (classes,
     * class loaders, enum constants, references) is treated as leaves so deltas between two
     * fixtures cancel it out.
     */
    public static synchronized long graphSize(Object graph) {
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
                long width = component.isPrimitive() ? primitiveBytes(component) : REFERENCE_BYTES;
                bytes += align(ARRAY_HEADER_BYTES + width * length);
                if (!component.isPrimitive()) {
                    for (int i = 0; i < length; i++) {
                        enqueue(Array.get(current, i), seen, pending);
                    }
                }
                continue;
            }
            long primitiveBytes = 0L;
            long referenceFields = 0L;
            for (Field field : instanceFields(type)) {
                Class<?> fieldType = field.getType();
                if (fieldType.isPrimitive()) {
                    primitiveBytes += primitiveBytes(fieldType);
                    continue;
                }
                referenceFields++;
                try {
                    field.setAccessible(true);
                    enqueue(field.get(current), seen, pending);
                } catch (IllegalAccessException | RuntimeException ignored) {
                    // Inaccessible content is left out; the acceptance band absorbs it.
                }
            }
            bytes += align(OBJECT_HEADER_BYTES + referenceFields * REFERENCE_BYTES + primitiveBytes);
        }
        return bytes;
    }

    private static void enqueue(
            Object value, IdentityHashMap<Object, Boolean> seen, Deque<Object> pending) {
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

    private static List<Field> instanceFields(Class<?> type) {
        List<Field> cached = FIELD_CACHE.get(type);
        if (cached != null) {
            return cached;
        }
        List<Field> fields = new ArrayList<>();
        for (Class<?> owner = type; owner != null && owner != Object.class;
                owner = owner.getSuperclass()) {
            for (Field field : owner.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    fields.add(field);
                }
            }
        }
        FIELD_CACHE.put(type, fields);
        return fields;
    }

    private static long primitiveBytes(Class<?> type) {
        if (type == long.class || type == double.class) {
            return 8L;
        }
        if (type == int.class || type == float.class) {
            return 4L;
        }
        if (type == short.class || type == char.class) {
            return 2L;
        }
        return 1L;
    }

    private static long align(long bytes) {
        return (bytes + 7L) & ~7L;
    }
}

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
import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.info.GraphPathRecord;

import java.lang.reflect.Field;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/** JOL oracle used only by estimator calibration tests. */
public final class EstimatorCalibrationAssertions {
    private static final double MAX_CONSERVATIVE_FACTOR = 1.10D;
    private static final boolean PRINT_RESULT = Boolean.getBoolean(
            "metacache.estimator.calibration.print");
    // Integer.valueOf/Long.valueOf serve -128..127 from JVM-wide static caches. A populated
    // fixture that reaches those shared instances (field ids, list indexes, small partition
    // values) must not be charged for them as retained growth, so every graph is measured
    // together with the same cache roots and the shared instances cancel out of the delta.
    private static final Integer[] SHARED_INTEGER_CACHE =
            IntStream.rangeClosed(-128, 127).boxed().toArray(Integer[]::new);
    private static final Long[] SHARED_LONG_CACHE =
            LongStream.rangeClosed(-128L, 127L).boxed().toArray(Long[]::new);
    // Accessor objects reference java.lang.Class instances (String.class, StructLike.class, ...).
    // JOL follows them into the JVM's per-class reflection and ClassValue caches, whose size
    // depends on unrelated reflective use earlier in the same JVM (Mockito, layout fingerprints,
    // JOL itself). Everything reached through a Class object is shared JVM state, not retained
    // cache payload, and is excluded from every measurement.
    private static final Field GRAPH_PATH_PARENT = graphPathParentField();

    private static Field graphPathParentField() {
        try {
            Field field = GraphPathRecord.class.getDeclaredField("parent");
            field.setAccessible(true);
            return field;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("JOL GraphPathRecord.parent is unavailable", e);
        }
    }

    static {
        // Doris expression graphs contain JVM hidden lambda classes. JOL cannot obtain their
        // offsets through the regular instrumentation path on JDK 17, so enable its Unsafe
        // fallback for these test-only retained-graph measurements. Skip all attach attempts:
        // Iceberg/Paimon calibration tests share their fork with Mockito's inline mock maker.
        System.setProperty("jol.magicFieldOffset", "true");
        System.setProperty("jol.skipInstallAttach", "true");
        System.setProperty("jol.skipDynamicAttach", "true");
        System.setProperty("jol.skipHotspotSAAttach", "true");
    }

    private EstimatorCalibrationAssertions() {
    }

    public static void assertConservativeDelta(
            String fixture, long emptyEstimate, long populatedEstimate,
            Object emptyGraph, Object populatedGraph) {
        long actualDelta = graphSize(populatedGraph) - graphSize(emptyGraph);
        long estimatedDelta = populatedEstimate - emptyEstimate;
        if (PRINT_RESULT) {
            System.out.printf("%s: estimated=%d, jol=%d, ratio=%.3f%n",
                    fixture, estimatedDelta, actualDelta,
                    actualDelta == 0L ? Double.NaN : (double) estimatedDelta / actualDelta);
        }
        Assert.assertTrue(fixture + " must add retained heap", actualDelta > 0L);
        Assert.assertTrue(fixture + " underestimates retained heap: estimated=" + estimatedDelta
                        + ", actual=" + actualDelta,
                estimatedDelta >= actualDelta);
        Assert.assertTrue(fixture + " estimate is excessively conservative: estimated=" + estimatedDelta
                        + ", actual=" + actualDelta,
                estimatedDelta <= Math.ceil(actualDelta * MAX_CONSERVATIVE_FACTOR));
    }

    /** Retained size of the graph excluding JVM-shared boxed-value caches and Class metadata. */
    public static long graphSize(Object graph) {
        long sharedCacheBytes = GraphLayout.parseInstance(
                SHARED_INTEGER_CACHE, SHARED_LONG_CACHE).totalSize();
        GraphLayout layout = GraphLayout.parseInstance(
                graph, SHARED_INTEGER_CACHE, SHARED_LONG_CACHE);
        long bytes = 0L;
        for (long address : layout.addresses()) {
            GraphPathRecord record = layout.record(address);
            if (!reachedThroughClassObject(record)) {
                bytes += record.size();
            }
        }
        return bytes - sharedCacheBytes;
    }

    private static boolean reachedThroughClassObject(GraphPathRecord record) {
        try {
            for (GraphPathRecord current = record; current != null;
                    current = (GraphPathRecord) GRAPH_PATH_PARENT.get(current)) {
                if (current.klass() == Class.class) {
                    return true;
                }
            }
            return false;
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}

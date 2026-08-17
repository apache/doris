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

/** JOL oracle used only by estimator calibration tests. */
public final class EstimatorCalibrationAssertions {
    private static final long MAX_CONSERVATIVE_FACTOR = 8L;
    private static final boolean PRINT_RESULT = Boolean.getBoolean(
            "metacache.estimator.calibration.print");

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
        long actualDelta = GraphLayout.parseInstance(populatedGraph).totalSize()
                - GraphLayout.parseInstance(emptyGraph).totalSize();
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
                estimatedDelta <= MetaCacheWeightUtils.saturatedMultiply(
                        actualDelta, MAX_CONSERVATIVE_FACTOR));
    }

    public static long graphSize(Object graph) {
        return GraphLayout.parseInstance(graph).totalSize();
    }
}

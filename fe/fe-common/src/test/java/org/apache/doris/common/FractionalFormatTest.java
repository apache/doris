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

package org.apache.doris.common;

import com.fasterxml.jackson.core.io.schubfach.DoubleToDecimal;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class FractionalFormatTest {
    private static final int RANDOM_VALUES = 100_000;
    private static final long RANDOM_SEED = 0xD0A15L;
    private static volatile long blackHole;

    @Test
    public void testBoundaryValues() {
        Assert.assertEquals("0", FractionalFormat.getFormatStringValue(0.0));
        Assert.assertEquals("-0", FractionalFormat.getFormatStringValue(-0.0));
        Assert.assertEquals("NaN", FractionalFormat.getFormatStringValue(Double.NaN));
        Assert.assertEquals("Infinity",
                FractionalFormat.getFormatStringValue(Double.POSITIVE_INFINITY));
        Assert.assertEquals("-Infinity",
                FractionalFormat.getFormatStringValue(Double.NEGATIVE_INFINITY));
        Assert.assertEquals("0.0001", FractionalFormat.getFormatStringValue(1e-4));
        Assert.assertEquals("1e-05", FractionalFormat.getFormatStringValue(1e-5));
        Assert.assertEquals("1000000000000000",
                FractionalFormat.getFormatStringValue(1e15));
        Assert.assertEquals("1e+16", FractionalFormat.getFormatStringValue(1e16));
        Assert.assertEquals("1e+23", FractionalFormat.getFormatStringValue(1e23));
        Assert.assertEquals("5.960464477539063e-08",
                FractionalFormat.getFormatStringValue(Math.scalb(1.0, -24)));
        Assert.assertEquals("5e-324", FractionalFormat.getFormatStringValue(Double.MIN_VALUE));
        Assert.assertEquals("1.7976931348623157e+308",
                FractionalFormat.getFormatStringValue(Double.MAX_VALUE));

        Assert.assertEquals("10000000", FractionalFormat.getFormatStringValue(1e7f));
        Assert.assertEquals("1.2621775e-29",
                FractionalFormat.getFormatStringValue(Math.scalb(1.0f, -96)));
        Assert.assertEquals("1e-45", FractionalFormat.getFormatStringValue(Float.MIN_VALUE));
        Assert.assertEquals("3.4028235e+38",
                FractionalFormat.getFormatStringValue(Float.MAX_VALUE));
    }

    @Test
    public void testRandomValuesRoundTrip() {
        Random random = new Random(RANDOM_SEED);
        for (int i = 0; i < 10_000; i++) {
            double value = nextFiniteDouble(random);
            String formatted = FractionalFormat.getFormatStringValue(value);
            Assert.assertEquals(Double.doubleToRawLongBits(value),
                    Double.doubleToRawLongBits(Double.parseDouble(formatted)));
        }
        for (int i = 0; i < 10_000; i++) {
            float value = nextFiniteFloat(random);
            String formatted = FractionalFormat.getFormatStringValue(value);
            Assert.assertEquals(Float.floatToRawIntBits(value),
                    Float.floatToRawIntBits(Float.parseFloat(formatted)));
        }
    }

    @Test
    public void testHighCardinalityPerformance() {
        double[] values = new double[RANDOM_VALUES];
        Random random = new Random(RANDOM_SEED);
        for (int i = 0; i < values.length; i++) {
            values[i] = nextFiniteDouble(random);
        }

        formatWithSchubfach(values);
        formatWithDoris(values);

        long schubfachNanos = bestElapsedNanos(values, false);
        long dorisNanos = bestElapsedNanos(values, true);

        // Formatting may add notation normalization on top of Schubfach, but must remain
        // within a small constant factor of the bounded-allocation converter.
        Assert.assertTrue("Doris formatting took " + dorisNanos
                        + "ns, Schubfach conversion took " + schubfachNanos + "ns",
                dorisNanos < schubfachNanos * 3);
    }

    private static long bestElapsedNanos(double[] values, boolean doris) {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 3; i++) {
            long start = System.nanoTime();
            long checksum = doris ? formatWithDoris(values) : formatWithSchubfach(values);
            best = Math.min(best, System.nanoTime() - start);
            blackHole = checksum;
        }
        return best;
    }

    private static long formatWithDoris(double[] values) {
        long checksum = 0;
        for (double value : values) {
            checksum += FractionalFormat.getFormatStringValue(value).length();
        }
        return checksum;
    }

    private static long formatWithSchubfach(double[] values) {
        long checksum = 0;
        for (double value : values) {
            checksum += DoubleToDecimal.toString(value).length();
        }
        return checksum;
    }

    private static double nextFiniteDouble(Random random) {
        double value;
        do {
            value = Double.longBitsToDouble(random.nextLong());
        } while (!Double.isFinite(value));
        return value;
    }

    private static float nextFiniteFloat(Random random) {
        float value;
        do {
            value = Float.intBitsToFloat(random.nextInt());
        } while (!Float.isFinite(value));
        return value;
    }
}

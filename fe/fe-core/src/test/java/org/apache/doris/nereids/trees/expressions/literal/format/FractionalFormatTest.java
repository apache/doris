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

package org.apache.doris.nereids.trees.expressions.literal.format;

import com.fasterxml.jackson.core.io.schubfach.DoubleToDecimal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.Random;

public class FractionalFormatTest {
    private static final int RANDOM_VALUES = 10_000_000;
    private static final long RANDOM_SEED = 0xD0A15L;
    private static volatile long blackHole;

    @Test
    public void testBoundaryValues() {
        Assertions.assertEquals("0", FractionalFormat.getFormatStringValue(0.0));
        Assertions.assertEquals("-0", FractionalFormat.getFormatStringValue(-0.0));
        Assertions.assertEquals("NaN", FractionalFormat.getFormatStringValue(Double.NaN));
        Assertions.assertEquals("Infinity",
                FractionalFormat.getFormatStringValue(Double.POSITIVE_INFINITY));
        Assertions.assertEquals("-Infinity",
                FractionalFormat.getFormatStringValue(Double.NEGATIVE_INFINITY));
        Assertions.assertEquals("0.0001", FractionalFormat.getFormatStringValue(1e-4));
        Assertions.assertEquals("1e-05", FractionalFormat.getFormatStringValue(1e-5));
        Assertions.assertEquals("1000000000000000",
                FractionalFormat.getFormatStringValue(1e15));
        Assertions.assertEquals("1e+16", FractionalFormat.getFormatStringValue(1e16));
        Assertions.assertEquals("1e+23", FractionalFormat.getFormatStringValue(1e23));
        Assertions.assertEquals("5.960464477539063e-08",
                FractionalFormat.getFormatStringValue(Math.scalb(1.0, -24)));
        Assertions.assertEquals("5e-324", FractionalFormat.getFormatStringValue(Double.MIN_VALUE));
        Assertions.assertEquals("1.7976931348623157e+308",
                FractionalFormat.getFormatStringValue(Double.MAX_VALUE));

        Assertions.assertEquals("10000000", FractionalFormat.getFormatStringValue(1e7f));
        Assertions.assertEquals("1.2621775e-29",
                FractionalFormat.getFormatStringValue(Math.scalb(1.0f, -96)));
        Assertions.assertEquals("1e-45", FractionalFormat.getFormatStringValue(Float.MIN_VALUE));
        Assertions.assertEquals("3.4028235e+38",
                FractionalFormat.getFormatStringValue(Float.MAX_VALUE));
    }

    @Test
    public void testRandomValuesRoundTrip() {
        Random random = new Random(RANDOM_SEED);
        for (int i = 0; i < 10_000; i++) {
            double value = nextFiniteDouble(random);
            String formatted = FractionalFormat.getFormatStringValue(value);
            Assertions.assertEquals(Double.doubleToRawLongBits(value),
                    Double.doubleToRawLongBits(Double.parseDouble(formatted)));
        }
        for (int i = 0; i < 10_000; i++) {
            float value = nextFiniteFloat(random);
            String formatted = FractionalFormat.getFormatStringValue(value);
            Assertions.assertEquals(Float.floatToRawIntBits(value),
                    Float.floatToRawIntBits(Float.parseFloat(formatted)));
        }
    }

    private void testHighCardinalityPerformance() {
        NumberFormat defaultFormat = NumberFormat.getInstance();

        System.out.println("Runnng double to string test with " + defaultFormat.format(RANDOM_VALUES) + " double values\n");
        double[] values = new double[RANDOM_VALUES];
        Random random = new Random(RANDOM_SEED);
        System.out.println("Generating test data");
        for (int i = 0; i < values.length; i++) {
            values[i] = nextFiniteDouble(random);
        }
        System.out.println("Generating test data done");

        System.out.println("test formatWithJDK");
        long jdkNanos = formatWithJDK(values);
        System.out.println("jdkNanos: " + defaultFormat.format(jdkNanos) + "\n");

        System.out.println("test formatWithSchubfach");
        long schubfachNanos = formatWithSchubfach(values);
        System.out.println("schubfachNanos: " + defaultFormat.format(schubfachNanos) + "\n");

        System.out.println("test formatWithDoris");
        long dorisNanos = formatWithDoris(values);
        System.out.println("formatWithDoris: " + defaultFormat.format(dorisNanos) + "\n");

        System.out.println("test oldGetFormatStringValue");
        long oldDorisNanos = oldGetFormatStringValueNanos(values);
        System.out.println("oldDorisNanos: " + defaultFormat.format(oldDorisNanos) + "\n");

        System.out.println("test getFormatStringValueLoop");
        long dorisLoopNanos = dorisLoopElapsedNanos(values);
        System.out.println("dorisLoopNanos: " + defaultFormat.format(dorisLoopNanos) + "\n");

        // Formatting may add notation normalization on top of Schubfach, but must remain
        // within a small constant factor of the bounded-allocation converter.
        // Assertions.assertTrue("Doris formatting took " + dorisNanos
        //                 + "ns, Schubfach conversion took " + schubfachNanos + "ns",
        //         dorisNanos < schubfachNanos * 3);
    }

    private static long oldGetFormatStringValueNanos(double[] values) {
        long start = System.nanoTime();
        for (double value : values) {
            oldGetFormatStringValue(value, 16, "%.15E");
        }
        return System.nanoTime() - start;
    }

    private static long dorisLoopElapsedNanos(double[] values) {
        long start = System.nanoTime();
        for (double value : values) {
            getFormatStringValueLoop(value);
        }
        return System.nanoTime() - start;
    }

    private static long formatWithJDK(double[] values) {
        long start = System.nanoTime();
        for (double value : values) {
            Double.toString(value);
        }
        return System.nanoTime() - start;
    }

    private static long formatWithDoris(double[] values) {
        long start = System.nanoTime();
        for (double value : values) {
            FractionalFormat.getFormatStringValue(value);
        }
        return System.nanoTime() - start;
    }

    private static long formatWithSchubfach(double[] values) {
        long start = System.nanoTime();
        for (double value : values) {
            DoubleToDecimal.toString(value);
        }
        return System.nanoTime() - start;
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

    // float: 7, 6
    // double: 16, 15
    private static String oldGetFormatStringValue(double value, int precision, String sciFormat) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (Double.isInfinite(value)) {
            return value > 0 ? "Infinity" : "-Infinity";
        }
        if (Double.compare(value, 0.0) == 0) {
            return "0";
        }
        if (Double.compare(value, -0.0) == 0) {
            return "-0";
        }
        int expLower = -4;
        int exponent = (int) Math.floor(Math.log10(Math.abs(value)));
        if (exponent < precision && exponent >= expLower) {
            BigDecimal bd = new BigDecimal(value);
            bd = bd.setScale(precision - bd.precision() + bd.scale(), RoundingMode.HALF_UP);
            String result = bd.toPlainString();
            if (result.contains(".")) {
                result = result.replaceAll("0+$", "");
                if (result.endsWith(".")) {
                    result = result.substring(0, result.length() - 1);
                }
            }
            return result;
        } else {
            return String.format(sciFormat, value).replaceAll("(\\.\\d*?[1-9])0*E", "$1E")
                    .replaceAll("\\.0*E", "E").replaceAll("E", "e");
        }
    }

    private static String getFormatStringValueLoop(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (Double.isInfinite(value)) {
            return value > 0 ? "Infinity" : "-Infinity";
        }
        if (value == 0) {
            return Double.doubleToRawLongBits(value) < 0 ? "-0" : "0";
        }
        BigDecimal exactValue = new BigDecimal(value);
        long bits = Double.doubleToRawLongBits(value);
        for (int precision = 1; precision < 17; precision++) {
            BigDecimal candidate = exactValue.round(new MathContext(precision, RoundingMode.HALF_EVEN));
            if (Double.doubleToRawLongBits(Double.parseDouble(candidate.toString())) == bits) {
                return format(candidate);
            }
        }
        return format(exactValue.round(new MathContext(17, RoundingMode.HALF_EVEN)));
    }

    private static String format(BigDecimal value) {
        BigDecimal normalized = value.stripTrailingZeros();
        int exponent = normalized.precision() - normalized.scale() - 1;
        if (exponent >= -4 && exponent < 16) {
            return normalized.toPlainString();
        }

        String digits = normalized.unscaledValue().abs().toString();
        StringBuilder result = new StringBuilder(digits.length() + 7);
        if (normalized.signum() < 0) {
            result.append('-');
        }
        result.append(digits.charAt(0));
        if (digits.length() > 1) {
            result.append('.').append(digits, 1, digits.length());
        }
        result.append(exponent < 0 ? "e-" : "e+");
        int absoluteExponent = Math.abs(exponent);
        if (absoluteExponent < 10) {
            result.append('0');
        }
        return result.append(absoluteExponent).toString();
    }
}

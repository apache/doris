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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

// Covers compareLiteral() for every legacy LiteralExpr subclass that the
// optimizer relies on for predicate dedup, partition pruning, and range
// intersection. MapLiteral/StructLiteral/TimeV2Literal are intentionally
// skipped: they all return 0 today, but SQL has no way to feed two of them
// into the dedup paths so the bug is unreachable from queries.
class LiteralExprCompareLiteralTest {

    @Nested
    class BoolLiteralCompare {
        @Test
        void sameValueIsZero() {
            Assertions.assertEquals(0, new BoolLiteral(true).compareLiteral(new BoolLiteral(true)));
            Assertions.assertEquals(0, new BoolLiteral(false).compareLiteral(new BoolLiteral(false)));
        }

        @Test
        void falseLessThanTrue() {
            Assertions.assertTrue(new BoolLiteral(false).compareLiteral(new BoolLiteral(true)) < 0);
            Assertions.assertTrue(new BoolLiteral(true).compareLiteral(new BoolLiteral(false)) > 0);
        }

        @Test
        void vsNullLiteralReturnsOne() {
            Assertions.assertEquals(1, new BoolLiteral(true).compareLiteral(new NullLiteral()));
        }

        @Test
        void vsMaxLiteralReturnsMinusOne() {
            Assertions.assertEquals(-1, new BoolLiteral(true).compareLiteral(MaxLiteral.MAX_VALUE));
        }

        @Test
        void vsPlaceHolderDelegatesToInner() {
            BoolLiteral self = new BoolLiteral(true);
            Assertions.assertEquals(0, self.compareLiteral(new PlaceHolderExpr(new BoolLiteral(true))));
            Assertions.assertTrue(self.compareLiteral(new PlaceHolderExpr(new BoolLiteral(false))) > 0);
        }
    }

    @Nested
    class IntLiteralCompare {
        @Test
        void sameValueIsZero() {
            Assertions.assertEquals(0, new IntLiteral(42).compareLiteral(new IntLiteral(42)));
        }

        @Test
        void orderingMatchesValue() {
            Assertions.assertTrue(new IntLiteral(1).compareLiteral(new IntLiteral(2)) < 0);
            Assertions.assertTrue(new IntLiteral(2).compareLiteral(new IntLiteral(1)) > 0);
        }

        @Test
        void vsNullLiteralReturnsOne() {
            Assertions.assertEquals(1, new IntLiteral(0).compareLiteral(new NullLiteral()));
        }

        @Test
        void vsMaxLiteralReturnsMinusOne() {
            Assertions.assertEquals(-1, new IntLiteral(Long.MAX_VALUE).compareLiteral(MaxLiteral.MAX_VALUE));
        }

        @Test
        void vsPlaceHolderDelegatesToInner() {
            Assertions.assertEquals(0, new IntLiteral(7).compareLiteral(new PlaceHolderExpr(new IntLiteral(7))));
        }
    }

    @Nested
    class FloatLiteralCompare {
        @Test
        void sameValueIsZero() {
            Assertions.assertEquals(0, new FloatLiteral(1.5).compareLiteral(new FloatLiteral(1.5)));
        }

        @Test
        void orderingMatchesValue() {
            Assertions.assertTrue(new FloatLiteral(1.0).compareLiteral(new FloatLiteral(2.0)) < 0);
            Assertions.assertTrue(new FloatLiteral(2.0).compareLiteral(new FloatLiteral(1.0)) > 0);
        }

        @Test
        void vsNullLiteralReturnsOne() {
            Assertions.assertEquals(1, new FloatLiteral(0.0).compareLiteral(new NullLiteral()));
        }

        @Test
        void vsMaxLiteralReturnsMinusOne() {
            Assertions.assertEquals(-1, new FloatLiteral(1.0).compareLiteral(MaxLiteral.MAX_VALUE));
        }
    }

    @Nested
    class DecimalLiteralCompare {
        @Test
        void sameValueIsZero() {
            BigDecimal v = new BigDecimal("12.34");
            DecimalLiteral a = new DecimalLiteral(v, ScalarType.createDecimalV3Type(10, 2));
            DecimalLiteral b = new DecimalLiteral(v, ScalarType.createDecimalV3Type(10, 2));
            Assertions.assertEquals(0, a.compareLiteral(b));
        }

        @Test
        void orderingMatchesValue() {
            DecimalLiteral small = new DecimalLiteral(new BigDecimal("1.00"), ScalarType.createDecimalV3Type(10, 2));
            DecimalLiteral big = new DecimalLiteral(new BigDecimal("9.99"), ScalarType.createDecimalV3Type(10, 2));
            Assertions.assertTrue(small.compareLiteral(big) < 0);
            Assertions.assertTrue(big.compareLiteral(small) > 0);
        }

        @Test
        void vsNullLiteralReturnsOne() {
            DecimalLiteral d = new DecimalLiteral(BigDecimal.ZERO, ScalarType.createDecimalV3Type(10, 2));
            Assertions.assertEquals(1, d.compareLiteral(new NullLiteral()));
        }

        @Test
        void vsMaxLiteralReturnsMinusOne() {
            DecimalLiteral d = new DecimalLiteral(BigDecimal.ZERO, ScalarType.createDecimalV3Type(10, 2));
            Assertions.assertEquals(-1, d.compareLiteral(MaxLiteral.MAX_VALUE));
        }
    }

    @Nested
    class LargeIntLiteralCompare {
        @Test
        void sameValueIsZero() {
            BigInteger v = new BigInteger("12345678901234567890");
            Assertions.assertEquals(0, new LargeIntLiteral(v).compareLiteral(new LargeIntLiteral(v)));
        }

        @Test
        void orderingMatchesValue() {
            LargeIntLiteral small = new LargeIntLiteral(BigInteger.ONE);
            LargeIntLiteral big = new LargeIntLiteral(new BigInteger("12345678901234567890"));
            Assertions.assertTrue(small.compareLiteral(big) < 0);
            Assertions.assertTrue(big.compareLiteral(small) > 0);
        }

        @Test
        void vsNullLiteralReturnsOne() {
            Assertions.assertEquals(1, new LargeIntLiteral(BigInteger.ZERO).compareLiteral(new NullLiteral()));
        }

        @Test
        void vsMaxLiteralReturnsMinusOne() {
            Assertions.assertEquals(-1, new LargeIntLiteral(BigInteger.ZERO).compareLiteral(MaxLiteral.MAX_VALUE));
        }
    }

    @Nested
    class DateLiteralCompare {
        @Test
        void sameValueIsZero() {
            Assertions.assertEquals(0,
                    new DateLiteral(2026, 5, 21).compareLiteral(new DateLiteral(2026, 5, 21)));
        }

        @Test
        void orderingMatchesValue() {
            DateLiteral earlier = new DateLiteral(2026, 5, 21);
            DateLiteral later = new DateLiteral(2026, 5, 22);
            Assertions.assertTrue(earlier.compareLiteral(later) < 0);
            Assertions.assertTrue(later.compareLiteral(earlier) > 0);
        }

        @Test
        void vsNullLiteralReturnsOne() {
            Assertions.assertEquals(1, new DateLiteral(2026, 5, 21).compareLiteral(new NullLiteral()));
        }

        @Test
        void vsMaxLiteralReturnsMinusOne() {
            Assertions.assertEquals(-1, new DateLiteral(2026, 5, 21).compareLiteral(MaxLiteral.MAX_VALUE));
        }
    }

    @Nested
    class StringLiteralCompare {
        @Test
        void sameValueIsZero() {
            Assertions.assertEquals(0, new StringLiteral("abc").compareLiteral(new StringLiteral("abc")));
        }

        @Test
        void orderingMatchesValue() {
            Assertions.assertTrue(new StringLiteral("abc").compareLiteral(new StringLiteral("abd")) < 0);
            Assertions.assertTrue(new StringLiteral("abd").compareLiteral(new StringLiteral("abc")) > 0);
        }

        @Test
        void vsNullLiteralReturnsOne() {
            Assertions.assertEquals(1, new StringLiteral("x").compareLiteral(new NullLiteral()));
        }

        @Test
        void vsMaxLiteralReturnsMinusOne() {
            Assertions.assertEquals(-1, new StringLiteral("zzz").compareLiteral(MaxLiteral.MAX_VALUE));
        }
    }

    @Nested
    class IPv4LiteralCompare {
        @Test
        void sameValueIsZero() throws AnalysisException {
            Assertions.assertEquals(0, new IPv4Literal("1.1.1.1").compareLiteral(new IPv4Literal("1.1.1.1")));
        }

        @Test
        void orderingMatchesValue() throws AnalysisException {
            Assertions.assertTrue(new IPv4Literal("1.1.1.1").compareLiteral(new IPv4Literal("1.1.1.2")) < 0);
            Assertions.assertTrue(new IPv4Literal("1.1.1.2").compareLiteral(new IPv4Literal("1.1.1.1")) > 0);
        }

        @Test
        void vsNullLiteralReturnsOne() throws AnalysisException {
            Assertions.assertEquals(1, new IPv4Literal("1.1.1.1").compareLiteral(new NullLiteral()));
        }

        @Test
        void vsMaxLiteralReturnsMinusOne() throws AnalysisException {
            Assertions.assertEquals(-1, new IPv4Literal("255.255.255.255").compareLiteral(MaxLiteral.MAX_VALUE));
        }

        @Test
        void vsPlaceHolderDelegatesToInner() throws AnalysisException {
            IPv4Literal self = new IPv4Literal("1.1.1.1");
            Assertions.assertEquals(0, self.compareLiteral(new PlaceHolderExpr(new IPv4Literal("1.1.1.1"))));
            Assertions.assertTrue(self.compareLiteral(new PlaceHolderExpr(new IPv4Literal("1.1.1.2"))) < 0);
        }

        @Test
        void crossTypeThrows() throws AnalysisException {
            Assertions.assertThrows(RuntimeException.class,
                    () -> new IPv4Literal("1.1.1.1").compareLiteral(new IntLiteral(1)));
        }
    }

    @Nested
    class IPv6LiteralCompare {
        @Test
        void sameValueIsZero() throws AnalysisException {
            Assertions.assertEquals(0, new IPv6Literal("::1").compareLiteral(new IPv6Literal("::1")));
        }

        @Test
        void canonicalizesEqualAddresses() throws AnalysisException {
            // "::1" and "0:0:0:0:0:0:0:1" are the same address; with the fix
            // they must compare equal even though the strings differ.
            Assertions.assertEquals(0,
                    new IPv6Literal("::1").compareLiteral(new IPv6Literal("0:0:0:0:0:0:0:1")));
        }

        @Test
        void orderingMatchesValue() throws AnalysisException {
            Assertions.assertTrue(new IPv6Literal("::1").compareLiteral(new IPv6Literal("::2")) < 0);
            Assertions.assertTrue(new IPv6Literal("::2").compareLiteral(new IPv6Literal("::1")) > 0);
        }

        @Test
        void vsNullLiteralReturnsOne() throws AnalysisException {
            Assertions.assertEquals(1, new IPv6Literal("::1").compareLiteral(new NullLiteral()));
        }

        @Test
        void vsMaxLiteralReturnsMinusOne() throws AnalysisException {
            Assertions.assertEquals(-1, new IPv6Literal("ffff::ffff").compareLiteral(MaxLiteral.MAX_VALUE));
        }

        @Test
        void crossTypeThrows() throws AnalysisException {
            Assertions.assertThrows(RuntimeException.class,
                    () -> new IPv6Literal("::1").compareLiteral(new StringLiteral("::1")));
        }

        @Test
        void ipv4MappedNotEqualToLoopback() throws AnalysisException {
            // ::ffff:0.0.0.1 must keep its full 128-bit value (the ::ffff:
            // prefix is part of the address) and order strictly above ::1.
            // Earlier the helper let Inet4Address collapse it to 4 bytes, so
            // both literals compared as BigInteger(1) and dedup folded them
            // into one range.
            Assertions.assertTrue(
                    new IPv6Literal("::ffff:0.0.0.1").compareLiteral(new IPv6Literal("::1")) > 0);
            Assertions.assertNotEquals(0,
                    new IPv6Literal("::ffff:0.0.0.1").compareLiteral(new IPv6Literal("::1")));
        }
    }

    @Nested
    class ArrayLiteralCompare {
        private final Type intArray = new ArrayType(Type.INT);

        @Test
        void sameElementsIsZero() {
            ArrayLiteral a = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(2));
            ArrayLiteral b = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(2));
            Assertions.assertEquals(0, a.compareLiteral(b));
        }

        @Test
        void elementWiseDifference() {
            ArrayLiteral a = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(2));
            ArrayLiteral b = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(3));
            Assertions.assertTrue(a.compareLiteral(b) < 0);
            Assertions.assertTrue(b.compareLiteral(a) > 0);
        }

        @Test
        void shorterIsLessWhenPrefixEqual() {
            ArrayLiteral shorter = new ArrayLiteral(intArray, new IntLiteral(1));
            ArrayLiteral longer = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(2));
            Assertions.assertTrue(shorter.compareLiteral(longer) < 0);
            Assertions.assertTrue(longer.compareLiteral(shorter) > 0);
        }
    }

    @Nested
    class VarBinaryLiteralCompare {
        @Test
        void sameBytesIsZero() throws AnalysisException {
            VarBinaryLiteral a = new VarBinaryLiteral(new byte[]{1, 2, 3});
            VarBinaryLiteral b = new VarBinaryLiteral(new byte[]{1, 2, 3});
            Assertions.assertEquals(0, a.compareLiteral(b));
        }

        @Test
        void unsignedByteOrdering() throws AnalysisException {
            // 0xFF must be > 0x01 because compare uses unsigned bytes.
            VarBinaryLiteral small = new VarBinaryLiteral(new byte[]{0x01});
            VarBinaryLiteral big = new VarBinaryLiteral(new byte[]{(byte) 0xFF});
            Assertions.assertTrue(small.compareLiteral(big) < 0);
            Assertions.assertTrue(big.compareLiteral(small) > 0);
        }
    }

    @Nested
    class JsonLiteralCompare {
        @Test
        void alwaysThrows() throws AnalysisException {
            JsonLiteral a = new JsonLiteral("{\"a\":1}");
            JsonLiteral b = new JsonLiteral("{\"a\":2}");
            Assertions.assertThrows(RuntimeException.class, () -> a.compareLiteral(b));
        }
    }

    @Nested
    class MaxLiteralCompare {
        @Test
        void vsMaxIsZero() {
            Assertions.assertEquals(0, MaxLiteral.MAX_VALUE.compareLiteral(MaxLiteral.MAX_VALUE));
        }

        @Test
        void vsAnythingElseIsOne() {
            Assertions.assertEquals(1, MaxLiteral.MAX_VALUE.compareLiteral(new IntLiteral(42)));
            Assertions.assertEquals(1, MaxLiteral.MAX_VALUE.compareLiteral(new NullLiteral()));
            Assertions.assertEquals(1, MaxLiteral.MAX_VALUE.compareLiteral(new StringLiteral("zzz")));
        }
    }

    @Nested
    class NullLiteralCompare {
        @Test
        void vsNullIsZero() {
            Assertions.assertEquals(0, new NullLiteral().compareLiteral(new NullLiteral()));
        }

        @Test
        void vsValueLiteralIsMinusOne() {
            Assertions.assertEquals(-1, new NullLiteral().compareLiteral(new IntLiteral(42)));
            Assertions.assertEquals(-1, new NullLiteral().compareLiteral(new StringLiteral("x")));
        }

        @Test
        void vsPlaceHolderDelegates() {
            Assertions.assertEquals(0, new NullLiteral().compareLiteral(new PlaceHolderExpr(new NullLiteral())));
        }
    }

    @Nested
    class PlaceHolderExprCompare {
        @Test
        void delegatesToWrappedLiteral() {
            PlaceHolderExpr wrapsTen = new PlaceHolderExpr(new IntLiteral(10));
            PlaceHolderExpr wrapsTwenty = new PlaceHolderExpr(new IntLiteral(20));
            Assertions.assertEquals(0, wrapsTen.compareLiteral(new IntLiteral(10)));
            Assertions.assertTrue(wrapsTen.compareLiteral(new IntLiteral(20)) < 0);
            Assertions.assertTrue(wrapsTwenty.compareLiteral(new IntLiteral(10)) > 0);
        }
    }

    // The Nereids counterparts for MAP / STRUCT / TIMEV2 do NOT implement
    // ComparableLiteral. In legacy these returned 0, silently making any two
    // such literals compare-equal in dedup paths. They now throw so the bug
    // surfaces loudly if the planner ever does feed them through compareLiteral.
    @Nested
    class MapLiteralCompare {
        @Test
        void alwaysThrows() {
            Assertions.assertThrows(RuntimeException.class,
                    () -> new MapLiteral().compareLiteral(new MapLiteral()));
        }
    }

    @Nested
    class StructLiteralCompare {
        @Test
        void alwaysThrows() {
            Assertions.assertThrows(RuntimeException.class,
                    () -> new StructLiteral().compareLiteral(new StructLiteral()));
        }
    }

    @Nested
    class TimeV2LiteralCompare {
        @Test
        void alwaysThrows() {
            Assertions.assertThrows(RuntimeException.class,
                    () -> new TimeV2Literal(1, 0, 0, 0, 0, false)
                            .compareLiteral(new TimeV2Literal(2, 0, 0, 0, 0, false)));
        }
    }
}

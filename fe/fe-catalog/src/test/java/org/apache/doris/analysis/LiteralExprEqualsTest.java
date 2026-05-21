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

// Covers equals()/hashCode() for every legacy LiteralExpr subclass that the
// optimizer uses for predicate dedup. Most types inherit LiteralExpr.equals
// (which itself calls compareLiteral); IPv4Literal / IPv6Literal /
// ArrayLiteral / NullLiteral override equals directly.
//
// Skipped: MapLiteral, StructLiteral, TimeV2Literal (SQL has no way to feed
// two of them into the dedup paths).
class LiteralExprEqualsTest {

    @Nested
    class BoolLiteralEquals {
        @Test
        void sameValue() {
            Assertions.assertEquals(new BoolLiteral(true), new BoolLiteral(true));
            Assertions.assertEquals(new BoolLiteral(false), new BoolLiteral(false));
        }

        @Test
        void differentValue() {
            Assertions.assertNotEquals(new BoolLiteral(true), new BoolLiteral(false));
        }

        @Test
        void notEqualToNullObject() {
            Assertions.assertNotEquals(null, new BoolLiteral(true));
        }

        @Test
        void self() {
            BoolLiteral b = new BoolLiteral(true);
            Assertions.assertEquals(b, b);
        }
    }

    @Nested
    class IntLiteralEquals {
        @Test
        void sameValue() {
            Assertions.assertEquals(new IntLiteral(42), new IntLiteral(42));
        }

        @Test
        void differentValue() {
            Assertions.assertNotEquals(new IntLiteral(1), new IntLiteral(2));
        }

        @Test
        void hashCodeMatchesEquality() {
            Assertions.assertEquals(new IntLiteral(42).hashCode(), new IntLiteral(42).hashCode());
        }
    }

    @Nested
    class FloatLiteralEquals {
        @Test
        void sameValue() {
            Assertions.assertEquals(new FloatLiteral(1.5), new FloatLiteral(1.5));
        }

        @Test
        void differentValue() {
            Assertions.assertNotEquals(new FloatLiteral(1.0), new FloatLiteral(2.0));
        }
    }

    @Nested
    class DecimalLiteralEquals {
        @Test
        void sameValue() {
            BigDecimal v = new BigDecimal("12.34");
            DecimalLiteral a = new DecimalLiteral(v, ScalarType.createDecimalV3Type(10, 2));
            DecimalLiteral b = new DecimalLiteral(v, ScalarType.createDecimalV3Type(10, 2));
            Assertions.assertEquals(a, b);
        }

        @Test
        void differentValue() {
            DecimalLiteral a = new DecimalLiteral(new BigDecimal("1.00"), ScalarType.createDecimalV3Type(10, 2));
            DecimalLiteral b = new DecimalLiteral(new BigDecimal("9.99"), ScalarType.createDecimalV3Type(10, 2));
            Assertions.assertNotEquals(a, b);
        }
    }

    @Nested
    class LargeIntLiteralEquals {
        @Test
        void sameValue() {
            BigInteger v = new BigInteger("12345678901234567890");
            Assertions.assertEquals(new LargeIntLiteral(v), new LargeIntLiteral(v));
        }

        @Test
        void differentValue() {
            Assertions.assertNotEquals(
                    new LargeIntLiteral(BigInteger.ONE),
                    new LargeIntLiteral(new BigInteger("12345678901234567890")));
        }
    }

    @Nested
    class DateLiteralEquals {
        @Test
        void sameValue() {
            Assertions.assertEquals(new DateLiteral(2026, 5, 21), new DateLiteral(2026, 5, 21));
        }

        @Test
        void differentValue() {
            Assertions.assertNotEquals(new DateLiteral(2026, 5, 21), new DateLiteral(2026, 5, 22));
        }
    }

    @Nested
    class StringLiteralEquals {
        @Test
        void sameValue() {
            Assertions.assertEquals(new StringLiteral("abc"), new StringLiteral("abc"));
        }

        @Test
        void differentValue() {
            Assertions.assertNotEquals(new StringLiteral("abc"), new StringLiteral("abd"));
        }

        @Test
        void notEqualToNonStringLiteral() {
            // LiteralExpr.equals short-circuits StringLiteral vs non-StringLiteral to false.
            Assertions.assertNotEquals(new StringLiteral("1"), new IntLiteral(1));
        }
    }

    @Nested
    class IPv4LiteralEquals {
        @Test
        void sameValue() throws AnalysisException {
            Assertions.assertEquals(new IPv4Literal("1.1.1.1"), new IPv4Literal("1.1.1.1"));
        }

        @Test
        void differentValue() throws AnalysisException {
            Assertions.assertNotEquals(new IPv4Literal("1.1.1.1"), new IPv4Literal("1.1.1.2"));
        }

        @Test
        void notEqualToNonIPv4Literal() throws AnalysisException {
            // Before the fix, equals would call compareLiteral which returned 0 for any
            // peer, making this assertion fail. The fix's instanceof short-circuit prevents
            // that, and also keeps equals from throwing on cross-type.
            Assertions.assertNotEquals(new IPv4Literal("1.1.1.1"), new StringLiteral("1.1.1.1"));
            Assertions.assertNotEquals(new IPv4Literal("0.0.0.0"), new IntLiteral(0));
        }

        @Test
        void hashCodeMatchesEquality() throws AnalysisException {
            Assertions.assertEquals(new IPv4Literal("1.1.1.1").hashCode(), new IPv4Literal("1.1.1.1").hashCode());
        }
    }

    @Nested
    class IPv6LiteralEquals {
        @Test
        void sameValue() throws AnalysisException {
            Assertions.assertEquals(new IPv6Literal("::1"), new IPv6Literal("::1"));
        }

        @Test
        void canonicalizedFormsEqual() throws AnalysisException {
            // "::1" and "0:0:0:0:0:0:0:1" must hash and compare-equal after canonicalize.
            IPv6Literal compact = new IPv6Literal("::1");
            IPv6Literal expanded = new IPv6Literal("0:0:0:0:0:0:0:1");
            Assertions.assertEquals(compact, expanded);
            Assertions.assertEquals(compact.hashCode(), expanded.hashCode());
        }

        @Test
        void differentValue() throws AnalysisException {
            Assertions.assertNotEquals(new IPv6Literal("::1"), new IPv6Literal("::2"));
        }

        @Test
        void notEqualToNonIPv6Literal() throws AnalysisException {
            Assertions.assertNotEquals(new IPv6Literal("::1"), new StringLiteral("::1"));
        }
    }

    @Nested
    class ArrayLiteralEquals {
        private final Type intArray = new ArrayType(Type.INT);

        @Test
        void sameElements() {
            ArrayLiteral a = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(2));
            ArrayLiteral b = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(2));
            Assertions.assertEquals(a, b);
        }

        @Test
        void differentElements() {
            ArrayLiteral a = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(2));
            ArrayLiteral b = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(3));
            Assertions.assertNotEquals(a, b);
        }

        @Test
        void differentLength() {
            ArrayLiteral shorter = new ArrayLiteral(intArray, new IntLiteral(1));
            ArrayLiteral longer = new ArrayLiteral(intArray, new IntLiteral(1), new IntLiteral(2));
            Assertions.assertNotEquals(shorter, longer);
        }

        @Test
        void notEqualToNonArray() {
            ArrayLiteral a = new ArrayLiteral(intArray, new IntLiteral(1));
            Assertions.assertNotEquals(a, new IntLiteral(1));
        }
    }

    @Nested
    class VarBinaryLiteralEquals {
        @Test
        void sameBytes() throws AnalysisException {
            Assertions.assertEquals(new VarBinaryLiteral(new byte[]{1, 2, 3}),
                    new VarBinaryLiteral(new byte[]{1, 2, 3}));
        }

        @Test
        void differentBytes() throws AnalysisException {
            Assertions.assertNotEquals(new VarBinaryLiteral(new byte[]{1, 2, 3}),
                    new VarBinaryLiteral(new byte[]{1, 2, 4}));
        }
    }

    @Nested
    class MaxLiteralEquals {
        @Test
        void singletonEqualsItself() {
            Assertions.assertEquals(MaxLiteral.MAX_VALUE, MaxLiteral.MAX_VALUE);
        }

        @Test
        void notEqualToValueLiteral() {
            Assertions.assertNotEquals(MaxLiteral.MAX_VALUE, new IntLiteral(Long.MAX_VALUE));
        }
    }

    @Nested
    class NullLiteralEquals {
        @Test
        void anyTwoNullsEqual() {
            Assertions.assertEquals(new NullLiteral(), new NullLiteral());
        }

        @Test
        void notEqualToValueLiteral() {
            Assertions.assertNotEquals(new NullLiteral(), new IntLiteral(0));
        }
    }

    @Nested
    class PlaceHolderExprEquals {
        @Test
        void delegatesToWrappedForEquality() {
            // PlaceHolderExpr inherits LiteralExpr.equals, which calls compareLiteral,
            // and PlaceHolderExpr.compareLiteral delegates to the wrapped literal.
            PlaceHolderExpr wrapsTen = new PlaceHolderExpr(new IntLiteral(10));
            Assertions.assertTrue(wrapsTen.equals(new IntLiteral(10)));
            Assertions.assertFalse(wrapsTen.equals(new IntLiteral(20)));
        }
    }

    // TimeV2Literal.compareLiteral now throws, but equals/hashCode are overridden
    // so dedup paths that don't go through compareLiteral still work.
    @Nested
    class TimeV2LiteralEquals {
        @Test
        void sameValue() {
            Assertions.assertEquals(new TimeV2Literal(1, 2, 3, 0, 0, false),
                    new TimeV2Literal(1, 2, 3, 0, 0, false));
        }

        @Test
        void differentValue() {
            Assertions.assertNotEquals(new TimeV2Literal(1, 2, 3, 0, 0, false),
                    new TimeV2Literal(4, 5, 6, 0, 0, false));
        }

        @Test
        void negativeAndPositiveDiffer() {
            Assertions.assertNotEquals(new TimeV2Literal(1, 0, 0, 0, 0, false),
                    new TimeV2Literal(1, 0, 0, 0, 0, true));
        }

        @Test
        void hashCodeMatchesEquality() {
            Assertions.assertEquals(new TimeV2Literal(1, 2, 3, 0, 0, false).hashCode(),
                    new TimeV2Literal(1, 2, 3, 0, 0, false).hashCode());
        }
    }
}

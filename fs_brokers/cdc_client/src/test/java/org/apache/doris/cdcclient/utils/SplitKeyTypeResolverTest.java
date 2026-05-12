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

package org.apache.doris.cdcclient.utils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class SplitKeyTypeResolverTest {

    // ─── cast: numeric types ──────────────────────────────────────────────────

    @Test
    void castIntegerToBigintReturnsLong() {
        Object out = SplitKeyTypeResolver.cast(Integer.valueOf(3), Types.BIGINT);
        assertEquals(Long.class, out.getClass());
        assertEquals(3L, out);
    }

    @Test
    void castLongToIntegerReturnsInteger() {
        Object out = SplitKeyTypeResolver.cast(Long.valueOf(7), Types.INTEGER);
        assertEquals(Integer.class, out.getClass());
        assertEquals(7, out);
    }

    @Test
    void castNumberToSmallintReturnsShort() {
        Object out = SplitKeyTypeResolver.cast(Integer.valueOf(100), Types.SMALLINT);
        assertEquals(Short.class, out.getClass());
        assertEquals((short) 100, out);
    }

    @Test
    void castNumberToTinyintReturnsByte() {
        Object out = SplitKeyTypeResolver.cast(Integer.valueOf(5), Types.TINYINT);
        assertEquals(Byte.class, out.getClass());
        assertEquals((byte) 5, out);
    }

    @Test
    void castNumberToDecimalReturnsBigDecimal() {
        Object out = SplitKeyTypeResolver.cast(Long.valueOf(42), Types.DECIMAL);
        assertEquals(BigDecimal.class, out.getClass());
        assertEquals(new BigDecimal("42"), out);
    }

    @Test
    void castNumberToNumericReturnsBigDecimal() {
        Object out = SplitKeyTypeResolver.cast(Integer.valueOf(9), Types.NUMERIC);
        assertEquals(BigDecimal.class, out.getClass());
        assertEquals(new BigDecimal("9"), out);
    }

    @Test
    void castNumberToFloatReturnsFloat() {
        Object out = SplitKeyTypeResolver.cast(Double.valueOf(1.5), Types.FLOAT);
        assertEquals(Float.class, out.getClass());
        assertEquals(1.5f, out);
    }

    @Test
    void castNumberToDoubleReturnsDouble() {
        Object out = SplitKeyTypeResolver.cast(Integer.valueOf(2), Types.DOUBLE);
        assertEquals(Double.class, out.getClass());
        assertEquals(2.0d, out);
    }

    // ─── cast: edge cases ─────────────────────────────────────────────────────

    @Test
    void castNullReturnsNull() {
        assertNull(SplitKeyTypeResolver.cast(null, Types.BIGINT));
    }

    @Test
    void castNonNumberReturnsAsIs() {
        String v = "abc";
        assertSame(v, SplitKeyTypeResolver.cast(v, Types.BIGINT));
    }

    @Test
    void castUuidReturnsAsIs() {
        UUID v = UUID.randomUUID();
        assertSame(v, SplitKeyTypeResolver.cast(v, Types.OTHER));
    }

    @Test
    void castUnknownSqlTypeReturnsAsIs() {
        Object v = Integer.valueOf(3);
        // VARCHAR is not in the switch -> returned as is.
        assertSame(v, SplitKeyTypeResolver.cast(v, Types.VARCHAR));
    }

    // ─── getOrCompute: cache behavior ─────────────────────────────────────────

    @Test
    void getOrComputeCachesAfterFirstCall() {
        String key = "test-cache-" + UUID.randomUUID();
        AtomicInteger callCount = new AtomicInteger();
        int v1 = SplitKeyTypeResolver.getOrCompute(key, () -> {
            callCount.incrementAndGet();
            return Types.BIGINT;
        });
        int v2 = SplitKeyTypeResolver.getOrCompute(key, () -> {
            callCount.incrementAndGet();
            return Types.INTEGER;     // would be wrong if called -> proves cache hit
        });
        assertEquals(Types.BIGINT, v1);
        assertEquals(Types.BIGINT, v2);
        assertEquals(1, callCount.get());
    }

    @Test
    void getOrComputeDifferentKeysComputeIndependently() {
        AtomicInteger callCount = new AtomicInteger();
        String keyA = "test-keyA-" + UUID.randomUUID();
        String keyB = "test-keyB-" + UUID.randomUUID();
        int a = SplitKeyTypeResolver.getOrCompute(keyA, () -> {
            callCount.incrementAndGet();
            return Types.BIGINT;
        });
        int b = SplitKeyTypeResolver.getOrCompute(keyB, () -> {
            callCount.incrementAndGet();
            return Types.INTEGER;
        });
        assertEquals(Types.BIGINT, a);
        assertEquals(Types.INTEGER, b);
        assertEquals(2, callCount.get());
    }
}

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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class SplitKeyTypeResolverTest {

    // ─── Number conversions ──────────────────────────────────────────────────

    @Test
    void castIntegerToLong() {
        Object out = SplitKeyTypeResolver.cast(Integer.valueOf(3), Long.class);
        assertEquals(Long.class, out.getClass());
        assertEquals(3L, out);
    }

    @Test
    void castLongToInteger() {
        Object out = SplitKeyTypeResolver.cast(Long.valueOf(7), Integer.class);
        assertEquals(Integer.class, out.getClass());
        assertEquals(7, out);
    }

    @Test
    void castIntegerToShort() {
        Object out = SplitKeyTypeResolver.cast(Integer.valueOf(100), Short.class);
        assertEquals(Short.class, out.getClass());
        assertEquals((short) 100, out);
    }

    @Test
    void castLongToBigDecimal() {
        Object out = SplitKeyTypeResolver.cast(Long.valueOf(42), BigDecimal.class);
        assertEquals(BigDecimal.class, out.getClass());
        assertEquals(new BigDecimal("42"), out);
    }

    @Test
    void castDoubleToFloat() {
        Object out = SplitKeyTypeResolver.cast(Double.valueOf(1.5), Float.class);
        assertEquals(Float.class, out.getClass());
        assertEquals(1.5f, out);
    }

    // ─── String to date-like conversions ─────────────────────────────────────

    @Test
    void castStringToSqlDate() {
        Object out = SplitKeyTypeResolver.cast("2024-05-12", java.sql.Date.class);
        assertEquals(java.sql.Date.class, out.getClass());
        assertEquals(java.sql.Date.valueOf("2024-05-12"), out);
    }

    @Test
    void castStringToUuid() {
        UUID expected = UUID.randomUUID();
        Object out = SplitKeyTypeResolver.cast(expected.toString(), UUID.class);
        assertEquals(UUID.class, out.getClass());
        assertEquals(expected, out);
    }

    // ─── Edge cases ──────────────────────────────────────────────────────────

    @Test
    void castNullReturnsNull() {
        assertNull(SplitKeyTypeResolver.cast(null, Long.class));
    }

    @Test
    void castSameTypeReturnsAsIs() {
        Long v = Long.valueOf(5);
        assertSame(v, SplitKeyTypeResolver.cast(v, Long.class));
    }

    @Test
    void castNullTargetClassReturnsAsIs() {
        Object v = Integer.valueOf(3);
        assertSame(v, SplitKeyTypeResolver.cast(v, null));
    }

    @Test
    void castUnconvertibleFallsBackToOriginal() {
        // String "abc" can't convert to Long -> fallback to original.
        Object v = "abc";
        Object out = SplitKeyTypeResolver.cast(v, Long.class);
        assertSame(v, out);
    }
}

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

import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link ConfigUtil}. */
class ConfigUtilTest {

    // ─── getServerId ──────────────────────────────────────────────────────────

    @Test
    void serverIdIsNonNegative() {
        // Any jobId hash should produce a non-negative result (bitwise AND strips sign bit).
        String result = ConfigUtil.getServerId("12345");
        assertTrue(Long.parseLong(result) >= 0, "serverId must be non-negative");
    }

    @Test
    void serverIdHandlesMinHashCode() {
        // Find a string whose hashCode() == Integer.MIN_VALUE to exercise the edge case
        // where Math.abs(Integer.MIN_VALUE) would return a negative number.
        // "polygenelubricants" is a well-known such string.
        String result = ConfigUtil.getServerId("polygenelubricants");
        assertTrue(Long.parseLong(result) >= 0, "serverId must be non-negative for MIN_VALUE hash");
    }

    // ─── getTableList ─────────────────────────────────────────────────────────

    @Test
    void tableListFromIncludeTables() {
        Map<String, String> config = new HashMap<>();
        config.put(DataSourceConfigKeys.INCLUDE_TABLES, "t1, t2, t3");
        String[] result = ConfigUtil.getTableList("public", config);
        assertArrayEquals(new String[]{"public.t1", "public.t2", "public.t3"}, result);
    }

    @Test
    void tableListFromSingleTable() {
        Map<String, String> config = new HashMap<>();
        config.put(DataSourceConfigKeys.TABLE, "orders");
        String[] result = ConfigUtil.getTableList("myschema", config);
        assertArrayEquals(new String[]{"myschema.orders"}, result);
    }

    @Test
    void tableListRejectsCommaInTableName() {
        Map<String, String> config = new HashMap<>();
        config.put(DataSourceConfigKeys.TABLE, "t1,t2");
        assertThrows(IllegalArgumentException.class,
                () -> ConfigUtil.getTableList("public", config));
    }

    @Test
    void tableListEmptyWhenNeitherSet() {
        Map<String, String> config = new HashMap<>();
        String[] result = ConfigUtil.getTableList("public", config);
        assertEquals(0, result.length);
    }
}

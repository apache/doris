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

import io.debezium.config.CommonConnectorConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.ServerIdRange;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link ConfigUtil}. */
class ConfigUtilTest {

    // ─── resolveServerIdRange ─────────────────────────────────────────────────

    // Value/format validation lives in FE DataSourceConfigValidator; here we only verify the
    // derivation algorithm and that valid input is passed through to flink-cdc's ServerIdRange.

    @Test
    void resolveDefaultDeriveSingle() {
        ServerIdRange range = ConfigUtil.resolveServerIdRange("12345", 1, null);
        assertEquals(1, range.getNumberOfServerIds());
        assertTrue(range.getStartServerId() >= 1);
    }

    @Test
    void resolveDefaultDeriveExpandsToParallelism() {
        ServerIdRange range = ConfigUtil.resolveServerIdRange("12345", 4, null);
        assertEquals(4, range.getNumberOfServerIds());
        assertEquals(range.getStartServerId() + 3, range.getEndServerId());
    }

    @Test
    void resolveDefaultDeriveHandlesMinHashCode() {
        // "polygenelubricants" hashCode() == Integer.MIN_VALUE; & MAX_VALUE strips sign bit.
        ServerIdRange range =
                ConfigUtil.resolveServerIdRange("polygenelubricants", 4, null);
        assertTrue(range.getStartServerId() >= 1);
        assertTrue(range.getEndServerId() <= Integer.MAX_VALUE);
    }

    @Test
    void resolveDefaultDeriveBumpsZeroHashToOne() {
        // Empty string hashCode() == 0; bump to 1 because MySQL server_id=0 disables replication.
        ServerIdRange range = ConfigUtil.resolveServerIdRange("", 1, null);
        assertEquals(1, range.getStartServerId());
    }

    @Test
    void resolveUserSingleValue() {
        ServerIdRange range = ConfigUtil.resolveServerIdRange("anyjob", 1, "5400");
        assertEquals(5400L, range.getStartServerId());
        assertEquals(5400L, range.getEndServerId());
    }

    @Test
    void resolveUserRange() {
        ServerIdRange range = ConfigUtil.resolveServerIdRange("anyjob", 4, "5400-5408");
        assertEquals(5400L, range.getStartServerId());
        assertEquals(5408L, range.getEndServerId());
    }

    @Test
    void resolveRejectsWidthLessThanParallelism() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ConfigUtil.resolveServerIdRange("anyjob", 4, "5400"));
        assertTrue(ex.getMessage().contains("snapshot_parallelism"));
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

    // ─── getDefaultDebeziumProps: queue byte cap ──────────────────────────────

    private static long queueBytes(Properties props) {
        return Long.parseLong(
                props.getProperty(CommonConnectorConfig.MAX_QUEUE_SIZE_IN_BYTES.name()));
    }

    @Test
    void defaultQueueBytesWithinClamp() {
        long bytes = queueBytes(ConfigUtil.getDefaultDebeziumProps());
        assertTrue(bytes >= 64L * 1024 * 1024 && bytes <= 256L * 1024 * 1024,
                "expected clamp to [64MB, 256MB] but got " + bytes);
    }

    @Test
    void sysPropOverridesAdaptiveValue() {
        String prev = System.getProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP);
        try {
            System.setProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP, "1048576");
            assertEquals(1048576L, queueBytes(ConfigUtil.getDefaultDebeziumProps()));
        } finally {
            restore(prev);
        }
    }

    @Test
    void negativeSysPropDisablesByteBound() {
        String prev = System.getProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP);
        try {
            System.setProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP, "-1");
            assertEquals(0L, queueBytes(ConfigUtil.getDefaultDebeziumProps()));
        } finally {
            restore(prev);
        }
    }

    @Test
    void malformedSysPropFallsBackToClamp() {
        String prev = System.getProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP);
        try {
            System.setProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP, "32MB");
            long bytes = queueBytes(ConfigUtil.getDefaultDebeziumProps());
            assertTrue(bytes >= 64L * 1024 * 1024 && bytes <= 256L * 1024 * 1024,
                    "malformed override should fall back to [64MB, 256MB] but got " + bytes);
        } finally {
            restore(prev);
        }
    }

    private static void restore(String prev) {
        if (prev == null) {
            System.clearProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP);
        } else {
            System.setProperty(ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP, prev);
        }
    }
}

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

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

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
        ServerIdRange range = ConfigUtil.resolveServerIdRange("polygenelubricants", 4, null);
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
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ConfigUtil.resolveServerIdRange("anyjob", 4, "5400"));
        assertTrue(ex.getMessage().contains("snapshot_parallelism"));
    }

    // ─── getTableList ─────────────────────────────────────────────────────────

    @Test
    void tableListFromIncludeTables() {
        Map<String, String> config = new HashMap<>();
        config.put(DataSourceConfigKeys.INCLUDE_TABLES, "t1, t2, t3");
        String[] result = ConfigUtil.getTableList("public", config);
        assertArrayEquals(new String[] {"public.t1", "public.t2", "public.t3"}, result);
    }

    @Test
    void tableListFromSingleTable() {
        Map<String, String> config = new HashMap<>();
        config.put(DataSourceConfigKeys.TABLE, "orders");
        String[] result = ConfigUtil.getTableList("myschema", config);
        assertArrayEquals(new String[] {"myschema.orders"}, result);
    }

    @Test
    void tableListRejectsCommaInTableName() {
        Map<String, String> config = new HashMap<>();
        config.put(DataSourceConfigKeys.TABLE, "t1,t2");
        assertThrows(
                IllegalArgumentException.class, () -> ConfigUtil.getTableList("public", config));
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

    // ─── server timezone parsing ──────────────────────────────────────────────

    @Test
    void mysqlServerTimezoneFromUrl() {
        assertEquals(
                ZoneId.of("UTC"),
                ConfigUtil.getServerTimeZoneFromJdbcUrl(
                        "jdbc:mysql://h:3306/db?serverTimezone=UTC"));
    }

    @Test
    void postgresTimezoneFromUrl() {
        assertEquals(
                ZoneId.of("Asia/Shanghai"),
                ConfigUtil.getServerTimeZoneFromJdbcUrl(
                        "jdbc:postgresql://h:5432/db?timezone=Asia/Shanghai"));
    }

    @Test
    void timezoneDefaultsToSystemZone() {
        assertEquals(
                ZoneId.systemDefault(),
                ConfigUtil.getServerTimeZoneFromJdbcUrl("jdbc:mysql://h:3306/db"));
        assertEquals(ZoneId.systemDefault(), ConfigUtil.getServerTimeZoneFromJdbcUrl(null));
        assertEquals(
                ZoneId.systemDefault(),
                ConfigUtil.getServerTimeZoneFromJdbcUrl("jdbc:oracle://h:1521/db"));
    }

    @Test
    void timeZoneFromProps() {
        assertEquals(
                ZoneId.of("UTC"),
                ConfigUtil.getTimeZoneFromProps(Map.of("serverTimezone", "UTC")));
        assertEquals(ZoneId.systemDefault(), ConfigUtil.getTimeZoneFromProps(new HashMap<>()));
    }

    // ─── is13Timestamp / isJson ───────────────────────────────────────────────

    @Test
    void is13Timestamp() {
        assertTrue(ConfigUtil.is13Timestamp("1700000000000"));
        assertFalse(ConfigUtil.is13Timestamp("170000000000")); // 12 digits
        assertFalse(ConfigUtil.is13Timestamp("17000000000000")); // 14 digits
        assertFalse(ConfigUtil.is13Timestamp("abc"));
        assertFalse(ConfigUtil.is13Timestamp(null));
    }

    @Test
    void isJson_onlyObjects() {
        assertTrue(ConfigUtil.isJson("{\"a\":1}"));
        assertTrue(ConfigUtil.isJson("{}"));
        assertFalse(ConfigUtil.isJson("[1,2]")); // arrays are not objects
        assertFalse(ConfigUtil.isJson("123"));
        assertFalse(ConfigUtil.isJson("not json"));
        assertFalse(ConfigUtil.isJson(""));
        assertFalse(ConfigUtil.isJson(null));
    }

    // ─── exclude columns / target table mappings ──────────────────────────────

    @Test
    void parseExcludeColumns_trimsAndDropsEmpty() {
        Map<String, String> config = new HashMap<>();
        config.put("table.t1.exclude_columns", "a, b ,, c");
        assertEquals(Set.of("a", "b", "c"), ConfigUtil.parseExcludeColumns(config, "t1"));
        assertTrue(ConfigUtil.parseExcludeColumns(config, "other").isEmpty());
    }

    @Test
    void parseAllExcludeColumns_scansAllTables() {
        Map<String, String> config = new HashMap<>();
        config.put("table.t1.exclude_columns", "a,b");
        config.put("table.t2.exclude_columns", "c");
        config.put("table.t3.target_table", "x"); // unrelated key ignored

        Map<String, Set<String>> all = ConfigUtil.parseAllExcludeColumns(config);

        assertEquals(2, all.size());
        assertEquals(Set.of("a", "b"), all.get("t1"));
        assertEquals(Set.of("c"), all.get("t2"));
    }

    @Test
    void parseAllTargetTableMappings_skipsEmptyValues() {
        Map<String, String> config = new HashMap<>();
        config.put("table.src.target_table", "dst");
        config.put("table.empty.target_table", "   ");

        Map<String, String> mappings = ConfigUtil.parseAllTargetTableMappings(config);

        assertEquals(1, mappings.size());
        assertEquals("dst", mappings.get("src"));
    }

    // ─── toStringMap ──────────────────────────────────────────────────────────

    @Test
    void toStringMap() {
        assertEquals(
                Map.of("a", "1", "b", "2"),
                ConfigUtil.toStringMap("{\"a\":\"1\",\"b\":\"2\"}"));
        assertNull(ConfigUtil.toStringMap("not json"));
        assertNull(ConfigUtil.toStringMap("[1,2]"));
    }
}

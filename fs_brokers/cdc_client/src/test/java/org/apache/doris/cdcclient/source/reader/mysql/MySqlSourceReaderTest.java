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

package org.apache.doris.cdcclient.source.reader.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.source.reader.oceanbase.OceanBaseSourceReader;
import org.apache.doris.job.cdc.DataSourceConfigKeys;

import com.github.shyiko.mysql.binlog.GtidSet;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class MySqlSourceReaderTest {

    private static final String SERVER_UUID = "24bc7850-2c16-11e6-a073-0242ac110002";
    private static final String EXCLUDE_HEARTBEAT_FROM_EVENT_COUNT =
            "doris.cdc.exclude.heartbeat.from.event.count";

    @Test
    void testNormalizeSslModeMapsAllLegalValues() {
        assertEquals("disabled", MySqlSourceReader.normalizeSslModeForMysql("disable"));
        assertEquals("required", MySqlSourceReader.normalizeSslModeForMysql("require"));
        assertEquals("verify_ca", MySqlSourceReader.normalizeSslModeForMysql("verify-ca"));
    }

    @Test
    void testNormalizeSslModeRejectsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MySqlSourceReader.normalizeSslModeForMysql(null));
    }

    @Test
    void testNormalizeSslModeRejectsMysqlUnderscoreSpelling() {
        // FE validator rejects this, but guard reader-side too.
        assertThrows(
                IllegalArgumentException.class,
                () -> MySqlSourceReader.normalizeSslModeForMysql("verify_ca"));
    }

    @Test
    void testNormalizeSslModeRejectsVerifyFull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MySqlSourceReader.normalizeSslModeForMysql("verify-full"));
    }

    @Test
    void testNormalizeSslModeRejectsUppercase() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MySqlSourceReader.normalizeSslModeForMysql("DISABLE"));
    }

    // A JSON resume offset carrying file/pos AND a multi-interval gtid set must keep the gtids
    // (and a non-null kind) through generateMySqlConfig, not just file/pos.
    @Test
    void jsonOffsetWithGtidsPreservesMultiIntervalGtidSet() throws Exception {
        String gtid = SERVER_UUID + ":1-3:5-7";
        BinlogOffset off =
                startupBinlogOffset(
                        "{\"file\":\"mysql-bin.000004\",\"pos\":\"1024\",\"gtids\":\""
                                + gtid
                                + "\"}");
        assertEquals(gtid, off.getGtidSet());
        assertNotNull(off.getOffsetKind());
        assertEquals(
                2, new GtidSet(off.getGtidSet()).getUUIDSet(SERVER_UUID).getIntervals().size());
    }

    // A gtid-only JSON offset (no file/pos) is accepted, matching Flink CDC's specific-offset rules.
    @Test
    void jsonOffsetGtidOnlyIsAccepted() throws Exception {
        String gtid = SERVER_UUID + ":1-7";
        BinlogOffset off = startupBinlogOffset("{\"gtids\":\"" + gtid + "\"}");
        assertEquals(gtid, off.getGtidSet());
        assertNotNull(off.getOffsetKind());
    }

    @Test
    void mysqlConfigIncludesSchemaChanges() throws Exception {
        MySqlSourceConfig config = sourceConfig("initial");

        assertTrue(config.isIncludeSchemaChanges());
    }

    @Test
    void mysqlConfigCanDisableSchemaChanges() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(DataSourceConfigKeys.SCHEMA_CHANGE_ENABLED, "false");
        MySqlSourceConfig config = sourceConfig("initial", overrides);

        assertFalse(config.isIncludeSchemaChanges());
    }

    @Test
    void mysqlHeartbeatContributesToRestartEventCount() throws Exception {
        MySqlSourceConfig config =
                sourceConfig(new MySqlSourceReader(), "initial", Map.of());

        assertFalse(
                config.getDbzConfiguration()
                        .getBoolean(EXCLUDE_HEARTBEAT_FROM_EVENT_COUNT));
    }

    @Test
    void oceanBaseHeartbeatDoesNotContributeToRestartEventCount() throws Exception {
        MySqlSourceConfig config =
                sourceConfig(new OceanBaseSourceReader(), "initial", Map.of());

        assertTrue(
                config.getDbzConfiguration()
                        .getBoolean(EXCLUDE_HEARTBEAT_FROM_EVENT_COUNT));
    }

    // Drive the real generateMySqlConfig JSON-offset path and return the rebuilt startup offset.
    private BinlogOffset startupBinlogOffset(String offsetJson) throws Exception {
        return sourceConfig(offsetJson).getStartupOptions().binlogOffset;
    }

    private MySqlSourceConfig sourceConfig(String offset) throws Exception {
        return sourceConfig(offset, Map.of());
    }

    private MySqlSourceConfig sourceConfig(String offset, Map<String, String> overrides)
            throws Exception {
        return sourceConfig(new MySqlSourceReader(), offset, overrides);
    }

    private MySqlSourceConfig sourceConfig(
            MySqlSourceReader reader, String offset, Map<String, String> overrides)
            throws Exception {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(DataSourceConfigKeys.JDBC_URL, "jdbc:mysql://localhost:3306/testdb");
        cfg.put(DataSourceConfigKeys.USER, "u");
        cfg.put(DataSourceConfigKeys.PASSWORD, "p");
        cfg.put(DataSourceConfigKeys.DATABASE, "testdb");
        cfg.put(DataSourceConfigKeys.TABLE, "t_test");
        cfg.put(DataSourceConfigKeys.OFFSET, offset);
        cfg.putAll(overrides);
        Method m =
                MySqlSourceReader.class.getDeclaredMethod(
                        "generateMySqlConfig", Map.class, String.class, int.class);
        m.setAccessible(true);
        return (MySqlSourceConfig) m.invoke(reader, cfg, "job-1", 0);
    }
}

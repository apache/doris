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

import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.JobBaseConfig;

import com.github.shyiko.mysql.binlog.GtidSet;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

public class MySqlSourceReaderTest {

    private static final String SERVER_UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

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
    void snapshotSplitContainsOnlyCurrentTableSchema() throws Exception {
        TableId tableId = TableId.parse("testdb.orders");
        TableId otherTableId = TableId.parse("testdb.other_orders");
        TableChanges.TableChange tableChange = tableChange(tableId, "pk");
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(tableId, tableChange);
        tableSchemas.put(otherTableId, tableChange(otherTableId, "other_id"));

        MySqlSourceReader reader =
                new MySqlSourceReader() {
                    @Override
                    protected Class<?> probeSplitKeyClass(
                            TableId ignoredTableId,
                            Column ignoredSplitColumn,
                            JobBaseConfig ignoredJobConfig) {
                        return Integer.class;
                    }
                };
        reader.setTableSchemas(tableSchemas);

        Method method =
                MySqlSourceReader.class.getDeclaredMethod(
                        "createSnapshotSplit", Map.class, JobBaseConfig.class);
        method.setAccessible(true);
        MySqlSnapshotSplit split =
                (MySqlSnapshotSplit)
                        method.invoke(
                                reader,
                                snapshotOffset("testdb.orders", "pk"),
                                new JobBaseConfig("job-1", "MYSQL", Map.of(), null));

        assertEquals(tableId, split.getTableId());
        assertEquals(Map.of(tableId, tableChange), split.getTableSchemas());
        assertFalse(split.getTableSchemas().containsKey(otherTableId));
        assertEquals(tableSchemas, reader.getTableSchemas());
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
        return (MySqlSourceConfig) m.invoke(new MySqlSourceReader(), cfg, "job-1", 0);
    }

    private static Map<String, Object> snapshotOffset(String tableId, String splitKey) {
        Map<String, Object> offset = new HashMap<>();
        offset.put("splitId", tableId + ":0");
        offset.put("tableId", tableId);
        offset.put("splitKey", List.of(splitKey));
        offset.put("splitStart", new Object[] {1});
        offset.put("splitEnd", new Object[] {10});
        return offset;
    }

    private static TableChanges.TableChange tableChange(TableId tableId, String splitKey) {
        TableEditor editor = Table.editor().tableId(tableId);
        editor.addColumns(
                Column.editor()
                        .name(splitKey)
                        .type("INT")
                        .jdbcType(Types.INTEGER)
                        .optional(false)
                        .create());
        editor.setPrimaryKeyNames(splitKey);
        return new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, editor.create());
    }
}

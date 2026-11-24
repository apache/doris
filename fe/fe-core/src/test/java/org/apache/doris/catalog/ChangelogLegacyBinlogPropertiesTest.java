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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.ModifyTablePropertyOperationLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The binlog keys the changelog.* properties replaced: DDL rejects them, and metadata written before the
 * change drops them, except from a table they enable changelog for, which can then only be dropped.
 */
public class ChangelogLegacyBinlogPropertiesTest extends TestWithFeService {
    private Database db;

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_feature_changelog = true;
        Config.enable_feature_binlog = true;
        Config.enable_table_stream = true;
        createDatabase("legacy_binlog");
        db = Env.getCurrentInternalCatalog().getDbOrDdlException("legacy_binlog");
        useDatabase("legacy_binlog");
    }

    @ParameterizedTest
    @ValueSource(strings = {"ROW", "row", "STATEMENT_AND_SNAPSHOT", "rows"})
    public void testCreateTableRejectsBinlogFormat(String format) {
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> createTable(
                "CREATE TABLE legacy_binlog.rejected (k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', 'binlog.enable'='true', 'binlog.format'='"
                        + format + "')"));
        Assertions.assertTrue(exception.getMessage().contains("Property binlog.format is no longer supported, "
                + "use \"changelog.enable\" = \"true\" to enable changelog"), exception.getMessage());
        Assertions.assertNull(db.getTableNullable("rejected"));
    }

    @Test
    public void testCreateTableRejectsBinlogNeedHistoricalValue() {
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> createTable(
                "CREATE TABLE legacy_binlog.rejected (k INT, v INT) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', 'enable_unique_key_merge_on_write'='true', "
                        + "'changelog.enable'='true', 'binlog.need_historical_value'='true')"));
        Assertions.assertTrue(exception.getMessage().contains("Property binlog.need_historical_value is no longer "
                + "supported, use changelog.need_historical_value instead"), exception.getMessage());
        Assertions.assertNull(db.getTableNullable("rejected"));
    }

    @Test
    public void testAlterTableRejectsLegacyKeys() throws Exception {
        createTable("CREATE TABLE legacy_binlog.ccr (k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'binlog.enable'='true')");
        OlapTable table = (OlapTable) db.getTableOrDdlException("ccr");
        for (String property : Lists.newArrayList("binlog.format", "binlog.need_historical_value")) {
            UserException exception = Assertions.assertThrows(UserException.class, () -> alterTableSync(
                    "ALTER TABLE legacy_binlog.ccr SET ('" + property + "'='false')"));
            Assertions.assertTrue(exception.getMessage().contains("Property " + property + " is no longer supported"),
                    exception.getMessage());
        }
        Assertions.assertTrue(table.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(table.hasChangelog());
    }

    @Test
    public void testDatabaseDdlRejectsLegacyKeys() {
        AnalysisException format = Assertions.assertThrows(AnalysisException.class,
                () -> createDatabaseWithSql("CREATE DATABASE legacy_binlog_rejected "
                        + "PROPERTIES ('binlog.enable'='true', 'binlog.format'='row')"));
        Assertions.assertTrue(format.getMessage().contains("Property binlog.format is no longer supported"),
                format.getMessage());
        AnalysisException historical = Assertions.assertThrows(AnalysisException.class,
                () -> createDatabaseWithSql("CREATE DATABASE legacy_binlog_rejected "
                        + "PROPERTIES ('changelog.enable'='true', 'binlog.need_historical_value'='true')"));
        Assertions.assertTrue(historical.getMessage().contains(
                "Property binlog.need_historical_value is no longer supported"), historical.getMessage());
        Assertions.assertNull(Env.getCurrentInternalCatalog().getDbNullable("legacy_binlog_rejected"));

        IllegalStateException alter = Assertions.assertThrows(IllegalStateException.class,
                () -> executeSql("ALTER DATABASE legacy_binlog SET PROPERTIES ('binlog.format'='rows')"));
        Assertions.assertTrue(alter.getMessage().contains("Property binlog.format is no longer supported"),
                alter.getMessage());
        Assertions.assertNull(db.getDbProperties().get("binlog.format"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testImageLoadDropsLegacyKeysOfPlainAndCcrTables(boolean ccr) {
        TableProperty loaded = reload(new TableProperty(legacyTableProperties(ccr, "STATEMENT_AND_SNAPSHOT")));
        assertLegacyKeysDropped(loaded.getProperties());
        Assertions.assertEquals(String.valueOf(ccr), loaded.getProperties().get("binlog.enable"));
        Assertions.assertFalse(loaded.isLegacyChangelog());
        Assertions.assertEquals(new BinlogConfig(ccr, 3600, 1024, 10), loaded.getBinlogConfig());
        Assertions.assertFalse(loaded.getChangelogConfig().isEnable());
    }

    @Test
    public void testDisabledRowFormatIsAPlainTable() {
        // binlog.format = ROW without binlog.enable never enabled changelog.
        TableProperty loaded = reload(new TableProperty(legacyTableProperties(false, "ROW")));
        assertLegacyKeysDropped(loaded.getProperties());
        Assertions.assertFalse(loaded.isLegacyChangelog());
        Assertions.assertFalse(loaded.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(loaded.getChangelogConfig().isEnable());
    }

    @Test
    public void testImageLoadKeepsLegacyChangelogTable() {
        Map<String, String> properties = legacyTableProperties(true, "row");
        properties.put("binlog.need_historical_value", "true");
        properties.put("__binlog_config_version", "3");
        TableProperty loaded = reload(new TableProperty(new HashMap<>(properties)));
        Assertions.assertTrue(loaded.isLegacyChangelog());
        // Nothing is translated or dropped, so the table is recognized again after the next image.
        Assertions.assertEquals(properties, loaded.getProperties());
        Assertions.assertTrue(reload(loaded).isLegacyChangelog());
        Assertions.assertFalse(loaded.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(loaded.getChangelogConfig().isEnable());
    }

    @Test
    public void testReplayDropsLegacyKeys() throws Exception {
        createTable("CREATE TABLE legacy_binlog.replayed (k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) db.getTableOrDdlException("replayed");
        // An OP_UPDATE_BINLOG_CONFIG journaled before the change carries the replaced keys.
        Env.getCurrentEnv().replayModifyTableProperty(OperationType.OP_UPDATE_BINLOG_CONFIG,
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), table.getName(),
                        legacyTableProperties(true, "STATEMENT_AND_SNAPSHOT")));
        assertLegacyKeysDropped(table.getTableProperty().getProperties());
        Assertions.assertFalse(table.isLegacyChangelog());
        Assertions.assertEquals(new BinlogConfig(true, 3600, 1024, 10), table.getBinlogConfig());
        Assertions.assertFalse(table.hasChangelog());
    }

    @Test
    public void testLegacyChangelogTableCanOnlyBeDropped() throws Exception {
        createTable("CREATE TABLE legacy_binlog.legacy_row (k INT, v INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) db.getTableOrDdlException("legacy_row");
        // What a table that enabled changelog with "binlog.format" = "ROW" looks like after the upgrade.
        table.getTableProperty().modifyTableProperties(Map.of("binlog.enable", "true", "binlog.format", "ROW"));
        table.getTableProperty().buildBinlogConfig();
        Assertions.assertTrue(table.isLegacyChangelog());
        Assertions.assertFalse(table.hasChangelog());
        Assertions.assertFalse(table.enableTso());
        Assertions.assertFalse(table.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(StatisticsUtil.supportAutoAnalyze(table));
        String message = table.getLegacyChangelogMessage();
        Assertions.assertEquals("Table legacy_binlog.legacy_row was created with \"binlog.format\" = \"ROW\", "
                + "which is no longer supported; drop the table and create it again with "
                + "\"changelog.enable\" = \"true\"", message);

        List<Executable> rejected = Lists.newArrayList(
                () -> executeSql("SELECT * FROM legacy_binlog.legacy_row"),
                () -> executeSql("SELECT * FROM legacy_binlog.legacy_row@incr('incrementType'='DETAIL')"),
                () -> executeSql("SELECT * FROM changelog('db' = 'legacy_binlog', 'table' = 'legacy_row')"),
                () -> executeSql("TRUNCATE TABLE legacy_binlog.legacy_row"),
                () -> executeSql("CREATE TABLE legacy_binlog.legacy_copy LIKE legacy_binlog.legacy_row"),
                () -> alterTableSync("ALTER TABLE legacy_binlog.legacy_row SET ('disable_auto_compaction'='true')"),
                () -> alterTableSync("ALTER TABLE legacy_binlog.legacy_row ADD COLUMN v2 INT"),
                () -> createTable("CREATE STREAM legacy_binlog.legacy_stream ON TABLE legacy_binlog.legacy_row "
                        + "PROPERTIES('show_initial_rows' = 'false')"),
                // Every load builds its sink this way.
                () -> new OlapTableSink(table, null, null).init(new TUniqueId(0, 0), 1L, db.getId(), 10L, 1,
                        false, false, 10L));
        for (Executable executable : rejected) {
            Exception exception = Assertions.assertThrows(Exception.class, executable);
            Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
        }
        Assertions.assertNull(db.getTableNullable("legacy_copy"));
        Assertions.assertNull(db.getTableNullable("legacy_stream"));
        Assertions.assertTrue(table.isLegacyChangelog());

        dropTable("legacy_binlog.legacy_row", true);
        Assertions.assertNull(db.getTableNullable("legacy_row"));
    }

    private static Map<String, String> legacyTableProperties(boolean enable, String format) {
        Map<String, String> properties = new HashMap<>();
        properties.put("binlog.enable", String.valueOf(enable));
        properties.put("binlog.ttl_seconds", "3600");
        properties.put("binlog.max_bytes", "1024");
        properties.put("binlog.max_history_nums", "10");
        properties.put("binlog.format", format);
        properties.put("binlog.need_historical_value", "false");
        properties.put("__binlog_config_version", "0");
        return properties;
    }

    private static void assertLegacyKeysDropped(Map<String, String> properties) {
        Assertions.assertFalse(properties.containsKey("binlog.format"), properties.toString());
        Assertions.assertFalse(properties.containsKey("binlog.need_historical_value"), properties.toString());
        Assertions.assertFalse(properties.containsKey("__binlog_config_version"), properties.toString());
    }

    private static TableProperty reload(TableProperty tableProperty) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(tableProperty), TableProperty.class);
    }
}

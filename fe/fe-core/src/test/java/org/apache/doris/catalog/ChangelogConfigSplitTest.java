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

import org.apache.doris.analysis.DbName;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.thrift.TBinlogConfig;
import org.apache.doris.thrift.TBinlogFormat;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Changelog is configured by the changelog.* properties, CCR binlog by the binlog.* ones, and a table or a
 * database never enables both.
 */
public class ChangelogConfigSplitTest extends TestWithFeService {
    private Database db;

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_feature_changelog = true;
        Config.enable_feature_binlog = true;
        createDatabase("split");
        db = Env.getCurrentInternalCatalog().getDbOrDdlException("split");
    }

    @Test
    public void testChangelogTableStoresOnlyChangelogKeys() throws Exception {
        OlapTable table = createDupTable(db, "changelog_keys", "'changelog.enable'='true', 'changelog.ttl_seconds'='600'");
        Assertions.assertTrue(table.hasChangelog());
        Assertions.assertTrue(table.enableTso());
        Assertions.assertNotNull(table.getChangelogIndexMeta());
        Assertions.assertEquals(new ChangelogConfig(true, 600, false), table.getChangelogConfig());
        Assertions.assertFalse(table.getBinlogConfig().isEnableForCCR());

        Map<String, String> properties = table.getTableProperty().getProperties();
        Assertions.assertEquals("true", properties.get("changelog.enable"));
        Assertions.assertEquals("600", properties.get("changelog.ttl_seconds"));
        Assertions.assertEquals("false", properties.get("changelog.need_historical_value"));
        Assertions.assertEquals("false", properties.get("binlog.enable"));
        // A new table has published no retention yet, and nothing of the replaced configuration is written.
        Assertions.assertFalse(properties.containsKey(ChangelogConfig.CONFIG_VERSION), properties.toString());
        assertNoLegacyKeys(properties);

        TBinlogConfig wire = table.getTabletBinlogConfig().toThrift();
        Assertions.assertTrue(wire.isEnable());
        Assertions.assertEquals(TBinlogFormat.CHANGELOG, wire.getBinlogFormat());
        Assertions.assertEquals(600, wire.getTtlSeconds());
    }

    @Test
    public void testPlainAndCcrTablesStoreTheFourBinlogKeys() throws Exception {
        for (String enable : Lists.newArrayList("false", "true")) {
            OlapTable table = createDupTable(db, "binlog_keys_" + enable, "'binlog.enable'='" + enable + "'");
            Assertions.assertFalse(table.hasChangelog());
            Map<String, String> properties = table.getTableProperty().getProperties();
            Assertions.assertEquals(Lists.newArrayList("binlog.enable", "binlog.max_bytes",
                    "binlog.max_history_nums", "binlog.ttl_seconds"), keysWithPrefix(properties, "binlog."));
            Assertions.assertTrue(keysWithPrefix(properties, "changelog.").isEmpty(), properties.toString());
            assertNoLegacyKeys(properties);
            Assertions.assertEquals(enable, properties.get("binlog.enable"));

            String ddl = getCreateTableStmt(table);
            Assertions.assertEquals(4, countOf(ddl, "\"binlog."), ddl);
            Assertions.assertTrue(ddl.contains("\"binlog.enable\" = \"" + enable + "\""), ddl);
            Assertions.assertFalse(ddl.contains("changelog."), ddl);
        }
    }

    @Test
    public void testShowCreateChangelogTable() throws Exception {
        OlapTable table = createDupTable(db, "show_changelog", "'changelog.enable'='true', 'changelog.ttl_seconds'='600'");
        String ddl = getCreateTableStmt(table);
        Assertions.assertTrue(ddl.contains("\"changelog.enable\" = \"true\""), ddl);
        Assertions.assertTrue(ddl.contains("\"changelog.ttl_seconds\" = \"600\""), ddl);
        Assertions.assertTrue(ddl.contains("\"changelog.need_historical_value\" = \"false\""), ddl);
        Assertions.assertTrue(ddl.contains("\"binlog.enable\" = \"false\""), ddl);
        Assertions.assertFalse(ddl.contains("binlog.format"), ddl);
        Assertions.assertFalse(ddl.contains(ChangelogConfig.CONFIG_VERSION), ddl);

        // The changelog section follows its own switch, the binlog section the CCR switch.
        Config.enable_feature_changelog = false;
        try {
            ddl = getCreateTableStmt(table);
            Assertions.assertFalse(ddl.contains("changelog."), ddl);
            Assertions.assertTrue(ddl.contains("\"binlog.enable\" = \"false\""), ddl);
        } finally {
            Config.enable_feature_changelog = true;
        }
        Config.enable_feature_binlog = false;
        try {
            ddl = getCreateTableStmt(table);
            Assertions.assertTrue(ddl.contains("\"changelog.enable\" = \"true\""), ddl);
            Assertions.assertFalse(ddl.contains("binlog."), ddl);
        } finally {
            Config.enable_feature_binlog = true;
        }
    }

    @Test
    public void testTableCanNotEnableBothBinlogAndChangelog() throws Exception {
        DdlException both = Assertions.assertThrows(DdlException.class, () -> createDupTable(db, "binlog_and_changelog",
                "'binlog.enable'='true', 'changelog.enable'='true'"));
        Assertions.assertTrue(
                both.getMessage().contains("Table binlog_and_changelog can not enable both binlog and changelog"),
                both.getMessage());
        Assertions.assertNull(db.getTableNullable("binlog_and_changelog"));

        createDatabaseWithSql("CREATE DATABASE split_ccr_db PROPERTIES ('binlog.enable'='true')");
        Database ccrDb = Env.getCurrentInternalCatalog().getDbOrDdlException("split_ccr_db");
        for (String properties : Lists.newArrayList("'changelog.enable'='true'",
                "'binlog.enable'='false', 'changelog.enable'='true'")) {
            DdlException inCcrDb = Assertions.assertThrows(DdlException.class,
                    () -> createDupTable(ccrDb, "changelog_in_ccr_db", properties));
            Assertions.assertTrue(inCcrDb.getMessage().contains("Can not create table changelog_in_ccr_db with "
                    + "changelog in database split_ccr_db because the database enables binlog"), inCcrDb.getMessage());
        }
        Assertions.assertNull(ccrDb.getTableNullable("changelog_in_ccr_db"));

        OlapTable changelog = createDupTable(db, "changelog_to_ccr", "'changelog.enable'='true'");
        DdlException turnOnCcr = Assertions.assertThrows(DdlException.class,
                () -> alterTableSync("ALTER TABLE split.changelog_to_ccr SET ('binlog.enable'='true')"));
        Assertions.assertTrue(turnOnCcr.getMessage().contains("Can not change binlog properties of table "
                + "changelog_to_ccr because the table enables changelog"), turnOnCcr.getMessage());
        // Repeating the current CCR setting changes nothing.
        alterTableSync("ALTER TABLE split.changelog_to_ccr SET ('binlog.enable'='false')");
        Assertions.assertTrue(changelog.hasChangelog());
        Assertions.assertFalse(changelog.getBinlogConfig().isEnableForCCR());

        OlapTable ccr = createDupTable(db, "ccr_to_changelog", "'binlog.enable'='true'");
        DdlException turnOnChangelog = Assertions.assertThrows(DdlException.class,
                () -> alterTableSync("ALTER TABLE split.ccr_to_changelog SET ('changelog.enable'='true')"));
        Assertions.assertTrue(turnOnChangelog.getMessage().contains(
                "not support change changelog.enable from false to true"), turnOnChangelog.getMessage());
        Assertions.assertTrue(ccr.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(ccr.hasChangelog());
    }

    @Test
    public void testDatabaseCanNotEnableBothBinlogAndChangelog() throws Exception {
        AnalysisException both = Assertions.assertThrows(AnalysisException.class, () -> createDatabaseWithSql(
                "CREATE DATABASE split_both_db PROPERTIES ('binlog.enable'='true', 'changelog.enable'='true')"));
        Assertions.assertTrue(both.getMessage().contains("Database split_both_db can not enable both binlog and "
                + "changelog"), both.getMessage());
        Assertions.assertNull(Env.getCurrentInternalCatalog().getDbNullable("split_both_db"));

        createDatabaseWithSql("CREATE DATABASE split_changelog_db PROPERTIES ('changelog.enable'='true')");
        IllegalStateException turnOnCcr = Assertions.assertThrows(IllegalStateException.class,
                () -> executeSql("ALTER DATABASE split_changelog_db SET PROPERTIES ('binlog.enable'='true')"));
        Assertions.assertTrue(turnOnCcr.getMessage().contains("Database split_changelog_db can not enable both "
                + "binlog and changelog"), turnOnCcr.getMessage());

        createDatabaseWithSql("CREATE DATABASE split_ccr_default_db PROPERTIES ('binlog.enable'='true')");
        IllegalStateException turnOnChangelog = Assertions.assertThrows(IllegalStateException.class,
                () -> executeSql("ALTER DATABASE split_ccr_default_db SET PROPERTIES ('changelog.enable'='true')"));
        Assertions.assertTrue(turnOnChangelog.getMessage().contains("Database split_ccr_default_db can not enable "
                + "both binlog and changelog"), turnOnChangelog.getMessage());

        createDatabase("split_holds_changelog_db");
        Database holder = Env.getCurrentInternalCatalog().getDbOrDdlException("split_holds_changelog_db");
        createDupTable(holder, "changelog_tbl", "'changelog.enable'='true'");
        IllegalStateException withChangelogTable = Assertions.assertThrows(IllegalStateException.class,
                () -> executeSql("ALTER DATABASE split_holds_changelog_db SET PROPERTIES ('binlog.enable'='true')"));
        Assertions.assertTrue(withChangelogTable.getMessage().contains("Can not enable binlog in database "
                + "split_holds_changelog_db because table changelog_tbl enables changelog"),
                withChangelogTable.getMessage());
        Assertions.assertFalse(holder.getBinlogConfig().isEnableForCCR());
    }

    @Test
    public void testForcedBinlogSkipsDatabaseThatSetsChangelog() {
        boolean original = Config.force_enable_feature_binlog;
        Config.force_enable_feature_binlog = true;
        try {
            Map<String, String> changelog = new HashMap<>();
            changelog.put("changelog.enable", "true");
            Assertions.assertFalse(new CreateDatabaseCommand(false, new DbName("", "forced_changelog"), changelog)
                    .getProperties().containsKey("binlog.enable"));
            Assertions.assertEquals("true", new CreateDatabaseCommand(false, new DbName("", "forced_plain"),
                    new HashMap<>()).getProperties().get("binlog.enable"));
        } finally {
            Config.force_enable_feature_binlog = original;
        }
    }

    @Test
    public void testAlterChangelogProperties() throws Exception {
        createTable("CREATE TABLE split.alter_mow (k INT, v INT) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'enable_unique_key_merge_on_write'='true', "
                + "'changelog.enable'='true', 'changelog.need_historical_value'='true')");
        OlapTable table = (OlapTable) db.getTableOrDdlException("alter_mow");
        Assertions.assertEquals(new ChangelogConfig(true, ChangelogConfig.TTL_SECONDS, true), table.getChangelogConfig());

        // Setting the current value is accepted.
        alterTableSync("ALTER TABLE split.alter_mow SET ('changelog.enable'='true')");
        Assertions.assertTrue(table.hasChangelog());
        assertAlterFails("ALTER TABLE split.alter_mow SET ('changelog.enable'='false')",
                "not support change changelog.enable from true to false");
        assertAlterFails("ALTER TABLE split.alter_mow SET ('changelog.need_historical_value'='false')",
                "not support change changelog.need_historical_value from true to false");
        assertAlterFails("ALTER TABLE split.alter_mow SET ('changelog.ttl_seconds'='0')",
                "changelog.ttl_seconds must be greater than 0");

        long version = table.getChangelogConfig().getConfigVersion();
        alterTableSync("ALTER TABLE split.alter_mow SET ('changelog.ttl_seconds'='7200')");
        Assertions.assertEquals(7200, table.getChangelogConfig().getTtlSeconds());
        Assertions.assertTrue(table.getChangelogConfig().getConfigVersion() > version);
        Assertions.assertEquals(String.valueOf(table.getChangelogConfig().getConfigVersion()),
                table.getTableProperty().getProperties().get(ChangelogConfig.CONFIG_VERSION));
        Assertions.assertEquals(7200, table.getTabletBinlogConfig().toThrift().getTtlSeconds());
        // Several changelog properties can be set together.
        alterTableSync("ALTER TABLE split.alter_mow SET ('changelog.enable'='true', 'changelog.ttl_seconds'='3600')");
        Assertions.assertEquals(3600, table.getChangelogConfig().getTtlSeconds());
        Assertions.assertTrue(table.getChangelogConfig().getNeedHistoricalValue());
        assertAlterFails("ALTER TABLE split.alter_mow SET ('changelog.ttl_second'='60')",
                "Unknown changelog property: changelog.ttl_second");

        Config.enable_feature_changelog = false;
        try {
            assertAlterFails("ALTER TABLE split.alter_mow SET ('changelog.ttl_seconds'='60')",
                    "Changelog feature is not enabled");
        } finally {
            Config.enable_feature_changelog = true;
        }
        Assertions.assertEquals(3600, table.getChangelogConfig().getTtlSeconds());

        OlapTable plain = createDupTable(db, "alter_plain", "");
        alterTableSync("ALTER TABLE split.alter_plain SET ('changelog.enable'='false')");
        alterTableSync("ALTER TABLE split.alter_plain SET ('changelog.need_historical_value'='false')");
        assertAlterFails("ALTER TABLE split.alter_plain SET ('changelog.enable'='true')",
                "not support change changelog.enable from false to true");
        assertAlterFails("ALTER TABLE split.alter_plain SET ('changelog.ttl_seconds'='60')",
                "changelog.ttl_seconds can not be set because changelog is not enabled for table: alter_plain");
        Assertions.assertFalse(plain.hasChangelog());
        Assertions.assertTrue(keysWithPrefix(plain.getTableProperty().getProperties(), "changelog.").isEmpty());
    }

    @Test
    public void testCreateTableValidatesChangelogProperties() {
        String dup = "(k INT, v INT) DUPLICATE KEY(k)";
        assertCreateFails("plain_with_ttl", dup, "'changelog.ttl_seconds'='10'",
                "Property changelog.ttl_seconds requires \"changelog.enable\" = \"true\"");
        assertCreateFails("plain_with_history", dup, "'changelog.need_historical_value'='false'",
                "Property changelog.need_historical_value requires \"changelog.enable\" = \"true\"");
        assertCreateFails("dup_with_history", dup, "'changelog.enable'='true', 'changelog.need_historical_value'='true'",
                "Duplicate table model don't support record historical value");
        assertCreateFails("agg_changelog", "(k INT, v INT SUM) AGGREGATE KEY(k)", "'changelog.enable'='true'",
                "Only duplicate and mow table model support changelog");
        assertCreateFails("invalid_enable", dup, "'changelog.enable'='yes'", "Invalid changelog.enable value: yes");
        assertCreateFails("invalid_ttl", dup, "'changelog.enable'='true', 'changelog.ttl_seconds'='x'",
                "Invalid changelog.ttl_seconds value: x");
        assertCreateFails("unknown_key", dup, "'changelog.enable'='true', 'changelog.format'='ROW'",
                "Unknown changelog property: changelog.format");
    }

    @Test
    public void testDatabaseChangelogDefaults() throws Exception {
        createDatabaseWithSql("CREATE DATABASE split_default_db "
                + "PROPERTIES ('changelog.enable'='true', 'changelog.ttl_seconds'='120')");
        Database defaults = Env.getCurrentInternalCatalog().getDbOrDdlException("split_default_db");
        Assertions.assertTrue(defaults.getChangelogConfig().isEnable());
        Assertions.assertFalse(defaults.getBinlogConfig().isEnableForCCR());

        OlapTable inherited = createDupTable(defaults, "inherited", "");
        Assertions.assertEquals(new ChangelogConfig(true, 120, false), inherited.getChangelogConfig());
        OlapTable overridden = createDupTable(defaults, "overridden", "'changelog.ttl_seconds'='60'");
        Assertions.assertEquals(60, overridden.getChangelogConfig().getTtlSeconds());
        OlapTable optedOut = createDupTable(defaults, "opted_out", "'changelog.enable'='false'");
        Assertions.assertFalse(optedOut.hasChangelog());

        executeSql("ALTER DATABASE split_default_db SET PROPERTIES ('changelog.enable'='false')");
        Assertions.assertFalse(defaults.getChangelogConfig().isEnable());
        Assertions.assertFalse(createDupTable(defaults, "after_alter", "")
                .hasChangelog());

        AnalysisException ttl = Assertions.assertThrows(AnalysisException.class, () -> createDatabaseWithSql(
                "CREATE DATABASE split_ttl_db PROPERTIES ('changelog.enable'='true', 'changelog.ttl_seconds'='0')"));
        Assertions.assertTrue(ttl.getMessage().contains("changelog.ttl_seconds must be greater than 0"),
                ttl.getMessage());
    }

    @Test
    public void testTemporaryTableNeverHasChangelog() throws Exception {
        IllegalStateException explicit = Assertions.assertThrows(IllegalStateException.class,
                () -> executeSql("CREATE TEMPORARY TABLE split.temp_changelog (k INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', 'changelog.enable'='true')"));
        Assertions.assertTrue(explicit.getMessage().contains("Cannot create temporary table with changelog enable"),
                explicit.getMessage());
        Assertions.assertTrue(temporaryTables(db, "temp_changelog").isEmpty());

        // The changelog default of the database is ignored, also while the feature is off.
        createDatabaseWithSql("CREATE DATABASE split_temp_db PROPERTIES ('changelog.enable'='true')");
        Database tempDb = Env.getCurrentInternalCatalog().getDbOrDdlException("split_temp_db");
        for (boolean changelogFeature : new boolean[] {true, false}) {
            String name = "temp_inherited_" + changelogFeature;
            Config.enable_feature_changelog = changelogFeature;
            try {
                executeSql("CREATE TEMPORARY TABLE split_temp_db." + name + " (k INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
            } finally {
                Config.enable_feature_changelog = true;
            }
            List<OlapTable> temporaryTables = temporaryTables(tempDb, name);
            Assertions.assertEquals(1, temporaryTables.size());
            Assertions.assertFalse(temporaryTables.get(0).hasChangelog());
            Assertions.assertFalse(temporaryTables.get(0).enableTso());
            Assertions.assertTrue(keysWithPrefix(temporaryTables.get(0).getTableProperty().getProperties(),
                    "changelog.").isEmpty());
        }
    }

    @Test
    public void testLikeKeepsTablesOutOfTheChangelogDefault() throws Exception {
        createDatabaseWithSql("CREATE DATABASE split_like_db PROPERTIES ('changelog.enable'='true')");
        Database likeDb = Env.getCurrentInternalCatalog().getDbOrDdlException("split_like_db");
        createTable("CREATE TABLE split_like_db.dup_plain (k INT, v INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1', 'changelog.enable'='false')");
        createTable("CREATE TABLE split_like_db.agg_plain (k INT, v INT SUM) AGGREGATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1', 'changelog.enable'='false')");
        createTable("CREATE TABLE split_like_db.ccr_plain (k INT, v INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'changelog.enable'='false', 'binlog.enable'='true')");
        for (String name : Lists.newArrayList("dup_plain", "agg_plain", "ccr_plain")) {
            OlapTable source = (OlapTable) likeDb.getTableOrDdlException(name);
            Assertions.assertFalse(source.hasChangelog());
            // The opt-out is only printed: the table does not store it.
            Assertions.assertTrue(keysWithPrefix(source.getTableProperty().getProperties(), "changelog.").isEmpty());
            String ddl = getCreateTableStmt(source);
            Assertions.assertEquals(1, countOf(ddl, "changelog."), ddl);
            Assertions.assertTrue(ddl.contains("\"changelog.enable\" = \"false\""), ddl);

            executeSql("CREATE TABLE split_like_db." + name + "_copy LIKE split_like_db." + name);
            OlapTable copy = (OlapTable) likeDb.getTableOrDdlException(name + "_copy");
            Assertions.assertFalse(copy.hasChangelog());
            Assertions.assertEquals(source.getBinlogConfig().isEnableForCCR(), copy.getBinlogConfig().isEnableForCCR());
        }
        Assertions.assertTrue(((OlapTable) likeDb.getTableOrDdlException("ccr_plain_copy")).getBinlogConfig()
                .isEnableForCCR());

        // A table with changelog in it keeps its changelog, and one in a database without the default prints no
        // changelog key.
        OlapTable inherited = createDupTable(likeDb, "inherited", "");
        Assertions.assertTrue(inherited.hasChangelog());
        executeSql("CREATE TABLE split_like_db.inherited_copy LIKE split_like_db.inherited");
        Assertions.assertTrue(((OlapTable) likeDb.getTableOrDdlException("inherited_copy")).hasChangelog());
        String ddl = getCreateTableStmt(createDupTable(db, "like_without_default", ""));
        Assertions.assertFalse(ddl.contains("changelog."), ddl);
    }

    @Test
    public void testRecoverChangelogTableIntoBinlogDatabase() throws Exception {
        createDatabase("split_recover_db");
        Database recoverDb = Env.getCurrentInternalCatalog().getDbOrDdlException("split_recover_db");
        createDupTable(recoverDb, "recovered", "'changelog.enable'='true'");
        dropTable("split_recover_db.recovered", true);
        // Without live tables the database can turn binlog on.
        executeSql("ALTER DATABASE split_recover_db SET PROPERTIES ('binlog.enable'='true')");
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> recoverTable("RECOVER TABLE split_recover_db.recovered"));
        Assertions.assertTrue(exception.getMessage().contains("Can not recover table recovered with changelog in "
                + "database split_recover_db because the database enables binlog"), exception.getMessage());
        Assertions.assertNull(recoverDb.getTableNullable("recovered"));

        executeSql("ALTER DATABASE split_recover_db SET PROPERTIES ('binlog.enable'='false')");
        recoverTable("RECOVER TABLE split_recover_db.recovered");
        Assertions.assertTrue(((OlapTable) recoverDb.getTableOrDdlException("recovered")).hasChangelog());
    }

    private static List<OlapTable> temporaryTables(Database database, String name) {
        return database.getTables().stream()
                .filter(table -> table.isTemporary() && table.getName().contains(name))
                .map(table -> (OlapTable) table).collect(Collectors.toList());
    }

    private OlapTable createDupTable(Database database, String name, String properties) throws Exception {
        createTable("CREATE TABLE " + database.getFullName() + "." + name + " (k INT, v INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1'"
                + (properties.isEmpty() ? "" : ", " + properties) + ")");
        return (OlapTable) database.getTableOrDdlException(name);
    }

    private void assertCreateFails(String name, String columnsAndKeys, String properties, String message) {
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> createTable(
                "CREATE TABLE split." + name + " " + columnsAndKeys + " DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', " + properties + ")"));
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
        Assertions.assertNull(db.getTableNullable(name));
    }

    private void assertAlterFails(String sql, String message) {
        UserException exception = Assertions.assertThrows(UserException.class, () -> alterTableSync(sql));
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }

    private static void assertNoLegacyKeys(Map<String, String> properties) {
        Assertions.assertFalse(properties.containsKey("binlog.format"), properties.toString());
        Assertions.assertFalse(properties.containsKey("binlog.need_historical_value"), properties.toString());
        Assertions.assertFalse(properties.containsKey("__binlog_config_version"), properties.toString());
    }

    private static List<String> keysWithPrefix(Map<String, String> properties, String prefix) {
        return properties.keySet().stream().filter(key -> key.startsWith(prefix)).sorted()
                .collect(Collectors.toList());
    }

    private static int countOf(String text, String part) {
        return text.split(java.util.regex.Pattern.quote(part), -1).length - 1;
    }

    private static String getCreateTableStmt(OlapTable table) {
        List<String> createTableStmt = Lists.newArrayList();
        Env.getDdlStmt(table, createTableStmt, null, null, false, true, -1L);
        return createTableStmt.get(0);
    }
}

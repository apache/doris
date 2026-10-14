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

import org.apache.doris.catalog.BinlogConfig.BinlogFormat;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class BinlogFormatCaseInsensitiveTest extends TestWithFeService {
    private Database db;

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_feature_binlog = true;
        createDatabase("binlog_format_case");
        db = Env.getCurrentInternalCatalog().getDbOrDdlException("binlog_format_case");
    }

    @Test
    public void testFromStringIgnoresCase() {
        Assertions.assertEquals(BinlogFormat.ROW, BinlogFormat.fromString("row"));
        Assertions.assertEquals(BinlogFormat.ROW, BinlogFormat.fromString("Row"));
        Assertions.assertEquals(BinlogFormat.STATEMENT_AND_SNAPSHOT,
                BinlogFormat.fromString("statement_and_snapshot"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> BinlogFormat.fromString("rows"));
    }

    @Test
    public void testCreateRowTableWithLowerCaseFormat() throws Exception {
        createTable("CREATE TABLE binlog_format_case.lower_row (k INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'binlog.enable'='true', 'binlog.format'='row')");
        OlapTable table = (OlapTable) db.getTableOrDdlException("lower_row");
        Assertions.assertTrue(table.getBinlogConfig().isEnableForStreaming());
        Assertions.assertTrue(table.enableTso());
        Assertions.assertEquals("ROW",
                table.getTableProperty().getProperties().get(PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT));

        // The format is still immutable, whatever the case of the new value.
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> alterTableSync(
                "ALTER TABLE binlog_format_case.lower_row SET ('binlog.format'='statement_and_snapshot')"));
        Assertions.assertTrue(exception.getMessage().contains(
                "not support change binlog format from ROW to STATEMENT_AND_SNAPSHOT"), exception.getMessage());
        Assertions.assertTrue(table.getBinlogConfig().isEnableForStreaming());
    }

    @Test
    public void testAlterStatementTableWithLowerCaseFormat() throws Exception {
        createTable("CREATE TABLE binlog_format_case.lower_statement (k INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'binlog.enable'='true', "
                + "'binlog.format'='Statement_And_Snapshot')");
        OlapTable table = (OlapTable) db.getTableOrDdlException("lower_statement");
        Assertions.assertTrue(table.getBinlogConfig().isEnableForCCR());
        Assertions.assertEquals("STATEMENT_AND_SNAPSHOT",
                table.getTableProperty().getProperties().get(PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT));

        // Repeating the current format in another case is a no-op.
        alterTableSync("ALTER TABLE binlog_format_case.lower_statement "
                + "SET ('binlog.format'='statement_and_snapshot')");
        Assertions.assertTrue(table.getBinlogConfig().isEnableForCCR());

        DdlException exception = Assertions.assertThrows(DdlException.class, () -> alterTableSync(
                "ALTER TABLE binlog_format_case.lower_statement SET ('binlog.format'='row')"));
        Assertions.assertTrue(exception.getMessage().contains(
                "not support change binlog format from STATEMENT_AND_SNAPSHOT to ROW"), exception.getMessage());
        Assertions.assertTrue(table.getBinlogConfig().isEnableForCCR());
    }

    @Test
    public void testRejectUnknownTableFormat() {
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> executeSql("CREATE TABLE binlog_format_case.unknown_format (k INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', 'binlog.enable'='true', 'binlog.format'='rows')"));
        Assertions.assertTrue(exception.getMessage().contains("Invalid binlog format value: rows"),
                exception.getMessage());
        Assertions.assertNull(db.getTableNullable("unknown_format"));
    }

    @Test
    public void testDatabaseFormatIsStoredByCanonicalName() throws Exception {
        createDatabaseWithSql("CREATE DATABASE binlog_format_case_db "
                + "PROPERTIES ('binlog.enable'='true', 'binlog.format'='row')");
        Database rowDb = Env.getCurrentInternalCatalog().getDbOrDdlException("binlog_format_case_db");
        // Database properties are persisted as given and parsed again on replay.
        Assertions.assertEquals("ROW", rowDb.getDbProperties().get(PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT));
        Assertions.assertTrue(rowDb.getBinlogConfig().isEnableForStreaming());
        Assertions.assertTrue(rowDb.getDbProperties().getBinlogConfig().isEnableForStreaming());

        // A table created in the database inherits the ROW format.
        createTable("CREATE TABLE binlog_format_case_db.inherited_row (k INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) rowDb.getTableOrDdlException("inherited_row");
        Assertions.assertTrue(table.getBinlogConfig().isEnableForStreaming());

        executeSql("ALTER DATABASE binlog_format_case_db SET PROPERTIES "
                + "('binlog.enable'='false', 'binlog.format'='statement_and_snapshot')");
        Assertions.assertEquals("STATEMENT_AND_SNAPSHOT",
                rowDb.getDbProperties().get(PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT));
        Assertions.assertEquals(BinlogFormat.STATEMENT_AND_SNAPSHOT, rowDb.getBinlogConfig().getBinlogFormat());
    }

    @Test
    public void testRejectUnknownDatabaseFormat() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> createDatabaseWithSql("CREATE DATABASE binlog_format_case_unknown "
                        + "PROPERTIES ('binlog.enable'='true', 'binlog.format'='rows')"));
        Assertions.assertTrue(exception.getMessage().contains("Invalid binlog format value: rows"),
                exception.getMessage());
        Assertions.assertNull(Env.getCurrentInternalCatalog().getDbNullable("binlog_format_case_unknown"));
    }

    @Test
    public void testReplayParsesNonCanonicalFormat() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, "true");
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT, "row");
        Assertions.assertTrue(new TableProperty(properties).buildBinlogConfig().getBinlogConfig()
                .isEnableForStreaming());
        Assertions.assertTrue(new DatabaseProperty(properties).getBinlogConfig().isEnableForStreaming());
    }
}

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

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

public class RowBinlogFeatureGateTest extends TestWithFeService {
    private boolean originalEnableFeatureBinlog;
    private Database db;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("row_binlog_gate");
        db = Env.getCurrentInternalCatalog().getDbOrDdlException("row_binlog_gate");
    }

    @BeforeEach
    public void disableRowBinlog() {
        originalEnableFeatureBinlog = Config.enable_feature_binlog;
        Config.enable_feature_binlog = false;
    }

    @AfterEach
    public void restoreRowBinlog() {
        Config.enable_feature_binlog = originalEnableFeatureBinlog;
    }

    @Test
    public void testRejectExplicitRowBinlog() {
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> createBinlogTable(db, "explicit_row", "'binlog.enable'='true', 'binlog.format'='ROW'"));
        Assertions.assertTrue(exception.getMessage().contains("enable_feature_binlog"));
        Assertions.assertNull(db.getTableNullable("explicit_row"));
    }

    @Test
    public void testRejectRowBinlogCtas() {
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> executeSql("CREATE TABLE row_binlog_gate.ctas_row "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', 'binlog.enable'='true', 'binlog.format'='ROW') "
                        + "AS SELECT 1 AS k"));
        Assertions.assertTrue(exception.getMessage().contains("enable_feature_binlog"));
        Assertions.assertNull(db.getTableNullable("ctas_row"));
    }

    @Test
    public void testRejectInheritedRowBinlog() throws Exception {
        createDatabase("row_binlog_gate_inherited");
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("row_binlog_gate_inherited");
        // Simulate database defaults restored from an image written while ROW binlog was enabled.
        db.replayUpdateDbProperties(Map.of("binlog.enable", "true", "binlog.format", "ROW"));
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> createTable("CREATE TABLE row_binlog_gate_inherited.inherited_row (k INT) "
                        + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')"));
        Assertions.assertTrue(exception.getMessage().contains("enable_feature_binlog"));
        Assertions.assertNull(db.getTableNullable("inherited_row"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDisabledRowFormatCannotBeEnabledDynamically(boolean rowFeature) throws Exception {
        String tableName = "disabled_row_" + rowFeature;
        OlapTable table = createBinlogTable(db, tableName,
                "'binlog.enable'='false', 'binlog.format'='ROW', 'binlog.ttl_seconds'='123'");
        Assertions.assertFalse(table.enableTso());
        Assertions.assertEquals(123, table.getBinlogConfig().getTtlSeconds());
        Config.enable_feature_binlog = rowFeature;
        Assertions.assertThrows(DdlException.class, () -> alterTableSync(
                "ALTER TABLE row_binlog_gate." + tableName + " SET ('binlog.enable'='true')"));
        Assertions.assertFalse(table.enableTso());
        Assertions.assertFalse(table.getBinlogConfig().getEnable());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCcrCannotSwitchToRowFormat(boolean rowFeature) throws Exception {
        Config.enable_feature_binlog = rowFeature;
        String tableName = "ccr_format_" + rowFeature;
        OlapTable table = createBinlogTable(db, tableName, "'binlog.enable'='true'");
        Assertions.assertThrows(DdlException.class, () -> alterTableSync(
                "ALTER TABLE row_binlog_gate." + tableName + " SET ('binlog.format'='ROW')"));
        Assertions.assertTrue(table.getBinlogConfig().isEnableForCCR());
        Assertions.assertFalse(table.enableTso());
    }

    private OlapTable createBinlogTable(Database db, String name, String properties) throws Exception {
        createTable("CREATE TABLE " + db.getFullName() + "." + name + " (k INT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', " + properties + ")");
        return (OlapTable) db.getTableOrDdlException(name);
    }
}

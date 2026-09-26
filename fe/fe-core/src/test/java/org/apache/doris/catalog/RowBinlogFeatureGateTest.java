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

import org.apache.doris.binlog.BinlogManager;
import org.apache.doris.binlog.DropTableRecord;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateTableLikeCommand;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RowBinlogFeatureGateTest extends TestWithFeService {
    private boolean originalEnableFeatureBinlog;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("row_binlog_gate");
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
                () -> createTable("CREATE TABLE row_binlog_gate.explicit_row (k INT) "
                        + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', 'binlog.enable'='true', 'binlog.format'='ROW')"));
        Assertions.assertTrue(exception.getMessage().contains("enable_feature_binlog"));
        Assertions.assertNull(Env.getCurrentInternalCatalog().getDbNullable("row_binlog_gate")
                .getTableNullable("explicit_row"));
    }

    @Test
    public void testRejectRowBinlogCtas() {
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> executeSql("CREATE TABLE row_binlog_gate.ctas_row "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES('replication_num'='1', 'binlog.enable'='true', 'binlog.format'='ROW') "
                        + "AS SELECT 1 AS k"));
        Assertions.assertTrue(exception.getMessage().contains("enable_feature_binlog"));
        Assertions.assertNull(Env.getCurrentInternalCatalog().getDbNullable("row_binlog_gate")
                .getTableNullable("ctas_row"));
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

    @Test
    public void testDisabledRowFormatAndCommonPropertiesRemainAllowed() throws Exception {
        createTable("CREATE TABLE row_binlog_gate.disabled_row (k INT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'binlog.enable'='false', 'binlog.format'='ROW', "
                + "'binlog.ttl_seconds'='123')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbOrDdlException("row_binlog_gate")
                .getTableOrDdlException("disabled_row");
        Assertions.assertFalse(table.enableTso());
        Assertions.assertEquals(123, table.getBinlogConfig().getTtlSeconds());
    }

    @Test
    public void testCcrCreateAndAlterRemainAllowed() throws Exception {
        createTable("CREATE TABLE row_binlog_gate.ccr_table (k INT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'binlog.enable'='true')");
        Assertions.assertDoesNotThrow(
                () -> alterTableSync("ALTER TABLE row_binlog_gate.ccr_table SET ('binlog.ttl_seconds'='123')"));
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbOrDdlException("row_binlog_gate")
                .getTableOrDdlException("ccr_table");
        Assertions.assertTrue(table.getBinlogConfig().isEnableForCCR());
        Assertions.assertEquals(123, table.getBinlogConfig().getTtlSeconds());
    }

    @Test
    public void testCcrCheckpointSurvivesDisabledRowFeature() throws Exception {
        createDatabase("row_binlog_checkpoint");
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("row_binlog_checkpoint");
        db.replayUpdateDbProperties(Map.of("binlog.enable", "true"));
        createTable("CREATE TABLE row_binlog_checkpoint.checkpoint_ccr (k INT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'binlog.enable'='true')");
        Table table = db.getTableOrDdlException("checkpoint_ccr");
        Env env = Env.getCurrentEnv();
        env.getBinlogManager().addDropTableRecord(new DropTableRecord(9999999,
                new DropInfo(db.getId(), table.getId(), table.getName(), false, false, 0)));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (CountingDataOutputStream out = new CountingDataOutputStream(bytes, 0)) {
            env.saveBinlogs(out, 0);
        }
        Assertions.assertTrue(bytes.size() > 0);
        BinlogManager restored = new BinlogManager();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored.read(in, 0);
        }
        // DROP_TABLE belongs to the database stream, not to the table stream.
        var result = restored.getBinlog(db.getId(), -1, 9999998);
        Assertions.assertEquals(TStatusCode.OK, result.first.getStatusCode());
        Assertions.assertEquals(9999999, result.second.getCommitSeq());
    }

    @Test
    public void testCcrDroppedTableMetadataSurvivesDisabledRowFeature() throws Exception {
        createTable("CREATE TABLE row_binlog_gate.dropped_ccr (k INT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'binlog.enable'='true')");
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("row_binlog_gate");
        Table table = db.getTableOrDdlException("dropped_ccr");
        Env.getCurrentEnv().getBinlogManager().addDropTableRecord(new DropTableRecord(9999999,
                new DropInfo(db.getId(), table.getId(), table.getName(), false, false, 0)));
        var metadata = Env.getMeta(db, List.of()).getDbMeta();
        Assertions.assertNotNull(metadata.getDroppedTableMap());
        Assertions.assertEquals(9999999L, metadata.getDroppedTableMap().get(table.getId()));
    }

    @Test
    public void testShowAndLikePreserveRowConfigWhenFeatureDisabled() throws Exception {
        Config.enable_feature_binlog = true;
        createTable("CREATE TABLE row_binlog_gate.source_row (k INT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'binlog.enable'='true', 'binlog.format'='ROW')");
        Config.enable_feature_binlog = false;
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("row_binlog_gate");
        OlapTable table = (OlapTable) db.getTableOrDdlException("source_row");
        List<String> ddl = new ArrayList<>();
        Env.getDdlStmt(table, ddl, null, null, false, false, -1);
        Assertions.assertTrue(ddl.get(0).contains("\"binlog.enable\" = \"true\""));
        Assertions.assertTrue(ddl.get(0).contains("\"binlog.format\" = \"ROW\""));

        String sql = "CREATE TABLE row_binlog_gate.copied_row LIKE row_binlog_gate.source_row";
        CreateTableLikeCommand command = (CreateTableLikeCommand) new NereidsParser().parseSingle(sql);
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> command.run(connectContext, new StmtExecutor(connectContext, sql)));
        Assertions.assertTrue(exception.getMessage().contains("enable_feature_binlog"));
        Assertions.assertNull(db.getTableNullable("copied_row"));

        long originalTtl = table.getBinlogConfig().getTtlSeconds();
        Assertions.assertThrows(DdlException.class,
                () -> alterTableSync("ALTER TABLE row_binlog_gate.source_row SET ('binlog.ttl_seconds'='123')"));
        Assertions.assertEquals(originalTtl, table.getBinlogConfig().getTtlSeconds());
        Assertions.assertDoesNotThrow(() -> alterTableSync("ALTER TABLE row_binlog_gate.source_row "
                + "SET ('binlog.ttl_seconds'='" + originalTtl + "')"));
    }
}

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

import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.DropStreamCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TRow;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class DropTableStreamTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;

        createDatabase("test_stream");
        String createTableStr1 = "create table if not exists test_stream.tbl1\n" + "(k1 int, k2 int)\n" + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW', "
                + "'binlog.need_historical_value' = 'true'); ";
        createTable(createTableStr1);

        String createStreamStr1 =  "create stream test_stream.s1 on table test_stream.tbl1\n"
                + "properties('show_initial_rows' = 'true'); ";
        createTable(createStreamStr1);
        String createStreamStr2 =  "create stream test_stream.s2 on table test_stream.tbl1\n"
                + "properties('type' = 'append_only', 'show_initial_rows' = 'true'); ";
        createTable(createStreamStr2);
    }

    private void dropStream(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof DropStreamCommand) {
            ((DropStreamCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private void createBaseTableAndStream(String tableName, String streamName) throws Exception {
        createTable("create table test_stream." + tableName + " (k1 int, k2 int) "
                + "unique key(k1) distributed by hash(k1) buckets 1 "
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW', "
                + "'binlog.need_historical_value' = 'true')");
        createTable("create stream test_stream." + streamName + " on table test_stream." + tableName
                + " properties('show_initial_rows' = 'true')");
    }

    @Test
    public void testNormalDropStream() throws Exception {
        // test drop
        ExceptionChecker
                .expectThrowsNoException(() ->
                        dropStream("drop stream test_stream.s1;"));
        // test force drop
        ExceptionChecker
                .expectThrowsNoException(() ->
                        dropStream("drop stream test_stream.s2 force;"));

        // test if exist
        ExceptionChecker
                .expectThrowsNoException(() ->
                        dropStream("drop stream if exists test_stream.s3;"));
    }

    @Test
    public void testAbnormalDropStream() throws Exception {
        // test not exist
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown table 's3' in test_stream",
                () -> dropStream("drop stream test_stream.s3;"));
    }

    @Test
    public void testCloudDropRequiresForce() {
        String previousCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "cloud_table_stream_ut";
        try {
            Exception exception = Assertions.assertThrows(Exception.class,
                    () -> dropStream("drop stream test_stream.not_reached;"));
            Assertions.assertTrue(exception.getMessage().contains("only supports DROP STREAM ... FORCE"));
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
        }
    }

    @Test
    public void testStreamStateFollowsRecoverableBaseTableDrop() throws Exception {
        createBaseTableAndStream("tbl_recover", "s_recover");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_recover");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s_recover");

        dropTableWithSql("drop table test_stream.tbl_recover");

        Assertions.assertTrue(baseTable.isDropped);
        Assertions.assertNull(stream.getBaseTableNullable());
        Assertions.assertTrue(stream.isDisabled());
        Assertions.assertTrue(stream.isStale());
        Assertions.assertEquals("Base table does not exist", stream.getStaleReason());
        Assertions.assertTrue(Env.getCurrentEnv().getTableStreamManager().getTableStreamIds(db)
                .contains(stream.getId()));

        List<TRow> rows = new ArrayList<>();
        Env.getCurrentEnv().getTableStreamManager().fillTableStreamValuesMetadataResult(rows);
        TRow streamRow = rows.stream()
                .filter(row -> "s_recover".equals(row.getColumnValue().get(1).getStringVal()))
                .findFirst()
                .orElseThrow(AssertionError::new);
        List<String> baseTableQualifiers = stream.getBaseTableFullQualifiers();
        Assertions.assertEquals(baseTableQualifiers.get(2), streamRow.getColumnValue().get(6).getStringVal());
        Assertions.assertEquals(baseTableQualifiers.get(1), streamRow.getColumnValue().get(7).getStringVal());
        Assertions.assertEquals(baseTableQualifiers.get(0), streamRow.getColumnValue().get(8).getStringVal());
        Assertions.assertEquals("N/A", streamRow.getColumnValue().get(9).getStringVal());
        Assertions.assertFalse(streamRow.getColumnValue().get(10).isBoolVal());
        Assertions.assertTrue(streamRow.getColumnValue().get(11).isBoolVal());
        Assertions.assertEquals("Base table does not exist",
                streamRow.getColumnValue().get(12).getStringVal());

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Unknown base table 'tbl_recover'",
                stream::getBaseTableOrNereidsAnalysisException);
        ExceptionChecker.expectThrowsWithMsg(IllegalStateException.class, "Table [tbl_recover] does not exist",
                () -> executeSql("select * from test_stream.s_recover"));

        recoverTable("recover table test_stream.tbl_recover");

        Assertions.assertSame(baseTable, stream.getBaseTableNullable());
        Assertions.assertEquals(baseTable.getId(), stream.getBaseTableNullable().getId());
        Assertions.assertFalse(stream.isDisabled());
        Assertions.assertFalse(stream.isStale());
        Assertions.assertEquals("N/A", stream.getStaleReason());
    }

    @Test
    public void testForceDropAndSameNameTableDoNotRestoreStream() throws Exception {
        createBaseTableAndStream("tbl_force", "s_force");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        long oldBaseTableId = db.getTableOrMetaException("tbl_force").getId();
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s_force");
        OlapTableStream deserializedStream = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(stream), OlapTableStream.class);

        dropTableWithSql("drop table test_stream.tbl_force force");

        Assertions.assertNull(stream.getBaseTableNullable());
        Assertions.assertNull(deserializedStream.getBaseTableNullable());
        Assertions.assertTrue(deserializedStream.isDisabled());
        Assertions.assertTrue(deserializedStream.isStale());
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown table 'tbl_force' or table id '-1' in test_stream",
                () -> recoverTable("recover table test_stream.tbl_force"));

        createTable("create table test_stream.tbl_force (k1 int, k2 int) "
                + "unique key(k1) distributed by hash(k1) buckets 1 "
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW', "
                + "'binlog.need_historical_value' = 'true')");

        Assertions.assertNotEquals(oldBaseTableId, db.getTableOrMetaException("tbl_force").getId());
        Assertions.assertNull(stream.getBaseTableNullable());
        Assertions.assertTrue(stream.isDisabled());
        Assertions.assertTrue(stream.isStale());
        Assertions.assertEquals("Base table does not exist", stream.getStaleReason());
        Assertions.assertTrue(Env.getCurrentEnv().getTableStreamManager().getTableStreamIds(db)
                .contains(stream.getId()));
    }

    @Override
    protected void runAfterAll() throws Exception {
        dropDatabase("test_stream");
    }
}

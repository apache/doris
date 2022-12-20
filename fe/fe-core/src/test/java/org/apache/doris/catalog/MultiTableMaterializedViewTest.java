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

import org.apache.doris.analysis.CreateMultiTableMaterializedViewStmt;
import org.apache.doris.analysis.DropMaterializedViewStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.MVRefreshInfo;
import org.apache.doris.analysis.MVRefreshIntervalTriggerInfo;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.DataInputBuffer;
import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

public class MultiTableMaterializedViewTest extends TestWithFeService {

    @BeforeEach
    protected void setUp() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        connectContext.getState().reset();
    }

    @AfterEach
    public void tearDown() {
        Env.getCurrentEnv().clear();
    }

    @Test
    public void testSerialization() throws Exception {
        createTable("create table test.t1 (pk int, v1 int sum) aggregate key (pk) "
                + "distributed by hash (pk) buckets 1 properties ('replication_num' = '1');");
        createTable("create table test.t2 (pk int, v2 int sum) aggregate key (pk) "
                + "distributed by hash (pk) buckets 1 properties ('replication_num' = '1');");

        String sql = "create materialized view mv build immediate refresh complete "
                + "key (mpk) distributed by hash (mpk) "
                + "as select test.t1.pk as mpk from test.t1, test.t2 where test.t1.pk = test.t2.pk";
        testSerialization(sql);

        sql = "create materialized view mv1 build immediate refresh complete start with '1:00' next 1 day "
                + "key (mpk) distributed by hash (mpk) "
                + "as select test.t1.pk as mpk from test.t1, test.t2 where test.t1.pk = test.t2.pk";
        testSerialization(sql);
    }

    private void testSerialization(String sql) throws UserException, IOException {
        MaterializedView mv = createMaterializedView(sql);
        DataOutputBuffer out = new DataOutputBuffer(1024);
        mv.write(out);
        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        MaterializedView other = new MaterializedView();
        other.readFields(in);

        Assertions.assertEquals(TableType.MATERIALIZED_VIEW, mv.getType());
        Assertions.assertEquals(mv.getType(), other.getType());
        Assertions.assertEquals(mv.getName(), other.getName());
        Assertions.assertEquals(mv.getQuery(), other.getQuery());

        MVRefreshInfo refreshInfo = mv.getRefreshInfo();
        MVRefreshInfo otherRefreshInfo = other.getRefreshInfo();
        Assertions.assertEquals(refreshInfo.isNeverRefresh(), otherRefreshInfo.isNeverRefresh());
        Assertions.assertEquals(refreshInfo.getRefreshMethod(), otherRefreshInfo.getRefreshMethod());

        Assertions.assertEquals(
                refreshInfo.getTriggerInfo().getRefreshTrigger(),
                otherRefreshInfo.getTriggerInfo().getRefreshTrigger()
        );

        MVRefreshIntervalTriggerInfo intervalTrigger = refreshInfo.getTriggerInfo().getIntervalTrigger();
        MVRefreshIntervalTriggerInfo otherIntervalTrigger = otherRefreshInfo.getTriggerInfo().getIntervalTrigger();
        if (intervalTrigger == null) {
            Assertions.assertNull(otherIntervalTrigger);
        } else {
            Assertions.assertEquals(intervalTrigger.getStartTime(), otherIntervalTrigger.getStartTime());
            Assertions.assertEquals(intervalTrigger.getInterval(), otherIntervalTrigger.getInterval());
            Assertions.assertEquals(intervalTrigger.getTimeUnit(), otherIntervalTrigger.getTimeUnit());
        }
    }

    private MaterializedView createMaterializedView(String sql) throws UserException {
        CreateMultiTableMaterializedViewStmt stmt = (CreateMultiTableMaterializedViewStmt) SqlParserUtils
                .parseAndAnalyzeStmt(sql, connectContext);
        MaterializedView mv = (MaterializedView) new OlapTableFactory()
                .init(OlapTableFactory.getTableType(stmt))
                .withTableId(0)
                .withTableName(stmt.getMVName())
                .withKeysType(stmt.getKeysDesc().getKeysType())
                .withSchema(stmt.getColumns())
                .withPartitionInfo(new SinglePartitionInfo())
                .withDistributionInfo(stmt.getDistributionDesc().toDistributionInfo(stmt.getColumns()))
                .withExtraParams(stmt)
                .build();
        mv.setBaseIndexId(1);
        mv.setIndexMeta(
                1,
                stmt.getMVName(),
                stmt.getColumns(),
                0,
                Util.generateSchemaHash(),
                Env.calcShortKeyColumnCount(stmt.getColumns(), stmt.getProperties()),
                TStorageType.COLUMN,
                stmt.getKeysDesc().getKeysType());
        return mv;
    }

    @Test
    void testShowCreateTables() throws Exception {
        createTable("create table test.t1 (pk int, v1 int sum) aggregate key (pk) "
                + "distributed by hash (pk) buckets 1 properties ('replication_num' = '1');");
        createTable("create table test.t2 (pk int, v2 int sum) aggregate key (pk) "
                + "distributed by hash (pk) buckets 1 properties ('replication_num' = '1');");
        new StmtExecutor(connectContext, "create materialized view mv "
                + "build immediate refresh complete key (mpk) distributed by hash (mpk) "
                + "properties ('replication_num' = '1') "
                + "as select test.t1.pk as mpk from test.t1, test.t2 where test.t1.pk = test.t2.pk").execute();
        Assertions.assertNull(connectContext.getState().getErrorCode(), connectContext.getState().getErrorMessage());

        ShowExecutor showExecutor = new ShowExecutor(connectContext,
                (ShowStmt) parseAndAnalyzeStmt("show create table mv"));
        ShowResultSet resultSet = showExecutor.execute();
        String result = resultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(result.contains("CREATE MATERIALIZED VIEW `mv`\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND\n"
                + "KEY(`mpk`)\n"
                + "DISTRIBUTED BY HASH(`mpk`) BUCKETS 10"));
    }

    @Test
    void testDropMaterializedView() throws Exception {
        createTable("create table test.t1 (pk int, v1 int sum) aggregate key (pk) "
                + "distributed by hash (pk) buckets 1 properties ('replication_num' = '1');");
        createTable("create table test.t2 (pk int, v2 int sum) aggregate key (pk) "
                + "distributed by hash (pk) buckets 1 properties ('replication_num' = '1');");
        new StmtExecutor(connectContext, "create materialized view mv "
                + "build immediate refresh complete key (mpk) distributed by hash (mpk) "
                + "properties ('replication_num' = '1') "
                + "as select test.t1.pk as mpk from test.t1, test.t2 where test.t1.pk = test.t2.pk").execute();
        Assertions.assertNull(connectContext.getState().getErrorCode(), connectContext.getState().getErrorMessage());

        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "is not TABLE",
                () -> Env.getCurrentInternalCatalog()
                        .dropTable((DropTableStmt) parseAndAnalyzeStmt("drop table mv")));

        ExceptionChecker.expectThrowsNoException(() -> connectContext.getEnv().dropMaterializedView(
                (DropMaterializedViewStmt) parseAndAnalyzeStmt("drop materialized view mv")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"AGGREGATE", "UNIQUE", "DUPLICATE"})
    public void testCreate(String keyType) throws Exception {
        String aggregation = keyType.equals("AGGREGATE") ? "SUM" : "";
        createTable("CREATE TABLE test.t1 ("
                + "  pk INT,"
                + "  v1 INT " + aggregation
                + ") " + keyType + " KEY (pk)"
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE test.t2 ("
                + "  pk INT,"
                + "  v2 INT " + aggregation
                + ") " + keyType + " KEY (pk)"
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        new StmtExecutor(connectContext, "CREATE MATERIALIZED VIEW mv "
                + "BUILD IMMEDIATE REFRESH COMPLETE KEY (pk) DISTRIBUTED BY HASH(pk) "
                + "PROPERTIES ('replication_num' = '1') "
                + "AS SELECT t1.pk, v1, v2 FROM test.t1, test.t2 WHERE test.t1.pk = test.t2.pk").execute();
        Assertions.assertNull(connectContext.getState().getErrorCode(), connectContext.getState().getErrorMessage());

        connectContext.getEnv().dropMaterializedView(
                (DropMaterializedViewStmt) parseAndAnalyzeStmt("DROP MATERIALIZED VIEW mv"));
        new StmtExecutor(connectContext, "CREATE MATERIALIZED VIEW mv "
                + "BUILD IMMEDIATE REFRESH COMPLETE DISTRIBUTED BY HASH(pk) "
                + "PROPERTIES ('replication_num' = '1') "
                + "AS SELECT t1.pk, v1, v2 FROM test.t1, test.t2 WHERE test.t1.pk = test.t2.pk").execute();
        Assertions.assertNull(connectContext.getState().getErrorCode(), connectContext.getState().getErrorMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"AGGREGATE", "UNIQUE", "DUPLICATE"})
    public void testCreateWithAliases(String keyType) throws Exception {
        String aggregation = keyType.equals("AGGREGATE") ? "SUM" : "";
        createTable("CREATE TABLE test.t1 ("
                + "  pk INT,"
                + "  v1 INT " + aggregation
                + ") " + keyType + " KEY (pk)"
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE test.t2 ("
                + "  pk INT,"
                + "  v2 INT " + aggregation
                + ") " + keyType + " KEY (pk)"
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        new StmtExecutor(connectContext, "CREATE MATERIALIZED VIEW mv "
                + "BUILD IMMEDIATE REFRESH COMPLETE KEY (mpk) DISTRIBUTED BY HASH(mpk) "
                + "PROPERTIES ('replication_num' = '1') "
                + "AS SELECT t1.pk AS mpk, v1, v2 FROM test.t1, test.t2 WHERE test.t1.pk = test.t2.pk").execute();
        Assertions.assertNull(connectContext.getState().getErrorCode(), connectContext.getState().getErrorMessage());

        connectContext.getEnv().dropMaterializedView(
                (DropMaterializedViewStmt) parseAndAnalyzeStmt("DROP MATERIALIZED VIEW mv"));
        new StmtExecutor(connectContext, "CREATE MATERIALIZED VIEW mv "
                + "BUILD IMMEDIATE REFRESH COMPLETE DISTRIBUTED BY HASH(mpk) "
                + "PROPERTIES ('replication_num' = '1') "
                + "AS SELECT t1.pk as mpk, v1, v2 FROM test.t1, test.t2 WHERE test.t1.pk = test.t2.pk").execute();
        Assertions.assertNull(connectContext.getState().getErrorCode(), connectContext.getState().getErrorMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"AGGREGATE", "UNIQUE", "DUPLICATE"})
    public void testCreateWithPartition(String keyType) throws Exception {
        String aggregation = keyType.equals("AGGREGATE") ? "SUM" : "";
        createTable("CREATE TABLE test.t1 ("
                + "  pk INT NOT NULL,"
                + "  v1 INT " + aggregation
                + ") " + keyType + " KEY (pk)"
                + "PARTITION BY RANGE(pk) ("
                + "  PARTITION p1 VALUES LESS THAN ('10'),"
                + "  PARTITION p2 VALUES LESS THAN ('20')"
                + ")"
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE test.t2 ("
                + "  pk INT NOT NULL,"
                + "  v2 INT " + aggregation
                + ") " + keyType + " KEY (pk)"
                + "PARTITION BY LIST(pk) ("
                + "  PARTITION odd VALUES IN ('10', '30', '50', '70', '90'),"
                + "  PARTITION even VALUES IN ('20', '40', '60', '80')"
                + ")"
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1')");

        new StmtExecutor(connectContext, "CREATE MATERIALIZED VIEW mv "
                + "BUILD IMMEDIATE REFRESH COMPLETE KEY (pk) "
                + "PARTITION BY (t1.pk)"
                + "DISTRIBUTED BY HASH(pk) "
                + "PROPERTIES ('replication_num' = '1') "
                + "AS SELECT t1.pk, v1, v2 FROM test.t1, test.t2 WHERE test.t1.pk = test.t2.pk").execute();
        Assertions.assertNull(connectContext.getState().getErrorCode(), connectContext.getState().getErrorMessage());

        connectContext.getEnv().dropMaterializedView(
                (DropMaterializedViewStmt) parseAndAnalyzeStmt("DROP MATERIALIZED VIEW mv"));
        new StmtExecutor(connectContext, "CREATE MATERIALIZED VIEW mv "
                + "BUILD IMMEDIATE REFRESH COMPLETE KEY (pk) "
                + "PARTITION BY (t2.pk)"
                + "DISTRIBUTED BY HASH(pk) "
                + "PROPERTIES ('replication_num' = '1') "
                + "AS SELECT t2.pk, v1, v2 FROM test.t1, test.t2 WHERE test.t1.pk = test.t2.pk").execute();
        Assertions.assertNull(connectContext.getState().getErrorCode(), connectContext.getState().getErrorMessage());

        connectContext.getEnv().dropMaterializedView(
                (DropMaterializedViewStmt) parseAndAnalyzeStmt("DROP MATERIALIZED VIEW mv"));

        connectContext.getState().reset();
        new StmtExecutor(connectContext, "CREATE MATERIALIZED VIEW mv "
                + "BUILD IMMEDIATE REFRESH COMPLETE KEY (pk) "
                + "PARTITION BY (t1.pk)"
                + "DISTRIBUTED BY HASH(pk) "
                + "PROPERTIES ('replication_num' = '1') "
                + "AS SELECT t2.pk, v1, v2 FROM test.t1, test.t2 WHERE test.t1.pk = test.t2.pk").execute();
        Assertions.assertTrue(
                connectContext.getState().getErrorMessage().contains("Failed to map the partition column name"));

        connectContext.getState().reset();
        new StmtExecutor(connectContext, "CREATE MATERIALIZED VIEW mv "
                + "BUILD IMMEDIATE REFRESH COMPLETE KEY (pk) "
                + "PARTITION BY (v1)"
                + "DISTRIBUTED BY HASH(pk) "
                + "PROPERTIES ('replication_num' = '1') "
                + "AS SELECT t2.pk, v1, v2 FROM test.t1, test.t2 WHERE test.t1.pk = test.t2.pk").execute();
        Assertions.assertTrue(
                connectContext.getState().getErrorMessage()
                        .contains("The partition columns doesn't match the ones in base table"));
    }
}

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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Merged suite: each of these classes only needed a bare FE and a spied access manager, so they all
 * paid a full FE startup for two or three assertions. Members that were byte-identical across the
 * originals are shared; test methods whose names collided are prefixed with their original class so
 * grepping the old name still finds them.
 *
 * <p>Replaces the former standalone classes:
 * <ul>
 *   <li>AdminRepairTableCommandTest</li>
 *   <li>AdminCopyTabletCommandTest</li>
 *   <li>AlterColumnStatsCommandTest</li>
 *   <li>CleanQueryStatsCommandTest</li>
 *   <li>ShowQueryStatsCommandTest</li>
 *   <li>ShowTabletsFromTableCommandTest</li>
 * </ul>
 */
public class CommandPrivilegeValidationTest extends TestWithFeService {

    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static boolean dbInitialized = false;
    private AccessControllerManager accessControllerManager;
    private ConnectContext connectContext;
    private Env env;
    private Database db;

    // -------------------------------------------------------------------------
    // from AdminRepairTableCommandTest
    // -------------------------------------------------------------------------

    private void adminRepairTable_runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void adminRepairTable_testValidateNormal() throws Exception {
        adminRepairTable_runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, "test_db", "test_tbl");
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("p1");
        partitionNames.add("p2");
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, partitionNames);
        TableRefInfo tableRefInfo = new TableRefInfo(tableNameInfo, null, null, partitionNamesInfo, null, null, null, null);
        AdminRepairTableCommand command = new AdminRepairTableCommand(tableRefInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test external catalog
        TableNameInfo tableNameInfo2 = new TableNameInfo("hive", "test_db", "test_tbl");
        TableRefInfo tableRefInfo2 = new TableRefInfo(tableNameInfo2, null, null, partitionNamesInfo, null, null, null, null);
        AdminRepairTableCommand command2 = new AdminRepairTableCommand(tableRefInfo2);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext),
                "External catalog 'hive' is not allowed in 'AdminCancelRepairTableCommand.class'");

        //test partitionNameInfo isTemp
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(true, partitionNames);
        TableRefInfo tableRefInfo3 = new TableRefInfo(tableNameInfo, null, null, partitionNamesInfo2, null, null, null, null);
        AdminRepairTableCommand command3 = new AdminRepairTableCommand(tableRefInfo3);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Do not support (cancel)repair temporary partitions");
    }

    @Test
    public void testValidateNoPriviledge() throws Exception {
        adminRepairTable_runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, "test_db", "test_tbl");
        List<String> partitionNames = new ArrayList<>();
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, partitionNames);
        TableRefInfo tableRefInfo = new TableRefInfo(tableNameInfo, null, null, partitionNamesInfo, null, null, null, null);
        AdminRepairTableCommand command = new AdminRepairTableCommand(tableRefInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Access denied; you need (at least one of) the (ADMIN) privilege(s) for this operation");
    }

    // -------------------------------------------------------------------------
    // from AdminCopyTabletCommandTest
    // -------------------------------------------------------------------------

    private void adminCopyTablet_runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void adminCopyTablet_testValidateNormal() throws Exception {
        adminCopyTablet_runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Map<String, String> properties = new HashMap<>();
        properties.put("version", "0");
        AdminCopyTabletCommand command = new AdminCopyTabletCommand(100, properties);
        Assertions.assertDoesNotThrow(() -> command.validate());

        Map<String, String> properties2 = new HashMap<>();
        properties2.put("backend_id", "10");
        AdminCopyTabletCommand command2 = new AdminCopyTabletCommand(100, properties2);
        Assertions.assertDoesNotThrow(() -> command2.validate());

        Map<String, String> properties3 = new HashMap<>();
        properties3.put("expiration_minutes", "10");
        AdminCopyTabletCommand command3 = new AdminCopyTabletCommand(100, properties3);
        Assertions.assertDoesNotThrow(() -> command3.validate());
    }

    @Test
    public void testNoPriviledge() throws Exception {
        adminCopyTablet_runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Map<String, String> properties = new HashMap<>();
        properties.put("version", "0");
        AdminCopyTabletCommand command = new AdminCopyTabletCommand(100, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(),
                "Access denied; you need (at least one of) the (Admin_priv) privilege(s) for this operation");
    }

    // -------------------------------------------------------------------------
    // from AlterColumnStatsCommandTest
    // -------------------------------------------------------------------------

    private void alterColumnStats_runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void alterColumnStats_testValidateNormal() throws Exception {
        alterColumnStats_runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(PrivPredicate.class));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        //test normal
        connectContext.getSessionVariable().enableStats = true;
        createDatabase("alter_column_stats_db");
        createTable("create table alter_column_stats_db.test_tbl\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ");

        TableNameInfo tableNameInfo =
                new TableNameInfo("alter_column_stats_db", "test_tbl");
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of("p1"));
        String indexName = null;
        String columnName = "k1";
        Map<String, String> properties = new HashMap<>();
        properties.put("row_count", "5");
        properties.put("avg_size", "100000");
        AlterColumnStatsCommand command = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test not a partitioned table
        createTable("create table alter_column_stats_db.test_tbl2(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
        TableNameInfo tableNameInfo2 =
                new TableNameInfo("alter_column_stats_db", "test_tbl2");
        AlterColumnStatsCommand command2 = new AlterColumnStatsCommand(tableNameInfo2, partitionNamesInfo, indexName, columnName, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext),
                "Not a partitioned table: test_tbl2");

        //test partition does not exist
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false,
                ImmutableList.of("k3"));
        AlterColumnStatsCommand command3 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo2, indexName, columnName, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Partition does not exist: k3");

        //test indexId not exist in OlapTable
        String indexName3 = "invalid_index";
        AlterColumnStatsCommand command4 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName3, columnName, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext),
                "Index invalid_index not exist in table test_tbl");

        //test invalid statistics
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("histogram", "invalide_value");
        AlterColumnStatsCommand command5 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties2);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(connectContext),
                "histogram is invalid statistics");

        //row_count is not exist
        Map<String, String> properties3 = new HashMap<>();
        properties2.put("avg_size", "100000");
        properties2.put("max_size", "100000000");
        AlterColumnStatsCommand command6 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties3);
        Assertions.assertThrows(AnalysisException.class, () -> command6.validate(connectContext),
                 "Set column stats must set row_count. e.g. 'row_count'='5'");

        //test enable stats
        connectContext.getSessionVariable().enableStats = false;
        AlterColumnStatsCommand command7 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties);
        Assertions.assertThrows(UserException.class, () -> command7.validate(connectContext),
                "Analyze function is forbidden, you should add `enable_stats=true` in your FE conf file");
    }

    @Test
    void alterColumnStats_testValidateNoPrivilege() throws IOException {
        alterColumnStats_runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(PrivPredicate.class));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        TableNameInfo tableNameInfo =
                    new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                    ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));

        String indexName = "index1";
        String columnName = "k1";
        Map<String, String> properties = new HashMap<>();

        AlterColumnStatsCommand command = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties);
        connectContext.getSessionVariable().enableStats = true;
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                    "ALTER TABLE STATS command denied to user 'null'@'null' for table 'alter_column_stats_db: test_tbl2'");
    }

    // -------------------------------------------------------------------------
    // from CleanQueryStatsCommandTest
    // -------------------------------------------------------------------------

    public void cleanQueryStats_runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void testAllNormal() throws IOException {
        cleanQueryStats_runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.any(PrivPredicate.class));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        CleanQueryStatsCommand command = new CleanQueryStatsCommand();
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testDB() throws Exception {
        cleanQueryStats_runBefore();
        connectContext.setDatabase("clean_query_stats_db");
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkDbPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        CleanQueryStatsCommand command = new CleanQueryStatsCommand("clean_query_stats_db");
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testTbl() throws Exception {
        cleanQueryStats_runBefore();
        createDatabase("clean_query_stats_db");
        createTable("create table clean_query_stats_db.test_tbl\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ");
        TableNameInfo tableNameInfo = new TableNameInfo("clean_query_stats_db", "test_tbl");
        connectContext.setDatabase("clean_query_stats_db");
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.any(TableNameInfo.class), Mockito.any(PrivPredicate.class));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        CleanQueryStatsCommand command = new CleanQueryStatsCommand(tableNameInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    // -------------------------------------------------------------------------
    // from ShowQueryStatsCommandTest
    // -------------------------------------------------------------------------

    private void showQueryStats_runBefore() throws Exception {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
        if (!dbInitialized) {
            createDatabaseWithSql("CREATE DATABASE IF NOT EXISTS " + CatalogMocker.TEST_DB_NAME);
            createTable("CREATE TABLE IF NOT EXISTS " + CatalogMocker.TEST_DB_NAME + "." + CatalogMocker.TEST_TBL_NAME
                    + " (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1"
                    + " PROPERTIES ('replication_num' = '1')");
            dbInitialized = true;
        }
    }

    @Test
    public void showQueryStats_testValidateWithPrivilege() throws Exception {
        showQueryStats_runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Mockito.doReturn(true).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.any(TableNameInfo.class),
                Mockito.eq(PrivPredicate.SHOW));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);

        // normal
        ShowQueryStatsCommand command = new ShowQueryStatsCommand(tableNameInfo.getDb(),
                tableNameInfo, false, false);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    void showQueryStats_testValidateNoPrivilege() throws Exception {
        showQueryStats_runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Mockito.doReturn(false).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.SHOW));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        ShowQueryStatsCommand command = new ShowQueryStatsCommand(tableNameInfo.getDb(),
                tableNameInfo, false, false);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));
    }

    // -------------------------------------------------------------------------
    // from ShowTabletsFromTableCommandTest
    // -------------------------------------------------------------------------

    private void showTabletsFromTable_runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void showTabletsFromTable_testValidateWithPrivilege() throws Exception {
        showTabletsFromTable_runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Mockito.doReturn(true).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Expression version = new UnboundSlot("version");

        Expression whereClauseNormal = new EqualTo(version, new IntegerLiteral(2));

        List<OrderKey> orderKeysNormal = new ArrayList<>();
        orderKeysNormal.add(new OrderKey(version, true, true));

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_SINGLE_PARTITION_NAME));

        // normal
        ShowTabletsFromTableCommand command = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseNormal, orderKeysNormal, 5, 0);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        // where clause error
        Expression error = new UnboundSlot("error");
        Expression whereClauseError = new EqualTo(error, new IntegerLiteral(2));

        ShowTabletsFromTableCommand command2 = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseError, orderKeysNormal, 5, 0);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext));

        // where clause contains or
        Expression backendId = new UnboundSlot("BackendId");
        Expression whereClauseOr = new Or(
                new EqualTo(version, new IntegerLiteral(2)),
                new EqualTo(backendId, new IntegerLiteral(2)));

        ShowTabletsFromTableCommand command3 = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseOr, orderKeysNormal, 5, 0);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext));

        // order by error
        List<OrderKey> orderKeysError = new ArrayList<>();
        orderKeysError.add(new OrderKey(error, true, true));

        ShowTabletsFromTableCommand command4 = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseNormal, orderKeysError, 5, 0);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext));
    }

    @Test
    void showTabletsFromTable_testValidateNoPrivilege() throws Exception {
        showTabletsFromTable_runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Mockito.doReturn(false).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Expression version = new UnboundSlot("version");

        Expression whereClauseNormal = new EqualTo(version, new IntegerLiteral(2));

        List<OrderKey> orderKeysNormal = new ArrayList<>();
        orderKeysNormal.add(new OrderKey(version, true, true));

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_SINGLE_PARTITION_NAME));

        ShowTabletsFromTableCommand command = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseNormal, orderKeysNormal, 5, 0);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));
    }
}

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
import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.RemoteSpillStatsPoller;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowDataCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final TableNameInfo tableNameInfo =
            new TableNameInfo(internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
    private static final OlapTable olapTable = new OlapTable(CatalogMocker.TEST_TBL_ID,
            CatalogMocker.TEST_TBL_NAME,
            CatalogMocker.TEST_TBL_BASE_SCHEMA,
            KeysType.AGG_KEYS,
            new SinglePartitionInfo(),
            new RandomDistributionInfo(32));

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private AccessControllerManager accessControllerManager = Mockito.mock(AccessControllerManager.class);
    private ConnectContext connectContext = Mockito.mock(ConnectContext.class);
    private Database database = Mockito.mock(Database.class);
    private NameSpaceContext nameSpaceContext = Mockito.mock(NameSpaceContext.class);

    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedConnectContext;
    private String savedDeployMode;
    private String savedCloudUniqueId;

    @BeforeEach
    public void setUp() {
        savedDeployMode = Config.deploy_mode;
        savedCloudUniqueId = Config.cloud_unique_id;
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedConnectContext = Mockito.mockStatic(ConnectContext.class);

        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedConnectContext.when(ConnectContext::get).thenReturn(connectContext);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(connectContext.getNameSpaceContext()).thenReturn(nameSpaceContext);
        Mockito.when(nameSpaceContext.getDefaultCatalog()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(connectContext.getState()).thenReturn(new QueryState());
    }

    @AfterEach
    public void tearDown() {
        Config.deploy_mode = savedDeployMode;
        Config.cloud_unique_id = savedCloudUniqueId;
        mockedConnectContext.close();
        mockedEnv.close();
    }

    @Test
    public void testWarehouseIncludesRemoteSpillInCloudTotal() throws Exception {
        Config.deploy_mode = "cloud";
        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        RemoteSpillStatsPoller poller = Mockito.mock(RemoteSpillStatsPoller.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(new CatalogRecycleBin());
        Mockito.when(cloudEnv.getRemoteSpillStatsPoller()).thenReturn(poller);
        Mockito.when(poller.getRemoteSpillBytes()).thenReturn(32L);
        Mockito.when(catalog.getUsedDataQuota()).thenReturn(Map.of("db1", 10L));
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(1L);
        Mockito.when(database.getName()).thenReturn("db1");
        Mockito.when(database.getTables()).thenReturn(ImmutableList.of());

        ShowDataCommand command = new ShowDataCommand(null, null,
                Map.of("entire_warehouse", "true"), false);
        ShowResultSet result = command.doRun(connectContext, null);
        Assertions.assertEquals(4, result.getMetaData().getColumnCount());
        Assertions.assertEquals(ImmutableList.of("db1", "10", "0", "0"), result.getResultRows().get(0));
        Assertions.assertEquals(ImmutableList.of("__remote_spill__", "32", "0", "0"),
                result.getResultRows().get(1));
        Assertions.assertEquals(ImmutableList.of("total", "42", "0", "0"), result.getResultRows().get(2));
    }

    @Test
    public void testWarehouseRestrictedToDatabasesDoesNotFetchRemoteSpill() throws Exception {
        Config.deploy_mode = "cloud";
        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        RemoteSpillStatsPoller poller = Mockito.mock(RemoteSpillStatsPoller.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(new CatalogRecycleBin());
        Mockito.when(cloudEnv.getRemoteSpillStatsPoller()).thenReturn(poller);
        Mockito.when(catalog.getUsedDataQuota()).thenReturn(Map.of("db1", 10L));
        Mockito.when(catalog.getDbNames()).thenReturn(ImmutableList.of("db1"));
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(database);
        Mockito.when(database.getName()).thenReturn("db1");
        Mockito.when(database.getTables()).thenReturn(ImmutableList.of());

        ShowDataCommand command = new ShowDataCommand(null, null,
                Map.of("entire_warehouse", "true", "db_names", "db1"), false);
        ShowResultSet result = command.doRun(connectContext, null);
        Assertions.assertEquals(ImmutableList.of(
                ImmutableList.of("db1", "10", "0", "0"),
                ImmutableList.of("total", "10", "0", "0")), result.getResultRows());
        Mockito.verifyNoInteractions(poller);
    }

    @Test
    public void testWarehouseDoesNotShowZeroWhenRemoteSpillStatsAreMissing() throws Exception {
        Config.deploy_mode = "cloud";
        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        RemoteSpillStatsPoller poller = Mockito.mock(RemoteSpillStatsPoller.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(new CatalogRecycleBin());
        Mockito.when(cloudEnv.getRemoteSpillStatsPoller()).thenReturn(poller);
        Mockito.when(poller.getRemoteSpillBytes()).thenThrow(new AnalysisException("spill stats are stale"));
        Mockito.when(catalog.getUsedDataQuota()).thenReturn(Map.of());

        ShowDataCommand command = new ShowDataCommand(null, null,
                Map.of("entire_warehouse", "true"), false);
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> command.doRun(connectContext, null));
        Assertions.assertTrue(error.getMessage().contains("stale"), error.getMessage());
    }

    @Test
    public void testWarehouseOmitsRemoteSpillOutsideCloudMode() throws Exception {
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(new CatalogRecycleBin());
        Mockito.when(catalog.getUsedDataQuota()).thenReturn(Map.of());

        ShowDataCommand command = new ShowDataCommand(null, null,
                Map.of("entire_warehouse", "true"), false);
        ShowResultSet result = command.doRun(connectContext, null);
        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("total", "0", "0", "0")),
                result.getResultRows());
    }

    @Test
    public void testValidateNormal() throws Exception {
        Mockito.doReturn(database).when(catalog).getDbOrAnalysisException(Mockito.anyString());
        Mockito.doReturn(olapTable).when(database).getTableOrMetaException(
                Mockito.anyString(), Mockito.any(TableIf.TableType.class));
        Mockito.when(accessControllerManager.checkTblPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.any(TableNameInfo.class),
                Mockito.any(PrivPredicate.class))).thenReturn(true);

        SlotReference tableName = new SlotReference("TableName", IntegerType.INSTANCE);
        List<OrderKey> keys = ImmutableList.of(
                new OrderKey(tableName, true, false)
        );

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);

        Map<String, String> properties = new HashMap<>();
        ShowDataCommand command = new ShowDataCommand(tableNameInfo, keys, properties, false);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        // Ensure show data result includes binlog columns in metadata.
        Assertions.assertTrue(command.getMetaData().getColumns().stream()
                        .anyMatch(c -> c.getName().equalsIgnoreCase("BinlogSize")),
                "SHOW DATA should contain BinlogSize column");
    }

    @Test
    public void testValidateShowAllDataNormal() throws Exception {
        Mockito.when(connectContext.getDatabase()).thenReturn(CatalogMocker.TEST_DB_NAME);
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
        mockedEnv.when(Env::getCurrentInvertedIndex).thenReturn(Mockito.mock(TabletInvertedIndex.class));
        Database mockDb = CatalogMocker.mockDb();
        Mockito.when(catalog.getDbOrAnalysisException(Mockito.anyString())).thenReturn(mockDb);
        Mockito.when(accessControllerManager.checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);

        SlotReference tableName = new SlotReference("TableName", IntegerType.INSTANCE);
        List<OrderKey> keys = ImmutableList.of(new OrderKey(tableName, true, false));
        ShowDataCommand command = new ShowDataCommand(null, keys, new HashMap<>(), false);

        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertTrue(command.getMetaData().getColumns().stream()
                        .anyMatch(c -> c.getName().equalsIgnoreCase("BinlogSize")),
                "SHOW DATA should contain BinlogSize column");
    }

    @Test
    public void testValidateShowDetailedDataRowsMatchMetaData() throws Exception {
        Mockito.when(connectContext.getDatabase()).thenReturn(CatalogMocker.TEST_DB_NAME);
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
        mockedEnv.when(Env::getCurrentInvertedIndex).thenReturn(Mockito.mock(TabletInvertedIndex.class));
        Database mockDb = CatalogMocker.mockDb();
        Mockito.when(catalog.getDbOrAnalysisException(Mockito.anyString())).thenReturn(mockDb);
        Mockito.when(accessControllerManager.checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);

        ShowDataCommand command = new ShowDataCommand(null, null, new HashMap<>(), true);

        ShowResultSet rs = command.doRun(connectContext, null);
        int columnCount = rs.getMetaData().getColumnCount();
        Assertions.assertEquals(9, columnCount);
        for (List<String> row : rs.getResultRows()) {
            Assertions.assertEquals(columnCount, row.size());
        }
    }

    @Test
    public void testValidateShowDetailedDataRowsMatchMetaDataForEmptyDb() throws Exception {
        Mockito.when(connectContext.getDatabase()).thenReturn(CatalogMocker.TEST_DB_NAME);
        Mockito.when(catalog.getDbOrAnalysisException(Mockito.anyString())).thenReturn(database);
        Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN)).thenReturn(true);
        Mockito.when(database.getTables()).thenReturn(ImmutableList.of());
        Mockito.when(database.getDataQuota()).thenReturn(1024L);
        Mockito.when(database.getReplicaQuota()).thenReturn(10L);
        Mockito.doNothing().when(database).readLock();
        Mockito.doNothing().when(database).readUnlock();

        ShowDataCommand command = new ShowDataCommand(null, null, new HashMap<>(), true);

        ShowResultSet rs = command.doRun(connectContext, null);
        List<List<String>> rows = rs.getResultRows();

        Assertions.assertEquals(9, rs.getMetaData().getColumnCount());
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(ImmutableList.of("Total", "0", DebugUtil.printByteWithUnit(0L),
                DebugUtil.printByteWithUnit(0L), DebugUtil.printByteWithUnit(0L), DebugUtil.printByteWithUnit(0L),
                DebugUtil.printByteWithUnit(0L), DebugUtil.printByteWithUnit(0L),
                DebugUtil.printByteWithUnit(0L)), rows.get(0));
        Assertions.assertEquals(ImmutableList.of("Quota", "10", DebugUtil.printByteWithUnit(1024L),
                "", "", "", "", "", ""), rows.get(1));
        Assertions.assertEquals(ImmutableList.of("Left", "10", DebugUtil.printByteWithUnit(1024L),
                "", "", "", "", "", ""), rows.get(2));
        for (List<String> row : rows) {
            Assertions.assertEquals(rs.getMetaData().getColumnCount(), row.size());
        }
    }

    @Test
    public void testValidateShowAllDataGetAllDbStats() throws Exception {
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(recycleBin);

        Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN)).thenReturn(true);
        Mockito.when(catalog.getDbNames()).thenReturn(ImmutableList.of("db1", "db2"));

        Database db1 = Mockito.mock(Database.class);
        Database db2 = Mockito.mock(Database.class);
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(db1);
        Mockito.when(catalog.getDbNullable("db2")).thenReturn(db2);

        OlapTable t1 = Mockito.mock(OlapTable.class);
        OlapTable t2 = Mockito.mock(OlapTable.class);

        Mockito.when(db1.getId()).thenReturn(101L);
        Mockito.when(db1.getUsedDataSize()).thenReturn(Pair.of(10L, 1L));
        Mockito.when(db1.getTables()).thenReturn(ImmutableList.of(t1));
        Mockito.doNothing().when(db1).readLock();
        Mockito.doNothing().when(db1).readUnlock();

        Mockito.when(db2.getId()).thenReturn(102L);
        Mockito.when(db2.getUsedDataSize()).thenReturn(Pair.of(20L, 2L));
        Mockito.when(db2.getTables()).thenReturn(ImmutableList.of(t2));
        Mockito.doNothing().when(db2).readLock();
        Mockito.doNothing().when(db2).readUnlock();

        Mockito.when(t1.isManagedTable()).thenReturn(true);
        Mockito.when(t2.isManagedTable()).thenReturn(true);
        Mockito.when(t1.getBinlogSize()).thenReturn(5L);
        Mockito.when(t2.getBinlogSize()).thenReturn(7L);

        SlotReference tableName = new SlotReference("TableName", IntegerType.INSTANCE);
        List<OrderKey> keys = ImmutableList.of(new OrderKey(tableName, true, false));
        ShowDataCommand command = new ShowDataCommand(null, keys, new HashMap<>(), false);

        ShowResultSet rs = command.doRun(connectContext, null);
        List<List<String>> rows = rs.getResultRows();

        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(ImmutableList.of("101", "db1", "10", "1", "5", "0", "0"), rows.get(0));
        Assertions.assertEquals(ImmutableList.of("102", "db2", "20", "2", "7", "0", "0"), rows.get(1));
        Assertions.assertEquals(ImmutableList.of("Total", "NULL", "30", "3", "12", "0", "0"), rows.get(2));
    }

    @Test
    public void testValidateNoPrivilege() throws Exception {
        Mockito.doReturn(database).when(catalog).getDbOrAnalysisException(Mockito.anyString());
        Mockito.doReturn(olapTable).when(database).getTableOrMetaException(
                Mockito.anyString(), Mockito.any(TableIf.TableType.class));

        SlotReference tableName = new SlotReference("TableName", IntegerType.INSTANCE);
        List<OrderKey> keys = ImmutableList.of(
                new OrderKey(tableName, true, false)
        );

        // test not exist table
        TableNameInfo tableNameInfoNotExist =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, "tbl_not_exist");

        Map<String, String> properties = new HashMap<>();
        ShowDataCommand command = new ShowDataCommand(tableNameInfoNotExist, keys, properties, false);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));

        // test no priv
        ShowDataCommand command2 = new ShowDataCommand(tableNameInfo, keys, properties, false);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext));
    }
}

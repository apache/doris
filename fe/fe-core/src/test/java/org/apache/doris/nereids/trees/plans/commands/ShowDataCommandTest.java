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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
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

    @BeforeEach
    public void setUp() {
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
        mockedConnectContext.close();
        mockedEnv.close();
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

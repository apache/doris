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

package org.apache.doris.datasource.fluss;

import org.apache.doris.datasource.ExternalCatalog;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FlussMetadataOpsTest {

    @Mock
    private ExternalCatalog mockCatalog;

    @Mock
    private Connection mockConnection;

    @Mock
    private Admin mockAdmin;

    private FlussMetadataOps metadataOps;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        metadataOps = new FlussMetadataOps(mockCatalog, mockConnection);
    }

    @Test
    public void testTableExist() throws Exception {
        String dbName = "test_db";
        String tblName = "test_table";
        TablePath tablePath = TablePath.of(dbName, tblName);
        TableInfo tableInfo = Mockito.mock(TableInfo.class);

        CompletableFuture<TableInfo> future = CompletableFuture.completedFuture(tableInfo);
        Mockito.when(mockAdmin.getTableInfo(tablePath)).thenReturn(future);

        boolean exists = metadataOps.tableExist(dbName, tblName);
        Assert.assertTrue(exists);
    }

    @Test
    public void testTableNotExist() throws Exception {
        String dbName = "test_db";
        String tblName = "non_existent_table";
        TablePath tablePath = TablePath.of(dbName, tblName);

        CompletableFuture<TableInfo> future = new CompletableFuture<>();
        future.completeExceptionally(new TableNotExistException("Table does not exist"));
        Mockito.when(mockAdmin.getTableInfo(tablePath)).thenReturn(future);

        boolean exists = metadataOps.tableExist(dbName, tblName);
        Assert.assertFalse(exists);
    }

    @Test
    public void testListTableNames() throws Exception {
        String dbName = "test_db";
        List<String> tableNames = new ArrayList<>();
        tableNames.add("table1");
        tableNames.add("table2");
        tableNames.add("table3");

        CompletableFuture<List<String>> future = CompletableFuture.completedFuture(tableNames);
        Mockito.when(mockAdmin.listTables(dbName)).thenReturn(future);

        List<String> result = metadataOps.listTableNames(dbName);
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.contains("table1"));
        Assert.assertTrue(result.contains("table2"));
        Assert.assertTrue(result.contains("table3"));
    }

    @Test
    public void testListTableNamesEmpty() throws Exception {
        String dbName = "empty_db";
        List<String> emptyList = new ArrayList<>();

        CompletableFuture<List<String>> future = CompletableFuture.completedFuture(emptyList);
        Mockito.when(mockAdmin.listTables(dbName)).thenReturn(future);

        List<String> result = metadataOps.listTableNames(dbName);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetTableInfo() throws Exception {
        String dbName = "test_db";
        String tblName = "test_table";
        TablePath tablePath = TablePath.of(dbName, tblName);
        TableInfo tableInfo = Mockito.mock(TableInfo.class);

        CompletableFuture<TableInfo> future = CompletableFuture.completedFuture(tableInfo);
        Mockito.when(mockAdmin.getTableInfo(tablePath)).thenReturn(future);

        TableInfo result = metadataOps.getTableInfo(dbName, tblName);
        Assert.assertNotNull(result);
        Assert.assertEquals(tableInfo, result);
    }

    @Test
    public void testGetAdmin() {
        Admin admin = metadataOps.getAdmin();
        Assert.assertNotNull(admin);
        Assert.assertEquals(mockAdmin, admin);
    }

    @Test
    public void testGetConnection() {
        Connection connection = metadataOps.getConnection();
        Assert.assertNotNull(connection);
        Assert.assertEquals(mockConnection, connection);
    }

    @Test
    public void testClose() {
        // Close should not throw exception
        metadataOps.close();
    }
}


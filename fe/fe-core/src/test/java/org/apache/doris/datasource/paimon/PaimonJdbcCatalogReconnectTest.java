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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.NameMapping;

import org.apache.paimon.catalog.Catalog;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLNonTransientConnectionException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PaimonJdbcCatalogReconnectTest {

    @Test
    public void testListDatabaseNamesReconnectsOnStaleJdbcConnection() throws Exception {
        Catalog brokenCatalog = Mockito.mock(Catalog.class);
        Catalog healthyCatalog = Mockito.mock(Catalog.class);
        SQLNonTransientConnectionException staleConnection =
                new SQLNonTransientConnectionException("stale connection", "08000");
        Mockito.when(brokenCatalog.listDatabases()).thenThrow(new RuntimeException(staleConnection));
        Mockito.when(healthyCatalog.listDatabases()).thenReturn(Collections.singletonList("db1"));

        TestPaimonExternalCatalog catalog = new TestPaimonExternalCatalog(
                PaimonExternalCatalog.PAIMON_JDBC, brokenCatalog, healthyCatalog);

        Assert.assertEquals(Collections.singletonList("db1"), catalog.listDatabaseNamesForTest());
        Assert.assertEquals(2, catalog.getInitCount());
        Mockito.verify(brokenCatalog).close();
    }

    @Test
    public void testGetPaimonTableReconnectsOnStaleJdbcConnection() throws Exception {
        Catalog brokenCatalog = Mockito.mock(Catalog.class);
        Catalog healthyCatalog = Mockito.mock(Catalog.class);
        org.apache.paimon.table.Table table = Mockito.mock(org.apache.paimon.table.Table.class);
        SQLNonTransientConnectionException staleConnection =
                new SQLNonTransientConnectionException("stale connection", "08000");
        Mockito.when(brokenCatalog.getTable(Mockito.any())).thenThrow(new RuntimeException(staleConnection));
        Mockito.when(healthyCatalog.getTable(Mockito.any())).thenReturn(table);

        TestPaimonExternalCatalog catalog = new TestPaimonExternalCatalog(
                PaimonExternalCatalog.PAIMON_JDBC, brokenCatalog, healthyCatalog);

        Assert.assertSame(table, catalog.getPaimonTable(NameMapping.createForTest("db1", "tbl1")));
        Assert.assertEquals(2, catalog.getInitCount());
        Mockito.verify(brokenCatalog).close();
    }

    @Test
    public void testDoesNotReconnectOnNonConnectionException() {
        Catalog brokenCatalog = Mockito.mock(Catalog.class);
        RuntimeException nonConnectionFailure = new RuntimeException("boom");
        Mockito.when(brokenCatalog.listDatabases()).thenThrow(nonConnectionFailure);

        TestPaimonExternalCatalog catalog = new TestPaimonExternalCatalog(
                PaimonExternalCatalog.PAIMON_JDBC, brokenCatalog);

        RuntimeException exception = Assert.assertThrows(RuntimeException.class, catalog::listDatabaseNamesForTest);
        Assert.assertSame(nonConnectionFailure, exception.getCause());
        Assert.assertEquals(1, catalog.getInitCount());
    }

    private static class TestPaimonExternalCatalog extends PaimonExternalCatalog {
        private final String testCatalogType;
        private final List<Catalog> testCatalogs;
        private int initCount = 0;

        TestPaimonExternalCatalog(String catalogType, Catalog... catalogs) {
            super(1L, "test_paimon_catalog", null, Collections.emptyMap(), "");
            this.testCatalogType = catalogType;
            this.testCatalogs = Arrays.asList(catalogs);
        }

        @Override
        protected void initLocalObjectsImpl() {
            catalogType = testCatalogType;
            catalog = testCatalogs.get(Math.min(initCount, testCatalogs.size() - 1));
            executionAuthenticator = new ExecutionAuthenticator() {};
            metadataOps = new PaimonMetadataOps(this, catalog);
            initCount++;
        }

        List<String> listDatabaseNamesForTest() {
            return super.listDatabaseNames();
        }

        int getInitCount() {
            return initCount;
        }
    }
}

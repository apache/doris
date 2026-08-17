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

package org.apache.doris.datasource.jdbc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mapping.IdentifierMapping;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;

public class JdbcExternalTableTest {
    private JdbcExternalCatalog catalog;
    private JdbcExternalDatabase database;
    private IdentifierMapping identifierMapping;
    private TestJdbcExternalTable table;

    @Before
    public void setUp() {
        catalog = Mockito.mock(JdbcExternalCatalog.class);
        database = Mockito.mock(JdbcExternalDatabase.class);
        identifierMapping = Mockito.mock(IdentifierMapping.class);

        Mockito.when(catalog.getName()).thenReturn("test_catalog");
        Mockito.when(catalog.getIdentifierMapping()).thenReturn(identifierMapping);
        Mockito.when(database.getFullName()).thenReturn("local_db");
        Mockito.when(database.getRemoteName()).thenReturn("remote_db");
        Mockito.when(identifierMapping.fromRemoteColumnName(
                        Mockito.anyString(), ArgumentMatchers.nullable(String.class), Mockito.anyString()))
                .thenAnswer(invocation -> invocation.getArgument(2));

        table = new TestJdbcExternalTable(1L, "local_table", null, catalog, database);
    }

    @Test
    public void testInitSchemaUsesEffectiveRemoteTableName() {
        Mockito.when(catalog.listColumns(
                        Mockito.anyString(), ArgumentMatchers.nullable(String.class)))
                .thenReturn(Lists.newArrayList(new Column("id", PrimitiveType.INT)));

        Optional<SchemaCacheValue> schema = table.initSchema();

        Assert.assertTrue(schema.isPresent());
        Mockito.verify(catalog).listColumns("remote_db", "local_table");
        Mockito.verify(identifierMapping).fromRemoteColumnName("remote_db", "local_table", "id");
    }

    @Test
    public void testConflictMessageUsesEffectiveRemoteTableName() {
        Mockito.when(catalog.listColumns(
                        Mockito.anyString(), ArgumentMatchers.nullable(String.class)))
                .thenReturn(Lists.newArrayList(
                        new Column("id", PrimitiveType.INT),
                        new Column("ID", PrimitiveType.INT)));

        RuntimeException exception = Assert.assertThrows(RuntimeException.class, table::initSchema);

        Assert.assertTrue(exception.getMessage(),
                exception.getMessage().contains("remote table 'remote_db.local_table'"));
    }

    @Test
    public void testFetchRowCountUsesEffectiveRemoteTableName() {
        Mockito.when(catalog.getDatabaseTypeName()).thenReturn(JdbcResource.MYSQL);

        Assert.assertEquals(1L, table.fetchRowCount());

        Assert.assertEquals("local_table", table.rowCountParams.get("tblName"));
    }

    private static class TestJdbcExternalTable extends JdbcExternalTable {
        private Map<String, String> rowCountParams;

        TestJdbcExternalTable(long id, String name, String remoteName,
                JdbcExternalCatalog catalog, JdbcExternalDatabase database) {
            super(id, name, remoteName, catalog, database);
        }

        @Override
        protected synchronized void makeSureInitialized() {
        }

        @Override
        protected long getRowCount(Map<String, String> params) {
            rowCountParams = params;
            return 1L;
        }
    }
}

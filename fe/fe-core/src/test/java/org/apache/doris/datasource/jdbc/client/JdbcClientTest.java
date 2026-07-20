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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JdbcClientTest {

    @Test
    public void testGetJdbcColumnsInfoFiltersWildcardSiblingTable() throws Exception {
        JdbcClient client = Mockito.mock(JdbcClient.class, Mockito.CALLS_REAL_METHODS);
        ResultSet resultSet = mockColumns(
                new String[] {"remote_db", "remote_db"},
                new String[] {"localXtable", "local_table"},
                new String[] {"sibling_column", "target_column"});
        mockMetadata(client, resultSet, "catalog", "remote_db", "local_table");

        List<JdbcFieldSchema> columns = client.getJdbcColumnsInfo("remote_db", "local_table");

        Assert.assertEquals(1, columns.size());
        Assert.assertEquals("target_column", columns.get(0).getColumnName());
    }

    @Test
    public void testGetJdbcColumnsInfoFiltersWildcardSiblingSchema() throws Exception {
        JdbcClient client = Mockito.mock(JdbcClient.class, Mockito.CALLS_REAL_METHODS);
        ResultSet resultSet = mockColumns(
                new String[] {"salesX2024", "sales_2024"},
                new String[] {"orders", "orders"},
                new String[] {"sibling_column", "target_column"});
        mockMetadata(client, resultSet, "catalog", "sales_2024", "orders");

        List<JdbcFieldSchema> columns = client.getJdbcColumnsInfo("sales_2024", "orders");

        Assert.assertEquals(1, columns.size());
        Assert.assertEquals("target_column", columns.get(0).getColumnName());
    }

    @Test
    public void testMySqlColumnsAcceptCanonicalLowercaseTableName() throws Exception {
        JdbcMySQLClient client = Mockito.mock(JdbcMySQLClient.class, Mockito.CALLS_REAL_METHODS);
        ResultSet resultSet = mockColumns(
                new String[] {"remote_db"},
                new String[] {"tusers"},
                new String[] {"target_column"});
        DatabaseMetaData databaseMetaData = mockMetadata(client, resultSet, null, "Remote_DB", "TUsers");
        Mockito.when(databaseMetaData.supportsMixedCaseIdentifiers()).thenReturn(false);

        List<JdbcFieldSchema> columns = client.getJdbcColumnsInfo("Remote_DB", "TUsers");

        Assert.assertEquals(1, columns.size());
        Assert.assertEquals("target_column", columns.get(0).getColumnName());
    }

    private DatabaseMetaData mockMetadata(JdbcClient client, ResultSet resultSet,
            String catalogName, String remoteDbName, String remoteTableName) throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        DatabaseMetaData databaseMetaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.doReturn(connection).when(client).getConnection();
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(connection.getCatalog()).thenReturn(catalogName);
        if (client instanceof JdbcMySQLClient) {
            Mockito.when(databaseMetaData.getColumns(remoteDbName, null, remoteTableName, null))
                    .thenReturn(resultSet);
        } else {
            Mockito.when(databaseMetaData.getColumns(catalogName, remoteDbName, remoteTableName, null))
                    .thenReturn(resultSet);
        }
        return databaseMetaData;
    }

    private ResultSet mockColumns(String[] databaseNames, String[] tableNames, String[] columnNames)
            throws Exception {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        AtomicInteger row = new AtomicInteger(-1);
        Mockito.when(resultSet.next()).thenAnswer(invocation -> row.incrementAndGet() < tableNames.length);
        Mockito.when(resultSet.getString(Mockito.anyString())).thenAnswer(invocation -> {
            String columnLabel = invocation.getArgument(0);
            if ("TABLE_SCHEM".equals(columnLabel) || "TABLE_CAT".equals(columnLabel)) {
                return databaseNames[row.get()];
            }
            if ("TABLE_NAME".equals(columnLabel)) {
                return tableNames[row.get()];
            }
            if ("COLUMN_NAME".equals(columnLabel)) {
                return columnNames[row.get()];
            }
            if ("TYPE_NAME".equals(columnLabel)) {
                return "INT";
            }
            return null;
        });
        Mockito.when(resultSet.getInt(Mockito.anyString())).thenAnswer(invocation -> {
            String columnLabel = invocation.getArgument(0);
            if ("DATA_TYPE".equals(columnLabel)) {
                return Types.INTEGER;
            }
            if ("COLUMN_SIZE".equals(columnLabel)) {
                return 11;
            }
            if ("NUM_PREC_RADIX".equals(columnLabel)) {
                return 10;
            }
            if ("NULLABLE".equals(columnLabel)) {
                return DatabaseMetaData.columnNullable;
            }
            return 0;
        });
        Mockito.when(resultSet.wasNull()).thenReturn(false);
        return resultSet;
    }
}

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

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcOceanBaseClientTest {
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;

    @Before
    public void setUp() throws Exception {
        connection = Mockito.mock(Connection.class);
        statement = Mockito.mock(Statement.class);
        resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery("SHOW VARIABLES LIKE 'ob_compatibility_mode'")).thenReturn(resultSet);
    }

    @Test
    public void testCloseTemporaryDataSourceAfterCreatingClient() throws Exception {
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getString(2)).thenReturn("MYSQL");

        try (MockedConstruction<HikariDataSource> mockedDataSources = mockDataSources()) {
            JdbcOceanBaseClient oceanBaseClient = new JdbcOceanBaseClient(createConfig());
            JdbcClient client = oceanBaseClient.createClient(createConfig());

            Assert.assertTrue(client instanceof JdbcMySQLClient);
            Assert.assertEquals(2, mockedDataSources.constructed().size());
            HikariDataSource temporaryDataSource = mockedDataSources.constructed().get(0);
            HikariDataSource clientDataSource = mockedDataSources.constructed().get(1);
            assertTemporaryResourcesClosed(temporaryDataSource);
            Mockito.verify(clientDataSource, Mockito.never()).close();

            client.closeClient();
            Mockito.verify(clientDataSource).close();
        }
    }

    @Test
    public void testCloseTemporaryDataSourceWhenCompatibilityModeIsMissing() throws Exception {
        Mockito.when(resultSet.next()).thenReturn(false);

        try (MockedConstruction<HikariDataSource> mockedDataSources = mockDataSources()) {
            JdbcOceanBaseClient oceanBaseClient = new JdbcOceanBaseClient(createConfig());

            JdbcClientException exception = Assert.assertThrows(
                    JdbcClientException.class, () -> oceanBaseClient.createClient(createConfig()));

            Assert.assertEquals("Failed to determine OceanBase compatibility mode", exception.getMessage());
            Assert.assertEquals(1, mockedDataSources.constructed().size());
            assertTemporaryResourcesClosed(mockedDataSources.constructed().get(0));
        }
    }

    private MockedConstruction<HikariDataSource> mockDataSources() {
        return Mockito.mockConstruction(HikariDataSource.class, (mock, context) ->
                Mockito.when(mock.getConnection()).thenReturn(connection));
    }

    private JdbcClientConfig createConfig() {
        return new JdbcClientConfig()
                .setCatalog("oceanbase_catalog")
                .setUser("user")
                .setPassword("password")
                .setJdbcUrl("jdbc:oceanbase://localhost:2881/test")
                .setDriverUrl("file:///tmp/oceanbase-jdbc.jar")
                .setDriverClass("com.oceanbase.jdbc.Driver");
    }

    private void assertTemporaryResourcesClosed(HikariDataSource temporaryDataSource) throws Exception {
        InOrder inOrder = Mockito.inOrder(resultSet, statement, connection, temporaryDataSource);
        inOrder.verify(resultSet).close();
        inOrder.verify(statement).close();
        inOrder.verify(connection).close();
        inOrder.verify(temporaryDataSource).close();
    }
}

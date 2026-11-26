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

package org.apache.doris.httpv2.websql;

import org.apache.doris.analysis.UserIdentity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;

class JdbcWebSqlConnectionFactoryTest {
    @Test
    void connectionUrlHasBoundedConnectAndSocketWaits() {
        String url = JdbcWebSqlConnectionFactory.connectionUrl(9030);

        Assertions.assertTrue(url.contains("connectTimeout=10000"));
        Assertions.assertTrue(url.contains("socketTimeout=1800000"));
    }

    @Test
    void rejectsAndClosesConnectionWhenLoopbackSelectsAnotherIdentity() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        JdbcWebSqlConnectionFactory factory = factory(connection, "'alice'@'%'");
        UserIdentity expected = UserIdentity.createAnalyzedUserIdentWithIp("alice", "10.%");

        WebSqlIdentityMismatchException exception = Assertions.assertThrows(
                WebSqlIdentityMismatchException.class, () -> factory.open(expected, "secret"));

        Assertions.assertTrue(exception.getMessage().contains("'alice'@'10.%'"));
        Assertions.assertTrue(exception.getMessage().contains("'alice'@'%'"));
        Mockito.verify(connection).close();
    }

    @Test
    void acceptsConnectionWhenLoopbackPreservesHttpIdentity() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        JdbcWebSqlConnectionFactory factory = factory(connection, "'alice'@'%'");
        UserIdentity expected = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");

        Assertions.assertSame(connection, factory.open(expected, "secret"));
        Mockito.verify(connection, Mockito.never()).close();
    }

    private JdbcWebSqlConnectionFactory factory(Connection connection, String currentUser) {
        return new JdbcWebSqlConnectionFactory() {
            @Override
            public Connection open(String user, String password) {
                return connection;
            }

            @Override
            String currentUser(Connection ignoredConnection) throws SQLException {
                return currentUser;
            }
        };
    }
}

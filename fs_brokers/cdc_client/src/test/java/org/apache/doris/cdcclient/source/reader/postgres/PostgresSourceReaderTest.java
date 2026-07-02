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

package org.apache.doris.cdcclient.source.reader.postgres;

import static org.junit.jupiter.api.Assertions.assertSame;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

class PostgresSourceReaderTest {

    @Test
    void cleanupConnectionUsesDebeziumDirectConnectionFactory() throws Exception {
        JdbcConfiguration config =
                JdbcConfiguration.adapt(
                        Configuration.create()
                                .with(JdbcConfiguration.HOSTNAME, "localhost")
                                .with(JdbcConfiguration.PORT, 5432)
                                .with(JdbcConfiguration.DATABASE, "postgres")
                                .with(JdbcConfiguration.USER, "user")
                                .with(JdbcConfiguration.PASSWORD, "password")
                                .build());

        try (PostgresConnection expected =
                        new PostgresConnection(config, PostgresConnection.CONNECTION_GENERAL);
                PostgresConnection actual =
                        PostgresSourceReader.createCleanupConnection(config)) {
            assertSame(connectionFactory(expected), connectionFactory(actual));
        }
    }

    private static Object connectionFactory(JdbcConnection connection) throws Exception {
        Field field = JdbcConnection.class.getDeclaredField("factory");
        field.setAccessible(true);
        return field.get(connection);
    }
}

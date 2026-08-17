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

package org.apache.doris.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;

class BaseJdbcExecutorTest {
    @Test
    void disableAutoCommitSkipsNonTransactionalDrivers() {
        AtomicBoolean setAutoCommitCalled = new AtomicBoolean();
        Connection connection = connectionWithTransactionSupport(false, setAutoCommitCalled, null);

        Assertions.assertDoesNotThrow(
                () -> BaseJdbcExecutor.disableAutoCommitIfSupported(connection));
        Assertions.assertFalse(setAutoCommitCalled.get());
    }

    @Test
    void disableAutoCommitAllowsUnsupportedTransactionalDrivers() {
        AtomicBoolean setAutoCommitCalled = new AtomicBoolean();
        Connection connection = connectionWithTransactionSupport(
                true, setAutoCommitCalled, new SQLFeatureNotSupportedException("unsupported"));

        Assertions.assertDoesNotThrow(
                () -> BaseJdbcExecutor.disableAutoCommitIfSupported(connection));
        Assertions.assertTrue(setAutoCommitCalled.get());
    }

    @Test
    void disableAutoCommitPropagatesOtherSqlErrors() {
        Connection connection = connectionWithTransactionSupport(
                true, new AtomicBoolean(), new SQLException("connection failed"));

        Assertions.assertThrows(SQLException.class,
                () -> BaseJdbcExecutor.disableAutoCommitIfSupported(connection));
    }

    private static Connection connectionWithTransactionSupport(
            boolean supportsTransactions, AtomicBoolean setAutoCommitCalled, SQLException exception) {
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                BaseJdbcExecutorTest.class.getClassLoader(),
                new Class<?>[] {DatabaseMetaData.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("supportsTransactions")) {
                        return supportsTransactions;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return (Connection) Proxy.newProxyInstance(
                BaseJdbcExecutorTest.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getMetaData")) {
                        return metadata;
                    }
                    if (method.getName().equals("setAutoCommit")) {
                        setAutoCommitCalled.set(true);
                        if (exception != null) {
                            throw exception;
                        }
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}

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

import java.sql.PreparedStatement;
import java.sql.SQLException;

/** Resource acquisition and cleanup boundaries shared by JDBC readers and writers. */
final class JdbcCleanup {
    private JdbcCleanup() {
    }

    @FunctionalInterface
    interface Action {
        void run() throws Exception;
    }

    static Exception attempt(Exception primary, Action action) {
        try {
            action.run();
        } catch (Exception cleanup) {
            if (primary == null) {
                return cleanup;
            }
            if (primary != cleanup) {
                primary.addSuppressed(cleanup);
            }
        }
        return primary;
    }

    static PreparedStatement setFetchSizeOrClose(PreparedStatement statement, int fetchSize) throws SQLException {
        try {
            statement.setFetchSize(fetchSize);
            return statement;
        } catch (SQLException | RuntimeException failure) {
            attempt(failure, statement::close);
            throw failure;
        }
    }
}

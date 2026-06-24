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

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValueConverter;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Strategy interface for database-specific JDBC type handling.
 *
 * <p>Each supported database type (MySQL, Oracle, PostgreSQL, etc.) has an implementation
 * that customizes how column values are read from ResultSets, how connections are configured,
 * and how output values are converted.
 *
 * <p>Used by {@link JdbcJniScanner} to provide database-specific behavior without
 * requiring the old {@link BaseJdbcExecutor} class hierarchy.
 */
public interface JdbcTypeHandler {

    /**
     * Extract a column value from the ResultSet with database-specific handling.
     *
     * @param rs           ResultSet positioned at the current row
     * @param columnIndex  1-based column index
     * @param type         Doris column type
     * @param metadata     ResultSetMetaData (for inspecting JDBC column type)
     * @return the column value, or null if the value is SQL NULL
     */
    Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                          ResultSetMetaData metadata) throws SQLException;

    /**
     * Create a converter for post-processing column values before writing to VectorTable.
     * Returns null if no conversion is needed for this column type.
     *
     * @param columnType    Doris column type
     * @param replaceString replacement string indicator ("bitmap", "hll", "jsonb", or "not_replace")
     * @return a converter, or null
     */
    ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString);

    /**
     * Set the HikariCP validation query for connection health checks.
     * Default: "SELECT 1". Override for databases with different syntax
     * (e.g., Oracle: "SELECT 1 FROM DUAL", DB2: "SELECT 1 FROM SYSIBM.SYSDUMMY1").
     */
    default void setValidationQuery(HikariDataSource ds) {
        ds.setConnectionTestQuery("SELECT 1");
    }

    /**
     * Initialize a PreparedStatement with database-specific settings.
     * Default: TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, fetchSize as given.
     * Override for databases that need special streaming settings (e.g., MySQL: Integer.MIN_VALUE).
     */
    default PreparedStatement initializeStatement(Connection conn, String sql,
                                                 int fetchSize) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }

    /**
     * Abort a read connection properly before closing resources.
     * Some databases (MySQL, SQLServer) need to abort() the connection to avoid
     * draining all remaining results, which can cause hangs on large result sets.
     */
    default void abortReadConnection(Connection conn, ResultSet rs) throws SQLException {
        // default: no-op
    }

    /**
     * Set JVM-level system properties needed by specific JDBC drivers.
     * Called once during scanner initialization.
     */
    default void setSystemProperties() {
        System.setProperty("com.zaxxer.hikari.useWeakReferences", "true");
    }
}

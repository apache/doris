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

/**
 * Factory to create the appropriate {@link JdbcTypeHandler} based on the database type.
 */
public class JdbcTypeHandlerFactory {

    /**
     * Create a type handler for the given database type string.
     *
     * @param tableType database type name (case-insensitive), matching TOdbcTableType enum names
     * @return the appropriate type handler
     */
    public static JdbcTypeHandler create(String tableType) {
        if (tableType == null || tableType.isEmpty()) {
            return new DefaultTypeHandler();
        }
        switch (tableType.toUpperCase()) {
            case "MYSQL":
            case "OCEANBASE":
                return new MySQLTypeHandler();
            case "ORACLE":
            case "OCEANBASE_ORACLE":
                return new OracleTypeHandler();
            case "POSTGRESQL":
                return new PostgreSQLTypeHandler();
            case "CLICKHOUSE":
                return new ClickHouseTypeHandler();
            case "SQLSERVER":
                return new SQLServerTypeHandler();
            case "DB2":
                return new DB2TypeHandler();
            case "SAP_HANA":
                return new SapHanaTypeHandler();
            case "TRINO":
            case "PRESTO":
                return new TrinoTypeHandler();
            case "GBASE":
                return new GbaseTypeHandler();
            default:
                return new DefaultTypeHandler();
        }
    }
}

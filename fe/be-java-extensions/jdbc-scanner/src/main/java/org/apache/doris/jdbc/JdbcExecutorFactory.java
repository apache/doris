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

import org.apache.doris.thrift.TOdbcTableType;

public class JdbcExecutorFactory {
    public static String getExecutorClass(TOdbcTableType type) {
        switch (type) {
            case MYSQL:
                return "org/apache/doris/jdbc/MySQLJdbcExecutor";
            case ORACLE:
                return "org/apache/doris/jdbc/OracleJdbcExecutor";
            case POSTGRESQL:
                return "org/apache/doris/jdbc/PostgreSQLJdbcExecutor";
            case SQLSERVER:
                return "org/apache/doris/jdbc/SQLServerJdbcExecutor";
            case DB2:
                return "org/apache/doris/jdbc/DB2JdbcExecutor";
            case SAP_HANA:
                return "org/apache/doris/jdbc/SapHanaJdbcExecutor";
            default:
                return "org/apache/doris/jdbc/DefaultJdbcExecutor";
        }
    }
}

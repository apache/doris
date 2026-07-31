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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.DorisConnectorException;

/**
 * Enumerates the supported JDBC database types and provides URL-based detection.
 */
public enum JdbcDbType {

    MYSQL("MYSQL"),
    POSTGRESQL("POSTGRESQL"),
    ORACLE("ORACLE"),
    SQLSERVER("SQLSERVER"),
    CLICKHOUSE("CLICKHOUSE"),
    SAP_HANA("SAP_HANA"),
    TRINO("TRINO"),
    PRESTO("PRESTO"),
    OCEANBASE("OCEANBASE"),
    OCEANBASE_ORACLE("OCEANBASE_ORACLE"),
    DB2("DB2"),
    GBASE("GBASE");

    private final String label;

    JdbcDbType(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    /**
     * Detect database type from a JDBC URL prefix.
     */
    public static JdbcDbType parseFromUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            throw new DorisConnectorException("JDBC URL is null");
        }
        String url = jdbcUrl.trim();
        if (url.startsWith("jdbc:mysql") || url.startsWith("jdbc:mariadb")) {
            return MYSQL;
        } else if (url.startsWith("jdbc:postgresql")) {
            return POSTGRESQL;
        } else if (url.startsWith("jdbc:oracle")) {
            return ORACLE;
        } else if (url.startsWith("jdbc:sqlserver")) {
            return SQLSERVER;
        } else if (url.startsWith("jdbc:clickhouse")) {
            return CLICKHOUSE;
        } else if (url.startsWith("jdbc:sap")) {
            return SAP_HANA;
        } else if (url.startsWith("jdbc:trino")) {
            return TRINO;
        } else if (url.startsWith("jdbc:presto")) {
            return PRESTO;
        } else if (url.startsWith("jdbc:oceanbase")) {
            return OCEANBASE;
        } else if (url.startsWith("jdbc:db2")) {
            return DB2;
        } else if (url.startsWith("jdbc:gbase")) {
            return GBASE;
        }
        throw new DorisConnectorException("Unsupported JDBC URL prefix: " + url);
    }
}

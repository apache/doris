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

package org.apache.doris.connector.jdbc.client;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.jdbc.JdbcDbType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * OceanBase-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcOceanBaseClient}.
 *
 * <p>OceanBase can run in MySQL-compatible or Oracle-compatible mode.
 * The compatibility mode is detected at runtime and type mapping
 * is delegated to the appropriate client.</p>
 */
public class JdbcOceanBaseConnectorClient extends JdbcConnectorClient {

    private static final Logger LOG = LogManager.getLogger(JdbcOceanBaseConnectorClient.class);

    private volatile JdbcConnectorClient delegate;

    public JdbcOceanBaseConnectorClient(
            String catalogName, JdbcDbType dbType, String jdbcUrl,
            boolean onlySpecifiedDatabase,
            Map<String, Boolean> includeDatabaseMap,
            Map<String, Boolean> excludeDatabaseMap,
            boolean enableMappingVarbinary,
            boolean enableMappingTimestampTz) {
        super(catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                includeDatabaseMap, excludeDatabaseMap,
                enableMappingVarbinary, enableMappingTimestampTz);
    }

    private JdbcConnectorClient getDelegate() {
        if (delegate == null) {
            synchronized (this) {
                if (delegate == null) {
                    delegate = detectCompatMode();
                }
            }
        }
        return delegate;
    }

    private JdbcConnectorClient detectCompatMode() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        JdbcConnectorClient result;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT ob_compatibility_mode()");
            if (rs.next()) {
                String mode = rs.getString(1);
                if ("ORACLE".equalsIgnoreCase(mode)) {
                    result = new JdbcOracleConnectorClient(
                            catalogName, JdbcDbType.ORACLE, jdbcUrl,
                            onlySpecifiedDatabase, includeDatabaseMap, excludeDatabaseMap,
                            enableMappingVarbinary, enableMappingTimestampTz);
                } else {
                    result = new JdbcMySQLConnectorClient(
                            catalogName, JdbcDbType.MYSQL, jdbcUrl,
                            onlySpecifiedDatabase, includeDatabaseMap, excludeDatabaseMap,
                            enableMappingVarbinary, enableMappingTimestampTz);
                }
            } else {
                result = new JdbcMySQLConnectorClient(
                        catalogName, JdbcDbType.MYSQL, jdbcUrl,
                        onlySpecifiedDatabase, includeDatabaseMap, excludeDatabaseMap,
                        enableMappingVarbinary, enableMappingTimestampTz);
            }
        } catch (SQLException e) {
            LOG.warn("Failed to detect OceanBase compatibility mode, defaulting to MySQL mode", e);
            result = new JdbcMySQLConnectorClient(
                    catalogName, JdbcDbType.MYSQL, jdbcUrl,
                    onlySpecifiedDatabase, includeDatabaseMap, excludeDatabaseMap,
                    enableMappingVarbinary, enableMappingTimestampTz);
        } finally {
            closeResources(rs, stmt, conn);
        }
        // Share the class loader and data source so the delegate can get connections
        result.classLoader = this.classLoader;
        result.dataSource = this.dataSource;
        return result;
    }

    @Override
    public List<String> getDatabaseNameList() {
        return getDelegate().getDatabaseNameList();
    }

    @Override
    public List<String> getTablesNameList(String remoteDbName) {
        return getDelegate().getTablesNameList(remoteDbName);
    }

    @Override
    public boolean isTableExist(String remoteDbName, String remoteTableName) {
        return getDelegate().isTableExist(remoteDbName, remoteTableName);
    }

    @Override
    public List<JdbcFieldInfo> getJdbcColumnsInfo(String remoteDbName, String remoteTableName) {
        return getDelegate().getJdbcColumnsInfo(remoteDbName, remoteTableName);
    }

    @Override
    public List<String> getPrimaryKeys(String remoteDbName, String remoteTableName) {
        return getDelegate().getPrimaryKeys(remoteDbName, remoteTableName);
    }

    @Override
    public String getTableComment(String remoteDbName, String remoteTableName) {
        return getDelegate().getTableComment(remoteDbName, remoteTableName);
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        return getDelegate().jdbcTypeToConnectorType(fieldInfo);
    }

    /**
     * Returns the effective database type after compatibility mode detection.
     * For Oracle mode, this returns {@link JdbcDbType#OCEANBASE_ORACLE} so that
     * query builders and function pushdown configs handle Oracle syntax correctly.
     */
    @Override
    public JdbcDbType getDbType() {
        JdbcConnectorClient d = delegate;
        if (d != null) {
            return d.getDbType() == JdbcDbType.ORACLE ? JdbcDbType.OCEANBASE_ORACLE : JdbcDbType.OCEANBASE;
        }
        return super.getDbType();
    }
}

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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.PassthroughQueryTableHandle;
import org.apache.doris.connector.api.write.ConnectorWriteConfig;
import org.apache.doris.connector.api.write.ConnectorWriteType;
import org.apache.doris.connector.jdbc.client.JdbcConnectorClient;
import org.apache.doris.connector.jdbc.client.JdbcFieldInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link ConnectorMetadata} implementation for JDBC sources.
 * Delegates metadata discovery to {@link JdbcConnectorClient}.
 */
public class JdbcConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(JdbcConnectorMetadata.class);

    private final JdbcConnectorClient client;
    private final Map<String, String> properties;

    public JdbcConnectorMetadata(JdbcConnectorClient client, Map<String, String> properties) {
        this.client = client;
        this.properties = properties;
    }

    private JdbcIdentifierMapper getIdentifierMapper(ConnectorSession session) {
        boolean isLowerCaseMetaNames = Boolean.parseBoolean(
                properties.getOrDefault(
                        JdbcConnectorProperties.LOWER_CASE_META_NAMES, "false"));
        String metaNamesMapping = properties.getOrDefault(
                JdbcConnectorProperties.META_NAMES_MAPPING, "");
        int lowerCaseTableNames = 0;
        if (session != null) {
            String val = session.getSessionProperties().get(
                    JdbcConnectorProperties.LOWER_CASE_TABLE_NAMES);
            if (val != null) {
                try {
                    lowerCaseTableNames = Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
        return new JdbcIdentifierMapper(
                lowerCaseTableNames != 0,
                isLowerCaseMetaNames,
                metaNamesMapping);
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return client.getDatabaseNameList();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return client.getDatabaseNameList().contains(dbName);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return client.getTablesNameList(dbName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        if (client.isTableExist(dbName, tableName)) {
            return Optional.of(new JdbcTableHandle(dbName, tableName));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        if (handle instanceof PassthroughQueryTableHandle) {
            return new ConnectorTableSchema("__passthrough_query__",
                    Collections.emptyList(), "JDBC", properties);
        }
        JdbcTableHandle jdbcHandle = (JdbcTableHandle) handle;
        String dbName = jdbcHandle.getRemoteDbName();
        String tableName = jdbcHandle.getRemoteTableName();

        List<JdbcFieldInfo> fields = client.getJdbcColumnsInfo(dbName, tableName);

        List<ConnectorColumn> columns = new ArrayList<>(fields.size());
        for (JdbcFieldInfo field : fields) {
            ConnectorType connectorType = client.jdbcTypeToConnectorType(field);
            // isKey defaults to true for all columns, matching legacy behavior.
            // The old JdbcClient.getJdbcColumnsInfo() hardcoded setKey(true) for all columns.
            columns.add(new ConnectorColumn(
                    field.getColumnName(),
                    connectorType,
                    field.getRemarks(),
                    field.isAllowNull(),
                    null,
                    true));
        }
        return new ConnectorTableSchema(tableName, columns, "JDBC", properties);
    }

    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session, ConnectorTableHandle handle) {
        if (handle instanceof PassthroughQueryTableHandle) {
            return Optional.empty();
        }
        JdbcTableHandle jdbcHandle = (JdbcTableHandle) handle;
        long rowCount = client.getRowCount(jdbcHandle.getRemoteDbName(), jdbcHandle.getRemoteTableName());
        if (rowCount >= 0) {
            return Optional.of(new ConnectorTableStatistics(rowCount, -1));
        }
        return Optional.empty();
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        if (handle instanceof PassthroughQueryTableHandle) {
            return Collections.emptyMap();
        }
        JdbcTableHandle jdbcHandle = (JdbcTableHandle) handle;
        String dbName = jdbcHandle.getRemoteDbName();
        String tableName = jdbcHandle.getRemoteTableName();

        JdbcIdentifierMapper mapper = getIdentifierMapper(session);
        List<JdbcFieldInfo> fields = client.getJdbcColumnsInfo(dbName, tableName);
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(fields.size());
        for (JdbcFieldInfo field : fields) {
            String remoteName = field.getColumnName();
            String localName = mapper.fromRemoteColumnName(dbName, tableName, remoteName);
            handles.put(localName, new JdbcColumnHandle(localName, remoteName));
        }
        return handles;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public org.apache.doris.thrift.TTableDescriptor buildTableDescriptor(
            ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        org.apache.doris.thrift.TJdbcTable tJdbcTable = new org.apache.doris.thrift.TJdbcTable();
        tJdbcTable.setCatalogId(catalogId);
        tJdbcTable.setJdbcUrl(properties.getOrDefault(JdbcConnectorProperties.JDBC_URL, ""));
        tJdbcTable.setJdbcUser(properties.getOrDefault(JdbcConnectorProperties.USER, ""));
        tJdbcTable.setJdbcPassword(properties.getOrDefault(JdbcConnectorProperties.PASSWORD, ""));
        tJdbcTable.setJdbcTableName(remoteName);
        tJdbcTable.setJdbcDriverClass(properties.getOrDefault(JdbcConnectorProperties.DRIVER_CLASS, ""));
        tJdbcTable.setJdbcDriverUrl(properties.getOrDefault(JdbcConnectorProperties.DRIVER_URL, ""));
        tJdbcTable.setJdbcResourceName("");
        tJdbcTable.setJdbcDriverChecksum(properties.getOrDefault(JdbcConnectorProperties.DRIVER_CHECKSUM, ""));
        tJdbcTable.setConnectionPoolMinSize(JdbcConnectorProperties.getInt(properties,
                JdbcConnectorProperties.CONNECTION_POOL_MIN_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MIN_SIZE));
        tJdbcTable.setConnectionPoolMaxSize(JdbcConnectorProperties.getInt(properties,
                JdbcConnectorProperties.CONNECTION_POOL_MAX_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_SIZE));
        tJdbcTable.setConnectionPoolMaxWaitTime(JdbcConnectorProperties.getInt(properties,
                JdbcConnectorProperties.CONNECTION_POOL_MAX_WAIT_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_WAIT_TIME));
        tJdbcTable.setConnectionPoolMaxLifeTime(JdbcConnectorProperties.getInt(properties,
                JdbcConnectorProperties.CONNECTION_POOL_MAX_LIFE_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_LIFE_TIME));
        tJdbcTable.setConnectionPoolKeepAlive(Boolean.parseBoolean(properties.getOrDefault(
                JdbcConnectorProperties.CONNECTION_POOL_KEEP_ALIVE,
                String.valueOf(JdbcConnectorProperties.DEFAULT_POOL_KEEP_ALIVE))));

        org.apache.doris.thrift.TTableDescriptor desc = new org.apache.doris.thrift.TTableDescriptor(
                tableId, org.apache.doris.thrift.TTableType.JDBC_TABLE,
                numCols, 0, tableName, "");
        desc.setJdbcTable(tJdbcTable);
        return desc;
    }

    // ========= ConnectorPushdownOps =========

    @Override
    public boolean supportsCastPredicatePushdown(ConnectorSession session) {
        return Boolean.parseBoolean(
                session.getSessionProperties()
                        .getOrDefault("enable_jdbc_cast_predicate_push_down", "true"));
    }

    // ========= ConnectorIdentifierOps =========

    @Override
    public String fromRemoteDatabaseName(ConnectorSession session, String remoteDatabaseName) {
        return getIdentifierMapper(session).fromRemoteDatabaseName(remoteDatabaseName);
    }

    @Override
    public String fromRemoteTableName(ConnectorSession session,
            String remoteDatabaseName, String remoteTableName) {
        return getIdentifierMapper(session).fromRemoteTableName(remoteDatabaseName, remoteTableName);
    }

    @Override
    public String fromRemoteColumnName(ConnectorSession session,
            String remoteDatabaseName, String remoteTableName,
            String remoteColumnName) {
        return getIdentifierMapper(session).fromRemoteColumnName(
                remoteDatabaseName, remoteTableName, remoteColumnName);
    }

    @Override
    public List<String> getPrimaryKeys(ConnectorSession session, String dbName, String tableName) {
        return client.getPrimaryKeys(dbName, tableName);
    }

    @Override
    public String getTableComment(ConnectorSession session, String dbName, String tableName) {
        return client.getTableComment(dbName, tableName);
    }

    @Override
    public void executeStmt(ConnectorSession session, String stmt) {
        client.executeStmt(stmt);
    }

    @Override
    public ConnectorTableSchema getColumnsFromQuery(ConnectorSession session, String query) {
        List<JdbcFieldInfo> fields = client.getColumnsFromQuery(query);
        List<ConnectorColumn> columns = new ArrayList<>(fields.size());
        for (JdbcFieldInfo field : fields) {
            ConnectorType connectorType = client.jdbcTypeToConnectorType(field);
            columns.add(new ConnectorColumn(
                    field.getColumnName(),
                    connectorType,
                    null,
                    true,
                    null,
                    true));
        }
        return new ConnectorTableSchema("query_result", columns, "JDBC", properties);
    }

    // ========= ConnectorWriteOps =========

    @Override
    public boolean supportsInsert() {
        return true;
    }

    @Override
    public ConnectorWriteConfig getWriteConfig(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        JdbcTableHandle jdbcHandle = (JdbcTableHandle) handle;
        String remoteDbName = jdbcHandle.getRemoteDbName();
        String remoteTableName = jdbcHandle.getRemoteTableName();
        JdbcDbType dbType = client.getDbType();

        // Build local column name list for INSERT SQL
        List<String> columnNames = columns.stream()
                .map(ConnectorColumn::getName)
                .collect(Collectors.toList());

        // Build local→remote column name mapping via column handles
        Map<String, ConnectorColumnHandle> colHandles = getColumnHandles(session, handle);
        Map<String, String> remoteColumnNames = new HashMap<>();
        for (Map.Entry<String, ConnectorColumnHandle> entry : colHandles.entrySet()) {
            JdbcColumnHandle ch = (JdbcColumnHandle) entry.getValue();
            remoteColumnNames.put(ch.getLocalName(), ch.getRemoteName());
        }

        String insertSql = JdbcIdentifierQuoter.buildInsertSql(
                dbType, remoteDbName, remoteTableName, remoteColumnNames, columnNames);

        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("jdbc_url", properties.getOrDefault(JdbcConnectorProperties.JDBC_URL, ""));
        writeProps.put("jdbc_user", properties.getOrDefault(JdbcConnectorProperties.USER, ""));
        writeProps.put("jdbc_password", properties.getOrDefault(JdbcConnectorProperties.PASSWORD, ""));
        writeProps.put("jdbc_driver_url", properties.getOrDefault(JdbcConnectorProperties.DRIVER_URL, ""));
        writeProps.put("jdbc_driver_class", properties.getOrDefault(JdbcConnectorProperties.DRIVER_CLASS, ""));
        writeProps.put("jdbc_driver_checksum",
                properties.getOrDefault(JdbcConnectorProperties.DRIVER_CHECKSUM, ""));
        writeProps.put("jdbc_table_name", remoteTableName);
        writeProps.put("jdbc_resource_name", "");
        writeProps.put("jdbc_table_type", dbType.name());
        writeProps.put("jdbc_insert_sql", insertSql);
        writeProps.put("jdbc_use_transaction",
                session.getSessionProperties().getOrDefault("enable_odbc_transcation", "false"));
        writeProps.put("jdbc_catalog_id", String.valueOf(session.getCatalogId()));

        // Connection pool settings
        writeProps.put("connection_pool_min_size", String.valueOf(
                JdbcConnectorProperties.getInt(properties,
                        JdbcConnectorProperties.CONNECTION_POOL_MIN_SIZE,
                        JdbcConnectorProperties.DEFAULT_POOL_MIN_SIZE)));
        writeProps.put("connection_pool_max_size", String.valueOf(
                JdbcConnectorProperties.getInt(properties,
                        JdbcConnectorProperties.CONNECTION_POOL_MAX_SIZE,
                        JdbcConnectorProperties.DEFAULT_POOL_MAX_SIZE)));
        writeProps.put("connection_pool_max_wait_time", String.valueOf(
                JdbcConnectorProperties.getInt(properties,
                        JdbcConnectorProperties.CONNECTION_POOL_MAX_WAIT_TIME,
                        JdbcConnectorProperties.DEFAULT_POOL_MAX_WAIT_TIME)));
        writeProps.put("connection_pool_max_life_time", String.valueOf(
                JdbcConnectorProperties.getInt(properties,
                        JdbcConnectorProperties.CONNECTION_POOL_MAX_LIFE_TIME,
                        JdbcConnectorProperties.DEFAULT_POOL_MAX_LIFE_TIME)));
        writeProps.put("connection_pool_keep_alive", String.valueOf(
                Boolean.parseBoolean(properties.getOrDefault(
                        JdbcConnectorProperties.CONNECTION_POOL_KEEP_ALIVE, "false"))));

        return ConnectorWriteConfig.builder(ConnectorWriteType.JDBC_WRITE)
                .properties(writeProps)
                .build();
    }

    @Override
    public ConnectorInsertHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        // JDBC writes are executed directly by BE via PreparedStatement.
        // No FE-side transaction to begin — return a no-op handle.
        return new JdbcInsertHandle();
    }

    @Override
    public void finishInsert(ConnectorSession session,
            ConnectorInsertHandle handle,
            Collection<byte[]> fragments) {
        // No-op: BE commits each row via JDBC directly.
    }

    /**
     * No-op insert handle for JDBC writes.
     * JDBC writes don't require FE-side transaction management.
     */
    private static class JdbcInsertHandle implements ConnectorInsertHandle {
    }
}

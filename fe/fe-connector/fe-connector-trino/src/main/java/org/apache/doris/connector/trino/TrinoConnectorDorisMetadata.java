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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.transaction.IsolationLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Implements Doris {@link ConnectorMetadata} by delegating to a Trino Connector's metadata.
 *
 * <p>Each method opens a read-only Trino transaction (READ_UNCOMMITTED), invokes the
 * corresponding Trino ConnectorMetadata method, and maps results to Doris connector API types.</p>
 */
public class TrinoConnectorDorisMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(TrinoConnectorDorisMetadata.class);

    private final io.trino.spi.connector.Connector trinoConnector;
    private final Session trinoSession;
    private final CatalogHandle trinoCatalogHandle;

    public TrinoConnectorDorisMetadata(
            io.trino.spi.connector.Connector trinoConnector,
            Session trinoSession,
            CatalogHandle trinoCatalogHandle) {
        this.trinoConnector = trinoConnector;
        this.trinoSession = trinoSession;
        this.trinoCatalogHandle = trinoCatalogHandle;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        io.trino.spi.connector.ConnectorSession connSession =
                trinoSession.toConnectorSession(trinoCatalogHandle);
        io.trino.spi.connector.ConnectorTransactionHandle txn =
                trinoConnector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true, true);
        io.trino.spi.connector.ConnectorMetadata metadata =
                trinoConnector.getMetadata(connSession, txn);
        return metadata.listSchemaNames(connSession);
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return listDatabaseNames(session).contains(dbName);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        io.trino.spi.connector.ConnectorSession connSession =
                trinoSession.toConnectorSession(trinoCatalogHandle);
        io.trino.spi.connector.ConnectorTransactionHandle txn =
                trinoConnector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true, true);
        io.trino.spi.connector.ConnectorMetadata metadata =
                trinoConnector.getMetadata(connSession, txn);

        Optional<String> schemaName = Optional.of(dbName);
        List<SchemaTableName> tables = metadata.listTables(connSession, schemaName);
        return tables.stream()
                .map(SchemaTableName::getTableName)
                .collect(Collectors.toList());
    }

    public boolean tableExists(ConnectorSession session, String dbName, String tableName) {
        return getTableHandle(session, dbName, tableName).isPresent();
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        if (dbName == null || dbName.isEmpty() || tableName == null || tableName.isEmpty()) {
            return Optional.empty();
        }

        io.trino.spi.connector.ConnectorSession connSession =
                trinoSession.toConnectorSession(trinoCatalogHandle);
        io.trino.spi.connector.ConnectorTransactionHandle txn =
                trinoConnector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true, true);
        io.trino.spi.connector.ConnectorMetadata metadata =
                trinoConnector.getMetadata(connSession, txn);

        SchemaTableName schemaTableName = new SchemaTableName(dbName, tableName);
        io.trino.spi.connector.ConnectorTableHandle trinoHandle =
                metadata.getTableHandle(connSession, schemaTableName,
                        Optional.empty(), Optional.empty());
        if (trinoHandle == null) {
            return Optional.empty();
        }

        // Eagerly resolve column handles + metadata for the table
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(connSession, trinoHandle);
        ImmutableMap.Builder<String, ColumnHandle> columnHandleMapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, ColumnMetadata> columnMetadataMapBuilder = ImmutableMap.builder();
        for (Map.Entry<String, ColumnHandle> entry : handles.entrySet()) {
            String colName = entry.getKey().toLowerCase(Locale.ENGLISH);
            columnHandleMapBuilder.put(colName, entry.getValue());
            ColumnMetadata colMeta = metadata.getColumnMetadata(
                    connSession, trinoHandle, entry.getValue());
            columnMetadataMapBuilder.put(colMeta.getName(), colMeta);
        }

        return Optional.of(new TrinoTableHandle(
                dbName, tableName, trinoHandle,
                columnHandleMapBuilder.buildOrThrow(),
                columnMetadataMapBuilder.buildOrThrow()));
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        TrinoTableHandle trinoHandle = (TrinoTableHandle) handle;

        io.trino.spi.connector.ConnectorSession connSession =
                trinoSession.toConnectorSession(trinoCatalogHandle);
        io.trino.spi.connector.ConnectorTransactionHandle txn =
                trinoConnector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true, true);
        io.trino.spi.connector.ConnectorMetadata metadata =
                trinoConnector.getMetadata(connSession, txn);

        Map<String, ColumnHandle> columnHandles = trinoHandle.getColumnHandleMap();
        if (columnHandles == null || columnHandles.isEmpty()) {
            columnHandles = metadata.getColumnHandles(
                    connSession, trinoHandle.getTrinoTableHandle());
        }

        List<ConnectorColumn> columns = new ArrayList<>();
        for (ColumnHandle columnHandle : columnHandles.values()) {
            ColumnMetadata colMeta = metadata.getColumnMetadata(
                    connSession, trinoHandle.getTrinoTableHandle(), columnHandle);
            if (colMeta.isHidden()) {
                continue;
            }
            ConnectorType connType = TrinoTypeMapping.toConnectorType(colMeta.getType());
            columns.add(new ConnectorColumn(
                    colMeta.getName(),
                    connType,
                    colMeta.getComment(),
                    true,
                    null));
        }

        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("trino.connector.table", "true");

        return new ConnectorTableSchema(
                trinoHandle.getTableName(),
                columns,
                "trino_connector",
                Collections.unmodifiableMap(tableProps));
    }

    @Override
    public Map<String, org.apache.doris.connector.api.handle.ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        TrinoTableHandle trinoHandle = (TrinoTableHandle) handle;
        Map<String, ColumnHandle> trinoHandles = trinoHandle.getColumnHandleMap();
        if (trinoHandles == null || trinoHandles.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, org.apache.doris.connector.api.handle.ConnectorColumnHandle> result = new HashMap<>();
        for (String colName : trinoHandles.keySet()) {
            result.put(colName, new TrinoColumnHandle(colName));
        }
        return result;
    }
}

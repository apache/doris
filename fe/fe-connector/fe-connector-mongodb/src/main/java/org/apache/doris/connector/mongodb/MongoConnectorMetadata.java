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

package org.apache.doris.connector.mongodb;

import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScopes;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class MongoConnectorMetadata implements ConnectorMetadata {
    static final String TABLE_NAMESPACE = "mongodb.table";
    private final MongoCatalogClient client;
    private final MongoCatalogProperties properties;

    MongoConnectorMetadata(MongoCatalogClient client, MongoCatalogProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return client.databases();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return listDatabaseNames(session).contains(dbName);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        if (!allowedDatabase(dbName)) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        for (Document object : client.collections(dbName)) {
            if (visible(object)) {
                names.add(object.getString("name"));
            }
        }
        return names;
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(ConnectorSession session, String dbName, String tableName) {
        if (!allowedDatabase(dbName) || tableName.startsWith("system.")) {
            return Optional.empty();
        }
        return ConnectorStatementScopes.resolveInStatement(session, TABLE_NAMESPACE, dbName, tableName,
                () -> client.collection(dbName, tableName).filter(MongoConnectorMetadata::visible)
                        .map(metadata -> new MongoTableHandle(dbName, metadata, properties)));
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
        MongoTableHandle table = (MongoTableHandle) handle;
        return new ConnectorTableSchema(table.getName(), table.getSchema().getColumns(), "MongoDB",
                Map.of("mongodb.object_kind", table.getObjectKind(),
                        "mongodb.schema_source", table.getSchema().getSource(),
                        "mongodb.schema_version", table.getSchema().getVersion()));
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle handle) {
        Map<String, ConnectorColumnHandle> columns = new LinkedHashMap<>();
        MongoSchema schema = ((MongoTableHandle) handle).getSchema();
        for (MongoSchema.Binding column : schema.getBindings()) {
            columns.put(column.getName(), new MongoColumnHandle(column, schema.getVersion()));
        }
        return columns;
    }

    @Override
    public TTableDescriptor buildTableDescriptor(ConnectorSession session, long tableId, String tableName,
            String dbName, String remoteName, int numCols, long catalogId) {
        // The generic file scan path expects a Hive-shaped descriptor, without source credentials.
        TTableDescriptor descriptor = new TTableDescriptor(tableId, TTableType.HIVE_TABLE, numCols, 0,
                tableName, dbName);
        descriptor.setHiveTable(new THiveTable(dbName, remoteName, new HashMap<>()));
        return descriptor;
    }

    private boolean allowedDatabase(String database) {
        return properties.getDatabases().isEmpty() || properties.getDatabases().contains(database);
    }

    private static boolean visible(Document metadata) {
        String name = metadata.getString("name");
        String kind = metadata.getString("type");
        return name != null && !name.startsWith("system.")
                && Set.of("collection", "view", "timeseries").contains(kind);
    }
}

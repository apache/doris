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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import io.delta.kernel.Snapshot;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

final class DeltaLakeConnectorMetadata implements ConnectorMetadata {
    private static final String TABLE_ID = "deltalake.table-id";
    private final DeltaLakeRuntime runtime;
    private final DeltaLakeCatalogProperties properties;
    private final DeltaLakeSnapshotCache snapshots;

    DeltaLakeConnectorMetadata(DeltaLakeRuntime runtime, DeltaLakeCatalogProperties properties,
            DeltaLakeSnapshotCache snapshots) {
        this.runtime = runtime;
        this.properties = properties;
        this.snapshots = snapshots;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return runtime.execute(runtime.resolver::listDatabases);
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String database) {
        return listDatabaseNames(session).contains(database);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String database) {
        return runtime.execute(() -> runtime.resolver.listTables(database));
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(ConnectorSession session, String database, String table) {
        return DeltaLakeStatementScope.resolve(session, database, table, () -> runtime.execute(() -> {
            Optional<String> location = runtime.resolver.tableLocation(database, table);
            if (location.isEmpty()) {
                return Optional.empty();
            }
            Snapshot snapshot = snapshots.latest(session, database, table, location.get(), runtime.engine);
            return Optional.of(new DeltaLakeTableHandle(database, table, snapshot));
        })).map(handle -> handle);
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
        DeltaLakeTableHandle delta = (DeltaLakeTableHandle) handle;
        return runtime.execute(() -> DeltaLakeSchemaBuilder.build(delta.getTable(),
                delta.snapshot(runtime.engine), properties));
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        return getTableSchema(session, applySnapshot(session, handle, snapshot));
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle handle) {
        Map<String, ConnectorColumnHandle> columns = new LinkedHashMap<>();
        for (ConnectorColumn column : getTableSchema(session, handle).getColumns()) {
            columns.put(column.getName(), new NamedColumnHandle(column.getName()));
        }
        return columns;
    }

    @Override
    public List<String> listPartitionNames(ConnectorSession session, ConnectorTableHandle handle) {
        return listPartitions(session, handle, Optional.empty()).stream()
                .map(ConnectorPartitionInfo::getPartitionName).collect(Collectors.toList());
    }

    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session, ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter) {
        return runtime.execute(() -> {
            Snapshot snapshot = ((DeltaLakeTableHandle) handle).snapshot(runtime.engine);
            if (snapshot.getPartitionColumnNames().isEmpty()) {
                return Collections.emptyList();
            }
            Map<String, ConnectorPartitionInfo> partitions = new LinkedHashMap<>();
            DeltaLakeScanFiles.forEach(snapshot, runtime.engine, false, row -> {
                Map<String, String> values = DeltaLakePartitionValues.normalize(
                        snapshot, DeltaKernelCompat.partitionValues(row));
                String name = DeltaLakePartitionValues.partitionName(values);
                partitions.computeIfAbsent(name, ignored -> new ConnectorPartitionInfo(name, values,
                        Collections.emptyMap(), values.values().stream().map(value -> value == null ? "" : value)
                                .collect(Collectors.toList()),
                        values.values().stream().map(value -> value == null).collect(Collectors.toList())));
            });
            return List.copyOf(partitions.values());
        });
    }

    @Override
    public boolean listsPartitionsAtSnapshot(ConnectorSession session, ConnectorTableHandle handle) {
        return true;
    }

    @Override
    public Optional<ConnectorMvccSnapshot> beginQuerySnapshot(ConnectorSession session, ConnectorTableHandle handle) {
        DeltaLakeTableHandle delta = (DeltaLakeTableHandle) handle;
        return Optional.of(ConnectorMvccSnapshot.builder().snapshotId(delta.getVersion())
                .schemaId(delta.getVersion()).retainSchema(true)
                .properties(Collections.singletonMap(TABLE_ID, delta.getTableId())).build());
    }

    @Override
    public ConnectorTableHandle applySnapshot(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        DeltaLakeTableHandle delta = (DeltaLakeTableHandle) handle;
        if (!delta.getTableId().equals(snapshot.getProperties().get(TABLE_ID))) {
            throw new DorisConnectorException("Delta snapshot belongs to another table: " + delta.getPath());
        }
        return runtime.execute(() -> delta.atVersion(runtime.engine, snapshot.getSnapshotId()));
    }

    @Override
    public TTableDescriptor buildTableDescriptor(ConnectorSession session, long tableId,
            String tableName, String database, String remoteName, int columns, long catalogId) {
        TTableDescriptor descriptor = new TTableDescriptor(
                tableId, TTableType.HIVE_TABLE, columns, 0, tableName, database);
        descriptor.setHiveTable(new THiveTable(database, tableName, Collections.emptyMap()));
        return descriptor;
    }
}

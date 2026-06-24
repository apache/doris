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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * ConnectorMetadata implementation for MaxCompute.
 * Delegates database/table discovery to {@link McStructureHelper}.
 */
public class MaxComputeConnectorMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(
            MaxComputeConnectorMetadata.class);

    private final Odps odps;
    private final McStructureHelper structureHelper;
    private final String defaultProject;

    public MaxComputeConnectorMetadata(Odps odps,
            McStructureHelper structureHelper,
            String defaultProject) {
        this.odps = odps;
        this.structureHelper = structureHelper;
        this.defaultProject = defaultProject;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return structureHelper.listDatabaseNames(odps, defaultProject);
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return structureHelper.databaseExist(odps, dbName);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session,
            String dbName) {
        return structureHelper.listTableNames(odps, dbName);
    }

    public boolean tableExists(ConnectorSession session, String dbName,
            String tableName) {
        return structureHelper.tableExist(odps, dbName, tableName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        if (!structureHelper.tableExist(odps, dbName, tableName)) {
            return Optional.empty();
        }
        Table odpsTable = structureHelper.getOdpsTable(
                odps, dbName, tableName);
        TableIdentifier tableId = structureHelper.getTableIdentifier(
                dbName, tableName);
        return Optional.of(new MaxComputeTableHandle(
                dbName, tableName, odpsTable, tableId));
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session,
            ConnectorTableHandle handle) {
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle;
        Table odpsTable = mcHandle.getOdpsTable();

        List<Column> dataColumns = odpsTable.getSchema().getColumns();
        List<Column> partColumns =
                odpsTable.getSchema().getPartitionColumns();

        List<ConnectorColumn> columns =
                new ArrayList<>(dataColumns.size() + partColumns.size());

        for (Column col : dataColumns) {
            columns.add(new ConnectorColumn(
                    col.getName(),
                    MCTypeMapping.toConnectorType(col.getTypeInfo()),
                    col.getComment(),
                    col.isNullable(),
                    null));
        }

        List<String> partitionColumnNames =
                new ArrayList<>(partColumns.size());
        for (Column partCol : partColumns) {
            partitionColumnNames.add(partCol.getName());
            columns.add(new ConnectorColumn(
                    partCol.getName(),
                    MCTypeMapping.toConnectorType(partCol.getTypeInfo()),
                    partCol.getComment(),
                    true,
                    null));
        }

        java.util.Map<String, String> props = new java.util.HashMap<>();
        if (!partitionColumnNames.isEmpty()) {
            props.put("partition_columns",
                    String.join(",", partitionColumnNames));
        }
        return new ConnectorTableSchema(
                mcHandle.getTableName(), columns, "MAX_COMPUTE", props);
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle;
        Table odpsTable = mcHandle.getOdpsTable();

        Map<String, ConnectorColumnHandle> result = new LinkedHashMap<>();
        for (Column col : odpsTable.getSchema().getColumns()) {
            result.put(col.getName(),
                    new MaxComputeColumnHandle(col.getName(), false));
        }
        for (Column partCol : odpsTable.getSchema().getPartitionColumns()) {
            result.put(partCol.getName(),
                    new MaxComputeColumnHandle(partCol.getName(), true));
        }
        return result;
    }
}

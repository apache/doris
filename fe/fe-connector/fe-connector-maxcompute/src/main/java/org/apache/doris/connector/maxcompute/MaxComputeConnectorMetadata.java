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
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorBucketSpec;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * ConnectorMetadata implementation for MaxCompute.
 * Delegates database/table discovery to {@link McStructureHelper}.
 */
public class MaxComputeConnectorMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(
            MaxComputeConnectorMetadata.class);

    private static final long MAX_LIFECYCLE_DAYS = 37231;
    private static final int MAX_BUCKET_NUM = 1024;

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

    // ==================== Partition listing ====================

    @Override
    public List<String> listPartitionNames(ConnectorSession session,
            ConnectorTableHandle handle) {
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle;
        List<Partition> partitions = structureHelper.getPartitions(
                odps, mcHandle.getDbName(), mcHandle.getTableName());
        List<String> names = new ArrayList<>(partitions.size());
        for (Partition partition : partitions) {
            names.add(partition.getPartitionSpec().toString(false, true));
        }
        return names;
    }

    /**
     * Lists all partitions. The {@code filter} is intentionally ignored: the
     * legacy SHOW PARTITIONS path ({@code MaxComputeExternalCatalog
     * #listPartitionNames}) returns the full partition set without pushing
     * predicates into ODPS, and this preserves that behavior. Partitions are
     * read directly from ODPS with no connector-side cache (P4-T02 / OQ-4).
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle;
        List<Partition> partitions = structureHelper.getPartitions(
                odps, mcHandle.getDbName(), mcHandle.getTableName());
        List<ConnectorPartitionInfo> result = new ArrayList<>(partitions.size());
        for (Partition partition : partitions) {
            PartitionSpec spec = partition.getPartitionSpec();
            Map<String, String> values = new LinkedHashMap<>();
            for (String key : spec.keys()) {
                values.put(key, spec.get(key));
            }
            result.add(new ConnectorPartitionInfo(
                    spec.toString(false, true), values, Collections.emptyMap()));
        }
        return result;
    }

    @Override
    public List<List<String>> listPartitionValues(ConnectorSession session,
            ConnectorTableHandle handle, List<String> partitionColumns) {
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle;
        List<Partition> partitions = structureHelper.getPartitions(
                odps, mcHandle.getDbName(), mcHandle.getTableName());
        List<List<String>> result = new ArrayList<>(partitions.size());
        for (Partition partition : partitions) {
            PartitionSpec spec = partition.getPartitionSpec();
            List<String> values = new ArrayList<>(partitionColumns.size());
            for (String column : partitionColumns) {
                values.add(spec.get(column));
            }
            result.add(values);
        }
        return result;
    }

    // ==================== Write / Transaction (P4-T03) ====================

    /**
     * Opens a connector transaction for a MaxCompute write statement. The
     * transaction id is the engine-side id allocated through the session, so it
     * matches the id registered in the engine transaction registry and stamped
     * into the data sink (see {@link MaxComputeConnectorTransaction}).
     *
     * <p>Gate-closed / dormant until the {@code max_compute} cutover: nothing
     * routes plugin-driven MaxCompute writes through this path yet. The ODPS
     * write session that backs commit / block allocation is created by the write
     * plan (P4-T04), which binds it via
     * {@link MaxComputeConnectorTransaction#setWriteSession}.</p>
     */
    @Override
    public ConnectorTransaction beginTransaction(ConnectorSession session) {
        return new MaxComputeConnectorTransaction(session.allocateTransactionId());
    }

    // ==================== DDL: Create/Drop Table ====================

    @Override
    public void createTable(ConnectorSession session,
            ConnectorCreateTableRequest request) {
        String dbName = request.getDbName();
        String tableName = request.getTableName();

        if (structureHelper.tableExist(odps, dbName, tableName)) {
            if (request.isIfNotExists()) {
                LOG.info("create table[{}.{}] which already exists",
                        dbName, tableName);
                return;
            }
            throw new DorisConnectorException("Table '" + tableName
                    + "' already exists in database '" + dbName + "'");
        }

        List<ConnectorColumn> columns = request.getColumns();
        validateColumns(columns);
        List<String> partitionColumns =
                identityPartitionColumns(request.getPartitionSpec());
        TableSchema schema = buildSchema(columns, partitionColumns);

        Long lifecycle = extractLifecycle(request.getProperties());
        Map<String, String> mcProperties =
                extractMaxComputeProperties(request.getProperties());
        Integer bucketNum = extractBucketNum(request.getBucketSpec());

        Tables.TableCreator creator = structureHelper.createTableCreator(
                odps, dbName, tableName, schema);
        if (request.isIfNotExists()) {
            creator.ifNotExists();
        }
        String comment = request.getComment();
        if (comment != null && !comment.isEmpty()) {
            creator.withComment(comment);
        }
        if (lifecycle != null) {
            creator.withLifeCycle(lifecycle);
        }
        if (!mcProperties.isEmpty()) {
            creator.withTblProperties(mcProperties);
        }
        if (bucketNum != null) {
            creator.withDeltaTableBucketNum(bucketNum);
        }

        try {
            creator.create();
        } catch (OdpsException e) {
            throw new DorisConnectorException("Failed to create MaxCompute table '"
                    + tableName + "': " + e.getMessage(), e);
        }
        LOG.info("created MaxCompute table {}.{}", dbName, tableName);
    }

    /**
     * Drops the table behind {@code handle}. The SPI signature carries no
     * {@code ifExists}; fe-core resolves the handle (absent when the table does
     * not exist) before routing here, so the remote drop is issued idempotently.
     */
    @Override
    public void dropTable(ConnectorSession session,
            ConnectorTableHandle handle) {
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle;
        String dbName = mcHandle.getDbName();
        String tableName = mcHandle.getTableName();
        try {
            structureHelper.dropTable(odps, dbName, tableName, true);
        } catch (OdpsException e) {
            throw new DorisConnectorException("Failed to drop MaxCompute table '"
                    + tableName + "': " + e.getMessage(), e);
        }
        LOG.info("dropped MaxCompute table {}.{}", dbName, tableName);
    }

    // ==================== DDL: Create/Drop Database ====================

    @Override
    public void createDatabase(ConnectorSession session, String dbName,
            Map<String, String> properties) {
        structureHelper.createDb(odps, dbName, false);
        LOG.info("created MaxCompute database {}", dbName);
    }

    @Override
    public void dropDatabase(ConnectorSession session, String dbName,
            boolean ifExists) {
        structureHelper.dropDb(odps, dbName, ifExists);
        LOG.info("dropped MaxCompute database {}", dbName);
    }

    // ==================== DDL helpers ====================

    private void validateColumns(List<ConnectorColumn> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new DorisConnectorException(
                    "Table must have at least one column.");
        }
        Set<String> seen = new HashSet<>();
        for (ConnectorColumn col : columns) {
            if (!seen.add(col.getName().toLowerCase())) {
                throw new DorisConnectorException(
                        "Duplicate column name: " + col.getName());
            }
            // Validate the type is representable in MaxCompute (throws otherwise).
            MCTypeMapping.toMcType(col.getType());
        }
    }

    /**
     * Extracts the identity partition column names, rejecting transform-based
     * partitioning (MaxCompute supports identity partitions only). Mirrors the
     * legacy {@code MaxComputeMetadataOps.validatePartitionDesc}.
     */
    private List<String> identityPartitionColumns(
            ConnectorPartitionSpec partitionSpec) {
        List<String> names = new ArrayList<>();
        if (partitionSpec == null) {
            return names;
        }
        for (ConnectorPartitionField field : partitionSpec.getFields()) {
            if (!"identity".equalsIgnoreCase(field.getTransform())) {
                throw new DorisConnectorException(
                        "MaxCompute does not support partition transform '"
                                + field.getTransform()
                                + "'. Only identity partitions are supported.");
            }
            names.add(field.getColumnName());
        }
        return names;
    }

    private TableSchema buildSchema(List<ConnectorColumn> columns,
            List<String> partitionColumns) {
        Set<String> partitionColLower = new HashSet<>();
        for (String name : partitionColumns) {
            partitionColLower.add(name.toLowerCase());
        }

        TableSchema schema = new TableSchema();
        for (ConnectorColumn col : columns) {
            if (!partitionColLower.contains(col.getName().toLowerCase())) {
                schema.addColumn(new Column(col.getName(),
                        MCTypeMapping.toMcType(col.getType()), col.getComment()));
            }
        }
        for (String partColName : partitionColumns) {
            ConnectorColumn col = findColumnByName(columns, partColName);
            if (col == null) {
                throw new DorisConnectorException("Partition column '"
                        + partColName + "' not found in column definitions.");
            }
            schema.addPartitionColumn(new Column(col.getName(),
                    MCTypeMapping.toMcType(col.getType()), col.getComment()));
        }
        return schema;
    }

    private ConnectorColumn findColumnByName(List<ConnectorColumn> columns,
            String name) {
        for (ConnectorColumn col : columns) {
            if (col.getName().equalsIgnoreCase(name)) {
                return col;
            }
        }
        return null;
    }

    private Long extractLifecycle(Map<String, String> properties) {
        String lifecycleStr = properties.get("mc.lifecycle");
        if (lifecycleStr == null) {
            lifecycleStr = properties.get("lifecycle");
        }
        if (lifecycleStr == null) {
            return null;
        }
        try {
            long lifecycle = Long.parseLong(lifecycleStr);
            if (lifecycle <= 0 || lifecycle > MAX_LIFECYCLE_DAYS) {
                throw new DorisConnectorException("Invalid lifecycle value: "
                        + lifecycle + ". Must be between 1 and "
                        + MAX_LIFECYCLE_DAYS + ".");
            }
            return lifecycle;
        } catch (NumberFormatException e) {
            throw new DorisConnectorException("Invalid lifecycle value: '"
                    + lifecycleStr + "'. Must be a positive integer.");
        }
    }

    private Map<String, String> extractMaxComputeProperties(
            Map<String, String> properties) {
        Map<String, String> mcProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith("mc.tblproperty.")) {
                mcProperties.put(
                        entry.getKey().substring("mc.tblproperty.".length()),
                        entry.getValue());
            }
        }
        return mcProperties;
    }

    private Integer extractBucketNum(ConnectorBucketSpec bucketSpec) {
        if (bucketSpec == null) {
            return null;
        }
        if (!"doris_default".equals(bucketSpec.getAlgorithm())) {
            throw new DorisConnectorException(
                    "MaxCompute only supports hash distribution. Got: "
                            + bucketSpec.getAlgorithm());
        }
        int bucketNum = bucketSpec.getNumBuckets();
        if (bucketNum <= 0 || bucketNum > MAX_BUCKET_NUM) {
            throw new DorisConnectorException("Invalid bucket number: "
                    + bucketNum + ". Must be between 1 and " + MAX_BUCKET_NUM + ".");
        }
        return bucketNum;
    }
}

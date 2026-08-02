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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Fluss metadata for one statement: a thin mapping from the connector SPI onto {@link FlussAdminOps}.
 *
 * <p>Fluss has a real two-level namespace (database, table), so the listing calls are direct
 * pass-throughs and carry no Doris-side naming convention.
 *
 * <p>Every method that needs the table's schema goes through {@link #tableInfo}, which memoizes the
 * fetch for the statement — the handle, the schema, the column handles and (later) split planning
 * therefore see one coherent version of the table for one round trip.
 */
public class FlussConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(FlussConnectorMetadata.class);

    /** What {@code getTableSchema} reports as the table's format; surfaces in DESCRIBE / EXPLAIN. */
    private static final String TABLE_FORMAT_TYPE = "FLUSS";

    private final FlussAdminOps adminOps;
    private final FlussTypeMapping.Options typeMappingOptions;

    public FlussConnectorMetadata(FlussAdminOps adminOps, FlussTypeMapping.Options typeMappingOptions) {
        this.adminOps = adminOps;
        this.typeMappingOptions = typeMappingOptions;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return adminOps.listDatabases();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return adminOps.databaseExists(dbName);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return adminOps.listTables(dbName);
    }

    /**
     * Resolves a table, or reports that it does not exist.
     *
     * <p>Only fluss's own "not there" exceptions become an empty handle. Anything else — an
     * unreachable coordinator, a timeout, an auth failure — propagates, because answering "no such
     * table" to those makes a broken catalog look like an empty one: {@code SELECT} then fails with
     * "table not found", a {@code CREATE TABLE IF NOT EXISTS} would look free to proceed, and the real
     * error never reaches the user.
     */
    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        try {
            return Optional.of(FlussTableHandle.of(tableInfo(session, TablePath.of(dbName, tableName))));
        } catch (TableNotExistException | DatabaseNotExistException e) {
            return Optional.empty();
        }
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
        FlussTableHandle flussHandle = (FlussTableHandle) handle;
        TableInfo info = tableInfo(session, flussHandle.toTablePath());

        List<Schema.Column> schemaColumns = info.getSchema().getColumns();
        List<ConnectorColumn> columns = new ArrayList<>(schemaColumns.size());
        for (Schema.Column column : schemaColumns) {
            columns.add(toConnectorColumn(column));
        }

        // LinkedHashMap: SHOW CREATE TABLE renders PROPERTIES from this map, and a stable order keeps
        // the rendered DDL from churning between runs.
        Map<String, String> properties = new LinkedHashMap<>(info.getProperties().toMap());
        if (flussHandle.isPartitioned()) {
            // "partition_columns" is the key the generic fe-core consumer reads; without it the table is
            // treated as unpartitioned and partition pruning is silently lost. Names are case-preserved
            // to stay matchable against the column names emitted above.
            properties.put(ConnectorTableSchema.PARTITION_COLUMNS_KEY,
                    String.join(",", flussHandle.getPartitionKeys()));
        }
        return new ConnectorTableSchema(
                flussHandle.getTableName(), columns, TABLE_FORMAT_TYPE, properties);
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        FlussTableHandle flussHandle = (FlussTableHandle) handle;
        List<Schema.Column> columns = tableInfo(session, flussHandle.toTablePath()).getSchema().getColumns();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).getName();
            handles.put(name, new FlussColumnHandle(name, i));
        }
        return handles;
    }

    @Override
    public String getTableComment(ConnectorSession session, String dbName, String tableName) {
        return tableInfo(session, TablePath.of(dbName, tableName)).getComment().orElse("");
    }

    @Override
    public List<String> listPartitionNames(ConnectorSession session, ConnectorTableHandle handle) {
        List<ConnectorPartitionInfo> partitions = listPartitions(session, handle, Optional.empty());
        List<String> names = new ArrayList<>(partitions.size());
        for (ConnectorPartitionInfo partition : partitions) {
            names.add(partition.getPartitionName());
        }
        return names;
    }

    /**
     * Lists the table's partitions in the Hive-style {@code k1=v1/k2=v2} naming every Doris catalog
     * uses, which is not fluss's own {@code v1$v2} spelling — fe-core parses the segments back out of
     * the name for {@code SHOW PARTITIONS} and the {@code partition_values} function. No escaping is
     * needed on the way: fluss rejects a partition value that is not ASCII alphanumerics, {@code _} or
     * {@code -} (TablePath#detectInvalidName, applied by PartitionUtils#validatePartitionValues), so a
     * value can contain neither the {@code =} nor the {@code /} that would make two partitions render
     * to one name. That same rule is why no value can be SQL NULL and the null-flag list stays empty.
     *
     * <p>{@code filter} is ignored: server-side partition pruning exists in fluss (a partial
     * {@code PartitionSpec}) but it takes a spec, not a predicate, and the predicate-to-spec reduction
     * belongs with split planning, which is the caller that has a predicate worth pushing. Listing
     * returns everything, as the paimon and maxcompute connectors do.
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        FlussTableHandle flussHandle = (FlussTableHandle) handle;
        List<String> partitionKeys = flussHandle.getPartitionKeys();
        if (partitionKeys.isEmpty()) {
            // Guard before the remote call: asking fluss for the partitions of an unpartitioned table is
            // an error there, and "this table has no partitions" is already known from the handle.
            return Collections.emptyList();
        }

        List<PartitionInfo> flussPartitions = adminOps.listPartitionInfos(flussHandle.toTablePath());
        List<ConnectorPartitionInfo> result = new ArrayList<>(flussPartitions.size());
        for (PartitionInfo partition : flussPartitions) {
            Map<String, String> spec = partition.getPartitionSpec().getSpecMap();
            // Both lists follow the partition-COLUMN order, not the spec's iteration order, because
            // fe-core zips them positionally against the partition columns.
            Map<String, String> values = new LinkedHashMap<>();
            List<String> orderedValues = new ArrayList<>(partitionKeys.size());
            StringBuilder name = new StringBuilder();
            for (String partitionKey : partitionKeys) {
                String value = spec.get(partitionKey);
                values.put(partitionKey, value);
                orderedValues.add(value);
                if (name.length() > 0) {
                    name.append('/');
                }
                name.append(partitionKey).append('=').append(value);
            }
            result.add(new ConnectorPartitionInfo(
                    name.toString(), values, Collections.emptyMap(),
                    orderedValues, Collections.emptyList()));
        }
        return result;
    }

    /**
     * The table's row count, when fluss has one.
     *
     * <p>Fluss reports row count only (no data size, no per-column statistics), and only for a table
     * whose statistics are enabled — otherwise the count comes back as zero, which is reported as
     * unknown rather than as "the table is empty". Statistics are best effort by contract: a failure
     * degrades to unknown instead of failing the statement, because this runs in background analysis
     * and in SHOW, where a transient coordinator error must not surface as a query error.
     */
    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session, ConnectorTableHandle handle) {
        FlussTableHandle flussHandle = (FlussTableHandle) handle;
        long rowCount;
        try {
            rowCount = adminOps.getTableStats(flussHandle.toTablePath()).getRowCount();
        } catch (Exception e) {
            LOG.warn("Failed to read fluss table statistics for {}", flussHandle, e);
            return Optional.empty();
        }
        if (rowCount <= 0) {
            return Optional.empty();
        }
        // -1 = unknown data size. Zero would tell the optimizer the table costs nothing to scan.
        return Optional.of(new ConnectorTableStatistics(rowCount, -1));
    }

    /**
     * The Thrift table descriptor the BE receives. A fluss scan reaches the BE through the same file
     * scan node the lake connectors use (the fluss ranges ride in its format-specific descriptor), so
     * the table descriptor is the generic hive-shaped one those connectors send, exactly as paimon and
     * hudi do; a fluss-specific Thrift table type would buy nothing the scan path reads.
     */
    @Override
    public TTableDescriptor buildTableDescriptor(ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        TTableDescriptor descriptor = new TTableDescriptor(
                tableId, TTableType.HIVE_TABLE, numCols, 0, tableName, dbName);
        descriptor.setHiveTable(new THiveTable(dbName, tableName, new LinkedHashMap<>()));
        return descriptor;
    }

    private ConnectorColumn toConnectorColumn(Schema.Column column) {
        ConnectorType type = FlussTypeMapping.toConnectorType(column.getDataType(), typeMappingOptions);
        // isKey=true for every column and nullable=true for every column: this is what every Doris
        // external catalog reports. The nullability one matters beyond convention — fluss marks its
        // primary-key columns NOT NULL, and propagating that would let the planner fold null-rejecting
        // predicates, while the same table read through its lake sibling (the paimon connector, which
        // reports every column nullable) would keep them. One table must not get two different plans
        // depending on which door it was read through.
        ConnectorColumn connectorColumn = new ConnectorColumn(
                column.getName(),
                type,
                column.getComment().orElse(""),
                true,
                null,
                true);
        // A "with local time zone" timestamp carries the WITH_TIMEZONE marker DESCRIBE shows in Extra.
        // Keyed on the SOURCE fluss type, so it survives whether enable.mapping.timestamp_tz mapped the
        // column to TIMESTAMPTZ or to plain DATETIME.
        if (column.getDataType().getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            connectorColumn = connectorColumn.withTimeZone();
        }
        return connectorColumn;
    }

    /** The table's metadata, fetched once per statement (see {@link FlussStatementScope}). */
    private TableInfo tableInfo(ConnectorSession session, TablePath tablePath) {
        return FlussStatementScope.sharedTableInfo(session, tablePath,
                () -> adminOps.getTableInfo(tablePath));
    }
}

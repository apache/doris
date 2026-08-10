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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorTableStatistics;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Fluss metadata for one statement: a thin mapping from the connector SPI onto {@link FlussAdminOps}.
 *
 * <p>Fluss has a real two-level namespace (database, table), so the listing calls are direct
 * pass-throughs and carry no Doris-side naming convention.
 *
 * <p>Every method that needs the table's schema goes through {@link #tableInfo}, which memoizes the
 * fetch for the statement — the handle, the schema, the column handles and (later) split planning
 * therefore see one coherent version of the table for one round trip.
 *
 * <p>It is also the <b>gateway</b> for lake tables: a datalake-enabled table exposes a {@code lake} system
 * table, so {@code tbl$lake} resolves to a handle made by the embedded paimon sibling and every later
 * per-handle call is forwarded to that sibling's metadata. Those forwards are the guarded methods below;
 * each one first asks {@link #siblingOwner} whether the handle is foreign, and only then falls through to
 * fluss's own implementation. A guard that is missing would not degrade gracefully — fluss's body casts to
 * {@link FlussTableHandle} and would throw {@link ClassCastException} on a paimon handle. <b>Every method
 * that performs that cast is guarded; a new one must be too.</b>
 *
 * <p>The methods this connector does NOT implement are deliberately left unguarded: they inherit the SPI's
 * neutral defaults, so a lake handle reaching them is answered "not supported" rather than crashing. That
 * costs the lake table paimon's MVCC pin and time travel, and the reason is structural rather than an
 * oversight: fe-core picks a table's MVCC-capable class from the FRONT DOOR connector's capabilities
 * ({@code SUPPORTS_MVCC_SNAPSHOT}), which fluss does not declare, so the engine never asks for a pin on a
 * fluss catalog's table — forwarding those calls today would be unreachable code. Schema and data stay
 * consistent (both read latest). Should fluss ever declare that capability, add forwards for
 * {@code beginQuerySnapshot} / {@code resolveTimeTravel} / {@code applySnapshot} and the snapshot overloads
 * of {@code getTableSchema} / {@code getTableStatistics} in the same breath.
 */
public class FlussConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(FlussConnectorMetadata.class);

    /** What {@code getTableSchema} reports as the table's format; surfaces in DESCRIBE / EXPLAIN. */
    private static final String TABLE_FORMAT_TYPE = "FLUSS";

    /**
     * The system-table name that reads a datalake table's lake side, i.e. {@code tbl$lake}. Matches the
     * suffix fluss's own Flink catalog uses ({@code FlinkCatalog.LAKE_TABLE_SPLITTER = "$lake"}); the
     * engine supplies the {@code $}.
     */
    private static final String LAKE_SYS_TABLE = "lake";
    private static final String LOG_SYS_TABLE = "log";

    /** The only lake format that can be delegated today; fluss also defines iceberg / lance / hudi. */
    private static final String PAIMON_LAKE_FORMAT = "paimon";

    private final FlussAdminOps adminOps;
    private final FlussTypeMapping.Options typeMappingOptions;
    private final Function<Map<String, String>, Connector> lakeSiblingFactory;
    private final Function<ConnectorTableHandle, Connector> siblingOwner;

    public FlussConnectorMetadata(FlussAdminOps adminOps, FlussTypeMapping.Options typeMappingOptions,
            Function<Map<String, String>, Connector> lakeSiblingFactory,
            Function<ConnectorTableHandle, Connector> siblingOwner) {
        this.adminOps = adminOps;
        this.typeMappingOptions = typeMappingOptions;
        this.lakeSiblingFactory = lakeSiblingFactory;
        this.siblingOwner = siblingOwner;
    }

    /**
     * Forwards one call to the lake sibling's metadata. Only SPI types are touched, and neither the metadata
     * nor any handle it produces may be cast (cross-loader {@code CCE}). {@link LakeSibling#forward} owns the
     * classloader pin and the per-statement instance, so both are shared with the scan planner.
     */
    private <T> T forward(ConnectorSession session, Connector sibling,
            Function<ConnectorMetadata, T> call) {
        return LakeSibling.forward(session, sibling, call);
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

    /**
     * Reports the {@code lake} and {@code log} system tables for a table that has a lake side, so
     * {@code tbl$lake} and {@code tbl$log} resolve. The two name complementary halves of the same table —
     * what has been tiered, and what has not — so they are offered together, behind one gate.
     *
     * <p>Gated on {@code table.datalake.enabled} alone, not on the lake FORMAT: a table with an unsupported
     * lake format still advertises them so that reading one produces {@link #getSysTableHandle}'s precise
     * "this format is not supported" error rather than fe-core's generic "no such table". A table with no
     * lake at all advertises nothing — announcing a sub-table that can only fail would be worse than not
     * offering it, and without a lake there is no boundary to split the table at in the first place.
     *
     * <p>A handle that IS already one of those halves advertises nothing: a half has no halves of its own,
     * and {@code tbl$log$lake} could not be resolved anyway (fe-core splits a table name at its LAST
     * {@code $}, so the base name it would look up is {@code tbl$log}, which is not a table).
     */
    @Override
    public List<String> listSupportedSysTables(ConnectorSession session,
            ConnectorTableHandle baseTableHandle) {
        Connector owner = siblingOwner.apply(baseTableHandle);
        if (owner != null) {
            return forward(session, owner, m -> m.listSupportedSysTables(session, baseTableHandle));
        }
        FlussTableHandle flussHandle = (FlussTableHandle) baseTableHandle;
        return flussHandle.isDataLakeEnabled() && !flussHandle.isLogOnly()
                ? Arrays.asList(LAKE_SYS_TABLE, LOG_SYS_TABLE)
                : Collections.emptyList();
    }

    /**
     * Resolves the two halves of a tiered table.
     *
     * <p>{@code tbl$lake} resolves to the paimon sibling's handle for the same {@code db.table} name — which
     * is the name fluss's tiering service writes the lake table under. From here on that table IS a paimon
     * table: the engine routes its scan by handle to the sibling's plan provider, and this metadata's
     * guarded methods forward the rest. The sibling is configured from THIS table's properties, where the
     * fluss coordinator puts the cluster's lake settings; see {@link PaimonSiblingProperties}.
     *
     * <p>{@code tbl$log} stays on this side: it is the same fluss table read from where the lake snapshot
     * ends, so it resolves to this handle re-read at {@link FlussTableHandle.ReadMode#LOG_ONLY} and is
     * planned by the fluss plan provider.
     */
    @Override
    public Optional<ConnectorTableHandle> getSysTableHandle(ConnectorSession session,
            ConnectorTableHandle baseTableHandle, String sysName) {
        Connector owner = siblingOwner.apply(baseTableHandle);
        if (owner != null) {
            return forward(session, owner, m -> m.getSysTableHandle(session, baseTableHandle, sysName));
        }
        boolean lake = LAKE_SYS_TABLE.equals(sysName);
        if (!lake && !LOG_SYS_TABLE.equals(sysName)) {
            return Optional.empty();
        }

        FlussTableHandle flussHandle = (FlussTableHandle) baseTableHandle;
        if (flussHandle.isLogOnly()) {
            // A half has no halves; see listSupportedSysTables. Empty, not an exception: nothing announced
            // this name, so fe-core's "no such table" is the honest answer.
            return Optional.empty();
        }
        // Re-checked rather than assumed from listSupportedSysTables: discovery and resolution are two
        // round trips, and the table could have had its lake turned off in between.
        if (!flussHandle.isDataLakeEnabled()) {
            throw new DorisConnectorException("Table '" + flussHandle.getDatabaseName() + "."
                    + flussHandle.getTableName() + "' has no lake table: it is not created with"
                    + " table.datalake.enabled = true");
        }
        if (!lake) {
            return Optional.of(logOnlyHandle(flussHandle));
        }
        String lakeFormat = flussHandle.getDataLakeFormat();
        if (lakeFormat == null || !PAIMON_LAKE_FORMAT.equalsIgnoreCase(lakeFormat)) {
            throw new DorisConnectorException("Cannot read the lake table of '"
                    + flussHandle.getDatabaseName() + "." + flussHandle.getTableName()
                    + "': its table.datalake.format is '" + lakeFormat
                    + "', and the fluss connector currently supports only '" + PAIMON_LAKE_FORMAT + "'");
        }

        Connector sibling = lakeSiblingFactory.apply(
                PaimonSiblingProperties.synthesize(flussHandle.getProperties()));
        Optional<ConnectorTableHandle> lakeHandle = forward(session, sibling, m -> m.getTableHandle(
                session, flussHandle.getDatabaseName(), flussHandle.getTableName()));
        if (!lakeHandle.isPresent()) {
            // The lake table is created by the tiering service on its first commit, so "not there" means
            // nothing has been tiered yet — a state that resolves itself and is worth saying out loud.
            // Returning empty here would instead surface as fe-core's generic "no such table", pointing the
            // user at a name that IS correct.
            throw new DorisConnectorException("The lake table of '" + flussHandle.getDatabaseName() + "."
                    + flussHandle.getTableName() + "' does not exist yet: nothing has been tiered to the"
                    + " lake. Start (or wait for) the fluss tiering service for this table");
        }
        // From here on this handle travels back through the engine and returns to the guards below, which
        // route it by asking the sibling whether it is its own. Checked once, here, where a failure still
        // has a cause attached to it.
        return Optional.of(LakeSibling.requireOwned(sibling, lakeHandle.get()));
    }

    /**
     * {@code tbl$log} for a log table: the same handle, read from where the lake stops.
     *
     * <p>Refused for a primary-key table. What such a table's log holds past the lake snapshot is a change
     * stream — inserts, updates and deletes against keys the lake already has — not a set of rows, so
     * "the part of the table that is not in the lake" is not something it can return. The base table
     * answers that by merging the two halves; reading fluss alone answers it by replaying the whole log.
     */
    private static ConnectorTableHandle logOnlyHandle(FlussTableHandle flussHandle) {
        if (flussHandle.hasPrimaryKey()) {
            throw new DorisConnectorException("Table '" + flussHandle.getDatabaseName() + "."
                    + flussHandle.getTableName() + "' has a primary key, so its log past the lake snapshot"
                    + " is a change stream rather than a set of rows and cannot be read as '$log'. Query '"
                    + flussHandle.getTableName() + "' itself for the merged view, or set '"
                    + FlussCatalogProperties.UNION_READ_MODE + "' to disabled to read the whole table from"
                    + " fluss alone.");
        }
        return flussHandle.asLogOnly();
    }

    @Override
    public boolean isPartitionValuesSysTable(ConnectorSession session,
            ConnectorTableHandle baseTableHandle, String sysName) {
        Connector owner = siblingOwner.apply(baseTableHandle);
        if (owner != null) {
            return forward(session, owner,
                    m -> m.isPartitionValuesSysTable(session, baseTableHandle, sysName));
        }
        // The lake table is a real data table served by the paimon sibling, not the generic
        // partition_values function.
        return false;
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
        Connector owner = siblingOwner.apply(handle);
        if (owner != null) {
            return forward(session, owner, m -> m.getTableSchema(session, handle));
        }
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
        Connector owner = siblingOwner.apply(handle);
        if (owner != null) {
            return forward(session, owner, m -> m.getColumnHandles(session, handle));
        }
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
        Connector owner = siblingOwner.apply(handle);
        if (owner != null) {
            return forward(session, owner, m -> m.listPartitionNames(session, handle));
        }
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
     * the name for {@code SHOW PARTITIONS} and the {@code partition_values} function. The rendering is
     * {@link FlussPartitions}', shared with split planning: the pruned names the engine derives from
     * this listing are what planning matches its partitions against, so one renderer or none.
     *
     * <p>{@code filter} is ignored: server-side partition pruning exists in fluss (a partial
     * {@code PartitionSpec}) but it takes a spec, not a predicate, and the predicate-to-spec reduction
     * belongs with split planning, which is the caller that has a predicate worth pushing. Listing
     * returns everything, as the paimon and maxcompute connectors do.
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        Connector owner = siblingOwner.apply(handle);
        if (owner != null) {
            return forward(session, owner, m -> m.listPartitions(session, handle, filter));
        }
        FlussTableHandle flussHandle = (FlussTableHandle) handle;
        List<String> partitionKeys = flussHandle.getPartitionKeys();
        if (partitionKeys.isEmpty()) {
            // Guard before the remote call: asking fluss for the partitions of an unpartitioned table is
            // an error there, and "this table has no partitions" is already known from the handle.
            return Collections.emptyList();
        }
        // Before the remote call and before any name is rendered, because the answer depends on the
        // table's schema alone: a partition column whose value fluss cannot store verbatim in a partition
        // name makes every partition of this table unreadable, whether it has any yet or not.
        rejectUnreadablePartitionColumns(flussHandle, partitionKeys);

        List<PartitionInfo> flussPartitions = adminOps.listPartitionInfos(flussHandle.toTablePath());
        List<ConnectorPartitionInfo> result = new ArrayList<>(flussPartitions.size());
        for (PartitionInfo partition : flussPartitions) {
            FlussScanRange.Partition resolved = FlussPartitions.toScanPartition(partition, partitionKeys);
            // The values already follow partition-COLUMN order (fe-core zips them positionally against
            // the partition columns); the null-flag list stays empty because fluss allows no null value.
            result.add(new ConnectorPartitionInfo(
                    resolved.getName(), resolved.getValues(), Collections.emptyMap(),
                    new ArrayList<>(resolved.getValues().values()), Collections.emptyList()));
        }
        return result;
    }

    /**
     * Refuses a table whose partition values cannot be read back out of the names fluss stores them in.
     *
     * <p>This is the only place it can be said well. Further down, the name is all there is: fe-core's
     * parser sees {@code 1_5}, has a FLOAT column to put it in, and reports that it failed to convert a
     * partition — naming neither the column nor fluss nor the property that would help. Here the column
     * and its fluss type are both still in hand.
     *
     * <p>Refusing is the answer rather than guessing because the rewriting is many-to-one: {@code 1_5}
     * was {@code 1.5}, and {@code 01-02-03} was {@code 01:02:03}, and neither the connector nor fluss
     * itself can say which character came back out.
     */
    private void rejectUnreadablePartitionColumns(FlussTableHandle handle, List<String> partitionKeys) {
        Map<String, DataType> keyColumnTypes = handle.getKeyColumnTypes();
        for (String partitionKey : partitionKeys) {
            DataType type = keyColumnTypes.get(partitionKey);
            if (type == null) {
                // The handle records a type for every partition column of the table it was built from, so
                // a missing one means this handle and that schema have come apart. Guessing "readable"
                // here would put the mangled name back on the path this method exists to close.
                throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                        + handle.getTableName() + "' lists '" + partitionKey + "' as a partition column"
                        + " but carries no type for it; refresh the catalog and try again.");
            }
            String rejection = FlussPartitionColumnTypes.rejection(type, typeMappingOptions);
            if (rejection != null) {
                throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                        + handle.getTableName() + "' cannot be read: its partition column '" + partitionKey
                        + "' has fluss type " + type + ", and " + rejection + ". Partition columns of type "
                        + FlussPartitionColumnTypes.READABLE_TYPES + " are stored as written.");
            }
        }
    }

    /**
     * The table's row count, when fluss has one.
     *
     * <p>Fluss reports row count only (no data size, no per-column statistics), and only for a table
     * whose statistics are enabled — otherwise the count comes back as zero, which is reported as
     * unknown rather than as "the table is empty". Statistics are best effort by contract: a failure
     * degrades to unknown instead of failing the statement, because this runs in background analysis
     * and in SHOW, where a transient coordinator error must not surface as a query error.
     *
     * <p>Unknown for {@code tbl$log}: what fluss reports is the WHOLE table's row count, and the log tail
     * is the small end of a table that has been tiering for a while — off by orders of magnitude, in the
     * direction that makes the optimizer treat the cheap half as the expensive one. Unknown is an answer
     * the optimizer already handles; a confidently wrong number is not.
     */
    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session, ConnectorTableHandle handle) {
        Connector owner = siblingOwner.apply(handle);
        if (owner != null) {
            return forward(session, owner, m -> m.getTableStatistics(session, handle));
        }
        FlussTableHandle flussHandle = (FlussTableHandle) handle;
        if (flussHandle.isLogOnly()) {
            return Optional.empty();
        }
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
     *
     * <p>Not guarded for a lake handle, and it could not be: the signature carries names, not a handle. It
     * needs no guard because the paimon connector builds the byte-identical descriptor
     * ({@code PaimonConnectorMetadata.buildTableDescriptor}) — verified, not assumed. If either side's
     * descriptor ever diverges, this becomes a real gap that a handle-less signature cannot close here.
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

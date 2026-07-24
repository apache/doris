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
import org.apache.doris.connector.api.ConnectorType;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * ConnectorMetadata implementation for MaxCompute.
 * Delegates database/table discovery to {@link McStructureHelper}.
 */
public class MaxComputeConnectorMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(
            MaxComputeConnectorMetadata.class);

    private static final long MAX_LIFECYCLE_DAYS = 37231;
    private static final int MAX_BUCKET_NUM = 1024;
    // Must stay byte-identical to the key ConnectorSessionBuilder.extractSessionProperties injects
    // (GC1 / FIX-BLOCKID-CAP-CONFIG); = the legacy fe-core Config field name, surfaced via session
    // properties because the connector cannot import fe-core Config.
    private static final String MAX_COMPUTE_WRITE_MAX_BLOCK_COUNT = "max_compute_write_max_block_count";

    private final Odps odps;
    private final McStructureHelper structureHelper;
    private final String defaultProject;
    private final String endpoint;
    private final String quota;
    private final Map<String, String> properties;
    private final MaxComputePartitionCache partitionCache;

    // Per-statement table-handle memo. This metadata instance is created fresh per statement
    // (MaxComputeDorisConnector.getMetadata), so a table resolved once -- one remote exists()
    // probe plus one lazy ODPS Table -- is reused by every planning site in the same statement
    // instead of each site re-probing and rebuilding it (killing the redundant exists() round
    // trips and the per-Table schema reload). ConcurrentHashMap because a scan reuses the same
    // per-statement session (hence this instance) across off-thread pool tasks. Key is the
    // (dbName, tableName) pair via List's value equality -- no separator collision to reason about.
    private final Map<List<String>, MaxComputeTableHandle> tableHandleMemo = new ConcurrentHashMap<>();

    public MaxComputeConnectorMetadata(Odps odps,
            McStructureHelper structureHelper,
            String defaultProject,
            String endpoint,
            String quota,
            Map<String, String> properties,
            MaxComputePartitionCache partitionCache) {
        this.odps = odps;
        this.structureHelper = structureHelper;
        this.defaultProject = defaultProject;
        this.endpoint = endpoint;
        this.quota = quota;
        this.properties = properties;
        this.partitionCache = partitionCache;
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
        // Only present tables are memoized: returning null from the mapping function records no
        // mapping, so an absent table re-probes on every call -- keeping the Optional.empty()
        // ("table not found") behavior byte-identical to before. computeIfAbsent runs the
        // mapping function at most once per key even under a concurrent first touch, so the
        // exists() probe fires once per (db, table) per statement.
        MaxComputeTableHandle handle = tableHandleMemo.computeIfAbsent(
                List.of(dbName, tableName), k -> {
                    if (!structureHelper.tableExist(odps, dbName, tableName)) {
                        return null;
                    }
                    Table odpsTable = structureHelper.getOdpsTable(
                            odps, dbName, tableName);
                    TableIdentifier tableId = structureHelper.getTableIdentifier(
                            dbName, tableName);
                    return new MaxComputeTableHandle(
                            dbName, tableName, odpsTable, tableId);
                });
        return Optional.ofNullable(handle);
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
            columns.add(buildColumn(
                    col.getName(),
                    MCTypeMapping.toConnectorType(col.getTypeInfo()),
                    col.getComment(),
                    col.isNullable()));
        }

        List<String> partitionColumnNames =
                new ArrayList<>(partColumns.size());
        for (Column partCol : partColumns) {
            partitionColumnNames.add(partCol.getName());
            columns.add(buildColumn(
                    partCol.getName(),
                    MCTypeMapping.toConnectorType(partCol.getTypeInfo()),
                    partCol.getComment(),
                    true));
        }

        java.util.Map<String, String> props = new java.util.HashMap<>();
        if (!partitionColumnNames.isEmpty()) {
            props.put(ConnectorTableSchema.PARTITION_COLUMNS_KEY,
                    String.join(",", partitionColumnNames));
        }
        return new ConnectorTableSchema(
                mcHandle.getTableName(), columns, "MAX_COMPUTE", props);
    }

    /**
     * Builds a {@link ConnectorColumn} for a MaxCompute external-table column with
     * {@code isKey=true}, mirroring legacy {@code MaxComputeExternalTable.initSchema} (every column
     * was a Doris key column). For external (non-OLAP) tables there is no key-based storage; the
     * flag drives DESCRIBE's {@code Key} display and the few non-OLAP-guarded planning/BE paths that
     * read {@code Column.isKey()} (e.g. predicate inference, slot descriptors) — all of which legacy
     * already fed {@code true}, so this restores exact legacy parity. {@code isAutoInc} stays false.
     */
    static ConnectorColumn buildColumn(String name, ConnectorType type, String comment,
            boolean nullable) {
        return new ConnectorColumn(name, type, comment, nullable, null, true);
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

    /**
     * Builds the typed MaxCompute table descriptor for the read path. The BE
     * {@code file_scanner} static_casts {@code table_desc()} to
     * {@code MaxComputeTableDescriptor} unconditionally for
     * {@code table_format_type=="max_compute"}, so the descriptor MUST be
     * {@code MAX_COMPUTE_TABLE} with {@code mcTable} set; the null / SCHEMA_TABLE
     * fallback would produce type confusion in BE. Mirrors legacy
     * {@code MaxComputeExternalTable.toThrift()}.
     *
     * <p>{@code project}/{@code table} use the remote-name params: the SPI read
     * session also addresses ODPS with remote names, so the descriptor must match
     * (see design OQ-7). The 6th ctor arg ({@code dbName}) mirrors legacy and is
     * unread by BE for MC reads. Fully-qualified thrift names match the jdbc/es
     * overrides and avoid new connector imports.</p>
     */
    @Override
    public org.apache.doris.thrift.TTableDescriptor buildTableDescriptor(
            ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        org.apache.doris.thrift.TMCTable tMcTable = new org.apache.doris.thrift.TMCTable();
        tMcTable.setEndpoint(endpoint);
        tMcTable.setQuota(quota);
        tMcTable.setProject(dbName);
        tMcTable.setTable(remoteName);
        tMcTable.setProperties(properties);
        org.apache.doris.thrift.TTableDescriptor desc = new org.apache.doris.thrift.TTableDescriptor(
                tableId, org.apache.doris.thrift.TTableType.MAX_COMPUTE_TABLE,
                numCols, 0, tableName, dbName);
        desc.setMcTable(tMcTable);
        return desc;
    }

    // ==================== Partition listing ====================

    @Override
    public List<String> listPartitionNames(ConnectorSession session,
            ConnectorTableHandle handle) {
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle;
        List<Partition> partitions = partitionCache.getPartitions(
                mcHandle.getDbName(), mcHandle.getTableName());
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
     * served through the connector-owned {@link MaxComputePartitionCache}
     * (keyed by db+table), so repeated / cross-method partition listings of the
     * same table share one ODPS round trip.
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle;
        List<Partition> partitions = partitionCache.getPartitions(
                mcHandle.getDbName(), mcHandle.getTableName());
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
        List<Partition> partitions = partitionCache.getPartitions(
                mcHandle.getDbName(), mcHandle.getTableName());
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

    // ==================== Write / Transaction (P4-T03 / P4-T04) ====================

    /**
     * Disables pushing predicates that contain implicit CAST expressions down to ODPS (F9 fix).
     *
     * <p>The shared {@code ExprToConnectorExpressionConverter} unwraps CAST shells, so without this
     * a predicate like {@code CAST(str_col AS INT) = 5} would be pushed to the ODPS read session as
     * the source-side filter {@code str_col = "5"} (quoted by the column's STRING type), which ODPS
     * evaluates as exact string equality and drops rows like {@code "05"}/{@code " 5"} <b>at the
     * source</b> — silent data loss, because BE re-evaluation can only filter the returned rows down,
     * never recover rows ODPS never returned. Returning {@code false} makes
     * {@code PluginDrivenScanNode.buildRemainingFilter} strip CAST-bearing conjuncts before pushdown
     * (they stay BE-only), restoring legacy parity: legacy {@code MaxComputeScanNode} likewise never
     * pushed CAST predicates (its {@code convertSlotRefToColumnName} threw on a CAST operand and the
     * conjunct was dropped). Mirrors {@code JdbcConnectorMetadata} and the contract documented on
     * {@link org.apache.doris.connector.api.ConnectorPushdownOps#supportsCastPredicatePushdown}.
     */
    @Override
    public boolean supportsCastPredicatePushdown(ConnectorSession session) {
        return false;
    }

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
        long maxBlockCount = resolveMaxBlockCount(session.getSessionProperties());
        return new MaxComputeConnectorTransaction(session.allocateTransactionId(), maxBlockCount);
    }

    /**
     * Resolves the write block-id cap from the session properties, into which fe-core's
     * {@code ConnectorSessionBuilder} surfaces the (tunable)
     * {@code Config.max_compute_write_max_block_count} (the connector cannot import fe-core
     * {@code Config}). Falls back to the legacy default when the value is absent or unparseable,
     * so any path without the injected value keeps the current behavior. Package-private +
     * map-typed for direct unit testing without a live session.
     */
    static long resolveMaxBlockCount(Map<String, String> sessionProperties) {
        String value = sessionProperties.get(MAX_COMPUTE_WRITE_MAX_BLOCK_COUNT);
        if (value == null) {
            return MaxComputeConnectorTransaction.DEFAULT_MAX_BLOCK_COUNT;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return MaxComputeConnectorTransaction.DEFAULT_MAX_BLOCK_COUNT;
        }
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
    public boolean supportsCreateDatabase() {
        return true;
    }

    @Override
    public void createDatabase(ConnectorSession session, String dbName,
            Map<String, String> properties) {
        structureHelper.createDb(odps, dbName, false);
        LOG.info("created MaxCompute database {}", dbName);
    }

    @Override
    public void dropDatabase(ConnectorSession session, String dbName,
            boolean ifExists, boolean force) {
        if (force) {
            // ODPS schemas().delete() does NOT auto-cascade; enumerate and drop each
            // table first (mirrors legacy MaxComputeMetadataOps.dropDbImpl force branch,
            // whose enumerate-loop is itself proof that the schema delete won't cascade).
            for (String tableName : structureHelper.listTableNames(odps, dbName)) {
                try {
                    structureHelper.dropTable(odps, dbName, tableName, true);
                } catch (OdpsException e) {
                    throw new DorisConnectorException("Failed to drop MaxCompute table '"
                            + tableName + "' during force-drop of database '" + dbName
                            + "': " + e.getMessage(), e);
                }
            }
        }
        structureHelper.dropDb(odps, dbName, ifExists);
        LOG.info("dropped MaxCompute database {} (force={})", dbName, force);
    }

    // ==================== DDL helpers ====================

    // package-private for unit test; reached only via createTable() in production.
    void validateColumns(List<ConnectorColumn> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new DorisConnectorException(
                    "Table must have at least one column.");
        }
        Set<String> seen = new HashSet<>();
        for (ConnectorColumn col : columns) {
            // MaxCompute cannot store auto-increment columns; reject them with the same message
            // as legacy MaxComputeMetadataOps.validateColumns (silent drop is a data-model
            // regression -- the user's AUTO_INCREMENT intent would be lost without warning).
            if (col.isAutoInc()) {
                throw new DorisConnectorException(
                        "Auto-increment columns are not supported for MaxCompute tables: "
                                + col.getName());
            }
            // MaxCompute has no aggregate-key model; reject aggregate columns (e.g. SUM/REPLACE),
            // mirroring legacy MaxComputeMetadataOps.validateColumns:426-429. The nereids non-OLAP
            // path does not reject these (validateKeyColumns is ENGINE_OLAP-gated), so without this
            // the user's aggregate intent is silently dropped to a plain column.
            if (col.isAggregated()) {
                throw new DorisConnectorException(
                        "Aggregation columns are not supported for MaxCompute tables: "
                                + col.getName());
            }
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

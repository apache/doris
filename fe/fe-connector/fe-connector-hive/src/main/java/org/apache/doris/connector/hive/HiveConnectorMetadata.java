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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
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
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsCreateDatabaseRequest;
import org.apache.doris.connector.hms.HmsCreateTableRequest;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.hms.HmsTypeMapping;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link ConnectorMetadata} implementation for Hive (HMS-based) catalogs.
 *
 * <p>Provides read-only metadata operations:
 * <ul>
 *   <li>List databases and tables</li>
 *   <li>Get table schema (columns + partition keys)</li>
 *   <li>Table format detection (HIVE/HUDI/ICEBERG)</li>
 *   <li>Partition name listing</li>
 *   <li>Column handle resolution for scan planning</li>
 *   <li>Partition pruning via {@code applyFilter}</li>
 * </ul>
 */
public class HiveConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(HiveConnectorMetadata.class);

    // FE-internal schema-control property key: a CSV of the RAW remote partition-column names. The generic
    // fe-core consumer (PluginDrivenExternalTable.toSchemaCacheValue) reads it to derive which of the emitted
    // columns are partition columns; it is the same key the paimon/iceberg/maxcompute connectors emit and is
    // stripped from the user-facing SHOW CREATE properties by fe-core.
    private static final String PARTITION_COLUMNS_PROPERTY = "partition_columns";

    // Connector-side spelling of fe-type ScalarType.MAX_VARCHAR_LENGTH (the connector must not import fe-type);
    // a hive `string` partition column is widened to varchar(65533) for legacy parity. Paimon hardcodes the
    // identical 65533.
    private static final int MAX_VARCHAR_LENGTH = 65533;

    private final HmsClient hmsClient;
    private final Map<String, String> properties;
    private final HmsTypeMapping.Options typeMappingOptions;
    // Carries the fe-core-injected environment (getEnvironment()) with the FE-global CREATE TABLE defaults
    // (hive_default_file_format / enable_create_hive_bucket_table / doris_version) that the plugin cannot
    // read from FE Config. The default getEnvironment() is an empty map, so direct-construction tests that
    // pass a bare context degrade to the hard-coded fallbacks in createTable.
    private final ConnectorContext context;

    public HiveConnectorMetadata(HmsClient hmsClient, Map<String, String> properties, ConnectorContext context) {
        this.hmsClient = hmsClient;
        this.properties = properties;
        this.context = context;
        this.typeMappingOptions = buildTypeMappingOptions(properties);
    }

    // ========== ConnectorSchemaOps ==========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return hmsClient.listDatabases();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        try {
            hmsClient.getDatabase(dbName);
            return true;
        } catch (HmsClientException e) {
            LOG.debug("Database '{}' not found: {}", dbName, e.getMessage());
            return false;
        }
    }

    // ========== ConnectorTableOps ==========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return hmsClient.listTables(dbName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        if (!hmsClient.tableExists(dbName, tableName)) {
            return Optional.empty();
        }
        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        HiveTableType tableType = HiveTableFormatDetector.detect(tableInfo);

        // Build partition key column names
        List<String> partKeyNames = Collections.emptyList();
        List<ConnectorColumn> partKeys = tableInfo.getPartitionKeys();
        if (partKeys != null && !partKeys.isEmpty()) {
            partKeyNames = partKeys.stream()
                    .map(ConnectorColumn::getName)
                    .collect(Collectors.toList());
        }

        HiveTableHandle handle = new HiveTableHandle.Builder(dbName, tableName, tableType)
                .inputFormat(tableInfo.getInputFormat())
                .serializationLib(tableInfo.getSerializationLib())
                .location(tableInfo.getLocation())
                .partitionKeyNames(partKeyNames)
                .sdParameters(tableInfo.getSdParameters())
                .tableParameters(tableInfo.getParameters())
                .build();
        return Optional.of(handle);
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        String dbName = hiveHandle.getDbName();
        String tableName = hiveHandle.getTableName();

        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        List<ConnectorColumn> columns = buildColumns(tableInfo);
        List<ConnectorColumn> partitionKeys = coercePartitionKeyStringToVarchar(buildPartitionKeys(tableInfo));

        // Merge: regular columns + partition columns (partition columns last, mirroring legacy
        // HMSExternalTable full-schema order: data columns then partition keys).
        List<ConnectorColumn> allColumns = new ArrayList<>(columns.size() + partitionKeys.size());
        allColumns.addAll(columns);
        allColumns.addAll(partitionKeys);

        String formatType = detectFormatType(tableInfo);
        // Copy the HMS table parameters so the FE-internal partition_columns marker can be stamped without
        // mutating the shared tableInfo map.
        Map<String, String> tableProperties = new HashMap<>(
                tableInfo.getParameters() != null ? tableInfo.getParameters() : Collections.emptyMap());
        // Mark which emitted columns are partition columns for the generic fe-core consumer. Without this
        // property every partitioned hive/hudi table is read as unpartitioned (wrong pruning/row count, MTMV
        // breakage). The value is a CSV of the RAW partition-key names in declaration order; hive partition-key
        // names are identifiers (no comma) so the CSV encoding is unambiguous.
        if (!partitionKeys.isEmpty()) {
            tableProperties.put(PARTITION_COLUMNS_PROPERTY, partitionKeys.stream()
                    .map(ConnectorColumn::getName).collect(Collectors.joining(",")));
        }

        return new ConnectorTableSchema(tableName, allColumns, formatType, tableProperties);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    // ========== ConnectorTableOps: Column Handles ==========

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        HmsTableInfo tableInfo = hmsClient.getTable(
                hiveHandle.getDbName(), hiveHandle.getTableName());

        Set<String> partKeyNames = hiveHandle.getPartitionKeyNames() != null
                ? hiveHandle.getPartitionKeyNames().stream().collect(Collectors.toSet())
                : Collections.emptySet();

        Map<String, ConnectorColumnHandle> result = new LinkedHashMap<>();
        List<ConnectorColumn> allCols = new ArrayList<>();
        if (tableInfo.getColumns() != null) {
            allCols.addAll(tableInfo.getColumns());
        }
        if (tableInfo.getPartitionKeys() != null) {
            allCols.addAll(tableInfo.getPartitionKeys());
        }
        for (ConnectorColumn col : allCols) {
            boolean isPartKey = partKeyNames.contains(col.getName());
            result.put(col.getName(), new HiveColumnHandle(
                    col.getName(), col.getType().getTypeName(), isPartKey));
        }
        return result;
    }

    /**
     * Builds the BE table descriptor for a hive table, a direct port of legacy
     * {@code HMSExternalTable.toThrift}: a {@code TTableType.HIVE_TABLE} carrying a {@link THiveTable}. Without
     * this override the SPI default returns {@code null} and fe-core ({@code PluginDrivenExternalTable.toThrift})
     * falls back to a generic {@code SCHEMA_TABLE} descriptor. Mirrors the iceberg connector's HIVE_TABLE
     * branch; the SPI signature carries no handle, so this single override serves base and system tables alike
     * (legacy used the identical fork for both).
     */
    @Override
    public TTableDescriptor buildTableDescriptor(ConnectorSession session,
            long tableId, String tableName, String dbName, String remoteName, int numCols, long catalogId) {
        THiveTable tHiveTable = new THiveTable(dbName, tableName, new HashMap<>());
        TTableDescriptor desc = new TTableDescriptor(
                tableId, TTableType.HIVE_TABLE, numCols, 0, tableName, dbName);
        desc.setHiveTable(tHiveTable);
        return desc;
    }

    // ========== ConnectorPushdownOps: Filter Pushdown ==========

    @Override
    public Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorFilterConstraint constraint) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        List<String> partKeyNames = hiveHandle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            return Optional.empty();
        }

        // Extract equality predicates on partition columns from the expression
        Map<String, List<String>> partitionPredicates = extractPartitionPredicates(
                constraint.getExpression(), partKeyNames);
        if (partitionPredicates.isEmpty()) {
            return Optional.empty();
        }

        // Build partition name filter patterns for HMS
        List<String> allPartNames = hmsClient.listPartitionNames(
                hiveHandle.getDbName(), hiveHandle.getTableName(), 100000);
        List<String> matchedPartNames = prunePartitionNames(
                allPartNames, partKeyNames, partitionPredicates);

        if (matchedPartNames.size() == allPartNames.size()) {
            // No pruning effect
            return Optional.empty();
        }

        List<HmsPartitionInfo> prunedPartitions = matchedPartNames.isEmpty()
                ? Collections.emptyList()
                : hmsClient.getPartitions(hiveHandle.getDbName(),
                        hiveHandle.getTableName(), matchedPartNames);

        LOG.info("Partition pruning: {}.{} all={} pruned={}",
                hiveHandle.getDbName(), hiveHandle.getTableName(),
                allPartNames.size(), prunedPartitions.size());

        HiveTableHandle newHandle = hiveHandle.toBuilder()
                .prunedPartitions(prunedPartitions)
                .build();
        return Optional.of(new FilterApplicationResult<>(
                newHandle, constraint.getExpression(), false));
    }

    // ========== ConnectorSchemaOps: DDL writes (create/drop database) ==========

    /**
     * Hive supports CREATE DATABASE. Declaring it lets {@code PluginDrivenExternalCatalog.createDb} consult
     * the remote database existence for IF NOT EXISTS (the SPI default {@code false} would skip that check).
     */
    @Override
    public boolean supportsCreateDatabase() {
        return true;
    }

    /**
     * Creates a Hive database, mirroring legacy {@code HiveMetadataOps.createDbImpl}: the {@code location}
     * property becomes the database location URI (and is dropped from the parameter map), the {@code comment}
     * property becomes the description, and the remaining properties become database parameters. Existence /
     * IF NOT EXISTS is resolved upstream by {@code PluginDrivenExternalCatalog.createDb}.
     */
    @Override
    public void createDatabase(ConnectorSession session, String dbName, Map<String, String> dbProperties) {
        Map<String, String> params = new HashMap<>(dbProperties);
        String location = params.remove(HiveConnectorProperties.CREATE_LOCATION);
        String comment = params.getOrDefault(HiveConnectorProperties.CREATE_COMMENT, "");
        try {
            hmsClient.createDatabase(new HmsCreateDatabaseRequest(dbName, location, comment, params));
        } catch (HmsClientException e) {
            throw new DorisConnectorException(
                    "Failed to create Hive database " + dbName + ": " + e.getMessage(), e);
        }
    }

    /**
     * Drops a Hive database, mirroring legacy {@code HiveMetadataOps.dropDbImpl}: with {@code force} every
     * table in the database is dropped first (a table that vanished remotely is skipped; a transactional table
     * is rejected exactly as a direct DROP TABLE would be), then the database itself. Existence / IF EXISTS is
     * resolved upstream by {@code PluginDrivenExternalCatalog.dropDb}, so {@code ifExists} is accepted for SPI
     * parity but not re-checked here.
     */
    @Override
    public void dropDatabase(ConnectorSession session, String dbName, boolean ifExists, boolean force) {
        try {
            if (force) {
                for (String tableName : hmsClient.listTables(dbName)) {
                    HmsTableInfo tableInfo;
                    try {
                        tableInfo = hmsClient.getTable(dbName, tableName);
                    } catch (HmsClientException e) {
                        // The table disappeared between listing and load (dropped out-of-band); skip it,
                        // mirroring legacy dropDbImpl which swallowed getTableOrDdlException and continued.
                        LOG.warn("failed to load table {}.{} during force drop database: {}",
                                dbName, tableName, e.getMessage());
                        continue;
                    }
                    dropTableChecked(dbName, tableName, tableInfo.getParameters());
                }
            }
            hmsClient.dropDatabase(dbName);
        } catch (HmsClientException e) {
            throw new DorisConnectorException(
                    "Failed to drop Hive database " + dbName + ": " + e.getMessage(), e);
        }
    }

    // ========== ConnectorTableOps: DDL writes (create/drop/truncate table) ==========

    /**
     * Creates a Hive table, a faithful port of legacy {@code HiveMetadataOps.createTableImpl}. All property
     * interpretation happens here (plugin side); fe-core does not parse hive properties. Existence /
     * IF NOT EXISTS is resolved upstream by {@code PluginDrivenExternalCatalog.createTable}.
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorCreateTableRequest request) {
        // Working copy of the user CREATE TABLE properties; the default owner is added here (legacy added it
        // to the same map before deriving the metastore parameters).
        Map<String, String> userProps = new HashMap<>(request.getProperties());
        if (session.getUser() != null) {
            userProps.putIfAbsent(HiveConnectorProperties.CREATE_OWNER, session.getUser());
        }
        // Reject a transactional table create (legacy parity: a hive transactional table only appears to
        // accept inserts). Matches legacy's case-sensitive "transactional" key check.
        String transactional = userProps.get(HiveConnectorProperties.CREATE_TRANSACTIONAL);
        if (transactional != null && transactional.equalsIgnoreCase("true")) {
            throw new DorisConnectorException("Not support create hive transactional table.");
        }
        Map<String, String> env = context.getEnvironment();
        String fileFormat = userProps.getOrDefault(HiveConnectorProperties.CREATE_FILE_FORMAT,
                env.getOrDefault(HiveConnectorProperties.ENV_HIVE_DEFAULT_FILE_FORMAT,
                        HiveConnectorProperties.DEFAULT_FILE_FORMAT));

        // Metastore table parameters: lower-case every key and stamp the file_format / location keys under a
        // doris. prefix so they round-trip (legacy HiveMetadataOps ddlProps loop).
        Map<String, String> tableParams = new HashMap<>();
        for (Map.Entry<String, String> entry : userProps.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (HiveConnectorProperties.DORIS_HIVE_KEYS.contains(key)) {
                tableParams.put(HiveConnectorProperties.DORIS_PROP_PREFIX + key, entry.getValue());
            } else {
                tableParams.put(key, entry.getValue());
            }
        }

        // Partition columns: LIST only (reject RANGE), and reject explicit partition value definitions
        // (hive external tables discover partitions from the data layout). Legacy parity.
        List<String> partitionColNames = new ArrayList<>();
        ConnectorPartitionSpec partitionSpec = request.getPartitionSpec();
        if (partitionSpec != null) {
            if (partitionSpec.getStyle() == ConnectorPartitionSpec.Style.RANGE) {
                throw new DorisConnectorException("Only support 'LIST' partition type in hive catalog.");
            }
            for (ConnectorPartitionField field : partitionSpec.getFields()) {
                partitionColNames.add(field.getColumnName());
            }
            if (partitionSpec.hasExplicitPartitionValues()) {
                throw new DorisConnectorException(
                        "Partition values expressions is not supported in hive catalog.");
            }
        }

        // DLF catalogs reject per-column default values (legacy parity).
        if (HmsClientConfig.METASTORE_TYPE_DLF.equals(properties.get(HmsClientConfig.METASTORE_TYPE_KEY))) {
            for (ConnectorColumn column : request.getColumns()) {
                if (column.getDefaultValue() != null) {
                    throw new DorisConnectorException("Default values are not supported with `DLF` catalog.");
                }
            }
        }

        HmsCreateTableRequest.Builder builder = HmsCreateTableRequest.builder()
                .dbName(request.getDbName())
                .tableName(request.getTableName())
                .location(userProps.get(HiveConnectorProperties.CREATE_LOCATION))
                .columns(request.getColumns())
                .partitionKeys(partitionColNames)
                .fileFormat(fileFormat)
                .comment(request.getComment())
                .properties(tableParams)
                .defaultTextCompression(resolveTextCompressionDefault(session))
                .dorisVersion(env.get(HiveConnectorProperties.ENV_DORIS_VERSION));

        // Bucketing: gated on the FE-global toggle, and hive supports hash bucketing only. Legacy checks the
        // enable gate first, then the hash requirement.
        ConnectorBucketSpec bucketSpec = request.getBucketSpec();
        if (bucketSpec != null) {
            boolean bucketEnabled = Boolean.parseBoolean(env.getOrDefault(
                    HiveConnectorProperties.ENV_ENABLE_CREATE_HIVE_BUCKET_TABLE, "false"));
            if (!bucketEnabled) {
                throw new DorisConnectorException(
                        "Create hive bucket table need set enable_create_hive_bucket_table to true");
            }
            if (HiveConnectorProperties.BUCKET_ALGO_RANDOM.equals(bucketSpec.getAlgorithm())) {
                throw new DorisConnectorException("External hive table only supports hash bucketing");
            }
            builder.bucketCols(bucketSpec.getColumns()).numBuckets(bucketSpec.getNumBuckets());
        }

        try {
            hmsClient.createTable(builder.build());
        } catch (HmsClientException | IllegalArgumentException e) {
            throw new DorisConnectorException("Failed to create Hive table "
                    + request.getDbName() + "." + request.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Drops a Hive table, mirroring legacy {@code HiveMetadataOps.dropTableImpl}: a transactional table is
     * rejected. {@code PluginDrivenExternalCatalog} has already resolved the handle / IF EXISTS upstream and
     * routed a view DROP elsewhere.
     */
    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle handle) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        try {
            // The handle was just built by the bridge's getTableHandle (which loaded the table), so its
            // parameters carry the transactional flag; reuse them instead of re-fetching, matching legacy's
            // AcidUtils.isTransactionalTable(client.getTable(...)) check.
            dropTableChecked(hiveHandle.getDbName(), hiveHandle.getTableName(),
                    hiveHandle.getTableParameters());
        } catch (HmsClientException e) {
            throw new DorisConnectorException("Failed to drop Hive table "
                    + hiveHandle.getDbName() + "." + hiveHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Truncates a Hive table, or the given partitions of it, mirroring legacy
     * {@code HiveMetadataOps.truncateTableImpl}. {@code partitions} is {@code null}/empty for a whole-table
     * truncate.
     */
    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle handle, List<String> partitions) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        try {
            hmsClient.truncateTable(hiveHandle.getDbName(), hiveHandle.getTableName(), partitions);
        } catch (HmsClientException e) {
            throw new DorisConnectorException("Failed to truncate Hive table "
                    + hiveHandle.getDbName() + "." + hiveHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    // RENAME TABLE is intentionally NOT overridden: hive does not support ALTER TABLE RENAME (legacy
    // HMSCachedClient has no rename), so the SPI default throw ("RENAME TABLE not supported") preserves the
    // pre-flip behavior.

    // ========== ConnectorWriteOps: transactions ==========

    /**
     * Opens a {@link HiveConnectorTransaction} for a hive non-ACID INSERT / INSERT OVERWRITE, mirroring the
     * iceberg one-liner (design D1: {@code planWrite} lives in {@code HiveWritePlanProvider}, the metadata
     * carries only the begin factory). The transaction id is the engine-allocated Doris global id (it is
     * registered in the engine transaction registry and stamped into the sink), so it must come from the
     * session, not be minted here. Dormant until the P7.4/P7.5 cutover.
     */
    @Override
    public ConnectorTransaction beginTransaction(ConnectorSession session) {
        return new HiveConnectorTransaction(session.allocateTransactionId(), hmsClient, context);
    }

    /**
     * Drops {@code dbName.tableName} after rejecting a transactional table, mirroring legacy
     * {@code HiveMetadataOps.dropTableImpl}. Shared by the direct DROP TABLE and the force DROP DATABASE
     * cascade.
     */
    private void dropTableChecked(String dbName, String tableName, Map<String, String> tableParameters) {
        if (isTransactionalTable(tableParameters)) {
            throw new DorisConnectorException("Not support drop hive transactional table.");
        }
        hmsClient.dropTable(dbName, tableName);
    }

    /**
     * Whether the metastore table parameters mark the table transactional, replicating Hive's
     * {@code AcidUtils.isTransactionalTable} (case-insensitive "true" under the "transactional" key, with the
     * upper-cased key as a fallback) without pulling in the hive-exec dependency.
     */
    private static boolean isTransactionalTable(Map<String, String> tableParameters) {
        if (tableParameters == null) {
            return false;
        }
        String value = tableParameters.get(HiveConnectorProperties.CREATE_TRANSACTIONAL);
        if (value == null) {
            value = tableParameters.get(
                    HiveConnectorProperties.CREATE_TRANSACTIONAL.toUpperCase(Locale.ROOT));
        }
        return "true".equalsIgnoreCase(value);
    }

    /**
     * Resolves the compression a {@code text} table falls back to when the user set no {@code compression}
     * property, replicating legacy {@code SessionVariable.hiveTextCompression()} (the "uncompressed" alias maps
     * to "plain"). The value rides on the request; the write converter only consults it for a text table.
     */
    private static String resolveTextCompressionDefault(ConnectorSession session) {
        String textCompression = session.getSessionProperties()
                .get(HiveConnectorProperties.SESSION_HIVE_TEXT_COMPRESSION);
        if (HiveConnectorProperties.TEXT_COMPRESSION_UNCOMPRESSED.equals(textCompression)) {
            return HiveConnectorProperties.TEXT_COMPRESSION_PLAIN;
        }
        return textCompression;
    }

    // ========== Internal helpers ==========

    private List<ConnectorColumn> buildColumns(HmsTableInfo tableInfo) {
        List<ConnectorColumn> spiColumns = tableInfo.getColumns();
        if (spiColumns == null || spiColumns.isEmpty()) {
            return Collections.emptyList();
        }
        // HmsTableInfo already returns ConnectorColumn with types mapped by HmsTypeMapping
        // during ThriftHmsClient.getTable(). Enrich with default values if available.
        Map<String, String> defaults = getDefaultValues(tableInfo);
        if (defaults.isEmpty()) {
            return spiColumns;
        }
        List<ConnectorColumn> enriched = new ArrayList<>(spiColumns.size());
        for (ConnectorColumn col : spiColumns) {
            String defaultVal = defaults.get(col.getName());
            if (defaultVal != null && col.getDefaultValue() == null) {
                enriched.add(new ConnectorColumn(
                        col.getName(), col.getType(), col.getComment(),
                        col.isNullable(), defaultVal));
            } else {
                enriched.add(col);
            }
        }
        return enriched;
    }

    private List<ConnectorColumn> buildPartitionKeys(HmsTableInfo tableInfo) {
        List<ConnectorColumn> partKeys = tableInfo.getPartitionKeys();
        if (partKeys == null) {
            return Collections.emptyList();
        }
        return partKeys;
    }

    /**
     * Widens a hive {@code string} partition column to {@code varchar(65533)}, replicating legacy
     * {@code HMSExternalTable.initPartitionColumns}: a bare-string partition column is coerced to
     * {@code varchar(ScalarType.MAX_VARCHAR_LENGTH)} "to be same as doris managed table", while every other
     * declared type (int/date/timestamp/decimal/varchar(n)/char(n)/...) is kept exactly as
     * {@code HmsTypeMapping} produced it. The gate is the mapped connector type name {@code STRING} (hive
     * {@code string}, and {@code binary} when not mapped to varbinary, both land on it), matching legacy's
     * {@code PrimitiveType.STRING} check. The widened column keeps the same name/comment/nullability/flags, so
     * the full-schema entry and the partition-column view carry the identical type (legacy mutated one shared
     * {@code Column} in place).
     */
    private static List<ConnectorColumn> coercePartitionKeyStringToVarchar(List<ConnectorColumn> partitionKeys) {
        if (partitionKeys.isEmpty()) {
            return partitionKeys;
        }
        List<ConnectorColumn> coerced = new ArrayList<>(partitionKeys.size());
        for (ConnectorColumn col : partitionKeys) {
            if ("STRING".equals(col.getType().getTypeName())) {
                coerced.add(new ConnectorColumn(col.getName(),
                        ConnectorType.of("VARCHAR", MAX_VARCHAR_LENGTH, -1),
                        col.getComment(), col.isNullable(), col.getDefaultValue(),
                        col.isKey(), col.isAutoInc(), col.isAggregated()));
            } else {
                coerced.add(col);
            }
        }
        return coerced;
    }

    private Map<String, String> getDefaultValues(HmsTableInfo tableInfo) {
        try {
            return hmsClient.getDefaultColumnValues(
                    tableInfo.getDbName(), tableInfo.getTableName());
        } catch (HmsClientException e) {
            LOG.debug("Could not get default column values for {}.{}: {}",
                    tableInfo.getDbName(), tableInfo.getTableName(), e.getMessage());
            return Collections.emptyMap();
        }
    }

    private String detectFormatType(HmsTableInfo tableInfo) {
        HiveTableType type = HiveTableFormatDetector.detect(tableInfo);
        switch (type) {
            case HIVE:
                return resolveHiveFileFormat(tableInfo.getInputFormat());
            case HUDI:
                return "HUDI";
            case ICEBERG:
                return "ICEBERG";
            default:
                return "UNKNOWN";
        }
    }

    /**
     * Resolve the Hive file format name from the input format class.
     */
    private static String resolveHiveFileFormat(String inputFormat) {
        if (inputFormat == null) {
            return "HIVE";
        }
        if (inputFormat.contains("Parquet") || inputFormat.contains("parquet")) {
            return "HIVE_PARQUET";
        }
        if (inputFormat.contains("Orc") || inputFormat.contains("orc")) {
            return "HIVE_ORC";
        }
        if (inputFormat.contains("Text") || inputFormat.contains("text")) {
            return "HIVE_TEXT";
        }
        return "HIVE";
    }

    private static HmsTypeMapping.Options buildTypeMappingOptions(Map<String, String> props) {
        boolean binaryAsString = Boolean.parseBoolean(
                props.getOrDefault(HiveConnectorProperties.ENABLE_MAPPING_BINARY_AS_STRING, "false"));
        boolean timestampTz = Boolean.parseBoolean(
                props.getOrDefault(HiveConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));
        return new HmsTypeMapping.Options(
                HmsTypeMapping.DEFAULT_TIME_SCALE, binaryAsString, timestampTz);
    }

    /**
     * Extracts equality predicates on partition columns from the expression tree.
     * Supports: col = 'value', col IN ('v1', 'v2', ...), AND combinations.
     */
    private Map<String, List<String>> extractPartitionPredicates(
            ConnectorExpression expr, List<String> partKeyNames) {
        Set<String> partKeySet = partKeyNames.stream().collect(Collectors.toSet());
        Map<String, List<String>> result = new HashMap<>();
        extractPredicatesRecursive(expr, partKeySet, result);
        return result;
    }

    private void extractPredicatesRecursive(ConnectorExpression expr,
            Set<String> partKeySet, Map<String, List<String>> result) {
        if (expr instanceof ConnectorAnd) {
            for (ConnectorExpression child : ((ConnectorAnd) expr).getConjuncts()) {
                extractPredicatesRecursive(child, partKeySet, result);
            }
        } else if (expr instanceof ConnectorComparison) {
            ConnectorComparison cmp = (ConnectorComparison) expr;
            if (cmp.getOperator() == ConnectorComparison.Operator.EQ) {
                String colName = extractColumnName(cmp.getLeft());
                String value = extractLiteralValue(cmp.getRight());
                if (colName != null && value != null && partKeySet.contains(colName)) {
                    result.computeIfAbsent(colName, k -> new ArrayList<>()).add(value);
                }
            }
        } else if (expr instanceof ConnectorIn) {
            ConnectorIn inExpr = (ConnectorIn) expr;
            if (!inExpr.isNegated()) {
                String colName = extractColumnName(inExpr.getValue());
                if (colName != null && partKeySet.contains(colName)) {
                    List<String> values = new ArrayList<>();
                    for (ConnectorExpression item : inExpr.getInList()) {
                        String val = extractLiteralValue(item);
                        if (val != null) {
                            values.add(val);
                        }
                    }
                    if (!values.isEmpty()) {
                        result.computeIfAbsent(colName, k -> new ArrayList<>()).addAll(values);
                    }
                }
            }
        }
    }

    private String extractColumnName(ConnectorExpression expr) {
        if (expr instanceof org.apache.doris.connector.api.pushdown.ConnectorColumnRef) {
            return ((org.apache.doris.connector.api.pushdown.ConnectorColumnRef) expr).getColumnName();
        }
        return null;
    }

    private String extractLiteralValue(ConnectorExpression expr) {
        if (expr instanceof ConnectorLiteral) {
            Object val = ((ConnectorLiteral) expr).getValue();
            return val != null ? String.valueOf(val) : null;
        }
        return null;
    }

    /**
     * Prunes partition names based on extracted equality predicates.
     * Partition names follow the Hive convention: key1=val1/key2=val2
     */
    private List<String> prunePartitionNames(List<String> allPartNames,
            List<String> partKeyNames, Map<String, List<String>> predicates) {
        List<String> matched = new ArrayList<>();
        for (String partName : allPartNames) {
            Map<String, String> partValues = parsePartitionName(partName, partKeyNames);
            if (matchesPredicates(partValues, predicates)) {
                matched.add(partName);
            }
        }
        return matched;
    }

    private Map<String, String> parsePartitionName(String partName,
            List<String> partKeyNames) {
        Map<String, String> values = new HashMap<>();
        String[] parts = partName.split("/");
        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                values.put(part.substring(0, eq), part.substring(eq + 1));
            }
        }
        return values;
    }

    private boolean matchesPredicates(Map<String, String> partValues,
            Map<String, List<String>> predicates) {
        for (Map.Entry<String, List<String>> entry : predicates.entrySet()) {
            String colName = entry.getKey();
            List<String> allowedValues = entry.getValue();
            String actualValue = partValues.get(colName);
            if (actualValue == null || !allowedValues.contains(actualValue)) {
                return false;
            }
        }
        return true;
    }
}

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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorColumnStatistics;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.ConnectorViewDefinition;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.ConnectorBucketSpec;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.PartitionFieldChange;
import org.apache.doris.connector.api.ddl.TagChange;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTableFreshness;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
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
import org.apache.doris.connector.hms.HmsColumnStatistics;
import org.apache.doris.connector.hms.HmsCreateDatabaseRequest;
import org.apache.doris.connector.hms.HmsCreateTableRequest;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.hms.HmsTypeMapping;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
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

    // Hive input formats eligible for Top-N lazy materialization, replicating legacy
    // HMSExternalTable.SUPPORTED_HIVE_TOPN_LAZY_FILE_FORMATS (parquet/orc only). The match is on the EXACT
    // input-format class (not a substring), so a HoodieParquetInputFormatBase hive table — which contains
    // "parquet" but is not a Top-N-lazy format in legacy — is correctly excluded.
    private static final String MAPRED_PARQUET_INPUT_FORMAT =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String ORC_INPUT_FORMAT =
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

    // HMS table type for a view, mirroring legacy HMSExternalTable.isView (which keyed off the view text flags);
    // a Hive view carries tableType VIRTUAL_VIEW.
    private static final String VIRTUAL_VIEW_TABLE_TYPE = "VIRTUAL_VIEW";

    // Presto/Trino view markers, replicating legacy HMSExternalTable.getViewText / parseTrinoViewDefinition. A
    // Presto/Trino-authored hive view stores the bare sentinel as its expanded text (skipped) and the real
    // definition as base64-encoded JSON inside the original text ("/* Presto View: <base64> */").
    private static final String PRESTO_VIEW_EXPANDED_SENTINEL = "/* Presto View */";
    private static final String PRESTO_VIEW_PREFIX = "/* Presto View: ";
    private static final String PRESTO_VIEW_SUFFIX = " */";
    private static final String PRESTO_VIEW_ORIGINAL_SQL_KEY = "originalSql";

    // Placeholder SQL dialect for a hive view. fe-core never reads ConnectorViewDefinition.getDialect() (the
    // view body is converted by the session dialect in BindRelation.parseAndAnalyzeExternalView), but the DTO
    // requires a non-null value; legacy getSqlDialect was likewise never consumed by the query path.
    private static final String HIVE_VIEW_DIALECT = "hive";

    // HMS table-parameter keys for table statistics, replicating legacy StatisticsUtil.getHiveRowCount /
    // getRowCountFromParameters / getTotalSizeFromHMS. numRows is the exact row count; totalSize is the on-disk
    // data size. Each has a spark-written alternative key (spark writes its own stats keys, not the standard
    // hive ones). Read as RAW facts only — the connector must NOT do the Doris-type-dependent estimation.
    private static final String PARAM_NUM_ROWS = "numRows";
    private static final String PARAM_SPARK_NUM_ROWS = "spark.sql.statistics.numRows";
    private static final String PARAM_TOTAL_SIZE = "totalSize";
    private static final String PARAM_SPARK_TOTAL_SIZE = "spark.sql.statistics.totalSize";

    // Partition-sampling cap for the file-list data-size estimate, matching the default of the legacy
    // Config.hive_stats_partition_sample_size (fe-common, unreadable from the plugin). Runtime tuning of that
    // specific config no longer applies on the plugin path (negligible — it is an internal estimation knob);
    // the on/off feature gate (enable_get_row_count_from_file_list) is still honored, fe-core side.
    private static final int STATS_PARTITION_SAMPLE_SIZE = 30;

    // Upper bound on partitions listed from HMS for the file-list estimate, matching HiveScanPlanProvider.
    private static final int MAX_PARTITIONS_FOR_STATS = 100000;

    // A Supplier installed by the 3-arg constructor when no iceberg sibling is available (hive-only
    // construction, e.g. unit tests exercising only hive-handle paths). It is invoked only when a NON-hive
    // handle is delegated — which such a construction never does — so it fails loud instead of NPEing.
    private static final Supplier<Connector> NO_ICEBERG_SIBLING = () -> {
        throw new DorisConnectorException("no iceberg sibling connector configured for this hive metadata");
    };

    // The hudi analog of NO_ICEBERG_SIBLING installed by the 3-arg constructor (hive-only construction). Invoked
    // only when a HUDI table is diverted BY TYPE — which such a construction never triggers — so it fails loud
    // instead of NPEing.
    private static final Supplier<Connector> NO_HUDI_SIBLING = () -> {
        throw new DorisConnectorException("no hudi sibling connector configured for this hive metadata");
    };

    // The by-handle owner resolver installed by the 3-arg constructor (hive-only construction). Invoked only when
    // a NON-hive handle reaches a per-handle guard-and-forward site — which such a construction never does — so it
    // fails loud instead of NPEing.
    private static final Function<ConnectorTableHandle, Connector> NO_SIBLING_OWNER = handle -> {
        throw new DorisConnectorException("no sibling connector configured for this hive metadata");
    };

    private final HmsClient hmsClient;
    private final Map<String, String> properties;
    private final HmsTypeMapping.Options typeMappingOptions;
    // Carries the fe-core-injected environment (getEnvironment()) with the FE-global CREATE TABLE defaults
    // (hive_default_file_format / enable_create_hive_bucket_table / doris_version) that the plugin cannot
    // read from FE Config. The default getEnvironment() is an empty map, so direct-construction tests that
    // pass a bare context degrade to the hard-coded fallbacks in createTable.
    private final ConnectorContext context;
    // Supplies the embedded iceberg SIBLING connector BY TYPE, for the getTableHandle ICEBERG divert only (an
    // iceberg-detected table has no handle yet to route by, so the sibling is force-built and asked directly).
    // Lazy: HiveConnector.getOrCreateIcebergSibling builds it only on first use, so a pure-hive query never
    // triggers it. Used ONLY through the parent-first Connector / ConnectorMetadata interfaces — its concrete
    // iceberg types are never cast here (cross-loader CCE).
    private final Supplier<Connector> icebergSiblingSupplier;
    // The hudi analog of icebergSiblingSupplier: supplies the embedded hudi SIBLING connector BY TYPE, for the
    // getTableHandle HUDI divert only (a hudi-detected table has no handle yet to route by). Lazy via
    // HiveConnector.getOrCreateHudiSibling; used ONLY through the parent-first Connector / ConnectorMetadata
    // interfaces — its concrete hudi types are never cast here (cross-loader CCE).
    private final Supplier<Connector> hudiSiblingSupplier;
    // Resolves the embedded sibling connector that OWNS a foreign (non-hive) table handle, for the per-handle
    // guard-and-forward methods below. Backed by HiveConnector.resolveSiblingOwner (a 3-way ownsHandle dispatch
    // over the ALREADY-BUILT iceberg / hudi siblings). Used ONLY through the parent-first Connector /
    // ConnectorMetadata interfaces — the owning sibling's concrete types are never cast here (cross-loader CCE).
    private final Function<ConnectorTableHandle, Connector> siblingOwnerResolver;
    // Connector-owned directory-listing cache, shared with the scan provider so estimateDataSizeByListingFiles
    // (the periodic ExternalRowCountCache refresh source) reuses listings a scan warmed and vice versa. Injected
    // by HiveConnector.newMetadata as the SAME instance; the convenience constructors below build a private
    // default (harmless for the direct-construction tests, which inject their file sizes and never list).
    private final HiveFileListingCache fileListingCache;

    public HiveConnectorMetadata(HmsClient hmsClient, Map<String, String> properties, ConnectorContext context) {
        this(hmsClient, properties, context, NO_ICEBERG_SIBLING, NO_HUDI_SIBLING, NO_SIBLING_OWNER);
    }

    public HiveConnectorMetadata(HmsClient hmsClient, Map<String, String> properties, ConnectorContext context,
            Supplier<Connector> icebergSiblingSupplier,
            Supplier<Connector> hudiSiblingSupplier,
            Function<ConnectorTableHandle, Connector> siblingOwnerResolver) {
        this(hmsClient, properties, context, icebergSiblingSupplier, hudiSiblingSupplier, siblingOwnerResolver,
                new HiveFileListingCache(properties));
    }

    public HiveConnectorMetadata(HmsClient hmsClient, Map<String, String> properties, ConnectorContext context,
            Supplier<Connector> icebergSiblingSupplier,
            Supplier<Connector> hudiSiblingSupplier,
            Function<ConnectorTableHandle, Connector> siblingOwnerResolver,
            HiveFileListingCache fileListingCache) {
        this.hmsClient = hmsClient;
        this.properties = properties;
        this.context = context;
        this.typeMappingOptions = buildTypeMappingOptions(properties);
        this.icebergSiblingSupplier = icebergSiblingSupplier;
        this.hudiSiblingSupplier = hudiSiblingSupplier;
        this.siblingOwnerResolver = siblingOwnerResolver;
        this.fileListingCache = fileListingCache;
    }

    /**
     * The embedded iceberg sibling's metadata resolved BY TYPE, for the getTableHandle ICEBERG divert only (an
     * iceberg-detected table has no handle yet, so the sibling is force-built and asked directly). Obtained fresh
     * per call — parity with fe-core, which acquires a ConnectorMetadata per operation; the heavy catalog/caches
     * live on the single (memoized) sibling connector, so this is cheap. The returned metadata and any handle it
     * produces are used ONLY through the parent-first SPI interfaces and MUST NOT be cast (the sibling's concrete
     * iceberg types would CCE across the loader split).
     *
     * <p>Package-private (not private) so HiveConnectorThreeWayRoutingTest can assert that
     * {@link HiveConnector#getMetadata} wires the iceberg by-TYPE supplier to THIS arm (the two same-typed
     * supplier args are otherwise transposable at that sole production wiring point).
     */
    ConnectorMetadata icebergSiblingMetadata(ConnectorSession session) {
        return icebergSiblingSupplier.get().getMetadata(session);
    }

    /**
     * The embedded hudi sibling's metadata resolved BY TYPE, for the getTableHandle HUDI divert only (a
     * hudi-detected table has no handle yet, so the sibling is force-built and asked directly). Same lifecycle and
     * casting contract as {@link #icebergSiblingMetadata}: obtained fresh per call, used ONLY through the
     * parent-first SPI interfaces, and never cast (the sibling's concrete hudi types would CCE across the loader
     * split).
     *
     * <p>Package-private (not private) so HiveConnectorThreeWayRoutingTest can assert that
     * {@link HiveConnector#getMetadata} wires the hudi by-TYPE supplier to THIS arm (see
     * {@link #icebergSiblingMetadata}).
     */
    ConnectorMetadata hudiSiblingMetadata(ConnectorSession session) {
        return hudiSiblingSupplier.get().getMetadata(session);
    }

    /**
     * The OWNING sibling's metadata for a foreign (non-hive) table handle, resolved BY HANDLE (3-way ownsHandle
     * dispatch over the already-built iceberg / hudi siblings — see HiveConnector.resolveSiblingOwner). Every
     * per-handle guard-and-forward method routes through here so a hudi handle reaches the hudi sibling and an
     * iceberg handle the iceberg sibling. Obtained fresh per call; the handle is used ONLY through the
     * parent-first SPI interfaces and MUST NOT be cast (cross-loader CCE).
     */
    private ConnectorMetadata siblingMetadata(ConnectorSession session, ConnectorTableHandle handle) {
        return siblingOwnerResolver.apply(handle).getMetadata(session);
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
        // Foreign-handle divert: an iceberg-on-HMS or hudi-on-HMS table registered in this HMS catalog is served by
        // the embedded iceberg / hudi SIBLING connector, not by hive. Return the sibling's OWN table handle (the
        // raw foreign iceberg/hudi handle) verbatim — NOT a HiveTableHandle stamped ICEBERG/HUDI — so the sibling's
        // scan/metadata path, which unconditionally casts the handle to its concrete IcebergTableHandle /
        // HudiTableHandle, succeeds. This is the pivot that activates the guard-and-forward overrides throughout
        // this class: every gateway consumer discriminates by `instanceof HiveTableHandle` (the gateway's OWN
        // hive-loader type) and forwards any non-hive handle to whichever sibling OWNS it (3-way ownsHandle
        // dispatch); the foreign handle is NEVER cast here (its concrete type is invisible across the classloader
        // split). Iceberg is checked before hudi, matching the detector's own precedence (a table carrying both
        // resolves iceberg). Dormant overall until hms enters SPI_READY_TYPES: today getTableHandle is never called
        // for this connector.
        if (tableType == HiveTableType.ICEBERG) {
            return icebergSiblingMetadata(session).getTableHandle(session, dbName, tableName);
        }
        if (tableType == HiveTableType.HUDI) {
            return hudiSiblingMetadata(session).getTableHandle(session, dbName, tableName);
        }
        // Fail-loud parity with legacy HMSExternalTable.supportedHiveTable(), which threw on a null or
        // unrecognized input format instead of silently degrading (the old detector returned UNKNOWN). A view
        // short-circuits: legacy returns true for a view before the format check — a view has no data files so
        // its (usually null) input format is irrelevant, and it is served through the view SPI, not the scan
        // path, so its handle keeps the UNKNOWN type (never scanned) rather than being rejected here.
        if (tableType == HiveTableType.UNKNOWN && !isViewTable(tableInfo)) {
            String inputFormat = tableInfo.getInputFormat();
            throw new DorisConnectorException(inputFormat == null
                    ? "remote table's storage input format is null"
                    : "Unsupported hive input format: " + inputFormat);
        }

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
                .firstColumnIsString(firstColumnIsString(tableInfo))
                .build();
        return Optional.of(handle);
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            // An iceberg/hudi-on-HMS table's schema is built by the embedded sibling connector, but fe-core's
            // PluginDrivenExternalTable.hasScanCapability only ever reads the CATALOG connector (this HIVE
            // connector), never the sibling — so a per-table scan capability the sibling declares connector-wide
            // (auto-analyze / Top-N lazy / nested-column prune) would be lost for the embedded table. Reflect the
            // owning sibling's connector-wide capability set onto the delegated schema as a per-table marker so it
            // survives delegation (mirrors Trino table-redirection, where the redirected-to connector's
            // capabilities govern the table). Only hasScanCapability consumers read the marker, so a capability
            // that is not per-table-refinable (view / show-create / mvcc) is inert here. Resolve the owner ONCE
            // (getMetadata is not free) and reuse it for the schema build and the capability read.
            Connector owner = siblingOwnerResolver.apply(handle);
            ConnectorTableSchema siblingSchema = owner.getMetadata(session).getTableSchema(session, handle);
            return reflectSiblingScanCapabilities(owner, siblingSchema);
        }
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

        // Per-table scan capabilities that the generic fe-core consumer refines the connector-wide capability
        // set with. Top-N lazy materialization is orc/parquet-only in hive (legacy
        // HMSExternalTable.supportedHiveTopNLazyTable), which the connector-wide SUPPORTS_TOPN_LAZY_MATERIALIZE
        // cannot express for a heterogeneous hive catalog; emit it per-table so fe-core enables the optimization
        // only for eligible tables and never for text/csv/json/view/hudi.
        List<String> perTableCapabilities = new ArrayList<>();
        // Legacy StatisticsUtil.supportAutoAnalyze admitted EVERY plain-hive (dlaType==HIVE) table into background
        // per-column auto-analyze regardless of file format. Emit it per-table for every plain-hive data table (any
        // format, view excluded) so fe-core's hasScanCapability admits them WITHOUT a connector-wide flag (which
        // would also admit hudi-on-HMS, which legacy excluded). This branch is reached only for a HiveTableHandle;
        // an iceberg-on-HMS table is served by the delegation branch above (which reflects the iceberg sibling's
        // own auto-analyze capability), and a hudi-on-HMS table's connector declares neither.
        if (supportsHiveColumnAutoAnalyze(tableInfo)) {
            perTableCapabilities.add(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE.name());
        }
        if (supportsHiveSampleAnalyze(tableInfo)) {
            perTableCapabilities.add(ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE.name());
        }
        if (supportsHiveTopNLazyMaterialize(tableInfo)) {
            perTableCapabilities.add(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE.name());
        }
        if (!perTableCapabilities.isEmpty()) {
            tableProperties.put(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY,
                    String.join(",", perTableCapabilities));
        }

        // Distribution (bucketing) columns for the flipped table's getDistributionColumnNames() — legacy
        // HMSExternalTable read getSd().getBucketCols(). Emitted RAW (fe-core lowercases, mirroring the legacy
        // getDistributionColumnNames); only a bucketed table carries it. Consumed by sampled ANALYZE to pick the
        // linear-vs-DUJ1 NDV estimator (a single bucket column that IS the analyzed column -> linear).
        List<String> bucketCols = tableInfo.getBucketCols();
        if (bucketCols != null && !bucketCols.isEmpty()) {
            tableProperties.put(ConnectorTableSchema.DISTRIBUTION_COLUMNS_KEY, String.join(",", bucketCols));
        }

        return new ConnectorTableSchema(tableName, allColumns, formatType, tableProperties);
    }

    /**
     * Reflects the owning sibling connector's connector-wide capability set onto a delegated (iceberg/hudi-on-HMS)
     * table's schema as a per-table {@link ConnectorTableSchema#PER_TABLE_CAPABILITIES_KEY} marker, merged with any
     * marker the sibling already emitted. fe-core's {@code PluginDrivenExternalTable.hasScanCapability} resolves a
     * per-table scan capability from the CATALOG (hive) connector-wide set OR this marker and NEVER consults the
     * sibling connector directly, so without this reflection an iceberg-on-HMS table would silently lose every scan
     * capability the iceberg sibling declares connector-wide (auto-analyze / Top-N lazy / nested-column prune).
     * Returns the sibling schema unchanged when the sibling declares no capabilities (e.g. a hudi sibling that
     * declares none). Only per-table-refinable capabilities are ever consulted from the marker, so reflecting the
     * whole set (including non-scan capabilities) is inert for the rest.
     */
    private ConnectorTableSchema reflectSiblingScanCapabilities(Connector owner, ConnectorTableSchema siblingSchema) {
        Set<ConnectorCapability> ownerCaps = owner.getCapabilities();
        if (ownerCaps.isEmpty()) {
            return siblingSchema;
        }
        LinkedHashSet<String> caps = new LinkedHashSet<>();
        String existing = siblingSchema.getProperties().get(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY);
        if (existing != null && !existing.isEmpty()) {
            for (String name : existing.split(",")) {
                String trimmed = name.trim();
                if (!trimmed.isEmpty()) {
                    caps.add(trimmed);
                }
            }
        }
        for (ConnectorCapability cap : ownerCaps) {
            caps.add(cap.name());
        }
        Map<String, String> props = new HashMap<>(siblingSchema.getProperties());
        props.put(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY, String.join(",", caps));
        return new ConnectorTableSchema(siblingSchema.getTableName(), siblingSchema.getColumns(),
                siblingSchema.getTableFormatType(), props);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    // ========== ConnectorTableOps: Column Handles ==========

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).getColumnHandles(session, handle);
        }
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

    // ========== ConnectorTableOps: Views ==========

    /**
     * Whether {@code dbName.viewName} is a hive VIEW, a connector-side port of legacy
     * {@code HMSExternalTable.isView}: the authoritative signal is the PRESENCE OF VIEW TEXT
     * ({@code viewOriginalText} or {@code viewExpandedText} set), not the {@code tableType} — a hive view
     * always carries view text and a base table never does. Consumed by {@code PluginDrivenExternalTable}
     * to resolve {@code isView()} (only when the connector declares {@link ConnectorCapability#SUPPORTS_VIEW})
     * and by {@code PluginDrivenExternalCatalog.dropTable} to route a DROP onto {@link #dropView}; returning
     * {@code false} for a base table is exactly what keeps a normal DROP TABLE on the table-handle path. This
     * uses the same single {@code getTable} the caller path needs and does NOT wrap in an auth context
     * (ThriftHmsClient authenticates internally, unlike the iceberg connector). A missing table is not a view.
     *
     * <p>Distinct from the {@code tableType}-based {@link #isView(HmsTableInfo)} the Top-N gate uses: that gate
     * only excludes views from an optimization (a tableType proxy is adequate there and its unit test relies on
     * it), whereas this view signal must be the legacy-exact text predicate so {@link #getViewDefinition} is
     * only reached when the text needed to build the view SQL exists.
     */
    @Override
    public boolean viewExists(ConnectorSession session, String dbName, String viewName) {
        try {
            return isViewTable(hmsClient.getTable(dbName, viewName));
        } catch (HmsClientException e) {
            LOG.debug("View existence check: '{}.{}' not found: {}", dbName, viewName, e.getMessage());
            return false;
        }
    }

    /**
     * Loads the stored definition of a hive view, a connector-side port of legacy
     * {@code HMSExternalTable.getViewText} plus the view-column half of {@code initHiveSchema}. ONE
     * {@code hmsClient.getTable} supplies both the SQL body (via {@link #resolveViewText}) and the view's
     * columns — a hive view exposes ordinary columns from its StorageDescriptor, built exactly like a base
     * table's data columns. fe-core ({@code PluginDrivenExternalTable.initSchema}) takes a view's columns
     * SOLELY from here (it never calls {@code getTableSchema} for a view), so the column list is non-empty for
     * a real view. The {@code dialect} is a required-non-null placeholder fe-core never reads. Callers gate on
     * {@link #viewExists}, so the view text is present; a defensive fail-loud guards the pathological
     * empty-text case rather than letting the DTO constructor NPE.
     */
    @Override
    public ConnectorViewDefinition getViewDefinition(ConnectorSession session, String dbName, String viewName) {
        HmsTableInfo tableInfo = hmsClient.getTable(dbName, viewName);
        String sql = resolveViewText(tableInfo);
        if (sql == null) {
            throw new DorisConnectorException(
                    "Hive view " + dbName + "." + viewName + " has no view definition text");
        }
        List<ConnectorColumn> columns = buildColumns(tableInfo);
        return new ConnectorViewDefinition(sql, HIVE_VIEW_DIALECT, columns);
    }

    /**
     * Drops a hive view, a connector-side port of the way legacy {@code HiveMetadataOps.dropTableImpl} dropped a
     * view: hive has no separate drop-view, a view is deleted through the same metastore {@code dropTable}. This
     * is reached only via {@code PluginDrivenExternalCatalog.dropTable} after {@link #viewExists} confirmed the
     * target is a view; a view is never transactional, so the transactional-table guard the table drop applies
     * is unnecessary here. Failures are normalized into a {@link DorisConnectorException} (not a bare
     * RuntimeException) so {@code PluginDrivenExternalCatalog.dropTable} rewraps them as a {@code DdlException}.
     */
    @Override
    public void dropView(ConnectorSession session, String dbName, String viewName) {
        try {
            hmsClient.dropTable(dbName, viewName);
        } catch (HmsClientException e) {
            throw new DorisConnectorException("Failed to drop Hive view "
                    + dbName + "." + viewName + ": " + e.getMessage(), e);
        }
    }

    // listViewNames is intentionally NOT overridden: hive's listTableNames (HMS get_all_tables) already
    // includes views, and PluginDrivenExternalCatalog.listTableNamesFromRemote merges listViewNames into
    // SHOW TABLES with a plain addAll (no dedup). Returning view names here would DOUBLE-list every hive view;
    // the SPI default (empty) keeps SHOW TABLES listing each view exactly once, matching legacy. This is the
    // opposite of iceberg, whose listTableNames subtracts views and whose listViewNames re-supplies them.

    /**
     * Whether the metastore table carries view text, the exact predicate of legacy
     * {@code HMSExternalTable.isView} ({@code isSetViewOriginalText() || isSetViewExpandedText()}).
     */
    private static boolean isViewTable(HmsTableInfo tableInfo) {
        return tableInfo.getViewOriginalText() != null || tableInfo.getViewExpandedText() != null;
    }

    /**
     * Resolves a hive view's SQL body, a byte-faithful port of legacy {@code HMSExternalTable.getViewText}:
     * prefer {@code viewExpandedText} unless it is empty or the bare {@code "/* Presto View *}{@code /"}
     * sentinel, otherwise parse the base64 Presto/Trino definition out of {@code viewOriginalText}.
     */
    private static String resolveViewText(HmsTableInfo tableInfo) {
        String expanded = tableInfo.getViewExpandedText();
        if (expanded != null && !expanded.isEmpty() && !PRESTO_VIEW_EXPANDED_SENTINEL.equals(expanded)) {
            return expanded;
        }
        return parseTrinoViewDefinition(tableInfo.getViewOriginalText());
    }

    /**
     * Extracts the SQL out of a Presto/Trino view definition stored in {@code originalText}, a port of legacy
     * {@code HMSExternalTable.parseTrinoViewDefinition}. The format is
     * {@code "/* Presto View: <base64-json> *}{@code /"} where the JSON carries an {@code originalSql} field.
     * Returns {@code originalText} unchanged when it is not a Presto view, and falls back to the raw
     * {@code originalText} on ANY decode/parse failure (legacy parity).
     */
    private static String parseTrinoViewDefinition(String originalText) {
        if (originalText == null || !originalText.contains(PRESTO_VIEW_PREFIX)) {
            return originalText;
        }
        try {
            String base64String = originalText.substring(
                    originalText.indexOf(PRESTO_VIEW_PREFIX) + PRESTO_VIEW_PREFIX.length(),
                    originalText.lastIndexOf(PRESTO_VIEW_SUFFIX)).trim();
            byte[] decodedBytes = Base64.getDecoder().decode(base64String);
            String decodedString = new String(decodedBytes, StandardCharsets.UTF_8);
            JsonObject jsonObject = new Gson().fromJson(decodedString, JsonObject.class);
            if (jsonObject.has(PRESTO_VIEW_ORIGINAL_SQL_KEY)) {
                return jsonObject.get(PRESTO_VIEW_ORIGINAL_SQL_KEY).getAsString();
            }
        } catch (Exception e) {
            LOG.warn("Decoding Presto view definition failed", e);
        }
        return originalText;
    }

    // ========== ConnectorStatisticsOps ==========

    /**
     * Table-level statistics for a hive table, a port of legacy {@code StatisticsUtil.getHiveRowCount} +
     * {@code getTotalSizeFromHMS} restricted to the two RAW metastore facts (no Doris-type math — the
     * connector must not import fe-type):
     * <ul>
     *   <li>{@code rowCount} = the exact HMS {@code numRows}, falling back to the spark-written
     *       {@code spark.sql.statistics.numRows} ONLY when {@code numRows} is present but non-positive
     *       (legacy {@code getRowCountFromParameters} — a table carrying only the spark key and no plain
     *       {@code numRows} deliberately does NOT surface a spark count here). A count {@code <= 0} maps to
     *       -1 (UNKNOWN), matching the legacy "0 -> UNKNOWN" gate and the paimon/iceberg connectors.</li>
     *   <li>{@code dataSize} = the on-disk {@code totalSize}, falling back to
     *       {@code spark.sql.statistics.totalSize} when the standard key is ABSENT (legacy size branch —
     *       note the asymmetry with the row-count fallback).</li>
     * </ul>
     * When the exact count is unknown but a size is present, fe-core
     * ({@code PluginDrivenExternalTable.fetchRowCount}) estimates the cardinality as
     * {@code dataSize / <Doris row width>} — the type-dependent division this connector cannot do. Returns
     * empty when neither fact is available (fe-core then falls through to the file-list estimate). Params
     * are read from the handle (loaded live by {@code getTableHandle}), so this adds no HMS round-trip.
     */
    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session, ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).getTableStatistics(session, handle);
        }
        Map<String, String> params = ((HiveTableHandle) handle).getTableParameters();
        if (params == null) {
            return Optional.empty();
        }

        long rowCount = -1;
        if (params.containsKey(PARAM_NUM_ROWS)) {
            rowCount = parseLongOrDefault(params.get(PARAM_NUM_ROWS), -1);
            if (rowCount <= 0 && params.containsKey(PARAM_SPARK_NUM_ROWS)) {
                rowCount = parseLongOrDefault(params.get(PARAM_SPARK_NUM_ROWS), -1);
            }
        }

        long dataSize = -1;
        if (params.containsKey(PARAM_TOTAL_SIZE)) {
            dataSize = parseLongOrDefault(params.get(PARAM_TOTAL_SIZE), -1);
        } else if (params.containsKey(PARAM_SPARK_TOTAL_SIZE)) {
            dataSize = parseLongOrDefault(params.get(PARAM_SPARK_TOTAL_SIZE), -1);
        }

        // Collapse a non-positive count/size to the -1 UNKNOWN sentinel (0 -> UNKNOWN, legacy parity).
        long reportedRows = rowCount > 0 ? rowCount : -1;
        long reportedSize = dataSize > 0 ? dataSize : -1;
        if (reportedRows < 0 && reportedSize < 0) {
            return Optional.empty();
        }
        return Optional.of(new ConnectorTableStatistics(reportedRows, reportedSize));
    }

    /**
     * Serves the query-planner column-statistics fast path from HMS-recorded (no-scan) column stats, a port of
     * legacy {@code HMSExternalTable.getHiveColumnStats}. Returns RAW facts only (rowCount / ndv / numNulls /
     * avgColLen) — fe-core does the Doris-type-dependent {@code dataSize}/{@code avgSize} math in
     * {@code PluginDrivenExternalTable.getColumnStatistic} (it must not import fe-type).
     *
     * <p>Empty (fe-core then falls back to a full ANALYZE) when: the table is not a plain-hive table
     * (iceberg-on-HMS is served by the iceberg sibling; hudi had no fast path); the table has no positive
     * {@code numRows} parameter (legacy required it as the per-column data-size basis, and does NOT fall back
     * to the spark count here, unlike the table-level size branch); or HMS holds no stats for the column.</p>
     */
    @Override
    public Optional<ConnectorColumnStatistics> getColumnStatistics(
            ConnectorSession session, ConnectorTableHandle handle, String columnName) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).getColumnStatistics(session, handle, columnName);
        }
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        if (hiveHandle.getTableType() != HiveTableType.HIVE) {
            return Optional.empty();
        }
        Map<String, String> params = hiveHandle.getTableParameters();
        if (params == null || !params.containsKey(PARAM_NUM_ROWS)) {
            return Optional.empty();
        }
        long rowCount = parseLongOrDefault(params.get(PARAM_NUM_ROWS), -1);
        if (rowCount <= 0) {
            return Optional.empty();
        }
        List<HmsColumnStatistics> stats = hmsClient.getTableColumnStatistics(
                hiveHandle.getDbName(), hiveHandle.getTableName(), Collections.singletonList(columnName));
        if (stats.isEmpty()) {
            return Optional.empty();
        }
        // Legacy read at most one stats object per column; take the first.
        HmsColumnStatistics stat = stats.get(0);
        return Optional.of(new ConnectorColumnStatistics(
                rowCount, stat.getNdv(), stat.getNumNulls(), stat.getAvgColLenBytes()));
    }

    /**
     * Parses a metastore numeric parameter defensively. Legacy read these with a bare {@code Long.parseLong}
     * under an outer try/catch that logged and returned UNKNOWN; returning {@code defaultValue} on a
     * null/blank/malformed value is the same net effect without letting one bad parameter abort the read.
     */
    private static long parseLongOrDefault(String value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Estimates the table's on-disk data size (bytes) by listing its data files, a port of the file-listing
     * half of legacy {@code HMSExternalTable.getRowCountFromFileList} (fe-core does the
     * {@code size / rowWidth} division). Only plain-hive tables are estimated (hudi/iceberg-on-HMS are served
     * by their own connectors; a view has no data files) — anything else returns -1. Partitions are sampled
     * ({@link #STATS_PARTITION_SAMPLE_SIZE}) and the sampled size scaled back up to the whole table, exactly
     * as legacy did. Best-effort: ANY error (unlistable location, remote failure) degrades to -1, never
     * throwing — statistics must not fail a query. The Hadoop {@code FileSystem} reflection resolves its
     * filesystem impl through the thread context classloader, so this pins the TCCL to the plugin classloader
     * for the duration (the statistics thread is not pinned by fe-core, unlike the scan thread).
     */
    @Override
    public long estimateDataSizeByListingFiles(ConnectorSession session, ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).estimateDataSizeByListingFiles(session, handle);
        }
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        if (hiveHandle.getTableType() != HiveTableType.HIVE) {
            return -1;
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            Configuration conf = buildHadoopConf();
            return estimateDataSize(hiveHandle, STATS_PARTITION_SAMPLE_SIZE,
                    location -> sumCachedFileSizes(hiveHandle, location, conf));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /**
     * Returns the raw byte length of every data file across ALL partitions (not sampled, not summed), a port of
     * legacy {@code HMSExternalTable.getChunkSizes} for {@code ANALYZE ... WITH SAMPLE}. Only plain-hive tables
     * are listed (iceberg/hudi-on-HMS are served by their own connectors via the sibling divert; a view has no
     * data files) — anything else returns empty. Lists EVERY partition (no {@link #STATS_PARTITION_SAMPLE_SIZE}
     * sampling, unlike {@link #estimateDataSizeByListingFiles}) because the fe-core sampler needs the individual
     * file sizes to seed-shuffle and cumulate. A listing error PROPAGATES here (unlike
     * {@link #estimateDataSizeByListingFiles}'s best-effort {@code -1}): this backs an explicit
     * {@code ANALYZE ... WITH SAMPLE}, and legacy {@code HMSExternalTable.getChunkSizes} failed the command loud
     * rather than let the sampler collapse the scale factor to {@code 1.0} while {@code TABLESAMPLE} still fires
     * (a silent stat undercount); a genuinely empty table still yields an empty list naturally. Pins the TCCL to
     * the plugin classloader for the {@code FileSystem} reflection (the statistics thread is not pinned by
     * fe-core), restored on the throw path by the finally.
     */
    @Override
    public List<Long> listFileSizes(ConnectorSession session, ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).listFileSizes(session, handle);
        }
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        if (hiveHandle.getTableType() != HiveTableType.HIVE) {
            return Collections.emptyList();
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            Configuration conf = buildHadoopConf();
            List<Long> sizes = new ArrayList<>();
            for (String location : resolvePartitionLocations(hiveHandle)) {
                for (HiveFileStatus file : fileListingCache.listDataFiles(
                        hiveHandle.getDbName(), hiveHandle.getTableName(), location, conf)) {
                    sizes.add(file.getLength());
                }
            }
            return sizes;
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /**
     * Engine-neutral rows for a connector metadata table (currently only the hudi commit timeline). A plain-hive
     * table has no metadata table, so the hive branch returns nothing; a foreign (hudi-on-HMS) handle is diverted
     * to the owning sibling connector, whose {@code getMetadataTableRows} produces the real timeline (and pins the
     * TCCL itself). Without this divert a flipped hudi-on-HMS table would fall through to the SPI-default empty list
     * even though {@link #reflectSiblingScanCapabilities} reflects the hudi sibling's {@code SUPPORTS_METADATA_TABLE}
     * and so makes the {@code hudi_meta()} / TIMELINE gate pass — i.e. OK-but-empty instead of the real timeline.
     * Mirrors the {@link #listFileSizes} / {@link #estimateDataSizeByListingFiles} per-handle divert.
     */
    @Override
    public List<List<String>> getMetadataTableRows(ConnectorSession session, ConnectorTableHandle handle,
            String kind) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).getMetadataTableRows(session, handle, kind);
        }
        return Collections.emptyList();
    }

    /**
     * Sampling + summing + scale-up core of {@link #estimateDataSizeByListingFiles}, isolated from the
     * {@code FileSystem} I/O (injected as {@code sizeOf}) so the estimation math is unit-testable. Returns -1
     * when the size cannot be estimated (no listable location, a zero/negative sum, or any error).
     */
    long estimateDataSize(HiveTableHandle handle, int sampleSize, ToLongFunction<String> sizeOf) {
        try {
            List<String> locations = resolvePartitionLocations(handle);
            if (locations.isEmpty()) {
                return -1;
            }
            int totalPartitions = locations.size();
            boolean sampled = sampleSize > 0 && sampleSize < totalPartitions;
            List<String> chosen = locations;
            if (sampled) {
                List<String> shuffled = new ArrayList<>(locations);
                Collections.shuffle(shuffled);
                chosen = shuffled.subList(0, sampleSize);
            }
            long totalSize = 0;
            for (String location : chosen) {
                totalSize += Math.max(0, sizeOf.applyAsLong(location));
            }
            if (totalSize <= 0) {
                return -1;
            }
            // Scale the sampled size up to the whole table (legacy: totalSize * total / sampled).
            if (sampled) {
                totalSize = scaleSampledSize(totalSize, totalPartitions, chosen.size());
            }
            return totalSize;
        } catch (RuntimeException e) {
            LOG.warn("Failed to estimate hive data size for {}.{} from file list",
                    handle.getDbName(), handle.getTableName(), e);
            return -1;
        }
    }

    /**
     * Scales a sampled data size up to the whole table: {@code sampledSize * totalPartitions /
     * sampledPartitions} (legacy {@code HMSExternalTable.getRowCountFromFileList}). Multiplies BEFORE dividing
     * to avoid early integer truncation (a divide-first ordering rounds the per-partition average down first
     * and yields a smaller, less accurate estimate). The multiply carries the same theoretical long-overflow
     * exposure as legacy for a petabyte-scale sample, accepted for parity.
     */
    static long scaleSampledSize(long sampledSize, int totalPartitions, int sampledPartitions) {
        return sampledSize * totalPartitions / sampledPartitions;
    }

    /**
     * Resolves the data locations to list: the table location for an unpartitioned table, else every
     * partition's location (bounded by {@link #MAX_PARTITIONS_FOR_STATS}). A partition or table with no
     * location contributes nothing.
     */
    private List<String> resolvePartitionLocations(HiveTableHandle handle) {
        List<String> partKeyNames = handle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            String location = handle.getLocation();
            return (location == null || location.isEmpty())
                    ? Collections.emptyList() : Collections.singletonList(location);
        }
        List<String> partNames = hmsClient.listPartitionNames(
                handle.getDbName(), handle.getTableName(), MAX_PARTITIONS_FOR_STATS);
        if (partNames.isEmpty()) {
            return Collections.emptyList();
        }
        List<HmsPartitionInfo> partitions = hmsClient.getPartitions(
                handle.getDbName(), handle.getTableName(), partNames);
        List<String> locations = new ArrayList<>(partitions.size());
        for (HmsPartitionInfo partition : partitions) {
            String location = partition.getLocation();
            if (location != null && !location.isEmpty()) {
                locations.add(location);
            }
        }
        return locations;
    }

    /**
     * Sums the sizes of the data files directly under {@code location}, served from the connector's shared
     * {@link HiveFileListingCache} (which does the non-recursive {@code listStatus} and filters directories and
     * {@code _}/{@code .}-prefixed hidden files — the same filter, and the same listing, the scan path uses). A
     * listing failure propagates as a {@link DorisConnectorException} so {@link #estimateDataSize} degrades the
     * whole estimate to -1 (legacy's file-list estimate was all-or-nothing best-effort). Routing through the
     * cache keeps the periodic row-count refresh from re-listing directories a scan already cached.
     */
    private long sumCachedFileSizes(HiveTableHandle handle, String location, Configuration conf) {
        long sum = 0;
        for (HiveFileStatus file : fileListingCache.listDataFiles(
                handle.getDbName(), handle.getTableName(), location, conf)) {
            sum += file.getLength();
        }
        return sum;
    }

    /**
     * Builds a Hadoop {@link Configuration} from the catalog properties, mirroring
     * {@code HiveScanPlanProvider.buildHadoopConf} (the connector must supply the storage credentials for the
     * FileSystem it lists).
     */
    private Configuration buildHadoopConf() {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        String defaultFs = properties.get("fs.defaultFS");
        if (defaultFs == null) {
            defaultFs = properties.get("hadoop.fs.defaultFS");
        }
        if (defaultFs != null) {
            conf.set("fs.defaultFS", defaultFs);
        }
        return conf;
    }

    // ========== ConnectorPushdownOps: Filter Pushdown ==========

    @Override
    public Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorFilterConstraint constraint) {
        if (!(handle instanceof HiveTableHandle)) {
            // Forward AND return the sibling's result UNMODIFIED (a rewrap would poison a downstream scan cast).
            return siblingMetadata(session, handle).applyFilter(session, handle, constraint);
        }
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

    // ========== ConnectorTableOps: partition listing ==========

    /**
     * Lists a partitioned table's partition display names (e.g. {@code "year=2024/month=01"}), taken
     * straight from the metastore's {@code get_partition_names}. Byte-parity with legacy
     * {@code HiveExternalMetaCache.loadPartitionValues}, whose hot partition-pruning path listed NAMES ONLY
     * (no per-partition metadata round-trip). An unpartitioned table lists nothing (the metastore has no
     * partitions; the guard mirrors {@code PaimonConnectorMetadata.collectPartitions}, avoiding a pointless
     * RPC).
     */
    @Override
    public List<String> listPartitionNames(ConnectorSession session, ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).listPartitionNames(session, handle);
        }
        return collectPartitionNames((HiveTableHandle) handle);
    }

    /**
     * Lists all partitions with metadata. The {@code filter} is intentionally ignored: legacy hive
     * materialized its full partition view and pruned FE-side (mirrors {@code PaimonConnectorMetadata} /
     * {@code MaxComputeConnectorMetadata}).
     *
     * <p>{@code lastModifiedMillis} is deliberately left {@link ConnectorPartitionInfo#UNKNOWN} (-1):
     * reading each partition's {@code transient_lastDdlTime} requires a {@code get_partitions_by_names}
     * round-trip that legacy's per-query partition-view path did NOT pay (it read only partition names),
     * so filling it here would regress every partitioned-hive query. Legacy fetched per-partition modify
     * time only at MTMV-refresh time; that freshness path is rewired connector-side in the MVCC/MTMV step
     * (until then a hive MTMV base table's per-partition freshness is UNKNOWN, harmless while hive is
     * dormant). {@code rowCount}/{@code sizeBytes}/{@code fileCount} are likewise UNKNOWN — hive does not
     * declare {@code SUPPORTS_PARTITION_STATS} (legacy SHOW PARTITIONS lists names only).</p>
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).listPartitions(session, handle, filter);
        }
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        List<String> partKeyNames = hiveHandle.getPartitionKeyNames();
        List<String> partitionNames = collectPartitionNames(hiveHandle);
        List<ConnectorPartitionInfo> result = new ArrayList<>(partitionNames.size());
        for (String partitionName : partitionNames) {
            result.add(new ConnectorPartitionInfo(partitionName,
                    toPartitionValueMap(partitionName, partKeyNames),
                    Collections.emptyMap()));
        }
        return result;
    }

    /**
     * Shared partition-name lister backing {@link #listPartitionNames} and {@link #listPartitions}. Returns
     * the metastore's rendered partition names ({@code key=value/...}); an unpartitioned table (no partition
     * keys) lists nothing without touching the metastore.
     */
    private List<String> collectPartitionNames(HiveTableHandle handle) {
        List<String> partKeyNames = handle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            return Collections.emptyList();
        }
        // -1 = "all partitions": ThriftHmsClient maps it to an unbounded HMS listing (no silent cap),
        // matching legacy's default (Config.max_hive_list_partition_num = -1).
        return hmsClient.listPartitionNames(handle.getDbName(), handle.getTableName(), -1);
    }

    /**
     * Parses a rendered partition name ({@code key1=v1/key2=v2}) into a remote-key -&gt; value map, unescaping
     * each value via {@link HiveWriteUtils#toPartitionValues} (the byte-faithful port of legacy
     * {@code HiveUtil.toPartitionValues}). Keyed by the handle's remote partition-column names in schema
     * order, which is how {@code PluginDrivenExternalTable.getNameToPartitionItems} reads the values back.
     * Returns an empty map when the parsed value arity does not match the partition-key arity (defensive; a
     * malformed name is logged-and-skipped by the fe-core partition-item builder).
     */
    private static Map<String, String> toPartitionValueMap(String partitionName, List<String> partKeyNames) {
        List<String> values = HiveWriteUtils.toPartitionValues(partitionName);
        if (partKeyNames == null || values.size() != partKeyNames.size()) {
            return Collections.emptyMap();
        }
        Map<String, String> valueMap = new LinkedHashMap<>();
        for (int i = 0; i < partKeyNames.size(); i++) {
            valueMap.put(partKeyNames.get(i), values.get(i));
        }
        return valueMap;
    }

    // ========== MTMV freshness (last-modified; MTMV refresh path only, NOT the scan hot path) ==========

    /** HMS parameter carrying a table/partition's last-DDL time in SECONDS (byte-parity with legacy hive). */
    private static final String TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";

    /**
     * The query-begin pin for a hive table: a non-MVCC EMPTY pin (snapshot id {@code -1}, no scan options — so
     * {@code applySnapshot} is a no-op and the scan reads current) but flagged {@code lastModifiedFreshness} so
     * the generic model serves this table's MTMV table/partition snapshots from {@link #getTableFreshness} /
     * {@link #getPartitionFreshnessMillis} (last-modified) instead of pinning a constant snapshot id. The flag
     * rides on the pin so fe-core reads it off the pin it already holds — a snapshot-id connector never fires
     * the freshness probe. (Iceberg/hudi-on-HMS delegation, which returns a real snapshot-id pin for those
     * handles, lands with the sibling-connector substep; until then every hive-connector handle is last-modified.)
     */
    @Override
    public Optional<ConnectorMvccSnapshot> beginQuerySnapshot(ConnectorSession session,
            ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            // Diverts in lockstep with getTableFreshness/getPartitionFreshnessMillis: the pin's
            // isLastModifiedFreshness flag (false for an iceberg snapshot-id pin) gates whether fe-core consults
            // freshness at all, so half-diverting the pin would corrupt MVCC.
            return siblingMetadata(session, handle).beginQuerySnapshot(session, handle);
        }
        return Optional.of(ConnectorMvccSnapshot.builder().snapshotId(-1L).lastModifiedFreshness(true).build());
    }

    /**
     * Whole-table MTMV freshness for hive: the table's newest modify time, wrapped by fe-core into an
     * {@code MTMVMaxTimestampSnapshot} (byte-parity with legacy {@code HiveDlaTable.getTableSnapshot}).
     * Hive's whole-table change signal is a last-modified TIMESTAMP, never a snapshot id.
     *
     * <ul>
     *   <li><b>Unpartitioned</b> &rArr; the table's {@code transient_lastDdlTime} (already on the handle, no
     *       round-trip), named by the table.</li>
     *   <li><b>Partitioned</b> &rArr; the max {@code transient_lastDdlTime} over all partitions, named by the
     *       partition owning it (an empty partition set &rArr; {@code (tableName, 0)}). This pays a
     *       {@code get_partitions_by_names} round-trip, which is why this lives on the MTMV path, NOT
     *       {@link #listPartitions} (the scan hot path stays names-only).</li>
     * </ul>
     */
    @Override
    public Optional<ConnectorTableFreshness> getTableFreshness(ConnectorSession session,
            ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).getTableFreshness(session, handle);
        }
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        List<String> partKeyNames = hiveHandle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            // Parity HiveDlaTable.getTableSnapshot UNPARTITIONED branch: MTMVMaxTimestampSnapshot(name, lastDdl).
            return Optional.of(new ConnectorTableFreshness(hiveHandle.getTableName(),
                    lastDdlMillis(hiveHandle.getTableParameters())));
        }
        List<String> partitionNames = collectPartitionNames(hiveHandle);
        if (partitionNames.isEmpty()) {
            // Parity: an empty partition list yields MTMVMaxTimestampSnapshot(tableName, 0).
            return Optional.of(new ConnectorTableFreshness(hiveHandle.getTableName(), 0L));
        }
        List<HmsPartitionInfo> partitions =
                hmsClient.getPartitions(hiveHandle.getDbName(), hiveHandle.getTableName(), partitionNames);
        String maxName = hiveHandle.getTableName();
        long maxMillis = 0L;
        for (HmsPartitionInfo partition : partitions) {
            long millis = lastDdlMillis(partition.getParameters());
            // Strictly-greater keeps the FIRST partition on a tie (parity HiveDlaTable's `> maxVersionTime`).
            if (millis > maxMillis) {
                maxMillis = millis;
                maxName = renderPartitionName(partKeyNames, partition.getValues());
            }
        }
        return Optional.of(new ConnectorTableFreshness(maxName, maxMillis));
    }

    /**
     * Per-partition last-modified millis for hive (parity {@code HiveDlaTable.getPartitionSnapshot} ->
     * {@code MTMVTimestampSnapshot(hivePartition.getLastModifiedTime())}). Fetched on demand on the MTMV
     * refresh path — {@link #listPartitions} withholds it (names-only) to keep partitioned queries cheap.
     * fe-core has already validated the partition exists in the materialized set; an {@code empty} return
     * therefore means the partition VANISHED between the materialize and this fetch (a refresh-time race), and
     * fe-core raises the legacy "can not find partition" error (parity {@code HiveDlaTable.checkPartitionExists}).
     */
    @Override
    public OptionalLong getPartitionFreshnessMillis(ConnectorSession session, ConnectorTableHandle handle,
            String partitionName) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).getPartitionFreshnessMillis(session, handle, partitionName);
        }
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        List<HmsPartitionInfo> partitions = hmsClient.getPartitions(hiveHandle.getDbName(),
                hiveHandle.getTableName(), Collections.singletonList(partitionName));
        if (partitions.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(lastDdlMillis(partitions.get(0).getParameters()));
    }

    /**
     * The last-DDL time in MILLIS from an HMS parameter map, byte-parity with legacy
     * {@code HivePartition.getLastModifiedTime} / {@code HMSExternalTable.getLastDdlTime}: the
     * {@code transient_lastDdlTime} value (seconds) times 1000, or 0 when the parameter is absent.
     */
    private static long lastDdlMillis(Map<String, String> parameters) {
        if (parameters == null || !parameters.containsKey(TRANSIENT_LAST_DDL_TIME)) {
            return 0L;
        }
        return Long.parseLong(parameters.get(TRANSIENT_LAST_DDL_TIME)) * 1000;
    }

    // ========== Iceberg-on-HMS sibling delegation (forward-absent) ==========
    // Handle-based methods hive does NOT implement (it uses the SPI default) but iceberg DOES. Without an
    // override here a delegated iceberg-on-HMS table would get hive's SPI default — a SILENT wrong answer
    // (empty MVCC/time-travel/sys-tables, or an unthreaded snapshot pin), not a fail-loud. Each forwards a
    // foreign handle to the sibling and reproduces the SPI default for a real hive handle. The handle-out
    // methods (apply* / getSysTableHandle) return the sibling's handle UNMODIFIED — a rewrap would poison a
    // downstream scan cast.

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).getTableSchema(session, handle, snapshot);
        }
        // Hive has no schema-at-snapshot; the SPI default ignores the snapshot and returns the latest schema.
        return getTableSchema(session, handle);
    }

    @Override
    public Optional<ConnectorMvccPartitionView> getMvccPartitionView(ConnectorSession session,
            ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).getMvccPartitionView(session, handle);
        }
        // Hive has no range-aware partition view; fe-core builds it from listPartitions (SPI default empty).
        return Optional.empty();
    }

    @Override
    public Optional<ConnectorMvccSnapshot> resolveTimeTravel(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorTimeTravelSpec spec) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).resolveTimeTravel(session, handle, spec);
        }
        // Hive has no time travel (SPI default empty): an explicit spec on a hive table is unsupported upstream.
        return Optional.empty();
    }

    @Override
    public ConnectorTableHandle applySnapshot(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).applySnapshot(session, handle, snapshot);
        }
        // Hive's empty pin carries no scan options; the SPI default returns the handle unchanged.
        return handle;
    }

    @Override
    public List<ConnectorExpression> getSyntheticScanPredicates(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        if (!(handle instanceof HiveTableHandle)) {
            // Route a foreign (iceberg / hudi) handle to its owning sibling so a hudi-on-HMS @incr read gets
            // its row-level `_hoodie_commit_time` window filter. Without this the foreign handle would inherit
            // the empty SPI default -> no filter -> out-of-window rows leak (a SILENT correctness bug).
            return siblingMetadata(session, handle).getSyntheticScanPredicates(session, handle, snapshot);
        }
        // Plain hive has no synthetic scan predicate (SPI default empty).
        return List.of();
    }

    @Override
    public ConnectorTableHandle applyRewriteFileScope(ConnectorSession session, ConnectorTableHandle handle,
            Set<String> rawDataFilePaths) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).applyRewriteFileScope(session, handle, rawDataFilePaths);
        }
        // Hive has no distributed rewrite scope; the SPI default returns the handle unchanged.
        return handle;
    }

    @Override
    public ConnectorTableHandle applyTopnLazyMaterialization(ConnectorSession session,
            ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).applyTopnLazyMaterialization(session, handle);
        }
        // Hive scan metadata already spans all columns; the SPI default returns the handle unchanged.
        return handle;
    }

    @Override
    public List<String> listSupportedSysTables(ConnectorSession session, ConnectorTableHandle baseTableHandle) {
        if (!(baseTableHandle instanceof HiveTableHandle)) {
            return siblingMetadata(session, baseTableHandle).listSupportedSysTables(session, baseTableHandle);
        }
        // Hive exposes no system tables (SPI default empty).
        return Collections.emptyList();
    }

    @Override
    public Optional<ConnectorTableHandle> getSysTableHandle(ConnectorSession session,
            ConnectorTableHandle baseTableHandle, String sysName) {
        if (!(baseTableHandle instanceof HiveTableHandle)) {
            // Return the sibling's sys-table handle UNMODIFIED (a rewrap would poison a downstream scan cast).
            return siblingMetadata(session, baseTableHandle).getSysTableHandle(session, baseTableHandle, sysName);
        }
        // Hive exposes no system tables (SPI default empty).
        return Optional.empty();
    }

    /**
     * Renders {@code key1=v1/key2=v2} from partition-key names + values, byte-parity with legacy
     * {@code HivePartition.getPartitionName} (a raw, unescaped {@code name=value} join). Used only to label
     * the {@code MTMVMaxTimestampSnapshot} with the partition owning the max modify time.
     */
    private static String renderPartitionName(List<String> partKeyNames, List<String> values) {
        StringBuilder sb = new StringBuilder();
        int n = Math.min(partKeyNames.size(), values.size());
        for (int i = 0; i < n; i++) {
            if (i != 0) {
                sb.append("/");
            }
            sb.append(partKeyNames.get(i)).append("=").append(values.get(i));
        }
        return sb.toString();
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
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).dropTable(session, handle);
            return;
        }
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
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).truncateTable(session, handle, partitions);
            return;
        }
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        try {
            hmsClient.truncateTable(hiveHandle.getDbName(), hiveHandle.getTableName(), partitions);
        } catch (HmsClientException e) {
            throw new DorisConnectorException("Failed to truncate Hive table "
                    + hiveHandle.getDbName() + "." + hiveHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    // ========== ConnectorTableOps: ALTER-DDL -- foreign (iceberg) handles divert to the sibling ==========
    //
    // Every mutating ALTER method already carries a handle. A foreign (iceberg-on-HMS) handle is forwarded to the
    // embedded iceberg sibling (which implements the real column / branch / tag / partition-field evolution); the
    // foreign handle is NEVER cast. A hive handle reproduces the pre-flip behavior: hive supports none of these
    // through this SPI, so its branch throws the exact SPI-default message it inherited before this override.
    // Dormant until hms enters SPI_READY_TYPES.

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle handle, String newName) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).renameTable(session, handle, newName);
            return;
        }
        // hive does not support ALTER TABLE RENAME (legacy HMSCachedClient has no rename).
        throw new DorisConnectorException("RENAME TABLE not supported");
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle handle, ConnectorColumn column,
            ConnectorColumnPosition position) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).addColumn(session, handle, column, position);
            return;
        }
        throw new DorisConnectorException("ADD COLUMN not supported");
    }

    @Override
    public void addColumns(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorColumn> columns) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).addColumns(session, handle, columns);
            return;
        }
        throw new DorisConnectorException("ADD COLUMNS not supported");
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle handle, String columnName) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).dropColumn(session, handle, columnName);
            return;
        }
        throw new DorisConnectorException("DROP COLUMN not supported");
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle handle, String oldName,
            String newName) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).renameColumn(session, handle, oldName, newName);
            return;
        }
        throw new DorisConnectorException("RENAME COLUMN not supported");
    }

    @Override
    public void modifyColumn(ConnectorSession session, ConnectorTableHandle handle, ConnectorColumn column,
            ConnectorColumnPosition position) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).modifyColumn(session, handle, column, position);
            return;
        }
        throw new DorisConnectorException("MODIFY COLUMN not supported");
    }

    @Override
    public void reorderColumns(ConnectorSession session, ConnectorTableHandle handle, List<String> newOrder) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).reorderColumns(session, handle, newOrder);
            return;
        }
        throw new DorisConnectorException("REORDER COLUMNS not supported");
    }

    @Override
    public void createOrReplaceBranch(ConnectorSession session, ConnectorTableHandle handle, BranchChange branch) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).createOrReplaceBranch(session, handle, branch);
            return;
        }
        throw new DorisConnectorException("CREATE/REPLACE BRANCH not supported");
    }

    @Override
    public void createOrReplaceTag(ConnectorSession session, ConnectorTableHandle handle, TagChange tag) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).createOrReplaceTag(session, handle, tag);
            return;
        }
        throw new DorisConnectorException("CREATE/REPLACE TAG not supported");
    }

    @Override
    public void dropBranch(ConnectorSession session, ConnectorTableHandle handle, DropRefChange branch) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).dropBranch(session, handle, branch);
            return;
        }
        throw new DorisConnectorException("DROP BRANCH not supported");
    }

    @Override
    public void dropTag(ConnectorSession session, ConnectorTableHandle handle, DropRefChange tag) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).dropTag(session, handle, tag);
            return;
        }
        throw new DorisConnectorException("DROP TAG not supported");
    }

    @Override
    public void addPartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).addPartitionField(session, handle, change);
            return;
        }
        throw new DorisConnectorException("ADD PARTITION FIELD not supported");
    }

    @Override
    public void dropPartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).dropPartitionField(session, handle, change);
            return;
        }
        throw new DorisConnectorException("DROP PARTITION FIELD not supported");
    }

    @Override
    public void replacePartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).replacePartitionField(session, handle, change);
            return;
        }
        throw new DorisConnectorException("REPLACE PARTITION FIELD not supported");
    }

    // ========== ConnectorWriteOps: write validation -- foreign (iceberg) handles divert to the sibling ==========
    //
    // Both validators carry a handle and run at analysis time. A foreign (iceberg-on-HMS) handle forwards to the
    // sibling so iceberg's real write-mode / static-partition rejections apply. A hive handle MUST reproduce the
    // permissive SPI default (return silently, NEVER throw) -- a throw here would newly reject legal plain-hive
    // row-level DML / static-partition INSERTs. Dormant until hms enters SPI_READY_TYPES.

    @Override
    public void validateRowLevelDmlMode(ConnectorSession session, ConnectorTableHandle handle, WriteOperation op) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).validateRowLevelDmlMode(session, handle, op);
            return;
        }
        // hive: no per-table row-level DML mode constraint (SPI default no-op).
    }

    @Override
    public void validateStaticPartitionColumns(ConnectorSession session, ConnectorTableHandle handle,
            List<String> staticPartitionColumnNames) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle)
                    .validateStaticPartitionColumns(session, handle, staticPartitionColumnNames);
            return;
        }
        // hive: no static-partition constraint (SPI default no-op).
    }

    /**
     * Rejects the dynamic partition-NAME list form ({@code INSERT ... PARTITION(p1, p2)}) on a hive table with the
     * exact legacy message. UNLIKE the two permissive validators above, a hive handle here THROWS on a non-empty
     * list — this is the net-new port of the legacy fe-core reject ({@code BindSink.bindHiveTableSink}), not a
     * silent no-op. A foreign (iceberg-on-HMS) handle forwards to the sibling, which accepts {@code
     * PARTITION(names)} exactly as a standalone {@code type=iceberg} catalog does (no heterogeneous-vs-standalone
     * divergence); the forward happens regardless of emptiness (the empty-early-return is hive-only). An empty
     * list returns silently for a hive handle (a plain {@code INSERT ... SELECT} or a static {@code
     * PARTITION(col='val')} INSERT is legal plain-hive). Dormant until hms enters SPI_READY_TYPES.
     */
    @Override
    public void validateWritePartitionNames(ConnectorSession session, ConnectorTableHandle handle,
            List<String> partitionNames) {
        if (!(handle instanceof HiveTableHandle)) {
            siblingMetadata(session, handle).validateWritePartitionNames(session, handle, partitionNames);
            return;
        }
        if (partitionNames != null && !partitionNames.isEmpty()) {
            throw new DorisConnectorException("Not support insert with partition spec in hive catalog.");
        }
    }

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
     * Per-handle transaction open: a FOREIGN (iceberg-on-HMS) handle forwards to the sibling so the
     * session-bound transaction is the sibling's {@code IcebergConnectorTransaction} that iceberg's write plan
     * downcasts; a hive handle falls through to the connector-level {@link #beginTransaction(ConnectorSession)}
     * (a {@code HiveConnectorTransaction} that the hive write plan downcasts). The two write plans downcast to
     * DIFFERENT concrete transaction types, so the selection MUST be symmetric — an always-forward (or
     * always-hive) shortcut breaks the opposite side. The engine passes the resolved write-target handle
     * (never null). Dormant until hms enters SPI_READY_TYPES.
     */
    @Override
    public ConnectorTransaction beginTransaction(ConnectorSession session, ConnectorTableHandle handle) {
        if (!(handle instanceof HiveTableHandle)) {
            return siblingMetadata(session, handle).beginTransaction(session, handle);
        }
        return beginTransaction(session);
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

    /**
     * Whether {@code tableInfo} is a plain-hive orc/parquet base table eligible for Top-N lazy materialization,
     * replicating legacy {@code HMSExternalTable.supportedHiveTopNLazyTable} plus the {@code getDlaType()==HIVE}
     * guard the legacy consumer ({@code MaterializeProbeVisitor}) applied: a view is excluded, an
     * iceberg/hudi-on-HMS table is excluded (those are served by their own connector, which declares the
     * capability connector-wide after the cutover), and only the parquet/orc input formats qualify.
     */
    private boolean supportsHiveTopNLazyMaterialize(HmsTableInfo tableInfo) {
        if (isView(tableInfo)) {
            return false;
        }
        if (HiveTableFormatDetector.detect(tableInfo) != HiveTableType.HIVE) {
            return false;
        }
        String inputFormat = tableInfo.getInputFormat();
        return MAPRED_PARQUET_INPUT_FORMAT.equals(inputFormat) || ORC_INPUT_FORMAT.equals(inputFormat);
    }

    /**
     * Whether {@code tableInfo} is a plain-hive data table (any file format) eligible for background per-column
     * auto-analyze, replicating legacy {@code StatisticsUtil.supportAutoAnalyze}'s {@code dlaType==HIVE} gate.
     * Unlike {@link #supportsHiveTopNLazyMaterialize} there is NO orc/parquet restriction (legacy analyzed any hive
     * format); a view is excluded (nothing to analyze) and an iceberg/hudi-on-HMS table is excluded here
     * ({@code detect() != HIVE}) — iceberg-on-HMS instead inherits the capability from its sibling via
     * {@link #reflectSiblingScanCapabilities}, and hudi-on-HMS is withheld.
     */
    private boolean supportsHiveColumnAutoAnalyze(HmsTableInfo tableInfo) {
        return !isView(tableInfo) && HiveTableFormatDetector.detect(tableInfo) == HiveTableType.HIVE;
    }

    /**
     * Whether {@code tableInfo} is a plain-hive data table (any file format) eligible for {@code ANALYZE ... WITH
     * SAMPLE}, replicating legacy {@code AnalysisManager.canSample}'s {@code dlaType==HIVE} gate. Like
     * {@link #supportsHiveColumnAutoAnalyze} there is NO orc/parquet restriction (legacy sampled any hive format);
     * a view is excluded and an iceberg/hudi-on-HMS table is excluded ({@code detect() != HIVE}) so sampled
     * analyze stays rejected for them (their {@code doSample} is unimplemented).
     */
    private boolean supportsHiveSampleAnalyze(HmsTableInfo tableInfo) {
        return !isView(tableInfo) && HiveTableFormatDetector.detect(tableInfo) == HiveTableType.HIVE;
    }

    /** Whether the HMS table is a view (tableType VIRTUAL_VIEW), mirroring legacy {@code HMSExternalTable.isView}. */
    private static boolean isView(HmsTableInfo tableInfo) {
        return VIRTUAL_VIEW_TABLE_TYPE.equalsIgnoreCase(tableInfo.getTableType());
    }

    /**
     * Whether the table's first (data) column is a {@code STRING}, reproducing legacy
     * {@code HMSExternalTable.firstColumnIsString} ({@code isScalarType(PrimitiveType.STRING)} — {@code STRING}
     * only, NOT {@code varchar}/{@code char}). Stamped onto the handle so the read-format detector can apply the
     * OpenX-JSON {@code read_hive_json_in_one_column} gate without a second metastore fetch. A table with no data
     * columns degrades to {@code false} (the OpenX one-column mode is nonsensical there).
     */
    private static boolean firstColumnIsString(HmsTableInfo tableInfo) {
        List<ConnectorColumn> columns = tableInfo.getColumns();
        if (columns == null || columns.isEmpty()) {
            return false;
        }
        return "STRING".equals(columns.get(0).getType().getTypeName());
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

    static Map<String, String> parsePartitionName(String partName,
            List<String> partKeyNames) {
        Map<String, String> values = new HashMap<>();
        String[] parts = partName.split("/");
        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                // Unescape the VALUE: HMS get_partition_names returns Hive-escaped names (e.g. "%3A" for ':').
                // The predicate literal side (extractLiteralValue) is unescaped, so matchesPredicates' string
                // compare needs the value unescaped too — otherwise an escaped partition value silently drops
                // rows. Mirrors the sibling partition-value parse (HiveWriteUtils.toPartitionValues) and legacy
                // FileUtils.unescapePathName. The key is a column name (never escaped), left as-is.
                values.put(part.substring(0, eq),
                        HiveWriteUtils.unescapePathName(part.substring(eq + 1)));
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

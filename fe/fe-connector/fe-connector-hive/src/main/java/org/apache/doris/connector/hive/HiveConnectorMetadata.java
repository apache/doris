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

import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.ConnectorViewDefinition;
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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
        if (supportsHiveTopNLazyMaterialize(tableInfo)) {
            perTableCapabilities.add(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE.name());
        }
        if (!perTableCapabilities.isEmpty()) {
            tableProperties.put(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY,
                    String.join(",", perTableCapabilities));
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
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        if (hiveHandle.getTableType() != HiveTableType.HIVE) {
            return -1;
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            Configuration conf = buildHadoopConf();
            return estimateDataSize(hiveHandle, STATS_PARTITION_SAMPLE_SIZE,
                    location -> sumFileSizesUnder(location, conf));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
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
     * Sums the sizes of the data files directly under {@code location} (non-recursive, skipping directories
     * and {@code _}/{@code .}-prefixed hidden files — the same filter as {@link HiveScanPlanProvider}). A
     * listing failure is surfaced as a {@link RuntimeException} so {@link #estimateDataSize} degrades the
     * whole estimate to -1 (legacy's file-list estimate was all-or-nothing best-effort).
     */
    private static long sumFileSizesUnder(String location, Configuration conf) {
        try {
            Path path = new Path(location);
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            FileStatus[] statuses = fs.listStatus(path);
            long sum = 0;
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    continue;
                }
                String fileName = status.getPath().getName();
                if (fileName.startsWith("_") || fileName.startsWith(".")) {
                    continue;
                }
                sum += status.getLen();
            }
            return sum;
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to list files under " + location, e);
        }
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

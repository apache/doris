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

package org.apache.doris.datasource;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.PassthroughQueryTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.api.pushdown.LimitApplicationResult;
import org.apache.doris.connector.api.pushdown.ProjectionApplicationResult;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TMaxComputeFileDesc;
import org.apache.doris.thrift.TPaimonDeletionFileDesc;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTrinoConnectorFileDesc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Generic scan node that delegates scan planning to the connector SPI.
 *
 * <p>Replaces connector-specific ScanNode subclasses for plugin-driven catalogs.
 * Extends {@link FileQueryScanNode} to reuse the existing split-to-Thrift
 * conversion pipeline. Uses {@code FORMAT_JNI} by default, which routes
 * to BE's JNI scanner framework.</p>
 *
 * <p>Scan flow:</p>
 * <ol>
 *   <li>{@link #getSplits} calls {@link ConnectorScanPlanProvider#planScan}
 *       to obtain {@link ConnectorScanRange}s from the connector plugin</li>
 *   <li>Each range is wrapped in a {@link PluginDrivenSplit}</li>
 *   <li>{@link FileQueryScanNode#createScanRangeLocations} distributes splits
 *       to backends</li>
 *   <li>{@link #setScanParams} populates {@link TTableFormatFileDesc} with
 *       connector-specific properties for each split</li>
 * </ol>
 */
public class PluginDrivenScanNode extends FileQueryScanNode {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenScanNode.class);

    private static final String TABLE_FORMAT_TYPE = "plugin_driven";

    /** Scan node property keys (shared with connector plugins). */
    private static final String PROP_FILE_FORMAT_TYPE = "file_format_type";
    private static final String PROP_PATH_PARTITION_KEYS = "path_partition_keys";
    private static final String PROP_LOCATION_PREFIX = "location.";
    private static final String PROP_HIVE_TEXT_PREFIX = "hive.text.";

    private final Connector connector;
    private final ConnectorSession connectorSession;

    // Set during filter pushdown; may be updated from the original table handle.
    private ConnectorTableHandle currentHandle;

    // Populated from ConnectorScanPlanProvider.getScanNodeProperties()
    private Map<String, String> scanNodeProperties;

    public PluginDrivenScanNode(PlanNodeId id, TupleDescriptor desc,
            boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext, Connector connector,
            ConnectorSession connectorSession, ConnectorTableHandle tableHandle) {
        super(id, desc, "PluginDrivenScanNode", scanContext, needCheckColumnPriv, sv);
        this.connector = connector;
        this.connectorSession = connectorSession;
        this.currentHandle = tableHandle;
    }

    /**
     * Creates a PluginDrivenScanNode by resolving the connector, session, and table handle
     * from the plugin-driven catalog and table.
     */
    public static PluginDrivenScanNode create(PlanNodeId id, TupleDescriptor desc,
            boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext, PluginDrivenExternalCatalog catalog,
            PluginDrivenExternalTable table) {
        Connector connector = catalog.getConnector();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        String dbName = table.getDb() != null ? table.getDb().getRemoteName() : "";
        String tableName = table.getRemoteName();
        ConnectorTableHandle handle = metadata.getTableHandle(session, dbName, tableName)
                .orElseThrow(() -> new RuntimeException(
                        "Table handle not found for plugin-driven table: " + dbName + "." + tableName));
        return new PluginDrivenScanNode(id, desc, needCheckColumnPriv, sv,
                scanContext, connector, session, handle);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (currentHandle instanceof PassthroughQueryTableHandle) {
            output.append(prefix).append("TABLE VALUE FUNCTION\n");
            String query = ((PassthroughQueryTableHandle) currentHandle).getQuery();
            output.append(prefix).append("QUERY: ").append(query).append("\n");
        } else {
            Map<String, String> props = getOrLoadScanNodeProperties();
            String query = props.get("query");
            output.append(prefix).append("TABLE: ")
                    .append(desc.getTable().getNameWithFullQualifiers()).append("\n");
            if (query != null) {
                output.append(prefix).append("QUERY: ").append(query).append("\n");
            }
            if (!conjuncts.isEmpty()) {
                Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
                output.append(prefix).append("PREDICATES: ")
                        .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE))
                        .append("\n");
            }
        }
        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }
        return output.toString();
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String format = props.get(PROP_FILE_FORMAT_TYPE);
        if (format != null) {
            return mapFileFormatType(format);
        }
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    protected List<String> getPathPartitionKeys() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String keys = props.get(PROP_PATH_PARTITION_KEYS);
        if (keys != null && !keys.isEmpty()) {
            return Arrays.asList(keys.split(","));
        }
        return Collections.emptyList();
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return desc.getTable();
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        Map<String, String> locationProps = new HashMap<>();
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(PROP_LOCATION_PREFIX)) {
                String realKey = entry.getKey().substring(PROP_LOCATION_PREFIX.length());
                locationProps.put(realKey, entry.getValue());
            }
        }
        return locationProps;
    }

    @Override
    protected TFileAttributes getFileAttributes() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String serDeLib = props.get(PROP_HIVE_TEXT_PREFIX + "serde_lib");
        if (serDeLib == null || serDeLib.isEmpty()) {
            return new TFileAttributes();
        }

        TFileAttributes attrs = new TFileAttributes();
        String skipLinesStr = props.get(PROP_HIVE_TEXT_PREFIX + "skip_lines");
        if (skipLinesStr != null) {
            try {
                attrs.setSkipLines(Integer.parseInt(skipLinesStr));
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        String colSep = props.get(PROP_HIVE_TEXT_PREFIX + "column_separator");
        if (colSep != null) {
            textParams.setColumnSeparator(colSep);
        }
        String lineSep = props.get(PROP_HIVE_TEXT_PREFIX + "line_delimiter");
        if (lineSep != null) {
            textParams.setLineDelimiter(lineSep);
        }
        String mapkvDelim = props.get(PROP_HIVE_TEXT_PREFIX + "mapkv_delimiter");
        if (mapkvDelim != null) {
            textParams.setMapkvDelimiter(mapkvDelim);
        }
        String collDelim = props.get(PROP_HIVE_TEXT_PREFIX + "collection_delimiter");
        if (collDelim != null) {
            textParams.setCollectionDelimiter(collDelim);
        }
        String escape = props.get(PROP_HIVE_TEXT_PREFIX + "escape");
        if (escape != null && !escape.isEmpty()) {
            textParams.setEscape(escape.getBytes()[0]);
        }
        String nullFmt = props.get(PROP_HIVE_TEXT_PREFIX + "null_format");
        if (nullFmt != null) {
            textParams.setNullFormat(nullFmt);
        }
        String enclose = props.get(PROP_HIVE_TEXT_PREFIX + "enclose");
        if (enclose != null && !enclose.isEmpty()) {
            textParams.setEnclose(enclose.getBytes()[0]);
            attrs.setTrimDoubleQuotes(true);
        }

        attrs.setTextParams(textParams);
        attrs.setHeaderType("");
        attrs.setEnableTextValidateUtf8(sessionVariable.enableTextValidateUtf8);

        String isJson = props.get(PROP_HIVE_TEXT_PREFIX + "is_json");
        if ("true".equals(isJson)) {
            attrs.setReadJsonByLine(true);
            attrs.setReadByColumnDef(true);
        }

        return attrs;
    }

    @Override
    protected void convertPredicate() {
        // Attempt filter pushdown via the connector SPI
        if (conjuncts == null || conjuncts.isEmpty()) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        ConnectorFilterConstraint constraint = buildFilterConstraint(conjuncts);
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(connectorSession, currentHandle, constraint);
        if (result.isPresent()) {
            FilterApplicationResult<ConnectorTableHandle> filterResult = result.get();
            currentHandle = filterResult.getHandle();

            // Consume remainingFilter to avoid duplicate predicate evaluation on BE:
            // - null means all predicates were fully pushed down → clear conjuncts
            // - non-null means some/all predicates remain → keep conjuncts (conservative)
            ConnectorExpression remaining = filterResult.getRemainingFilter();
            if (remaining == null) {
                conjuncts.clear();
                LOG.debug("Filter fully pushed down for plugin-driven scan, cleared conjuncts");
            } else {
                // Partial or full remaining: keep all conjuncts for BE-side evaluation.
                // Fine-grained conjunct removal (matching individual remaining sub-expressions
                // back to original Expr conjuncts) is deferred to a future enhancement.
                LOG.debug("Filter pushdown accepted with remaining filter, keeping conjuncts");
            }
        }
        // Invalidate cached properties so they are rebuilt with the updated conjuncts/handle.
        scanNodeProperties = null;
    }

    /**
     * Attempts to push the limit down via the SPI applyLimit() protocol.
     * Called before getSplits(), after filter pushdown.
     *
     * <p>If the connector accepts the limit, the handle is updated.
     * The limit is still passed to planScan() as a parameter for
     * connectors that handle limit directly in planScan().</p>
     */
    private void tryPushDownLimit() {
        if (limit <= 0) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Optional<LimitApplicationResult<ConnectorTableHandle>> result =
                metadata.applyLimit(connectorSession, currentHandle, limit);
        if (result.isPresent()) {
            currentHandle = result.get().getHandle();
            LOG.debug("Limit {} pushed down via applyLimit for plugin-driven scan", limit);
        }
    }

    /**
     * Attempts to push the projection down via the SPI applyProjection() protocol.
     * Called before getSplits(), after filter and limit pushdown.
     *
     * <p>If the connector accepts the projection, the handle is updated.</p>
     */
    private void tryPushDownProjection(List<ConnectorColumnHandle> columns) {
        if (columns.isEmpty()) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(connectorSession, currentHandle, columns);
        if (result.isPresent()) {
            currentHandle = result.get().getHandle();
            LOG.debug("Projection pushed down via applyProjection for plugin-driven scan");
        }
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        // Attempt limit and projection pushdown via SPI protocol
        tryPushDownLimit();

        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider == null) {
            LOG.warn("Connector does not provide a scan plan provider, returning empty splits");
            return Collections.emptyList();
        }

        List<ConnectorColumnHandle> columns = buildColumnHandles();
        tryPushDownProjection(columns);
        Optional<ConnectorExpression> remainingFilter = buildRemainingFilter();

        List<ConnectorScanRange> ranges = scanProvider.planScan(
                connectorSession, currentHandle, columns, remainingFilter, limit);

        List<Split> splits = new ArrayList<>(ranges.size());
        for (ConnectorScanRange range : ranges) {
            splits.add(new PluginDrivenSplit(range));
        }
        return splits;
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (!(split instanceof PluginDrivenSplit)) {
            return;
        }
        PluginDrivenSplit pluginSplit = (PluginDrivenSplit) split;
        ConnectorScanRange scanRange = pluginSplit.getConnectorScanRange();

        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(scanRange.getTableFormatType());

        String formatType = scanRange.getTableFormatType();
        if ("max_compute".equals(formatType)) {
            setMaxComputeParams(tableFormatFileDesc, scanRange, rangeDesc);
        } else if ("trino_connector".equals(formatType)) {
            setTrinoConnectorParams(tableFormatFileDesc, scanRange);
        } else if ("hive".equals(formatType)) {
            setHiveParams(tableFormatFileDesc, scanRange);
        } else if ("transactional_hive".equals(formatType)) {
            setTransactionalHiveParams(tableFormatFileDesc, scanRange);
        } else if ("hudi".equals(formatType)) {
            setHudiParams(tableFormatFileDesc, scanRange, rangeDesc);
        } else if ("paimon".equals(formatType)) {
            setPaimonParams(tableFormatFileDesc, scanRange, rangeDesc);
        } else {
            setGenericParams(tableFormatFileDesc, scanRange);
        }

        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    /**
     * Sets MaxCompute-specific scan params via TMaxComputeFileDesc.
     * BE expects typed Thrift fields, not a generic Map.
     */
    private void setMaxComputeParams(TTableFormatFileDesc formatDesc,
            ConnectorScanRange scanRange, TFileRangeDesc rangeDesc) {
        Map<String, String> props = scanRange.getProperties();
        TMaxComputeFileDesc fileDesc = new TMaxComputeFileDesc();
        fileDesc.setPartitionSpec("deprecated");
        fileDesc.setTableBatchReadSession(
                props.getOrDefault("table_batch_read_session", ""));
        fileDesc.setSessionId(props.getOrDefault("session_id", ""));
        fileDesc.setReadTimeout(
                Integer.parseInt(props.getOrDefault("read_timeout", "120")));
        fileDesc.setConnectTimeout(
                Integer.parseInt(props.getOrDefault("connect_timeout", "10")));
        fileDesc.setRetryTimes(
                Integer.parseInt(props.getOrDefault("retry_times", "4")));
        formatDesc.setMaxComputeParams(fileDesc);

        rangeDesc.setPath("[ " + scanRange.getStart() + " , " + scanRange.getLength() + " ]");
        rangeDesc.setStartOffset(scanRange.getStart());
        rangeDesc.setSize(scanRange.getLength());
    }

    /**
     * Sets Trino connector-specific scan params via TTrinoConnectorFileDesc.
     * All values are pre-serialized JSON strings from the plugin module.
     */
    private void setTrinoConnectorParams(TTableFormatFileDesc formatDesc,
            ConnectorScanRange scanRange) {
        Map<String, String> props = scanRange.getProperties();
        TTrinoConnectorFileDesc fileDesc = new TTrinoConnectorFileDesc();
        fileDesc.setCatalogName(props.getOrDefault("catalog_name", ""));
        fileDesc.setDbName(props.getOrDefault("db_name", ""));
        fileDesc.setTableName(props.getOrDefault("table_name", ""));
        fileDesc.setTrinoConnectorSplit(
                props.getOrDefault("trino_connector_split", ""));
        fileDesc.setTrinoConnectorTableHandle(
                props.getOrDefault("trino_connector_table_handle", ""));
        fileDesc.setTrinoConnectorColumnHandles(
                props.getOrDefault("trino_connector_column_handles", ""));
        fileDesc.setTrinoConnectorColumnMetadata(
                props.getOrDefault("trino_connector_column_metadata", ""));
        fileDesc.setTrinoConnectorTrascationHandle(
                props.getOrDefault("trino_connector_trascation_handle", ""));

        // Options is a map — parse from JSON or use directly
        String optionsJson = props.getOrDefault("trino_connector_options", "{}");
        try {
            @SuppressWarnings("unchecked")
            Map<String, String> options = new com.fasterxml.jackson.databind.ObjectMapper()
                    .readValue(optionsJson,
                            new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
            fileDesc.setTrinoConnectorOptions(options);
        } catch (Exception e) {
            LOG.warn("Failed to parse trino_connector_options JSON, using empty map", e);
            fileDesc.setTrinoConnectorOptions(new HashMap<>());
        }
        formatDesc.setTrinoConnectorParams(fileDesc);
    }

    /**
     * Sets generic scan params via jdbc_params map (for JDBC, plugin_driven, etc.).
     */
    private void setGenericParams(TTableFormatFileDesc formatDesc,
            ConnectorScanRange scanRange) {
        Map<String, String> props = new HashMap<>(scanRange.getProperties());
        props.put("connector_scan_range_type", scanRange.getRangeType().name());
        props.put("connector_file_format", scanRange.getFileFormat());

        Map<String, String> partValues = scanRange.getPartitionValues();
        if (partValues != null && !partValues.isEmpty()) {
            for (Map.Entry<String, String> entry : partValues.entrySet()) {
                props.put("partition." + entry.getKey(), entry.getValue());
            }
        }

        formatDesc.setJdbcParams(props);
    }

    /**
     * Sets Hive-specific scan params. For non-transactional Hive tables,
     * the TTableFormatFileDesc carries "hive" as the table format type.
     * The file path/offset/length are already set by the FileSplit pipeline.
     */
    private void setHiveParams(TTableFormatFileDesc formatDesc,
            ConnectorScanRange scanRange) {
        // For non-transactional hive, minimal params needed.
        // The file format, partition values, and text properties are
        // handled at the scan-node level via getFileFormatType(),
        // getPathPartitionKeys(), and getFileAttributes().
        // No per-split TTableFormatFileDesc fields needed beyond the type.
    }

    /**
     * Sets transactional Hive (ACID) scan params. Populates
     * TTransactionalHiveDesc with partition location and delete deltas.
     */
    private void setTransactionalHiveParams(TTableFormatFileDesc formatDesc,
            ConnectorScanRange scanRange) {
        Map<String, String> props = scanRange.getProperties();
        org.apache.doris.thrift.TTransactionalHiveDesc txnDesc =
                new org.apache.doris.thrift.TTransactionalHiveDesc();

        String partLoc = props.get("acid.partition_location");
        if (partLoc != null) {
            txnDesc.setPartition(partLoc);
        }

        String countStr = props.get("acid.delete_delta_count");
        if (countStr != null) {
            int count = Integer.parseInt(countStr);
            List<org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc> deltas =
                    new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String deltaStr = props.get("acid.delete_delta." + i);
                if (deltaStr != null) {
                    org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc delta =
                            new org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc();
                    delta.setDirectoryLocation(deltaStr.contains("|")
                            ? deltaStr.substring(0, deltaStr.indexOf('|'))
                            : deltaStr);
                    deltas.add(delta);
                }
            }
            txnDesc.setDeleteDeltas(deltas);
        }

        formatDesc.setTransactionalHiveParams(txnDesc);
    }

    /**
     * Sets Hudi-specific scan params via THudiFileDesc.
     * Handles dynamic format downgrade: MOR splits with no delta logs
     * fall back to native Parquet/ORC reader.
     */
    private void setHudiParams(TTableFormatFileDesc formatDesc,
            ConnectorScanRange scanRange, TFileRangeDesc rangeDesc) {
        Map<String, String> props = scanRange.getProperties();
        org.apache.doris.thrift.THudiFileDesc fileDesc = new org.apache.doris.thrift.THudiFileDesc();

        String fileFormat = scanRange.getFileFormat();
        boolean isJni = "jni".equalsIgnoreCase(fileFormat);

        // Dynamic format downgrade: if JNI but no delta logs, use native reader
        if (isJni) {
            String deltaLogs = props.get("hudi.delta_logs");
            if (deltaLogs == null || deltaLogs.isEmpty()) {
                String dataFilePath = props.getOrDefault("hudi.data_file_path", "");
                if (!dataFilePath.isEmpty()) {
                    String lower = dataFilePath.toLowerCase();
                    if (lower.endsWith(".parquet")) {
                        rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
                        isJni = false;
                    } else if (lower.endsWith(".orc")) {
                        rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
                        isJni = false;
                    }
                }
            }
        }

        if (isJni) {
            // JNI reader: full metadata needed for Hudi merge reader
            fileDesc.setInstantTime(props.getOrDefault("hudi.instant_time", ""));
            fileDesc.setSerde(props.getOrDefault("hudi.serde", ""));
            fileDesc.setInputFormat(props.getOrDefault("hudi.input_format", ""));
            fileDesc.setBasePath(props.getOrDefault("hudi.base_path", ""));
            fileDesc.setDataFilePath(props.getOrDefault("hudi.data_file_path", ""));
            fileDesc.setDataFileLength(
                    Long.parseLong(props.getOrDefault("hudi.data_file_length", "0")));

            String deltaLogs = props.get("hudi.delta_logs");
            if (deltaLogs != null && !deltaLogs.isEmpty()) {
                fileDesc.setDeltaLogs(Arrays.asList(deltaLogs.split(",")));
            }
            String colNames = props.get("hudi.column_names");
            if (colNames != null && !colNames.isEmpty()) {
                fileDesc.setColumnNames(Arrays.asList(colNames.split(",")));
            }
            String colTypes = props.get("hudi.column_types");
            if (colTypes != null && !colTypes.isEmpty()) {
                fileDesc.setColumnTypes(Arrays.asList(colTypes.split(",")));
            }
        }

        formatDesc.setHudiParams(fileDesc);

        // Set partition values for path-based partition extraction
        Map<String, String> partValues = scanRange.getPartitionValues();
        if (partValues != null && !partValues.isEmpty()) {
            List<String> pathKeys = new ArrayList<>();
            List<String> pathValues = new ArrayList<>();
            for (Map.Entry<String, String> entry : partValues.entrySet()) {
                pathKeys.add(entry.getKey());
                pathValues.add(entry.getValue());
            }
            rangeDesc.setColumnsFromPathKeys(pathKeys);
            rangeDesc.setColumnsFromPath(pathValues);
        }
    }

    /**
     * Sets Paimon-specific scan params via TPaimonFileDesc.
     * Handles both JNI reader (serialized split) and native reader (file path) paths.
     */
    private void setPaimonParams(TTableFormatFileDesc formatDesc,
            ConnectorScanRange scanRange, TFileRangeDesc rangeDesc) {
        Map<String, String> props = scanRange.getProperties();
        TPaimonFileDesc fileDesc = new TPaimonFileDesc();

        String paimonSplit = props.get("paimon.split");
        if (paimonSplit != null) {
            // JNI reader path
            rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
            fileDesc.setPaimonSplit(paimonSplit);
            String tableLocation = props.get("paimon.table_location");
            if (tableLocation != null) {
                fileDesc.setPaimonTable(tableLocation);
            }
            String weightStr = props.get("paimon.self_split_weight");
            if (weightStr != null) {
                rangeDesc.setSelfSplitWeight(Long.parseLong(weightStr));
            }
        } else {
            // Native reader path — format already set by file extension
            String fileFormat = scanRange.getFileFormat();
            if ("orc".equals(fileFormat)) {
                rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
            } else if ("parquet".equals(fileFormat)) {
                rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
            }
            String schemaIdStr = props.get("paimon.schema_id");
            if (schemaIdStr != null) {
                fileDesc.setSchemaId(Long.parseLong(schemaIdStr));
            }
        }

        fileDesc.setFileFormat(scanRange.getFileFormat());

        // Deletion file
        String deletionPath = props.get("paimon.deletion_file.path");
        if (deletionPath != null) {
            TPaimonDeletionFileDesc deletionFile = new TPaimonDeletionFileDesc();
            deletionFile.setPath(deletionPath);
            deletionFile.setOffset(Long.parseLong(
                    props.getOrDefault("paimon.deletion_file.offset", "0")));
            deletionFile.setLength(Long.parseLong(
                    props.getOrDefault("paimon.deletion_file.length", "0")));
            fileDesc.setDeletionFile(deletionFile);
        }

        // Row count for count pushdown
        String rowCountStr = props.get("paimon.row_count");
        if (rowCountStr != null) {
            formatDesc.setTableLevelRowCount(Long.parseLong(rowCountStr));
        } else {
            formatDesc.setTableLevelRowCount(-1);
        }

        formatDesc.setPaimonParams(fileDesc);

        // Partition values
        Map<String, String> partValues = scanRange.getPartitionValues();
        if (partValues != null && !partValues.isEmpty()) {
            List<String> pathKeys = new ArrayList<>();
            List<String> pathValues = new ArrayList<>();
            List<Boolean> pathIsNull = new ArrayList<>();
            for (Map.Entry<String, String> entry : partValues.entrySet()) {
                pathKeys.add(entry.getKey());
                pathValues.add(entry.getValue() != null ? entry.getValue() : "");
                pathIsNull.add(entry.getValue() == null);
            }
            rangeDesc.setColumnsFromPathKeys(pathKeys);
            rangeDesc.setColumnsFromPath(pathValues);
            rangeDesc.setColumnsFromPathIsNull(pathIsNull);
        }
    }

    @Override
    protected Optional<String> getSerializedTable() {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String serializedTable = props.get("paimon.serialized_table");
        if (serializedTable != null) {
            return Optional.of(serializedTable);
        }
        return Optional.empty();
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
        setPaimonScanLevelParams();
    }

    /**
     * Sets Paimon scan-level params (predicate + options) on the TFileScanRangeParams.
     * These apply to all splits, not per-split.
     */
    private void setPaimonScanLevelParams() {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String predicate = props.get("paimon.predicate");
        if (predicate != null) {
            params.setPaimonPredicate(predicate);
        }

        String optionsJson = props.get("paimon.options_json");
        if (optionsJson != null && !optionsJson.isEmpty()) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, String> options = new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(optionsJson,
                                new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
                params.setPaimonOptions(options);
            } catch (Exception e) {
                LOG.warn("Failed to parse paimon.options_json, using empty map", e);
            }
        }
    }

    /**
     * Lazily loads scan node properties from the connector's scan plan provider.
     */
    private Map<String, String> getOrLoadScanNodeProperties() {
        if (scanNodeProperties == null) {
            ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
            if (scanProvider != null) {
                List<ConnectorColumnHandle> columns = buildColumnHandles();
                Optional<ConnectorExpression> filter = buildRemainingFilter();
                scanNodeProperties = scanProvider.getScanNodeProperties(
                        connectorSession, currentHandle, columns, filter);
            }
            if (scanNodeProperties == null) {
                scanNodeProperties = Collections.emptyMap();
            }
        }
        return scanNodeProperties;
    }

    /**
     * Maps a file format name string to the corresponding TFileFormatType.
     */
    private static TFileFormatType mapFileFormatType(String format) {
        switch (format.toLowerCase()) {
            case "parquet":
                return TFileFormatType.FORMAT_PARQUET;
            case "orc":
                return TFileFormatType.FORMAT_ORC;
            case "text":
            case "csv":
                return TFileFormatType.FORMAT_CSV_PLAIN;
            case "json":
                return TFileFormatType.FORMAT_JSON;
            case "avro":
                return TFileFormatType.FORMAT_AVRO;
            default:
                return TFileFormatType.FORMAT_JNI;
        }
    }

    /**
     * Builds column handles from the tuple descriptor's slot descriptors.
     * These tell the connector which columns are needed for the query,
     * enabling optimized column selection (e.g., SELECT col1, col2 instead of SELECT *).
     */
    private List<ConnectorColumnHandle> buildColumnHandles() {
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Map<String, ConnectorColumnHandle> allHandles =
                metadata.getColumnHandles(connectorSession, currentHandle);
        if (allHandles.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorColumnHandle> selected = new ArrayList<>();
        for (org.apache.doris.analysis.SlotDescriptor slot : desc.getSlots()) {
            if (slot.getColumn() != null) {
                ConnectorColumnHandle ch = allHandles.get(slot.getColumn().getName());
                if (ch != null) {
                    selected.add(ch);
                }
            }
        }
        return selected;
    }

    /**
     * Builds a {@link ConnectorFilterConstraint} from the current conjuncts.
     */
    private ConnectorFilterConstraint buildFilterConstraint(List<Expr> exprs) {
        ConnectorExpression combined = ExprToConnectorExpressionConverter.convertConjuncts(exprs);
        return new ConnectorFilterConstraint(combined, Collections.emptyMap());
    }

    /**
     * Builds the remaining filter expression from unconsumed conjuncts.
     * If no conjuncts remain, returns {@link Optional#empty()}.
     * Filters out CAST-containing predicates when the connector does not support CAST pushdown.
     */
    private Optional<ConnectorExpression> buildRemainingFilter() {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return Optional.empty();
        }
        List<Expr> pushableConjuncts = conjuncts;
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        if (!metadata.supportsCastPredicatePushdown(connectorSession)) {
            pushableConjuncts = conjuncts.stream()
                    .filter(expr -> !containsCastExpr(expr))
                    .collect(Collectors.toList());
        }
        if (pushableConjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ExprToConnectorExpressionConverter.convertConjuncts(pushableConjuncts));
    }

    private static boolean containsCastExpr(Expr expr) {
        List<CastExpr> castExprs = new ArrayList<>();
        expr.collect(CastExpr.class, castExprs);
        return !castExprs.isEmpty();
    }
}

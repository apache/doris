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

package org.apache.doris.planner;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWriteConfig;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.api.write.ConnectorWriteType;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.PluginDrivenInsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveColumn;
import org.apache.doris.thrift.THiveColumnType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THiveTableSink;
import org.apache.doris.thrift.TJdbcTable;
import org.apache.doris.thrift.TJdbcTableSink;
import org.apache.doris.thrift.TOdbcTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Generic data sink for plugin-driven external tables.
 *
 * <p>Extends {@link BaseExternalTableDataSink} and constructs the appropriate
 * Thrift {@link TDataSink} based on {@link ConnectorWriteConfig} obtained from
 * the connector SPI. This allows different connector plugins to produce their
 * write configuration without knowing Thrift types, while the engine handles
 * the Thrift serialization.</p>
 *
 * <p>Supported write types and their Thrift mappings:</p>
 * <ul>
 *   <li>{@link ConnectorWriteType#FILE_WRITE} → {@link TDataSinkType#HIVE_TABLE_SINK}</li>
 *   <li>{@link ConnectorWriteType#JDBC_WRITE} → {@link TDataSinkType#JDBC_TABLE_SINK}</li>
 *   <li>Others → determined by per-connector migration</li>
 * </ul>
 */
public class PluginDrivenTableSink extends BaseExternalTableDataSink {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenTableSink.class);

    // Well-known property keys in ConnectorWriteConfig.properties
    public static final String PROP_DB_NAME = "db_name";
    public static final String PROP_TABLE_NAME = "table_name";
    public static final String PROP_OVERWRITE = "overwrite";
    public static final String PROP_WRITE_PATH = "write_path";
    public static final String PROP_TARGET_PATH = "target_path";
    public static final String PROP_ORIGINAL_WRITE_PATH = "original_write_path";

    // JDBC-specific property keys
    public static final String PROP_JDBC_URL = "jdbc_url";
    public static final String PROP_JDBC_USER = "jdbc_user";
    public static final String PROP_JDBC_PASSWORD = "jdbc_password";
    public static final String PROP_JDBC_DRIVER_URL = "jdbc_driver_url";
    public static final String PROP_JDBC_DRIVER_CLASS = "jdbc_driver_class";
    public static final String PROP_JDBC_DRIVER_CHECKSUM = "jdbc_driver_checksum";
    public static final String PROP_JDBC_TABLE_NAME = "jdbc_table_name";
    public static final String PROP_JDBC_RESOURCE_NAME = "jdbc_resource_name";
    public static final String PROP_JDBC_TABLE_TYPE = "jdbc_table_type";
    public static final String PROP_JDBC_INSERT_SQL = "jdbc_insert_sql";
    public static final String PROP_JDBC_USE_TRANSACTION = "jdbc_use_transaction";
    public static final String PROP_JDBC_CATALOG_ID = "jdbc_catalog_id";
    public static final String PROP_JDBC_POOL_MIN = "connection_pool_min_size";
    public static final String PROP_JDBC_POOL_MAX = "connection_pool_max_size";
    public static final String PROP_JDBC_POOL_MAX_WAIT = "connection_pool_max_wait_time";
    public static final String PROP_JDBC_POOL_MAX_LIFE = "connection_pool_max_life_time";
    public static final String PROP_JDBC_POOL_KEEP_ALIVE = "connection_pool_keep_alive";

    private final PluginDrivenExternalTable targetTable;
    // Config-bag mode: the connector returns a ConnectorWriteConfig (property bag) and the
    // engine builds the Thrift sink (jdbc / hive-shaped file writes). Null in plan-provider mode.
    private final ConnectorWriteConfig writeConfig;
    // Plan-provider mode (W5): the connector builds its own opaque TDataSink via planWrite().
    // Mutually exclusive with writeConfig -- exactly one is non-null. Used by connectors whose
    // sink cannot be expressed as a generic ConnectorWriteConfig (e.g. maxcompute / iceberg).
    private final ConnectorWritePlanProvider writePlanProvider;
    private final ConnectorSession connectorSession;
    private final ConnectorTableHandle tableHandle;
    private final List<ConnectorColumn> connectorColumns;

    public PluginDrivenTableSink(PluginDrivenExternalTable targetTable,
            ConnectorWriteConfig writeConfig) {
        super();
        this.targetTable = targetTable;
        this.writeConfig = writeConfig;
        this.writePlanProvider = null;
        this.connectorSession = null;
        this.tableHandle = null;
        this.connectorColumns = null;
    }

    /**
     * Plan-provider mode (W5): the connector supplies a {@link ConnectorWritePlanProvider}
     * and builds its own opaque {@link TDataSink} via
     * {@link ConnectorWritePlanProvider#planWrite}. The config-bag constructor remains for
     * connectors that only provide a {@link ConnectorWriteConfig} (e.g. jdbc).
     */
    public PluginDrivenTableSink(PluginDrivenExternalTable targetTable,
            ConnectorWritePlanProvider writePlanProvider, ConnectorSession connectorSession,
            ConnectorTableHandle tableHandle, List<ConnectorColumn> connectorColumns) {
        super();
        this.targetTable = targetTable;
        this.writeConfig = null;
        this.writePlanProvider = writePlanProvider;
        this.connectorSession = connectorSession;
        this.tableHandle = tableHandle;
        this.connectorColumns = connectorColumns;
    }

    /**
     * The connector session this sink's write plan reads (plan-provider mode). The insert
     * executor binds the connector transaction onto it (via
     * {@link ConnectorSession#setCurrentTransaction}) before {@code bindDataSink} runs, so
     * the connector's {@code planWrite} sees the active transaction.
     */
    public ConnectorSession getConnectorSession() {
        return connectorSession;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        // Connector determines format through write config; accept all
        return EnumSet.allOf(TFileFormatType.class);
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("PLUGIN-DRIVEN TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return sb.toString();
        }
        if (writeConfig == null) {
            // Plan-provider mode (W5, e.g. maxcompute): the connector builds its own sink via
            // planWrite; there is no ConnectorWriteConfig to describe here.
            sb.append(prefix).append("  WRITE: plan-provider\n");
            sb.append(prefix).append("  TABLE: ").append(targetTable.getName()).append("\n");
            return sb.toString();
        }
        sb.append(prefix).append("  WRITE TYPE: ").append(writeConfig.getWriteType()).append("\n");
        sb.append(prefix).append("  TABLE: ").append(targetTable.getName()).append("\n");
        if (writeConfig.getWriteType() == ConnectorWriteType.JDBC_WRITE) {
            Map<String, String> props = writeConfig.getProperties();
            sb.append(prefix).append("  TABLE TYPE: ")
                    .append(props.getOrDefault(PROP_JDBC_TABLE_TYPE, "")).append("\n");
            sb.append(prefix).append("  INSERT SQL: ")
                    .append(props.getOrDefault(PROP_JDBC_INSERT_SQL, "")).append("\n");
            sb.append(prefix).append("  USE TRANSACTION: ")
                    .append(props.getOrDefault(PROP_JDBC_USE_TRANSACTION, "false")).append("\n");
        } else {
            if (writeConfig.getFileFormat() != null) {
                sb.append(prefix).append("  FORMAT: ").append(writeConfig.getFileFormat()).append("\n");
            }
            if (writeConfig.getWriteLocation() != null) {
                sb.append(prefix).append("  LOCATION: ").append(writeConfig.getWriteLocation()).append("\n");
            }
        }
        return sb.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {
        if (writePlanProvider != null) {
            bindViaWritePlanProvider(insertCtx);
            return;
        }
        ConnectorWriteType writeType = writeConfig.getWriteType();
        switch (writeType) {
            case FILE_WRITE:
                bindFileWriteSink(insertCtx);
                break;
            case JDBC_WRITE:
                bindJdbcWriteSink(insertCtx);
                break;
            default:
                throw new AnalysisException(
                        "Unsupported write type for plugin-driven sink: " + writeType);
        }
    }

    /**
     * Plan-provider mode: delegate sink construction to the connector, which returns its own
     * opaque {@link TDataSink}; the engine dispatches it to BE unchanged. The
     * {@link ConnectorWriteHandle} carries the bound target table handle and write columns.
     *
     * <p>Connector-specific write context (OVERWRITE flag, static partition spec) is read from
     * the {@link PluginDrivenInsertCommandContext} and passed through to the connector. The
     * W-phase established this seam with an empty context; the per-connector adopter (P4+) fills
     * it here.</p>
     */
    private void bindViaWritePlanProvider(Optional<InsertCommandContext> insertCtx) {
        boolean overwrite = false;
        Map<String, String> writeContext = Collections.emptyMap();
        if (insertCtx.isPresent() && insertCtx.get() instanceof PluginDrivenInsertCommandContext) {
            PluginDrivenInsertCommandContext ctx = (PluginDrivenInsertCommandContext) insertCtx.get();
            overwrite = ctx.isOverwrite();
            writeContext = ctx.getStaticPartitionSpec();
        }
        ConnectorWriteHandle handle = new PluginDrivenWriteHandle(
                tableHandle, connectorColumns, overwrite, writeContext);
        ConnectorSinkPlan sinkPlan = writePlanProvider.planWrite(connectorSession, handle);
        this.tDataSink = sinkPlan.getDataSink();
    }

    /**
     * Returns the write config associated with this sink.
     * Used by the insert executor to access connector write configuration.
     */
    public ConnectorWriteConfig getWriteConfig() {
        return writeConfig;
    }

    /**
     * Returns the target table.
     */
    public PluginDrivenExternalTable getTargetTable() {
        return targetTable;
    }

    /**
     * Builds a THiveTableSink for file-based writes.
     *
     * <p>BE's Hive table sink is the generic file writer that handles
     * Parquet/ORC/Text output. Connectors provide all necessary
     * configuration through {@link ConnectorWriteConfig}.</p>
     */
    private void bindFileWriteSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {
        Map<String, String> props = writeConfig.getProperties();
        THiveTableSink tSink = new THiveTableSink();

        // DB and table names
        tSink.setDbName(props.getOrDefault(PROP_DB_NAME, targetTable.getDbName()));
        tSink.setTableName(props.getOrDefault(PROP_TABLE_NAME, targetTable.getName()));

        // Columns: build from target table schema + partition info from write config
        Set<String> partNames = new HashSet<>(writeConfig.getPartitionColumns());
        List<Column> allColumns = targetTable.getColumns();
        List<THiveColumn> targetColumns = new ArrayList<>();
        for (Column col : allColumns) {
            THiveColumn tHiveColumn = new THiveColumn();
            tHiveColumn.setName(col.getName());
            tHiveColumn.setColumnType(
                    partNames.contains(col.getName())
                            ? THiveColumnType.PARTITION_KEY
                            : THiveColumnType.REGULAR);
            targetColumns.add(tHiveColumn);
        }
        tSink.setColumns(targetColumns);

        // File format
        if (writeConfig.getFileFormat() != null) {
            TFileFormatType formatType = getTFileFormatType(writeConfig.getFileFormat());
            tSink.setFileFormat(formatType);
        }

        // Compression
        if (writeConfig.getCompression() != null) {
            tSink.setCompressionType(getTFileCompressType(writeConfig.getCompression()));
        }

        // Location
        String writePath = props.getOrDefault(PROP_WRITE_PATH, writeConfig.getWriteLocation());
        String targetPath = props.getOrDefault(PROP_TARGET_PATH, writeConfig.getWriteLocation());
        if (writePath != null) {
            THiveLocationParams locationParams = new THiveLocationParams();
            locationParams.setWritePath(writePath);
            locationParams.setOriginalWritePath(
                    props.getOrDefault(PROP_ORIGINAL_WRITE_PATH, writePath));
            locationParams.setTargetPath(targetPath);
            LocationPath locationPath = LocationPath.of(targetPath,
                    targetTable.getCatalog().getCatalogProperty().getStoragePropertiesMap());
            TFileType fileType = locationPath.getTFileTypeForBE();
            locationParams.setFileType(fileType);
            tSink.setLocation(locationParams);

            if (fileType.equals(TFileType.FILE_BROKER)) {
                tSink.setBrokerAddresses(
                        getBrokerAddresses(targetTable.getCatalog().bindBrokerName()));
            }
        }

        // Overwrite flag
        if (props.containsKey(PROP_OVERWRITE)) {
            tSink.setOverwrite(Boolean.parseBoolean(props.get(PROP_OVERWRITE)));
        }

        // Hadoop/storage config for BE access
        Map<String, String> beStorageProps = targetTable.getCatalog()
                .getCatalogProperty().getBackendStorageProperties();
        tSink.setHadoopConfig(beStorageProps);

        // Any extra connector-specific properties: pass through via hadoop_config
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            if (!isWellKnownProperty(key)) {
                tSink.putToHadoopConfig(key, entry.getValue());
            }
        }

        tDataSink = new TDataSink(TDataSinkType.HIVE_TABLE_SINK);
        tDataSink.setHiveTableSink(tSink);
    }

    /**
     * Builds a TJdbcTableSink for JDBC-based writes.
     */
    private void bindJdbcWriteSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {
        Map<String, String> props = writeConfig.getProperties();

        TJdbcTableSink jdbcSink = new TJdbcTableSink();

        TJdbcTable tJdbcTable = new TJdbcTable();
        tJdbcTable.setJdbcUrl(props.getOrDefault(PROP_JDBC_URL, ""));
        tJdbcTable.setJdbcUser(props.getOrDefault(PROP_JDBC_USER, ""));
        tJdbcTable.setJdbcPassword(props.getOrDefault(PROP_JDBC_PASSWORD, ""));
        tJdbcTable.setJdbcDriverUrl(props.getOrDefault(PROP_JDBC_DRIVER_URL, ""));
        tJdbcTable.setJdbcDriverClass(props.getOrDefault(PROP_JDBC_DRIVER_CLASS, ""));
        tJdbcTable.setJdbcDriverChecksum(props.getOrDefault(PROP_JDBC_DRIVER_CHECKSUM, ""));
        tJdbcTable.setJdbcTableName(props.getOrDefault(PROP_JDBC_TABLE_NAME, ""));
        tJdbcTable.setJdbcResourceName(props.getOrDefault(PROP_JDBC_RESOURCE_NAME, ""));
        tJdbcTable.setCatalogId(Long.parseLong(props.getOrDefault(PROP_JDBC_CATALOG_ID, "0")));
        tJdbcTable.setConnectionPoolMinSize(
                Integer.parseInt(props.getOrDefault(PROP_JDBC_POOL_MIN, "1")));
        tJdbcTable.setConnectionPoolMaxSize(
                Integer.parseInt(props.getOrDefault(PROP_JDBC_POOL_MAX, "10")));
        tJdbcTable.setConnectionPoolMaxWaitTime(
                Integer.parseInt(props.getOrDefault(PROP_JDBC_POOL_MAX_WAIT, "5000")));
        tJdbcTable.setConnectionPoolMaxLifeTime(
                Integer.parseInt(props.getOrDefault(PROP_JDBC_POOL_MAX_LIFE, "1800000")));
        tJdbcTable.setConnectionPoolKeepAlive(
                Boolean.parseBoolean(props.getOrDefault(PROP_JDBC_POOL_KEEP_ALIVE, "false")));
        jdbcSink.setJdbcTable(tJdbcTable);

        String insertSql = props.getOrDefault(PROP_JDBC_INSERT_SQL, "");
        jdbcSink.setInsertSql(insertSql);

        boolean useTxn = Boolean.parseBoolean(
                props.getOrDefault(PROP_JDBC_USE_TRANSACTION, "false"));
        jdbcSink.setUseTransaction(useTxn);

        String tableType = props.getOrDefault(PROP_JDBC_TABLE_TYPE, "");
        if (!tableType.isEmpty()) {
            jdbcSink.setTableType(TOdbcTableType.valueOf(tableType));
        }

        tDataSink = new TDataSink(TDataSinkType.JDBC_TABLE_SINK);
        tDataSink.setJdbcTableSink(jdbcSink);
    }

    private boolean isWellKnownProperty(String key) {
        return key.equals(PROP_DB_NAME) || key.equals(PROP_TABLE_NAME)
                || key.equals(PROP_OVERWRITE)
                || key.equals(PROP_WRITE_PATH) || key.equals(PROP_TARGET_PATH)
                || key.equals(PROP_ORIGINAL_WRITE_PATH)
                || key.startsWith("jdbc_");
    }

    /** Bound {@link ConnectorWriteHandle} passed to {@link ConnectorWritePlanProvider#planWrite}. */
    private static final class PluginDrivenWriteHandle implements ConnectorWriteHandle {
        private final ConnectorTableHandle tableHandle;
        private final List<ConnectorColumn> columns;
        private final boolean overwrite;
        private final Map<String, String> writeContext;

        private PluginDrivenWriteHandle(ConnectorTableHandle tableHandle, List<ConnectorColumn> columns,
                boolean overwrite, Map<String, String> writeContext) {
            this.tableHandle = tableHandle;
            this.columns = columns;
            this.overwrite = overwrite;
            this.writeContext = writeContext;
        }

        @Override
        public ConnectorTableHandle getTableHandle() {
            return tableHandle;
        }

        @Override
        public List<ConnectorColumn> getColumns() {
            return columns;
        }

        @Override
        public boolean isOverwrite() {
            return overwrite;
        }

        @Override
        public Map<String, String> getWriteContext() {
            return writeContext;
        }
    }
}

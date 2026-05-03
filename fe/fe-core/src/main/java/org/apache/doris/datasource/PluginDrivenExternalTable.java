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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Generic {@link ExternalTable} for plugin-driven catalogs.
 *
 * <p>Provides table implementation that fetches schema from the connector SPI.
 * Connector-specific behavior is accessed through the parent catalog's
 * {@link org.apache.doris.connector.api.Connector} using opaque handles.</p>
 */
public class PluginDrivenExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenExternalTable.class);

    /** No-arg constructor for GSON deserialization. */
    public PluginDrivenExternalTable() {
    }

    public PluginDrivenExternalTable(long id, String name, String remoteName,
            ExternalCatalog catalog, ExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.PLUGIN_EXTERNAL_TABLE);
    }

    /**
     * Returns whether the underlying connector supports multiple concurrent writers.
     * Used by the planner to decide GATHER (single writer) vs parallel distribution.
     */
    public boolean supportsParallelWrite() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_PARALLEL_WRITE);
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);

        String dbName = db != null ? db.getRemoteName() : "";
        String tableName = getRemoteName();
        Optional<ConnectorTableHandle> handleOpt = metadata.getTableHandle(session, dbName, tableName);
        if (!handleOpt.isPresent()) {
            LOG.warn("Table handle not found for plugin-driven table: {}.{}", dbName, tableName);
            return Optional.empty();
        }

        ConnectorTableSchema tableSchema = metadata.getTableSchema(session, handleOpt.get());

        // Apply identifier mapping to column names (lowercase / explicit mapping)
        List<ConnectorColumn> mappedColumns = new ArrayList<>(tableSchema.getColumns().size());
        for (ConnectorColumn col : tableSchema.getColumns()) {
            String mappedName = metadata.fromRemoteColumnName(session, dbName, tableName, col.getName());
            if (!mappedName.equals(col.getName())) {
                mappedColumns.add(new ConnectorColumn(mappedName, col.getType(),
                        col.getComment(), col.isNullable(), col.getDefaultValue(), col.isKey()));
            } else {
                mappedColumns.add(col);
            }
        }

        List<Column> columns = ConnectorColumnConverter.convertColumns(mappedColumns);
        return Optional.of(new SchemaCacheValue(columns));
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    @Override
    public long getCachedRowCount() {
        // Do NOT call makeSureInitialized() here.
        // ExternalTable.getCachedRowCount() intentionally returns -1 for uninitialized tables
        // so that SHOW TABLE STATUS / information_schema.tables stays non-blocking.
        if (!isObjectCreated()) {
            return -1;
        }
        return Env.getCurrentEnv().getExtMetaCacheMgr().getRowCountCache()
                .getCachedRowCount(catalog.getId(), dbId, id);
    }

    @Override
    public String getComment() {
        return getComment(false);
    }

    @Override
    public String getComment(boolean escapeQuota) {
        String remoteDbName = db != null ? db.getRemoteName() : "";
        try {
            PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
            Connector connector = pluginCatalog.getConnector();
            ConnectorSession session = pluginCatalog.buildConnectorSession();
            ConnectorMetadata metadata = connector.getMetadata(session);
            String tableName = getRemoteName();
            String comment = metadata.getTableComment(session, remoteDbName, tableName);
            if (escapeQuota && comment != null) {
                return comment.replace("'", "\\'");
            }
            return comment != null ? comment : "";
        } catch (Exception e) {
            LOG.debug("Failed to get table comment for {}.{}", remoteDbName, name, e);
            return "";
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        // After deserializing a migrated old table (e.g., EsExternalTable → PluginDrivenExternalTable),
        // fix the table type so that BindRelation routes to LogicalFileScan (new path).
        if (type != TableType.PLUGIN_EXTERNAL_TABLE) {
            LOG.info("Migrating table '{}' type from {} to PLUGIN_EXTERNAL_TABLE", name, type);
            type = TableType.PLUGIN_EXTERNAL_TABLE;
        }
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);

        String dbName = db != null ? db.getRemoteName() : "";
        String tableName = getRemoteName();
        Optional<ConnectorTableHandle> handleOpt = metadata.getTableHandle(session, dbName, tableName);
        if (!handleOpt.isPresent()) {
            return UNKNOWN_ROW_COUNT;
        }

        Optional<ConnectorTableStatistics> statsOpt = metadata.getTableStatistics(session, handleOpt.get());
        if (statsOpt.isPresent() && statsOpt.get().getRowCount() >= 0) {
            return statsOpt.get().getRowCount();
        }
        return UNKNOWN_ROW_COUNT;
    }

    @Override
    public String getEngine() {
        // Return the legacy engine name based on the actual catalog type,
        // not the generic "Plugin" from PLUGIN_EXTERNAL_TABLE.toEngineName().
        // This preserves user-visible compatibility for migrated JDBC/ES tables
        // across SHOW TABLE STATUS, information_schema.tables, REST API, etc.
        String catalogType = catalog instanceof PluginDrivenExternalCatalog
                ? ((PluginDrivenExternalCatalog) catalog).getType() : "";
        switch (catalogType) {
            case "jdbc":
                return TableType.JDBC_EXTERNAL_TABLE.toEngineName();
            case "es":
                return TableType.ES_EXTERNAL_TABLE.toEngineName();
            default:
                return super.getEngine();
        }
    }

    @Override
    public String getEngineTableTypeName() {
        String catalogType = catalog instanceof PluginDrivenExternalCatalog
                ? ((PluginDrivenExternalCatalog) catalog).getType() : "";
        switch (catalogType) {
            case "jdbc":
                return TableType.JDBC_EXTERNAL_TABLE.name();
            case "es":
                return TableType.ES_EXTERNAL_TABLE.name();
            default:
                return TableType.PLUGIN_EXTERNAL_TABLE.name();
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);

        String dbName = db != null ? db.getRemoteName() : "";
        List<Column> schema = getFullSchema();
        TTableDescriptor desc = metadata.buildTableDescriptor(session,
                getId(), getName(), dbName, getRemoteName(),
                schema.size(), pluginCatalog.getId());
        if (desc != null) {
            return desc;
        }
        LOG.warn("Connector returned null table descriptor for plugin table {}.{}, "
                + "using generic fallback", dbName, getName());
        return new TTableDescriptor(getId(), TTableType.SCHEMA_TABLE,
                schema.size(), 0, getName(), dbName);
    }
}

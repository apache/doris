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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.DefaultConnectorValidationContext;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.ddl.CreateTableInfoToConnectorRequestConverter;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.PluginDrivenTransactionManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * An {@link ExternalCatalog} backed by a Connector SPI plugin.
 *
 * <p>This adapter bridges the connector SPI ({@link Connector}) with the existing
 * ExternalCatalog hierarchy. Metadata operations are delegated to the connector's
 * {@link org.apache.doris.connector.api.ConnectorMetadata} implementation.</p>
 *
 * <p>When created via {@link CatalogFactory}, the Connector instance is provided
 * directly. After GSON deserialization (FE restart), the Connector is recreated
 * from catalog properties during {@link #initLocalObjectsImpl()}.</p>
 */
public class PluginDrivenExternalCatalog extends ExternalCatalog {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenExternalCatalog.class);

    // Volatile for cross-thread visibility; all mutations happen under synchronized(this)
    // via makeSureInitialized() → initLocalObjectsImpl(), or resetToUninitialized() → onClose().
    private transient volatile Connector connector;

    /** No-arg constructor for GSON deserialization. */
    public PluginDrivenExternalCatalog() {
    }

    /**
     * Creates a plugin-driven catalog with an already-created Connector.
     *
     * @param catalogId unique catalog id
     * @param name catalog name
     * @param resource optional resource name
     * @param props catalog properties
     * @param comment catalog comment
     * @param connector the SPI connector instance
     */
    public PluginDrivenExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment, Connector connector) {
        super(catalogId, name, InitCatalogLog.Type.PLUGIN, comment);
        this.catalogProperty = new CatalogProperty(resource, props);
        this.connector = connector;
    }

    @Override
    protected void initLocalObjectsImpl() {
        // Always (re-)create the connector so it gets the proper engine context,
        // including the catalog's execution authenticator for Kerberos/secured HMS.
        // The connector created by CatalogFactory used a lightweight context
        // without auth (the catalog didn't exist yet); we replace it now.
        Connector oldConnector = connector;
        Connector newConnector = createConnectorFromProperties();
        if (newConnector != null) {
            connector = newConnector;
            // Close the old connector (e.g., the one injected by CatalogFactory during
            // checkWhenCreating) to release its connection pool and classloader reference.
            if (oldConnector != null && oldConnector != newConnector) {
                try {
                    oldConnector.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close old connector during re-initialization "
                            + "for catalog {}", name, e);
                }
            }
        }
        if (connector == null) {
            throw new RuntimeException("No ConnectorProvider found for plugin-driven catalog: "
                    + name + ", type: " + getType()
                    + ". Ensure the connector plugin is installed.");
        }
        transactionManager = new PluginDrivenTransactionManager();
        initPreExecutionAuthenticator();
    }

    @Override
    protected synchronized void initPreExecutionAuthenticator() {
        if (executionAuthenticator != null) {
            return;
        }
        try {
            MetastoreProperties msp = catalogProperty.getMetastoreProperties();
            if (msp != null) {
                executionAuthenticator = msp.getExecutionAuthenticator();
                return;
            }
        } catch (Exception ignored) {
            // Not all catalog types have metastore properties (e.g., JDBC, ES)
        }
        super.initPreExecutionAuthenticator();
    }

    /**
     * Creates a new Connector from catalog properties. Extracted as a protected method
     * so tests can override without depending on the static ConnectorFactory registry.
     */
    protected Connector createConnectorFromProperties() {
        // Use getType() which falls back to logType when "type" is not in properties.
        // This handles image deserialization of old resource-backed catalogs whose
        // properties never contained "type" (it was derived from the Resource object).
        String catalogType = getType();
        return ConnectorFactory.createConnector(catalogType,
                catalogProperty.getProperties(),
                new DefaultConnectorContext(name, id, this::getExecutionAuthenticator));
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        String catalogType = getType();
        try {
            ConnectorFactory.validateProperties(catalogType, catalogProperty.getProperties());
        } catch (IllegalArgumentException e) {
            throw new DdlException(e.getMessage());
        }
        // Validate function_rules JSON if present (shared across all connector types).
        String functionRules = catalogProperty.getOrDefault("function_rules", null);
        ExternalFunctionRules.check(functionRules);
    }

    @Override
    public void checkWhenCreating() throws DdlException {
        // Let the connector perform its type-specific pre-creation validation
        // (e.g., JDBC driver security, checksum computation).
        DefaultConnectorValidationContext validationCtx =
                new DefaultConnectorValidationContext(getId(), catalogProperty);
        try {
            connector.preCreateValidation(validationCtx);
        } catch (DdlException e) {
            throw e;
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        }

        boolean testConnection = Boolean.parseBoolean(
                catalogProperty.getOrDefault(ExternalCatalog.TEST_CONNECTION,
                        String.valueOf(connector.defaultTestConnection())));
        if (!testConnection) {
            return;
        }
        // Delegate FE→external connectivity testing to the connector SPI.
        ConnectorSession session = buildConnectorSession();
        ConnectorTestResult result = connector.testConnection(session);
        if (!result.isSuccess()) {
            throw new DdlException("Connectivity test failed for catalog '"
                    + name + "': " + result.getMessage());
        }
        LOG.info("Connectivity test passed for plugin-driven catalog '{}': {}", name, result);

        // Execute any BE→external connectivity test the connector registered.
        validationCtx.executePendingBeTests();
    }

    /**
     * Handles catalog property updates. Delegates to the parent which resets
     * caches, sets objectCreated=false, and calls onClose() to release the
     * current connector. The next makeSureInitialized() call will trigger
     * initLocalObjectsImpl() which creates a new connector with the updated
     * properties and proper engine context (auth, etc.).
     *
     * <p>This follows the same lifecycle pattern as all other ExternalCatalog
     * subclasses: reset → lazy re-initialization on next access.</p>
     */
    @Override
    public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        super.notifyPropertiesUpdated(updatedProps);
    }

    @Override
    protected List<String> listDatabaseNames() {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session).listDatabaseNames(session);
    }

    @Override
    protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session).listTableNames(session, dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session)
                .getTableHandle(session, dbName, tblName).isPresent();
    }

    @Override
    public String getType() {
        // Return the actual catalog type (e.g., "es", "jdbc") from properties,
        // not the internal "plugin" logType.
        return catalogProperty.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, super.getType());
    }

    /** Returns the underlying SPI connector. Ensures the catalog is initialized first. */
    public Connector getConnector() {
        makeSureInitialized();
        return connector;
    }

    /**
     * Routes {@code CREATE TABLE} through the SPI's
     * {@code ConnectorTableOps.createTable(session, request)} instead of the
     * legacy {@code metadataOps} path used by other {@link ExternalCatalog}
     * subclasses.
     *
     * <p>Connectors that have not overridden the new SPI default fall through
     * to the SPI's "CREATE TABLE not supported" exception, which is wrapped
     * here as a {@link DdlException} to match the existing caller contract.</p>
     *
     * <p>The SPI signature is {@code void}: it does not distinguish
     * "newly created" from "already existed (IF NOT EXISTS)". This override
     * conservatively assumes creation happened and writes the edit log, matching
     * the more common branch of the legacy path. Refining this when a connector
     * actually needs the distinction is left to P5/P6/P7 connector migrations.</p>
     */
    @Override
    public boolean createTable(CreateTableInfo createTableInfo) throws UserException {
        makeSureInitialized();
        ConnectorSession session = buildConnectorSession();
        ConnectorCreateTableRequest request = CreateTableInfoToConnectorRequestConverter
                .convert(createTableInfo, createTableInfo.getDbName());
        try {
            connector.getMetadata(session).createTable(session, request);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        org.apache.doris.persist.CreateTableInfo persistInfo =
                new org.apache.doris.persist.CreateTableInfo(
                        getName(),
                        createTableInfo.getDbName(),
                        createTableInfo.getTableName());
        Env.getCurrentEnv().getEditLog().logCreateTable(persistInfo);
        LOG.info("finished to create table {}.{}.{}", getName(),
                createTableInfo.getDbName(), createTableInfo.getTableName());
        return false;
    }

    @Override
    public String fromRemoteDatabaseName(String remoteDatabaseName) {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session).fromRemoteDatabaseName(session, remoteDatabaseName);
    }

    @Override
    public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session).fromRemoteTableName(session, remoteDatabaseName, remoteTableName);
    }

    /**
     * Builds a {@link ConnectorSession} from the current thread's {@link ConnectContext}.
     */
    public ConnectorSession buildConnectorSession() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            return ConnectorSessionBuilder.from(ctx)
                    .withCatalogId(getId())
                    .withCatalogName(getName())
                    .withCatalogProperties(catalogProperty.getProperties())
                    .build();
        }
        return ConnectorSessionBuilder.create()
                .withCatalogId(getId())
                .withCatalogName(getName())
                .withCatalogProperties(catalogProperty.getProperties())
                .build();
    }

    @Override
    protected ExternalDatabase<? extends ExternalTable> buildDbForInit(String remoteDbName, String localDbName,
            long dbId, InitCatalogLog.Type logType, boolean checkExists) {
        // Always use PLUGIN logType regardless of what was serialized (e.g., ES from migration).
        return super.buildDbForInit(remoteDbName, localDbName, dbId, InitCatalogLog.Type.PLUGIN, checkExists);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        // For old resource-backed catalogs (e.g., ES, JDBC), the "type" property was never
        // persisted — it was derived from the Resource object at runtime. After image
        // deserialization with registerCompatibleSubtype, those catalogs land here as
        // PluginDrivenExternalCatalog with logType still set to the original value (ES/JDBC).
        // Backfill "type" from logType before we overwrite it below, so that
        // createConnectorFromProperties() and getType() can resolve the catalog type.
        if (logType != null && logType != InitCatalogLog.Type.PLUGIN
                && logType != InitCatalogLog.Type.UNKNOWN) {
            String oldType = legacyLogTypeToCatalogType(logType);
            if (catalogProperty.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, "").isEmpty()) {
                LOG.info("Backfilling missing 'type' property for catalog '{}' from logType: {}",
                        name, oldType);
                catalogProperty.addProperty(CatalogMgr.CATALOG_TYPE_PROP, oldType);
            }
        }
        // After deserializing a migrated old catalog (e.g., ES → PluginDriven), fix logType
        // so that buildDbForInit uses PLUGIN path.
        if (logType != InitCatalogLog.Type.PLUGIN) {
            LOG.info("Migrating catalog '{}' logType from {} to PLUGIN", name, logType);
            logType = InitCatalogLog.Type.PLUGIN;
        }
    }

    // CatalogFactory type strings don't all match Type.name().toLowerCase():
    // TRINO_CONNECTOR → "trino-connector" (hyphen), not "trino_connector".
    // Add cases here whenever a connector's CatalogFactory key diverges from
    // the lowercase enum name.
    private static String legacyLogTypeToCatalogType(InitCatalogLog.Type logType) {
        switch (logType) {
            case TRINO_CONNECTOR:
                return "trino-connector";
            default:
                return logType.name().toLowerCase(Locale.ROOT);
        }
    }

    @Override
    public void onClose() {
        super.onClose();
        if (connector != null) {
            try {
                connector.close();
            } catch (IOException e) {
                LOG.warn("Failed to close connector for catalog {}", name, e);
            }
            connector = null;
        }
    }
}

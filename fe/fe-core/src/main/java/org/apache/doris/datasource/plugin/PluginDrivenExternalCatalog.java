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

package org.apache.doris.datasource.plugin;

import org.apache.doris.analysis.ColumnPath;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.catalog.info.CreateOrReplaceBranchInfo;
import org.apache.doris.catalog.info.CreateOrReplaceTagInfo;
import org.apache.doris.catalog.info.DropBranchInfo;
import org.apache.doris.catalog.info.DropTagInfo;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.DefaultConnectorValidationContext;
import org.apache.doris.connector.ddl.CreateTableInfoToConnectorRequestConverter;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPath;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.ddl.PartitionFieldChange;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalFunctionRules;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.connector.converter.ConnectorBranchTagConverter;
import org.apache.doris.datasource.connector.converter.ConnectorColumnConverter;
import org.apache.doris.datasource.connector.converter.ConnectorPartitionFieldConverter;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.log.InitCatalogLog;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionFieldOp;
import org.apache.doris.persist.CreateDbInfo;
import org.apache.doris.persist.DropDbInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.PluginDrivenTransactionManager;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * An {@link ExternalCatalog} backed by a Connector SPI plugin.
 *
 * <p>This adapter bridges the connector SPI ({@link Connector}) with the existing
 * ExternalCatalog hierarchy. Metadata operations are delegated to the connector's
 * {@link org.apache.doris.connector.spi.ConnectorMetadata} implementation.</p>
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

    // The engine-owned context shared by the connector (and any sibling it builds via createSiblingConnector).
    // Held so the catalog can close the context's cached engine FileSystem (DefaultConnectorContext.getFileSystem)
    // on teardown -- connectors only borrow that FS and must not close it. Null until the real connector is built
    // (the lightweight CatalogFactory context is not tracked here; its FS is never built).
    private transient volatile DefaultConnectorContext connectorContext;

    // The displayed engine name, resolved from the provider on first use (see getDisplayEngineName).
    private transient volatile String displayEngineName;

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
        // Capture the old context before createConnectorFromProperties() overwrites connectorContext, so we can
        // close its cached FileSystem when the connector is actually replaced.
        DefaultConnectorContext oldContext = connectorContext;
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
                // ...and close the replaced context's cached engine FileSystem (never the live one).
                if (oldContext != null && oldContext != connectorContext) {
                    closeConnectorContextQuietly(oldContext);
                }
            }
        }
        if (connector == null) {
            throw new RuntimeException("No ConnectorProvider found for plugin-driven catalog: "
                    + name + ", type: " + getType()
                    + ". Ensure the connector plugin is installed.");
        }
        // Design S8: the connector owns storage-property derivation (e.g. the iceberg hadoop
        // warehouse -> fs.defaultFS bridge); fe-core folds the connector-derived defaults into its storage map
        // instead of parsing metastore properties. Read the connector field lazily so an ALTER-rebuilt (or
        // dropped) connector is honored at storage-access time.
        catalogProperty.setPluginDerivedStorageDefaultsSupplier(() -> {
            Connector activeConnector = connector;
            return activeConnector != null
                    ? activeConnector.deriveStorageProperties(catalogProperty.getProperties())
                    : java.util.Collections.emptyMap();
        });
        transactionManager = new PluginDrivenTransactionManager();
        // Design S6: a plugin catalog's pre-execution Kerberos auth is owned entirely by the connector
        // (TcclPinningConnectorContext runs each remote op under the connector's own plugin-side authenticator —
        // storage Kerberos and, via {Iceberg,Paimon}Connector.buildPluginAuthenticator, HMS-metastore Kerberos).
        // fe-core keeps only the base no-op ExecutionAuthenticator handle (non-null so
        // BaseExternalTableInsertExecutor / ExternalCatalog.getExecutionAuthenticator can call it
        // unconditionally, but it performs no doAs — the connector's inner doAs is authoritative). Hence no
        // plugin-specific initPreExecutionAuthenticator override: inherit the base no-op.
        initPreExecutionAuthenticator();
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
        // Build the context up front and stash it so the catalog can close its cached engine FileSystem on
        // teardown (onClose / connector replacement). The connector — and any sibling it builds — shares this
        // one context instance, so there is a single cached FS per catalog.
        DefaultConnectorContext context = new DefaultConnectorContext(name, id, this::getExecutionAuthenticator,
                () -> catalogProperty.getStorageAdaptersMap(),
                catalogProperty::getEffectiveRawStorageProperties);
        this.connectorContext = context;
        // The standalone entry point, same as CatalogFactory uses: this is the second door onto a catalog (the
        // lazy build after image deserialization), and both doors must agree on what may become a catalog.
        return ConnectorFactory.createStandaloneCatalogConnector(
                catalogType, catalogProperty.getProperties(), context);
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        checkHiveParquetTimeZone(catalogProperty);
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
    public boolean validatePropertiesBeforeUpdate(
            Map<String, String> currentProperties, Map<String, String> updatedProperties) throws DdlException {
        Map<String, String> candidate = currentProperties == null
                ? new HashMap<>() : new HashMap<>(currentProperties);
        candidate.putAll(updatedProperties);
        CatalogProperty candidateProperty = new CatalogProperty(null, candidate);
        super.checkProperties(candidateProperty);
        // Validate the detached candidate before journaling so every accepted ALTER remains
        // readable by the scan-planning path that parses the same catalog property later.
        checkHiveParquetTimeZone(candidateProperty);
        try {
            // Connector validation must observe the complete candidate without making it visible
            // to concurrent catalog initialization; the provider handles legacy-value compatibility.
            ConnectorFactory.validatePropertiesForUpdate(getType(), currentProperties, updatedProperties);
        } catch (IllegalArgumentException e) {
            throw new DdlException(e.getMessage(), e);
        }
        ExternalFunctionRules.check(candidateProperty.getOrDefault("function_rules", null));
        return true;
    }

    private void checkHiveParquetTimeZone(CatalogProperty property) throws DdlException {
        String catalogType = getType();
        if ("hms".equalsIgnoreCase(catalogType) || "hudi".equalsIgnoreCase(catalogType)) {
            String hiveParquetTimeZone = property.getOrDefault(
                    FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, null);
            if (hiveParquetTimeZone != null) {
                FileFormatUtils.parseHiveParquetTimeZone(hiveParquetTimeZone);
            }
        }
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

    /**
     * Invalidates connector-owned metadata before FE caches because connector metadata feeds row-count loading.
     * The connector field is read directly so refreshing an uninitialized catalog does not initialize it.
     */
    @Override
    public void onRefreshCache(boolean invalidCache) {
        if (!invalidCache) {
            super.onRefreshCache(false);
            return;
        }
        try {
            invalidateAllConnectorCachesIfPresent();
        } finally {
            super.onRefreshCache(true);
        }
    }

    /**
     * Invalidates connector-owned caches without initializing or rebuilding the connector.
     *
     * <p>Replay also uses this when no database object is cached and remote cache keys cannot be recovered.
     */
    public void invalidateAllConnectorCachesIfPresent() {
        Connector localConnector = connector;
        if (localConnector != null) {
            localConnector.invalidateAll();
        }
    }

    @Override
    protected List<String> listDatabaseNames() {
        try {
            ConnectorSession session = buildCrossStatementSession();
            return PluginDrivenMetadata.get(session, connector).listDatabaseNames(session);
        } catch (RuntimeException e) {
            // The connector connects lazily: initLocalObjectsImpl() only constructs it, so the
            // first metastore round-trip happens here — inside the meta-cache loader, which runs
            // OUTSIDE makeSureInitialized()'s try/catch. Capture the failure so `show catalogs`
            // surfaces it; makeSureInitialized() clears errorMsg again on the next successful
            // (re-)initialization (e.g. after `alter catalog ... set properties`). This stays
            // connector-agnostic: any plugin that connects lazily gets the same treatment.
            recordDeferredInitError(e);
            throw e;
        }
    }

    @Override
    protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
        ConnectorSession session = buildCrossStatementSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        List<String> tableNames = metadata.listTableNames(session, dbName);
        // Deliberately the raw field, NOT hasConnectorCapability(): this already runs inside an initialized
        // catalog, so re-entering makeSureInitialized() here would be pointless work on a listing path.
        if (!connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_VIEW)) {
            return tableNames;
        }
        // Mirror legacy IcebergExternalCatalog.listTableNamesFromRemote: for a view-exposing connector
        // (iceberg) SHOW TABLES includes both tables AND views, because the connector's listTableNames
        // subtracts the view names. Re-merge the connector's view names here (the two sets are disjoint
        // by construction, so a plain addAll cannot introduce duplicates).
        List<String> viewNames = metadata.listViewNames(session, dbName);
        if (viewNames.isEmpty()) {
            return tableNames;
        }
        List<String> merged = new ArrayList<>(tableNames);
        merged.addAll(viewNames);
        return merged;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        ConnectorSession session = buildConnectorSession();
        return PluginDrivenMetadata.get(session, connector)
                .getTableHandle(session, dbName, tblName).isPresent();
    }

    @Override
    public String getType() {
        // Return the actual catalog type (e.g., "es", "jdbc") from properties,
        // not the internal "plugin" logType.
        return catalogProperty.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, super.getType());
    }

    /**
     * The engine name this catalog's tables display, asked of the connector's <em>provider</em> so that the
     * engine holds no mapping from data source to displayed name. Falls back to the catalog type when no
     * provider claims it, which is also the provider's own default — a catalog whose plugin is not installed
     * therefore still displays what it displayed before.
     *
     * <p>Resolved once and remembered. Both callers ({@code PluginDrivenExternalTable.getEngine} and
     * {@code getEngineTableTypeName}) sit in a per-table loop — {@code FrontendServiceImpl.listTableStatus}
     * calls this for every table of a database — and {@link #getProperties()} copies the whole property map on
     * every call, so resolving per table would copy that map per table. Remembering is safe because the
     * provider set is fixed for the life of the FE: {@code Env.initConnectorPluginManager} runs once at
     * startup, before any catalog is touched, and nothing re-registers afterwards. The field is transient, so
     * after a restart the first caller recomputes it — a local lookup among loaded plugins that touches
     * nothing remote and cannot force this catalog to initialize.</p>
     */
    public String getDisplayEngineName() {
        String name = displayEngineName;
        if (name == null) {
            String type = getType();
            name = ConnectorFactory.findProvider(type, getProperties())
                    .map(ConnectorProvider::displayEngineName)
                    .orElse(type);
            displayEngineName = name;
        }
        return name;
    }

    /** Returns the underlying SPI connector. Ensures the catalog is initialized first. */
    public Connector getConnector() {
        makeSureInitialized();
        return connector;
    }

    /**
     * Whether the backing connector declares {@code capability} catalog-wide. The single entry point for the
     * capability checks that do NOT have a table in hand — the alternative is each caller repeating
     * {@code getConnector() != null && getConnector().getCapabilities().contains(...)}, which is how one of
     * them ended up without the null check and throwing instead of rejecting cleanly.
     *
     * <p><b>Forces initialization</b> (via {@link #getConnector()}), so it is for callers OUTSIDE this class,
     * which already paid that cost. Code inside this catalog that runs before or during initialization must
     * keep reading the {@code connector} field directly: routing it here would make a capability check
     * initialize the catalog, which is exactly what those sites avoid.</p>
     *
     * <p>Only the capabilities {@link ConnectorCapability} documents as catalog-scoped belong here; a
     * table-scoped one is resolved by {@code PluginDrivenExternalTable} instead, as the union of this set and
     * the table's own.</p>
     */
    public boolean hasConnectorCapability(ConnectorCapability capability) {
        Connector conn = getConnector();
        return conn != null && conn.getCapabilities().contains(capability);
    }

    /**
     * Answers from the connector's <em>provider</em>, not the connector: this runs while a statement is being
     * analyzed, and {@link #getConnector()} would force the catalog to initialize, turning a mistyped engine
     * name into a metastore connection error. Provider lookup is a lookup among already-registered plugins,
     * keyed on the persisted catalog type, and touches nothing remote.
     *
     * <p>A catalog whose plugin is not installed has no provider, so every explicit engine is rejected with
     * the same mismatch message the base interface produces — the missing-plugin diagnosis still comes later,
     * from initialization, exactly as it does today.</p>
     */
    @Override
    public void validateCreateTableEngine(String engineName) throws AnalysisException {
        boolean accepted = ConnectorFactory.findProvider(getType(), getProperties())
                .map(provider -> provider.acceptedCreateTableEngineNames().contains(engineName))
                .orElse(false);
        if (!accepted) {
            throw new AnalysisException(CatalogIf.engineMismatchError(engineName, getName()));
        }
    }

    /**
     * Registers a newly-observed database into this catalog, driven by the metastore-event sync's
     * REGISTER_DATABASE change (via {@code CatalogMgr.registerExternalDatabaseFromEvent}). Pulled up from
     * {@code HMSExternalCatalog} so a flipped (generic) catalog no longer throws
     * {@code NotImplementedException} on a create/rename-database event. The body is fully generic
     * (buildDbForInit + the shared metadata-cache update protocol) and mirrors the legacy HMS implementation.
     */
    @Override
    public void registerDatabaseFromEvent(String remoteDbName, String localDbName) {
        long dbId = Util.genIdByName(getName(), localDbName);
        try {
            ExternalDatabase<? extends ExternalTable> db =
                    buildDbForInit(remoteDbName, localDbName, dbId, logType, false);
            if (isInitialized()) {
                updateDatabaseCache(db.getRemoteName(), db.getFullName(), db);
            }
        } finally {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDb(getId(), dbId, localDbName);
        }
    }

    /**
     * Applies a connector-provided schema-cache TTL to the ephemeral cache configuration. An explicit schema TTL
     * takes precedence, and an unavailable connector leaves the engine default unchanged.
     */
    @Override
    public void overlayMetaCacheConfig(Map<String, String> metaCacheProperties) {
        if (metaCacheProperties.containsKey(SCHEMA_CACHE_TTL_SECOND)) {
            return;
        }
        Connector localConnector = connector;
        if (localConnector == null) {
            return;
        }
        OptionalLong override = localConnector.schemaCacheTtlSecondOverride();
        if (override.isPresent()) {
            metaCacheProperties.put(SCHEMA_CACHE_TTL_SECOND, String.valueOf(override.getAsLong()));
        }
    }

    /**
     * Routes CREATE TABLE through the connector SPI. Returning {@code true} for an existing table preserves
     * the caller's CTAS short-circuit contract. A newly created table persists canonical local names; every
     * successful path invalidates connector, metadata, and row-count caches in that order.
     */
    @Override
    public boolean createTable(CreateTableInfo createTableInfo) throws UserException {
        makeSureInitialized();
        // The database already has a remote identity; the new table name is itself the remote target.
        ExternalDatabase<? extends ExternalTable> db = getDbNullable(createTableInfo.getDbName());
        if (db == null) {
            throw new DdlException("Failed to get database: '" + createTableInfo.getDbName()
                    + "' in catalog: " + getName());
        }
        String localTableName = db.canonicalLocalTableNameFromRemote(createTableInfo.getTableName());
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        // Both views matter: name folding can expose a local conflict that the remote lookup does not see.
        boolean remoteExists = metadata.getTableHandle(session, db.getRemoteName(),
                createTableInfo.getTableName()).isPresent();
        boolean localExists = db.getTableNullable(localTableName) != null;
        if (remoteExists || localExists) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}.{}.{}] which already exists; skipping (IF NOT EXISTS)",
                        getName(), createTableInfo.getDbName(), createTableInfo.getTableName());
                return executeConstraintMetadataMutation(() -> {
                    invalidateCreatedTableCaches(db, createTableInfo.getTableName(), localTableName);
                    return true;
                });
            }
            // Report the established MySQL error before a local-only conflict can create a remote duplicate.
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR,
                    createTableInfo.getTableName());
        }
        ConnectorCreateTableRequest request = CreateTableInfoToConnectorRequestConverter
                .convert(createTableInfo, db.getRemoteName());
        return executeConstraintMetadataMutation(() -> {
            try {
                metadata.createTable(session, request);
            } catch (DorisConnectorException e) {
                // The probe and create are not atomic. A successful re-probe turns a lost IF NOT EXISTS race into
                // the same CTAS-short-circuit result as an initially existing table.
                if (createTableInfo.isIfNotExists()
                        && metadata.getTableHandle(session, db.getRemoteName(),
                                createTableInfo.getTableName()).isPresent()) {
                    LOG.info("create table[{}.{}.{}] lost the race to a concurrent creator; "
                                    + "treating as an IF NOT EXISTS no-op", getName(),
                            createTableInfo.getDbName(), createTableInfo.getTableName());
                    invalidateCreatedTableCaches(db, createTableInfo.getTableName(), localTableName);
                    return true;
                }
                throw new DdlException(e.getMessage(), e);
            }
            org.apache.doris.persist.CreateTableInfo persistInfo =
                    new org.apache.doris.persist.CreateTableInfo(
                            getName(), db.getFullName(), localTableName);
            Env.getCurrentEnv().getEditLog().logCreateTable(persistInfo);
            invalidateCreatedTableCaches(db, createTableInfo.getTableName(), localTableName);
            LOG.info("finished to create table {}.{}.{}", getName(),
                    createTableInfo.getDbName(), createTableInfo.getTableName());
            return false;
        });
    }

    /** Routes CREATE DATABASE through the connector SPI while enforcing IF NOT EXISTS on both FE and remote state. */
    @Override
    public void createDb(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException {
        makeSureInitialized();
        if (ifNotExists) {
            ExternalDatabase<? extends ExternalTable> existingDb = getDbNullable(dbName);
            if (existingDb != null) {
                executeConstraintMetadataMutation(() -> {
                    invalidateCreatedDatabaseCaches(existingDb.getRemoteName(), existingDb.getFullName());
                    return null;
                });
                return;
            }
        }
        String localDbName = canonicalLocalDatabaseNameFromRemote(dbName);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        // A remote hit also satisfies IF NOT EXISTS, including for connectors that cannot create databases.
        if (ifNotExists && metadata.databaseExists(session, dbName)) {
            LOG.info("create database[{}] which already exists remotely, skip", dbName);
            executeConstraintMetadataMutation(() -> {
                invalidateCreatedDatabaseCaches(dbName, localDbName);
                return null;
            });
            return;
        }
        executeConstraintMetadataMutation(() -> {
            try {
                metadata.createDatabase(session, dbName, properties);
            } catch (DorisConnectorException e) {
                if (ifNotExists && metadata.databaseExists(session, dbName)) {
                    LOG.info("create database[{}] lost the race to a concurrent creator; "
                            + "treating as an IF NOT EXISTS no-op", dbName);
                    invalidateCreatedDatabaseCaches(dbName, localDbName);
                    return null;
                }
                throw new DdlException(e.getMessage(), e);
            }
            Env.getCurrentEnv().getEditLog().logCreateDb(new CreateDbInfo(getName(), localDbName, null));
            invalidateCreatedDatabaseCaches(dbName, localDbName);
            LOG.info("finished to create database {}.{}", getName(), dbName);
            return null;
        });
    }

    /**
     * Routes {@code DROP DATABASE} through the SPI's
     * {@code ConnectorSchemaOps.dropDatabase(session, dbName, ifExists)}.
     *
     * <p>{@code force} is forwarded to the connector, which performs the table
     * cascade (mirroring legacy {@code MaxComputeMetadataOps.dropDbImpl}; ODPS
     * {@code schemas().delete()} does not auto-cascade). On success it writes the
     * edit log and unregisters the database from the cache (mirroring the legacy
     * {@code metadataOps.afterDropDb()}); legacy emits no per-table editlog for the
     * cascaded tables, so the single {@code logDropDb} + {@code unregisterDatabase}
     * below is the complete legacy db-level FE bookkeeping.</p>
     */
    @Override
    public void dropDb(String dbName, boolean ifExists, boolean force) throws DdlException {
        makeSureInitialized();
        // Resolve the local db name to its remote name before handing it to the connector, mirroring
        // the sibling dropTable / legacy IcebergMetadataOps.performDropDb (dorisDb.getRemoteName()).
        // Name-mapped catalogs (lower_case_meta_names / meta_names_mapping, where the local display
        // name differs from the remote name) would otherwise address the wrong remote namespace.
        ExternalDatabase<? extends ExternalTable> db = getDbNullable(dbName);
        if (db == null) {
            if (ifExists) {
                return;
            }
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + getName());
        }
        ConnectorSession session = buildConnectorSession();
        executeConstraintMetadataMutation(() -> {
            try {
                PluginDrivenMetadata.get(session, connector)
                        .dropDatabase(session, db.getRemoteName(), ifExists, force);
            } catch (DorisConnectorException e) {
                throw new DdlException(e.getMessage(), e);
            }
            Env.getCurrentEnv().getEditLog().logDropDb(new DropDbInfo(getName(), db.getFullName()));
            try {
                connector.invalidateDb(db.getRemoteName());
            } finally {
                try {
                    unregisterDatabase(db.getFullName());
                } finally {
                    dropDatabaseConstraintsAndInvalidateMtmvs(db.getFullName(),
                            "after dropping external database " + getName() + "." + db.getFullName());
                }
            }
            LOG.info("finished to drop database {}.{}", getName(), dbName);
            return null;
        });
    }

    /**
     * Routes {@code DROP TABLE} through the SPI's
     * {@code ConnectorTableOps.dropTable(session, handle)}.
     *
     * <p>The SPI takes a {@link ConnectorTableHandle} and carries no {@code ifExists};
     * this override resolves the handle first (absent = table does not exist) and
     * enforces {@code IF EXISTS} FE-side. On success it writes the edit log and
     * unregisters the table from the cache (mirroring {@code metadataOps.afterDropTable()}).</p>
     */
    @Override
    public void dropTable(String dbName, String tableName, boolean isView, boolean isMtmv, boolean isStream,
                          boolean ifExists, boolean mustTemporary, boolean force) throws DdlException {
        makeSureInitialized();
        // Resolve the local db/table names to their remote (ODPS) names before handing them to the
        // connector, mirroring base ExternalCatalog.dropTable -- the exact path legacy
        // MaxComputeMetadataOps.dropTableImpl ran through, which used dorisTable.getRemoteDbName() /
        // getRemoteName(). Without this, name-mapped catalogs would locate the wrong remote table
        // (IF EXISTS silently no-ops / non-IF-EXISTS wrongly reports "not found"). Matching base:
        // a missing db ALWAYS throws (even with IF EXISTS); a missing table honors IF EXISTS.
        ExternalDatabase<? extends ExternalTable> db = getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + getName());
        }
        ExternalTable dorisTable = db.getTableNullable(tableName);
        if (dorisTable == null) {
            if (ifExists) {
                return;
            }
            throw new DdlException("Failed to get table: '" + tableName + "' in database: " + dbName);
        }
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        // Route a DROP on a VIEW to dropView, mirroring legacy IcebergMetadataOps.dropTableImpl's
        // viewExists -> performDropView dispatch: a connector that exposes views keeps them in a separate
        // namespace, so getTableHandle/tableExists below is false for a view and the table-handle path
        // could never drop it. For view-less connectors viewExists defaults to false (no remote call), so
        // this routing is inert and the table path runs unchanged. The edit log + cache invalidation use
        // the LOCAL names (follower-replay parity), identical to the table path.
        executeConstraintMetadataMutation(() -> {
            TableNameInfo tableNameInfo =
                    new TableNameInfo(getName(), db.getFullName(), dorisTable.getName());
            boolean viewExists = metadata.viewExists(
                    session, dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
            Optional<ConnectorTableHandle> handle = viewExists ? Optional.empty() : metadata.getTableHandle(
                    session, dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
            if (!viewExists && !handle.isPresent()) {
                if (ifExists) {
                    finishDropTable(db, dorisTable, tableNameInfo);
                    return null;
                }
                throw new DdlException("Failed to get table: '" + tableName + "' in database: " + dbName);
            }
            Env.getCurrentEnv().getConstraintManager()
                    .checkNoReferencingForeignKeys(tableNameInfo);
            try {
                if (viewExists) {
                    metadata.dropView(session, dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
                } else {
                    metadata.dropTable(session, handle.get());
                }
            } catch (DorisConnectorException e) {
                throw new DdlException(e.getMessage(), e);
            }
            finishDropTable(db, dorisTable, tableNameInfo);
            if (viewExists) {
                LOG.info("finished to drop view {}.{}.{}", getName(), dbName, tableName);
            } else {
                LOG.info("finished to drop table {}.{}.{}", getName(), dbName, tableName);
            }
            return null;
        });
    }

    /**
     * Routes {@code ALTER TABLE ... RENAME} through the SPI's {@code ConnectorTableOps.renameTable} instead of
     * the base {@link ExternalCatalog#renameTable} (which throws on {@code metadataOps == null}).
     *
     * <p>Resolves the SOURCE table by REMOTE names (like {@link #dropTable}); {@code newTableName} is passed
     * through as the target's name in the same remote database, mirroring legacy
     * {@code IcebergMetadataOps.renameTableImpl} (which feeds the SQL name straight to
     * {@code catalog.renameTable}) and createTable (which keeps the SQL name as the remote name). On success
     * runs {@link #afterExternalRename} for the cache fix + constraint rename + editlog the base op delegated
     * to {@code metadataOps}.</p>
     */
    @Override
    public void renameTable(String dbName, String oldTableName, String newTableName) throws DdlException {
        makeSureInitialized();
        ExternalDatabase<? extends ExternalTable> db = getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + getName());
        }
        ExternalTable dorisTable = db.getTableNullable(oldTableName);
        if (dorisTable == null) {
            throw new DdlException("Failed to get table: '" + oldTableName + "' in database: " + dbName);
        }
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(dorisTable, session, metadata);
        String localNewTableName = db.canonicalLocalTableNameFromRemote(newTableName);
        executeConstraintMetadataMutation(() -> {
            try {
                metadata.renameTable(session, handle, newTableName);
            } catch (DorisConnectorException e) {
                throw new DdlException(e.getMessage(), e);
            }
            afterExternalRename(db, dorisTable, newTableName, localNewTableName);
            return null;
        });
    }

    /**
     * Routes {@code TRUNCATE TABLE} through the SPI's {@code ConnectorTableOps.truncateTable(session, handle,
     * partitions)} instead of the base {@link ExternalCatalog#truncateTable} (which throws on
     * {@code metadataOps == null}).
     *
     * <p>Resolves the table by REMOTE names for the connector (like {@link #dropTable}); {@code partitions} is
     * {@code null} for a whole-table truncate or the named partitions otherwise. On success it emits the same
     * {@link TruncateTableInfo} edit log the base op writes and refreshes the local table cache (mirroring legacy
     * {@code HiveMetadataOps.afterTruncateTable -> RefreshManager.refreshTableInternal}); followers refresh via
     * {@link #replayTruncateTable}. {@code forceDrop} / {@code rawTruncateSql} carry no external semantics (the
     * connector truncates the remote table directly) and are ignored, matching the legacy path.</p>
     */
    @Override
    public void truncateTable(String dbName, String tableName, PartitionNamesInfo partitionNamesInfo,
                              boolean forceDrop, String rawTruncateSql) throws DdlException {
        makeSureInitialized();
        ExternalDatabase<? extends ExternalTable> db = getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + getName());
        }
        ExternalTable dorisTable = db.getTableNullable(tableName);
        if (dorisTable == null) {
            throw new DdlException("Failed to get table: '" + tableName + "' in database: " + dbName);
        }
        List<String> partitions = partitionNamesInfo == null ? null : partitionNamesInfo.getPartitionNames();
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(dorisTable, session, metadata);
        try {
            metadata.truncateTable(session, handle, partitions);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        long updateTime = System.currentTimeMillis();
        Env.getCurrentEnv().getEditLog().logTruncateTable(
                new TruncateTableInfo(getName(), db.getFullName(), dorisTable.getName(), partitions, updateTime));
        Env.getCurrentEnv().getRefreshManager().refreshTableInternal(db, dorisTable, updateTime);
        LOG.info("finished to truncate table {}.{}.{}", getName(), dbName, tableName);
    }

    /**
     * Replays cache invalidation for a connector-driven truncate without loading remote metadata. A cached table
     * follows the normal refresh path. Otherwise canonical local names identify engine and row-count caches, while
     * connector invalidation widens to the database or catalog scope when the remote table name is unavailable.
     */
    @Override
    public void replayTruncateTable(TruncateTableInfo info) {
        Optional<ExternalDatabase<? extends ExternalTable>> db = getDbForReplay(info.getDb());
        Optional<? extends ExternalTable> table = db.flatMap(database -> database.getTableForReplay(info.getTable()));
        if (table.isPresent()) {
            Env.getCurrentEnv().getRefreshManager()
                    .refreshTableInternal(db.get(), table.get(), info.getUpdateTime());
            return;
        }
        ExternalMetaCacheMgr cacheMgr = Env.getCurrentEnv().getExtMetaCacheMgr();
        try {
            Connector replayConnector = connector;
            if (isInitialized() && replayConnector != null) {
                if (db.isPresent()) {
                    replayConnector.invalidateDb(db.get().getRemoteName());
                } else {
                    replayConnector.invalidateAll();
                }
            }
        } finally {
            long dbId = Util.genIdByName(getName(), info.getDb());
            cacheMgr.invalidateTable(getId(), dbId, info.getDb(),
                    Util.genIdByName(getName(), info.getDb(), info.getTable()), info.getTable());
        }
    }

    @Override
    public void replayDropTable(String dbName, String tblName) {
        try (ConstraintMetadataMutationGuard ignored = beginConstraintMetadataMutation()) {
            try {
                invalidateTableConnectorCacheForReplay(dbName, tblName);
            } finally {
                try {
                    invalidateDroppedTable(dbName, tblName);
                } finally {
                    dropTableConstraintsAndInvalidateMtmvs(
                            new TableNameInfo(getName(), dbName, tblName),
                            "after replaying external table drop " + getName() + "." + dbName + "." + tblName);
                }
            }
        }
    }

    @Override
    public void replayDropDb(String dbName) {
        try (ConstraintMetadataMutationGuard ignored = beginConstraintMetadataMutation()) {
            try {
                invalidateDatabaseConnectorCacheForReplay(dbName);
            } finally {
                try {
                    unregisterDatabase(dbName);
                } finally {
                    dropDatabaseConstraintsAndInvalidateMtmvs(dbName,
                            "after replaying external database drop " + getName() + "." + dbName);
                }
            }
        }
    }

    @Override
    public void replayCreateTable(String dbName, String tblName) {
        try {
            invalidateDatabaseConnectorCacheForReplay(dbName);
        } finally {
            super.replayCreateTable(dbName, tblName);
        }
    }

    @Override
    public void replayCreateDb(String dbName) {
        try {
            invalidateDatabaseConnectorCacheForReplay(dbName);
        } finally {
            super.replayCreateDb(dbName);
        }
    }

    // Replay never initializes a cold catalog; missing local-to-remote identity widens the live connector scope.
    private void invalidateDatabaseConnectorCacheForReplay(String localDbName) {
        Connector replayConnector = connector;
        if (isInitialized() && replayConnector != null) {
            Optional<ExternalDatabase<? extends ExternalTable>> db = getDbForReplay(localDbName);
            if (db.isPresent()) {
                replayConnector.invalidateDb(db.get().getRemoteName());
            } else {
                replayConnector.invalidateAll();
            }
        }
    }

    private void invalidateTableConnectorCacheForReplay(String localDbName, String localTableName) {
        Connector replayConnector = connector;
        if (!isInitialized() || replayConnector == null) {
            return;
        }
        Optional<ExternalDatabase<? extends ExternalTable>> db = getDbForReplay(localDbName);
        if (!db.isPresent()) {
            replayConnector.invalidateAll();
            return;
        }
        Optional<? extends ExternalTable> table = db.get().getTableForReplay(localTableName);
        if (table.isPresent()) {
            replayConnector.invalidateTable(db.get().getRemoteName(), table.get().getRemoteName());
        } else {
            replayConnector.invalidateDb(db.get().getRemoteName());
        }
    }

    /**
     * Routes {@code ALTER TABLE ... ADD/DROP/RENAME/MODIFY/REORDER COLUMN} through the SPI's
     * {@code ConnectorTableOps} column-evolution methods instead of the legacy {@code metadataOps} path used
     * by other {@link ExternalCatalog} subclasses (which PluginDriven never sets, so the base ops would
     * throw {@code metadataOps == null}).
     *
     * <p>Each override resolves the connector handle (by REMOTE names, like {@link #dropTable}), converts the
     * Doris {@link Column}/{@link ColumnPosition} to the neutral SPI types, dispatches, wraps a
     * {@link DorisConnectorException} as a {@link DdlException}, and runs {@link #afterExternalDdl} for the
     * editlog + cache invalidation the base op delegated to {@code metadataOps}.</p>
     */
    @Override
    public void addColumn(TableIf dorisTable, Column column, ColumnPosition position) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime,
                () -> metadata.addColumn(session, handle,
                        ConnectorColumnConverter.toConnectorColumn(column),
                        toConnectorPosition(position)));
    }

    @Override
    public void addColumns(TableIf dorisTable, List<Column> columns) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime,
                () -> metadata.addColumns(
                        session, handle, ConnectorColumnConverter.toConnectorColumns(columns)));
    }

    @Override
    public void dropColumn(TableIf dorisTable, String columnName) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime, columnName,
                () -> metadata.dropColumn(session, handle, columnName));
    }

    @Override
    public void renameColumn(TableIf dorisTable, String oldName, String newName) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime, oldName,
                () -> metadata.renameColumn(session, handle, oldName, newName));
    }

    @Override
    public void modifyColumn(TableIf dorisTable, Column column, ColumnPosition position) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime,
                () -> metadata.modifyColumn(session, handle,
                        ConnectorColumnConverter.toConnectorColumn(column),
                        toConnectorPosition(position)));
    }

    @Override
    public void reorderColumns(TableIf dorisTable, List<String> newOrder) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime,
                () -> metadata.reorderColumns(session, handle, newOrder));
    }

    /**
     * {@code ColumnPath} column-DDL overrides. #65329 rewired {@code Alter.java} to dispatch every external
     * column op through the {@code ColumnPath} overloads; the base {@link ExternalCatalog} throws for them, so
     * without these overrides even top-level iceberg column DDL would fall through to "not supported for
     * catalog". Each override handles the top-level (non-nested) case by delegating to the matching flat
     * override above (which routes to {@code ConnectorTableOps}); nested paths are neutralized to
     * {@link ConnectorColumnPath} and dispatched to the path-addressed connector ops. {@code MODIFY COLUMN
     * COMMENT} (a #65329 op with no flat equivalent) always goes through the path op.
     */
    @Override
    public void addColumn(TableIf dorisTable, ColumnPath columnPath, Column column, ColumnPosition position)
            throws UserException {
        if (!columnPath.isNested()) {
            addColumn(dorisTable, column, position);
            return;
        }
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime,
                () -> metadata.addNestedColumn(session, handle, toConnectorPath(columnPath),
                        ConnectorColumnConverter.toConnectorColumn(column),
                        toConnectorPosition(position)));
    }

    @Override
    public void dropColumn(TableIf dorisTable, ColumnPath columnPath) throws UserException {
        if (!columnPath.isNested()) {
            dropColumn(dorisTable, columnPath.getTopLevelName());
            return;
        }
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime,
                () -> metadata.dropNestedColumn(session, handle, toConnectorPath(columnPath)));
    }

    @Override
    public void renameColumn(TableIf dorisTable, ColumnPath columnPath, String newName) throws UserException {
        if (!columnPath.isNested()) {
            renameColumn(dorisTable, columnPath.getTopLevelName(), newName);
            return;
        }
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime,
                () -> metadata.renameNestedColumn(
                        session, handle, toConnectorPath(columnPath), newName));
    }

    @Override
    public void modifyColumn(TableIf dorisTable, ColumnPath columnPath, Column column, ColumnPosition position)
            throws UserException {
        if (!columnPath.isNested()) {
            modifyColumn(dorisTable, column, position);
            return;
        }
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        executeSchemaMutation(externalTable, updateTime,
                () -> metadata.modifyNestedColumn(session, handle, toConnectorPath(columnPath),
                        ConnectorColumnConverter.toConnectorColumn(column),
                        toConnectorPosition(position)));
    }

    @Override
    public void modifyColumnComment(TableIf dorisTable, ColumnPath columnPath, String comment) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.modifyColumnComment(session, handle, toConnectorPath(columnPath), comment);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    /**
     * Routes {@code ALTER TABLE ... CREATE/REPLACE/DROP BRANCH/TAG} through the SPI's {@code ConnectorTableOps}
     * branch/tag methods instead of the legacy {@code metadataOps} path (which PluginDriven never sets, so the
     * base ops throw {@code metadataOps == null}).
     *
     * <p>Each override resolves the connector handle (by REMOTE names, like {@link #dropTable}), neutralizes the
     * nereids info type to the SPI carrier ({@link ConnectorBranchTagConverter}), dispatches, wraps a
     * {@link DorisConnectorException} as a {@link DdlException}, and runs {@link #afterExternalDdl} for the
     * editlog + cache invalidation the base op delegated to {@code metadataOps}. A branch/tag op is a
     * table-level change whose cache effect ({@code refreshTableInternal}) is identical to a column evolution, so
     * the column-op bookkeeping helper is reused (the base {@code OP_BRANCH_OR_TAG} editlog's replay is
     * {@code metadataOps}-gated and would be a no-op for PluginDriven; the replay-neutral
     * {@code OP_REFRESH_EXTERNAL_TABLE} that {@code afterExternalDdl} emits yields the same refresh on
     * followers).</p>
     */
    @Override
    public void createOrReplaceBranch(TableIf dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.createOrReplaceBranch(session, handle,
                    ConnectorBranchTagConverter.toBranchChange(branchInfo));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void createOrReplaceTag(TableIf dorisTable, CreateOrReplaceTagInfo tagInfo) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.createOrReplaceTag(session, handle,
                    ConnectorBranchTagConverter.toTagChange(tagInfo));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void dropBranch(TableIf dorisTable, DropBranchInfo branchInfo) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.dropBranch(session, handle,
                    ConnectorBranchTagConverter.toDropRefChange(branchInfo));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void dropTag(TableIf dorisTable, DropTagInfo tagInfo) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.dropTag(session, handle,
                    ConnectorBranchTagConverter.toDropRefChange(tagInfo));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    /**
     * Routes {@code ALTER TABLE ... ADD/DROP/REPLACE PARTITION KEY} (Iceberg partition evolution) through the
     * SPI's {@code ConnectorTableOps} partition-field methods, replacing the legacy {@code Alter.java}
     * {@code instanceof IcebergExternalTable} dispatch. Each override resolves the connector handle (by REMOTE
     * names, like {@link #dropTable}), neutralizes the nereids op to {@link PartitionFieldChange} via
     * {@link ConnectorPartitionFieldConverter}, dispatches, wraps a {@link DorisConnectorException} as a
     * {@link DdlException}, and runs {@link #afterExternalDdl} for the editlog + cache invalidation (a partition
     * spec change is a table-level change whose {@code refreshTableInternal} effect matches a column evolution).
     */
    @Override
    public void addPartitionField(TableIf dorisTable, AddPartitionFieldOp op) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.addPartitionField(session, handle, ConnectorPartitionFieldConverter.toAddChange(op));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void dropPartitionField(TableIf dorisTable, DropPartitionFieldOp op) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.dropPartitionField(session, handle, ConnectorPartitionFieldConverter.toDropChange(op));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void replacePartitionField(TableIf dorisTable, ReplacePartitionFieldOp op) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.replacePartitionField(session, handle, ConnectorPartitionFieldConverter.toReplaceChange(op));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    /** Initializes + checks the table is an {@link ExternalTable}, mirroring the base {@link ExternalCatalog}. */
    private ExternalTable checkExternalTable(TableIf dorisTable) {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        return (ExternalTable) dorisTable;
    }

    /**
     * Resolves the connector handle for an ALTER by the table's REMOTE names (mirroring {@link #dropTable}),
     * failing loud as a {@link DdlException} when the table no longer exists remotely.
     */
    private ConnectorTableHandle resolveAlterHandle(ExternalTable externalTable, ConnectorSession session,
            ConnectorMetadata metadata) throws DdlException {
        Optional<ConnectorTableHandle> handle = metadata.getTableHandle(
                session, externalTable.getRemoteDbName(), externalTable.getRemoteName());
        if (!handle.isPresent()) {
            throw new DdlException("Failed to get table: '" + externalTable.getName()
                    + "' in database: " + externalTable.getDbName());
        }
        return handle.get();
    }

    private <T, E extends Exception> T executeConstraintMetadataMutation(
            ConstraintMetadataOperation<T, E> operation) throws E {
        try (ConstraintMetadataMutationGuard ignored = beginConstraintMetadataMutation()) {
            return operation.run();
        }
    }

    private void executeSchemaMutation(ExternalTable externalTable, long updateTime,
            ConnectorSchemaMutation mutation) throws DdlException {
        executeSchemaMutation(externalTable, updateTime, null, mutation);
    }

    private void executeSchemaMutation(ExternalTable externalTable, long updateTime,
            String constrainedColumn, ConnectorSchemaMutation mutation) throws DdlException {
        executeConstraintMetadataMutation(() -> {
            if (constrainedColumn != null) {
                String constraintName = Env.getCurrentEnv().getConstraintManager()
                        .findConstraintWithColumn(
                                new TableNameInfo(getName(), externalTable.getDbName(),
                                        externalTable.getName()),
                                constrainedColumn);
                if (constraintName != null) {
                    throw new DdlException(String.format(
                            "Cannot modify column '%s' because it is used by constraint '%s'. "
                                    + "Drop the constraint first.",
                            constrainedColumn, constraintName));
                }
            }
            try {
                mutation.run();
            } catch (DorisConnectorException e) {
                throw new DdlException(e.getMessage(), e);
            }
            afterExternalDdl(externalTable, updateTime);
            return null;
        });
    }

    @FunctionalInterface
    private interface ConstraintMetadataOperation<T, E extends Exception> {
        T run() throws E;
    }

    @FunctionalInterface
    private interface ConnectorSchemaMutation {
        void run();
    }

    /** Neutralizes the fe-catalog {@link ColumnPosition} to the SPI {@link ConnectorColumnPosition}; null-safe. */
    private static ConnectorColumnPosition toConnectorPosition(ColumnPosition position) {
        if (position == null) {
            return null;
        }
        return position.isFirst()
                ? ConnectorColumnPosition.FIRST
                : ConnectorColumnPosition.after(position.getLastCol());
    }

    private static ConnectorColumnPath toConnectorPath(ColumnPath columnPath) {
        return ConnectorColumnPath.of(columnPath.getParts());
    }

    /** Persists replay identity, then refreshes the cached table when present or the resolved transient table. */
    protected void afterExternalDdl(ExternalTable externalTable, long updateTime) {
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(
                ExternalObjectLog.createForRefreshTable(
                        getId(), externalTable.getDbName(), externalTable.getName(), updateTime));
        ExternalTable refreshTarget = getDbForReplay(externalTable.getDbName())
                .<ExternalTable>flatMap(db -> db.getTableForReplay(externalTable.getName()))
                .orElse(externalTable);
        Env.getCurrentEnv().getRefreshManager()
                .refreshTableInternal(refreshTarget.getDb(), refreshTarget, updateTime);
    }

    private void invalidateCreatedDatabaseCaches(String remoteDbName, String localDbName) {
        try {
            connector.invalidateDb(remoteDbName);
        } finally {
            invalidateCreatedDatabase(localDbName);
        }
    }

    private void invalidateCreatedTableCaches(ExternalDatabase<? extends ExternalTable> db,
            String remoteTableName, String localTableName) {
        try {
            connector.invalidateTable(db.getRemoteName(), remoteTableName);
        } finally {
            invalidateCreatedTable(db.getFullName(), localTableName);
        }
    }

    private void finishDropTable(ExternalDatabase<? extends ExternalTable> db, ExternalTable table,
            TableNameInfo tableNameInfo) {
        Env.getCurrentEnv().getEditLog().logDropTable(
                new DropInfo(getName(), db.getFullName(), table.getName()));
        try {
            connector.invalidateTable(table.getRemoteDbName(), table.getRemoteName());
        } finally {
            try {
                invalidateDroppedTable(db.getFullName(), table.getName());
            } finally {
                dropTableConstraintsAndInvalidateMtmvs(tableNameInfo,
                        "after dropping external table " + tableNameInfo);
            }
        }
    }

    private void dropTableConstraintsAndInvalidateMtmvs(TableNameInfo tableNameInfo, String reason) {
        List<TableNameInfo> affectedTables = Env.getCurrentEnv().getConstraintManager()
                .dropTableConstraints(tableNameInfo);
        MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(affectedTables, reason);
    }

    private void dropDatabaseConstraintsAndInvalidateMtmvs(String dbName, String reason) {
        List<TableNameInfo> affectedTables = Env.getCurrentEnv().getConstraintManager()
                .dropDatabaseConstraints(getName(), dbName);
        MTMVUtil.invalidateRewriteCachesByTableNamesBestEffort(affectedTables, reason);
    }

    protected void afterExternalRename(ExternalDatabase<? extends ExternalTable> db,
            ExternalTable table, String remoteNewTableName, String localNewTableName) {
        String dbName = db.getFullName();
        String oldTableName = table.getName();
        // The remote rename has committed; persist replay identity before fallible local bookkeeping.
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(
                ExternalObjectLog.createForRenameTable(
                        getId(), dbName, oldTableName, localNewTableName));
        try {
            try {
                connector.invalidateTable(table.getRemoteDbName(), table.getRemoteName());
            } finally {
                connector.invalidateTable(table.getRemoteDbName(), remoteNewTableName);
            }
        } finally {
            try {
                getDbForReplay(dbName).ifPresent(
                        cachedDb -> cachedDb.invalidateTableRename(oldTableName, localNewTableName));
            } finally {
                try {
                    Env.getCurrentEnv().getConstraintManager().renameTable(
                            new TableNameInfo(getName(), dbName, oldTableName),
                            new TableNameInfo(getName(), dbName, localNewTableName));
                } finally {
                    Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableRename(
                            getId(), db.getId(), dbName, table.getId(), oldTableName,
                            Util.genIdByName(getName(), dbName, localNewTableName), localNewTableName);
                }
            }
        }
    }

    @Override
    public String fromRemoteDatabaseName(String remoteDatabaseName) {
        ConnectorSession session = buildCrossStatementSession();
        return PluginDrivenMetadata.get(session, connector).fromRemoteDatabaseName(session, remoteDatabaseName);
    }

    @Override
    public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
        ConnectorSession session = buildCrossStatementSession();
        return PluginDrivenMetadata.get(session, connector)
                .fromRemoteTableName(session, remoteDatabaseName, remoteTableName);
    }

    /**
     * Builds a {@link ConnectorSession} from the current thread's {@link ConnectContext}.
     */
    public ConnectorSession buildConnectorSession() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            // Interactive path: inject the user's delegated credential when the connector opts in
            // (SUPPORTS_USER_SESSION). The credential rides the session and is consumed connector-side.
            return ConnectorSessionBuilder.from(ctx)
                    .withCatalogId(getId())
                    .withCatalogName(getName())
                    .withCatalogProperties(catalogProperty.getProperties())
                    .withUserSessionCapability(supportsUserSession())
                    .build();
        }
        // Background/internal path (no ConnectContext): never carries a delegated credential — a
        // session=user connector then fails closed on interactive callers and gets no borrowed identity here.
        return ConnectorSessionBuilder.create()
                .withCatalogId(getId())
                .withCatalogName(getName())
                .withCatalogProperties(catalogProperty.getProperties())
                .build();
    }

    /**
     * Builds a {@link ConnectorSession} for a CROSS-STATEMENT background loader — one that fills a cache
     * living longer than any single statement (database/table name caches, schema cache, column-statistic
     * cache, row-count cache, the BE-driven metadata TVF). Identical to {@link #buildConnectorSession()}
     * (same credential handling) except the per-statement scope is forced to
     * {@link ConnectorStatementScope#NONE}. That makes the read-through a contract rather than an accident:
     * a metadata resolved through {@link PluginDrivenMetadata#get} with this session is built fresh and never
     * memoized into — nor closed with — some live statement's scope, even when the loader happens to run on a
     * request/ANALYZE thread that has one (e.g. {@code fetchRowCount} reached synchronously from
     * {@code AnalysisManager.buildAnalysisJobInfo}). Under NONE the funnel memoizes nothing, so this is
     * byte-identical to a bare {@code getMetadata} call.
     */
    public ConnectorSession buildCrossStatementSession() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            return ConnectorSessionBuilder.from(ctx)
                    .withCatalogId(getId())
                    .withCatalogName(getName())
                    .withCatalogProperties(catalogProperty.getProperties())
                    .withUserSessionCapability(supportsUserSession())
                    .withStatementScope(ConnectorStatementScope.NONE)
                    .build();
        }
        return ConnectorSessionBuilder.create()
                .withCatalogId(getId())
                .withCatalogName(getName())
                .withCatalogProperties(catalogProperty.getProperties())
                .withStatementScope(ConnectorStatementScope.NONE)
                .build();
    }

    /**
     * Whether the backing connector projects the querying user's delegated credential onto the remote
     * metadata source ({@link ConnectorCapability#SUPPORTS_USER_SESSION}), gating both the FE credential
     * injection above and the shared-cache bypass ({@link #shouldBypassTableNameCache}).
     */
    private boolean supportsUserSession() {
        // Deliberately the raw field, NOT hasConnectorCapability(): this runs while building a session and on
        // the cache-bypass path, where forcing initialization would be an init-order inversion.
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_USER_SESSION);
    }

    /**
     * Under a {@link ConnectorCapability#SUPPORTS_USER_SESSION} connector carrying a per-request delegated
     * credential, the remote source returns PER-USER table metadata, so the shared (catalog+name-keyed, NOT
     * user-keyed) table-name cache must be bypassed — otherwise one user's REST-authorized/vended table set
     * would be served to another (cross-user leakage). A session with no credential keeps the shared cache;
     * the fail-closed rejection then happens connector-side on the actual metadata read, never here.
     */
    @Override
    protected boolean shouldBypassTableNameCache(SessionContext ctx) {
        return supportsUserSession() && ctx != null && ctx.hasDelegatedCredential();
    }

    /**
     * Db-level analog of {@link #shouldBypassTableNameCache}: under a session=user connector with a per-request
     * credential the remote source returns PER-USER databases, so the shared db-name cache is bypassed to avoid
     * leaking one user's visible database set to another (O2). Same capability + credential gate.
     */
    @Override
    protected boolean shouldBypassDbNameCache(SessionContext ctx) {
        return supportsUserSession() && ctx != null && ctx.hasDelegatedCredential();
    }

    /**
     * Schema-level analog of {@link #shouldBypassTableNameCache}: under a session=user connector with a per-request
     * credential the remote {@code loadTable} returns PER-USER schema (and authorizes per user), so the shared
     * name-keyed schema cache is bypassed to avoid serving one user's schema to another who could list but not
     * load the table (the "list != load" disclosure). Same capability + credential gate; a session with no
     * credential keeps the shared cache and the fail-closed rejection happens connector-side.
     */
    @Override
    protected boolean shouldBypassSchemaCache(SessionContext ctx) {
        return supportsUserSession() && ctx != null && ctx.hasDelegatedCredential();
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
    // MAX_COMPUTE needs no case: the default branch yields "max_compute", which
    // already matches its CatalogFactory key — do not add a redundant case.
    private static String legacyLogTypeToCatalogType(InitCatalogLog.Type logType) {
        switch (logType) {
            case TRINO_CONNECTOR:
                return "trino-connector";
            default:
                return logType.name().toLowerCase(Locale.ROOT);
        }
    }

    private void closeConnectorContextQuietly(DefaultConnectorContext context) {
        if (context == null) {
            return;
        }
        try {
            context.close();
        } catch (Throwable e) {
            LOG.warn("Failed to close connector context filesystem for catalog {}", name, e);
        }
    }

    @Override
    protected void closeResources() {
        try {
            super.closeResources();
        } catch (Throwable e) {
            LOG.warn("Failed to close common resources for plugin-driven catalog {}", name, e);
        }

        // Detach every stage before invoking external code. A throwing connector must not remain reachable
        // for another close attempt or prevent the connector-context stage from running.
        Connector connectorToClose = connector;
        connector = null;
        if (connectorToClose != null) {
            try {
                connectorToClose.close();
            } catch (Throwable e) {
                LOG.warn("Failed to close connector for catalog {}", name, e);
            }
        }
        // Close the shared context's cached engine FileSystem AFTER the connector(s) release their borrowed
        // reference to it. No-op when no FS was ever built (e.g. non-hive plugin catalogs never call
        // getFileSystem()).
        DefaultConnectorContext contextToClose = connectorContext;
        connectorContext = null;
        closeConnectorContextQuietly(contextToClose);
    }
}

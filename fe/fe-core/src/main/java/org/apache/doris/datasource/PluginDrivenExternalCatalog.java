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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.DefaultConnectorValidationContext;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.ddl.CreateTableInfoToConnectorRequestConverter;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.persist.CreateDbInfo;
import org.apache.doris.persist.DropDbInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.PluginDrivenTransactionManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
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
                // Wire any storage-derived authenticator first (rereview2 M-8): the paimon
                // filesystem/jdbc flavors build their HDFS Kerberos authenticator from the catalog's
                // storage properties here, because their legacy initializeCatalog() — which did this —
                // is dead on the plugin/cutover path. Default no-op for every other metastore type.
                msp.initExecutionAuthenticator(catalogProperty.getOrderedStoragePropertiesList());
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
                new DefaultConnectorContext(name, id, this::getExecutionAuthenticator,
                        () -> catalogProperty.getStoragePropertiesMap()));
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
     * FIX-4: let the connector's own cache knob also govern the schema cache (restoring the legacy single-knob
     * semantics — e.g. paimon's {@code meta.cache.paimon.table.ttl-second} sized the whole table cache, schema
     * included). Applied to the engine's EPHEMERAL cache-sizing property copy only (never persisted). An
     * explicit user {@code schema.cache.ttl-second} wins. Uses the {@code connector} field directly (no forced
     * init / no throw): this hook only runs during a cache read, by which point the catalog is already
     * initialized; a null connector (uninitialized or concurrently dropped) simply leaves the engine default.
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
     * Routes {@code CREATE TABLE} through the SPI's
     * {@code ConnectorTableOps.createTable(session, request)} instead of the
     * legacy {@code metadataOps} path used by other {@link ExternalCatalog}
     * subclasses.
     *
     * <p>Connectors that have not overridden the new SPI default fall through
     * to the SPI's "CREATE TABLE not supported" exception, which is wrapped
     * here as a {@link DdlException} to match the existing caller contract.</p>
     *
     * <p>The SPI {@code createTable} is {@code void} and this override has no
     * {@code metadataOps}, so it mirrors legacy
     * {@code MaxComputeMetadataOps.createTableImpl}: when the table already exists
     * and {@code IF NOT EXISTS} was given it returns {@code true} and skips the
     * connector create + edit log + cache reset (so a {@code CREATE TABLE IF NOT
     * EXISTS ... AS SELECT} short-circuits per the {@code Env.createTable} contract
     * instead of INSERTing into the existing table); otherwise it creates the table,
     * writes the edit log, resets the cache, and returns {@code false}.</p>
     */
    @Override
    public boolean createTable(CreateTableInfo createTableInfo) throws UserException {
        makeSureInitialized();
        // Resolve the local db name to its remote (ODPS) name before handing it to the connector,
        // mirroring legacy MaxComputeMetadataOps.createTableImpl (db.getRemoteName()). Without this,
        // name-mapped catalogs (lower_case_meta_names / meta_names_mapping, where the local display
        // name differs from the remote name) would address the wrong remote schema. The table name
        // is intentionally NOT remote-resolved (legacy parity: the table does not exist yet, so
        // there is no local->remote mapping for it).
        ExternalDatabase<? extends ExternalTable> db = getDbNullable(createTableInfo.getDbName());
        if (db == null) {
            throw new DdlException("Failed to get database: '" + createTableInfo.getDbName()
                    + "' in catalog: " + getName());
        }
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        // Mirror legacy MaxComputeMetadataOps.createTableImpl:178-197 -- probe BOTH the remote
        // (connector) and the local FE cache for an existing table. On IF NOT EXISTS this lets CTAS
        // short-circuit (Env.createTable contract: return true when the table already exists), so a
        // "CREATE TABLE IF NOT EXISTS ... AS SELECT" does NOT fall through to an INSERT into the
        // pre-existing table. The table name is intentionally NOT remote-resolved (legacy parity).
        boolean remoteExists = metadata.getTableHandle(session, db.getRemoteName(),
                createTableInfo.getTableName()).isPresent();
        boolean localExists = db.getTableNullable(createTableInfo.getTableName()) != null;
        if (remoteExists || localExists) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}.{}.{}] which already exists; skipping (IF NOT EXISTS)",
                        getName(), createTableInfo.getDbName(), createTableInfo.getTableName());
                return true;
            }
            // !IF NOT EXISTS: a table that already exists -- whether remotely (connector) OR only in the
            // local FE cache (a case-variant name folded onto an existing table under lower_case_meta_names
            // while the case-sensitive remote has no such table) -- must be rejected HERE with MySQL errno
            // 1050 (ERR_TABLE_EXISTS_ERROR / SQLSTATE 42S01). Mirrors legacy {Paimon,MaxCompute}MetadataOps,
            // which report ERR_TABLE_EXISTS_ERROR for BOTH the remote arm (PaimonMetadataOps:195 /
            // MaxComputeMetadataOps:184) and the local arm (:212 / :195). Reporting before
            // metadata.createTable also keeps a local-cache-only conflict from being CREATED remotely
            // (the connector would otherwise create a duplicate). Reaching here already guarantees
            // (remoteExists || localExists) && !isIfNotExists; reportDdlException throws.
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR,
                    createTableInfo.getTableName());
        }
        ConnectorCreateTableRequest request = CreateTableInfoToConnectorRequestConverter
                .convert(createTableInfo, db.getRemoteName());
        try {
            metadata.createTable(session, request);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        org.apache.doris.persist.CreateTableInfo persistInfo =
                new org.apache.doris.persist.CreateTableInfo(
                        getName(),
                        createTableInfo.getDbName(),
                        createTableInfo.getTableName());
        Env.getCurrentEnv().getEditLog().logCreateTable(persistInfo);
        // Invalidate the FE-side table-name cache so the new table is immediately visible on
        // this FE. The legacy metadataOps path did this via afterCreateTable(); since
        // PluginDrivenExternalCatalog has no metadataOps, the override must do it here.
        // (Edit log and cache invalidation deliberately use the LOCAL db/table names for
        // follower-replay consistency; only the connector-bound name is remote-resolved.)
        getDbForReplay(createTableInfo.getDbName()).ifPresent(d -> d.resetMetaCacheNames());
        LOG.info("finished to create table {}.{}.{}", getName(),
                createTableInfo.getDbName(), createTableInfo.getTableName());
        return false;
    }

    /**
     * Routes {@code CREATE DATABASE} through the SPI's
     * {@code ConnectorSchemaOps.createDatabase(session, dbName, properties)}.
     *
     * <p>The SPI signature carries no {@code ifNotExists}; this override honors it
     * FE-side. It short-circuits on the local FE cache, and — for connectors that
     * support CREATE DATABASE ({@code supportsCreateDatabase()}) — also consults the
     * remote {@code databaseExists} so {@code CREATE DATABASE IF NOT EXISTS} on a
     * database that exists remotely but is not yet in this FE's cache cleanly no-ops
     * instead of surfacing a remote "already exists" error (mirroring legacy
     * {@code MaxComputeMetadataOps.createDbImpl}, which checked both). On success it
     * writes the edit log and invalidates the cached db-name list (mirroring the
     * legacy {@code metadataOps.afterCreateDb()} the plugin path no longer has).</p>
     */
    @Override
    public void createDb(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException {
        makeSureInitialized();
        // Fast path: FE-cache hit + IF NOT EXISTS => no-op (legacy createDbImpl: dorisDb != null).
        if (ifNotExists && getDbNullable(dbName) != null) {
            return;
        }
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        // FE-cache miss but the db may already exist REMOTELY (created on another FE / before this
        // FE's db-name cache was populated). Legacy MaxComputeMetadataOps.createDbImpl consulted
        // BOTH getDbNullable AND the remote databaseExist, and IF NOT EXISTS then no-oped. Mirror
        // that remote check. Gated on supportsCreateDatabase() so connectors that cannot create
        // databases (jdbc/es/trino) keep their prior behavior (fall through to createDatabase ->
        // "not supported"); the && short-circuit means they never even issue the remote query.
        if (ifNotExists && metadata.supportsCreateDatabase() && metadata.databaseExists(session, dbName)) {
            LOG.info("create database[{}] which already exists remotely, skip", dbName);
            return;
        }
        try {
            metadata.createDatabase(session, dbName, properties);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        Env.getCurrentEnv().getEditLog().logCreateDb(new CreateDbInfo(getName(), dbName, null));
        resetMetaCacheNames();
        LOG.info("finished to create database {}.{}", getName(), dbName);
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
        if (getDbNullable(dbName) == null) {
            if (ifExists) {
                return;
            }
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + getName());
        }
        ConnectorSession session = buildConnectorSession();
        try {
            connector.getMetadata(session).dropDatabase(session, dbName, ifExists, force);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        Env.getCurrentEnv().getEditLog().logDropDb(new DropDbInfo(getName(), dbName));
        unregisterDatabase(dbName);
        LOG.info("finished to drop database {}.{}", getName(), dbName);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
        Optional<ConnectorTableHandle> handle = metadata.getTableHandle(
                session, dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
        // The table is present in the FE cache but may have been dropped out-of-band on the remote
        // side; preserve the existing IF EXISTS handling for that case.
        if (!handle.isPresent()) {
            if (ifExists) {
                return;
            }
            throw new DdlException("Failed to get table: '" + tableName + "' in database: " + dbName);
        }
        try {
            metadata.dropTable(session, handle.get());
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        // Edit log and cache invalidation deliberately use the LOCAL db/table names for
        // follower-replay consistency; only the connector-bound names are remote-resolved.
        Env.getCurrentEnv().getEditLog().logDropTable(new DropInfo(getName(), dbName, tableName));
        getDbForReplay(dbName).ifPresent(d -> d.unregisterTable(tableName));
        LOG.info("finished to drop table {}.{}.{}", getName(), dbName, tableName);
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

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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.catalog.info.CreateOrReplaceBranchInfo;
import org.apache.doris.catalog.info.CreateOrReplaceTagInfo;
import org.apache.doris.catalog.info.DropBranchInfo;
import org.apache.doris.catalog.info.DropTagInfo;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.DefaultConnectorValidationContext;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.PartitionFieldChange;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.ddl.CreateTableInfoToConnectorRequestConverter;
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
        return ConnectorFactory.createConnector(catalogType,
                catalogProperty.getProperties(),
                new DefaultConnectorContext(name, id, this::getExecutionAuthenticator,
                        () -> catalogProperty.getStoragePropertiesMap(),
                        catalogProperty::getEffectiveRawStorageProperties));
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

    /**
     * {@code REFRESH CATALOG} must also drop the connector's OWN caches (e.g. the iceberg latest-snapshot
     * cache, default TTL 24h, and the manifest cache). The base {@link #onRefreshCache} only invalidates the
     * registered engine caches via the route resolver — which for a plugin catalog resolves to the schema-only
     * {@code ENGINE_DEFAULT} bucket and never reaches the connector-owned caches. And {@code REFRESH CATALOG}
     * does NOT rebuild the connector (that only happens on {@code ADD}/{@code MODIFY CATALOG} via
     * {@link #resetToUninitialized}), so without this the connector keeps serving stale metadata until TTL.
     *
     * <p>Connector-agnostic: {@link Connector#invalidateAll()} is a generic SPI (no-op default; paimon clears
     * its own latest-snapshot cache too). Reads the {@code connector} field directly (no forced init, mirroring
     * {@link #overlayMetaCacheConfig}): an uninitialized catalog — or one whose connector was just nulled by
     * {@code resetToUninitialized}'s {@code onClose()} before this runs — has no connector caches to drop, and
     * the next access lazily rebuilds the connector with empty caches.
     */
    @Override
    public void onRefreshCache(boolean invalidCache) {
        super.onRefreshCache(invalidCache);
        if (invalidCache) {
            Connector localConnector = connector;
            if (localConnector != null) {
                localConnector.invalidateAll();
            }
        }
    }

    @Override
    protected List<String> listDatabaseNames() {
        try {
            ConnectorSession session = buildConnectorSession();
            return connector.getMetadata(session).listDatabaseNames(session);
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
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        List<String> tableNames = metadata.listTableNames(session, dbName);
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
     * Registers a newly-observed database into this catalog, driven by the metastore-event sync's
     * REGISTER_DATABASE change (via {@code CatalogMgr.registerExternalDatabaseFromEvent}). Pulled up from
     * {@code HMSExternalCatalog} so a flipped (generic) catalog no longer throws
     * {@code NotImplementedException} on a create/rename-database event. The body is fully generic
     * (buildDbForInit + metaCache, name-derived id) and mirrors the legacy HMS implementation.
     */
    @Override
    public void registerDatabase(long dbId, String dbName) {
        ExternalDatabase<? extends ExternalTable> db = buildDbForInit(dbName, null, dbId, logType, false);
        if (isInitialized()) {
            metaCache.updateCache(db.getRemoteName(), db.getFullName(), db,
                    Util.genIdByName(name, db.getFullName()));
        }
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
        // Drop any stale connector-owned cache entry for this name before the new table goes live
        // (belt-and-suspenders with the DROP path, which is the load-bearing invalidation for drop+recreate).
        // Connector-agnostic: invalidateTable is a no-op SPI default; hive/iceberg/paimon drop their own
        // per-table caches (metastore/file-listing, latest-snapshot pin). The table name is intentionally NOT
        // remote-resolved (a new table has no local->remote mapping — parity with the create request + editlog).
        connector.invalidateTable(db.getRemoteName(), createTableInfo.getTableName());
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
        try {
            connector.getMetadata(session).dropDatabase(session, db.getRemoteName(), ifExists, force);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        // Drop the connector's own caches for every table in this db so a subsequent same-name CREATE
        // DATABASE and the next reads go live rather than serving dropped tables up to the connector TTL.
        // Connector-agnostic (no-op SPI default); keyed by the REMOTE db name, mirroring
        // RefreshManager.refreshDbInternal. (createDb is intentionally NOT hooked: a brand-new db has no
        // table-keyed connector entries that this dropDb did not already clear.)
        connector.invalidateDb(db.getRemoteName());
        // Edit log + cache invalidation intentionally use the LOCAL name: followers replay the
        // persisted DropDbInfo and the on-FE cache is keyed by local name (follower-replay parity).
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
        // Route a DROP on a VIEW to dropView, mirroring legacy IcebergMetadataOps.dropTableImpl's
        // viewExists -> performDropView dispatch: a connector that exposes views keeps them in a separate
        // namespace, so getTableHandle/tableExists below is false for a view and the table-handle path
        // could never drop it. For view-less connectors viewExists defaults to false (no remote call), so
        // this routing is inert and the table path runs unchanged. The edit log + cache invalidation use
        // the LOCAL names (follower-replay parity), identical to the table path.
        if (metadata.viewExists(session, dorisTable.getRemoteDbName(), dorisTable.getRemoteName())) {
            try {
                metadata.dropView(session, dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
            } catch (DorisConnectorException e) {
                throw new DdlException(e.getMessage(), e);
            }
            // Uniform with the table branch: drop the connector's own caches for this name (harmless no-op
            // for a view, which carries no snapshot pin). Keyed by the REMOTE names.
            connector.invalidateTable(dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
            Env.getCurrentEnv().getEditLog().logDropTable(new DropInfo(getName(), dbName, tableName));
            getDbForReplay(dbName).ifPresent(d -> d.unregisterTable(tableName));
            LOG.info("finished to drop view {}.{}.{}", getName(), dbName, tableName);
            return;
        }
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
        // Drop the connector's own caches for this table (paimon/iceberg latest-snapshot pin, hive
        // metastore + file-listing) so a subsequent same-name CREATE and the next read go live rather than
        // serving the dropped table up to the connector TTL — the load-bearing fix for drop+recreate.
        // Connector-agnostic (no-op SPI default); keyed by the REMOTE db/table names the connector caches
        // under, mirroring RefreshManager.refreshTableInternal.
        connector.invalidateTable(dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
        // Edit log and cache invalidation deliberately use the LOCAL db/table names for
        // follower-replay consistency; only the connector-bound names are remote-resolved.
        Env.getCurrentEnv().getEditLog().logDropTable(new DropInfo(getName(), dbName, tableName));
        getDbForReplay(dbName).ifPresent(d -> d.unregisterTable(tableName));
        LOG.info("finished to drop table {}.{}.{}", getName(), dbName, tableName);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = resolveAlterHandle(dorisTable, session, metadata);
        try {
            metadata.renameTable(session, handle, newTableName);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalRename(dbName, oldTableName, newTableName);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = resolveAlterHandle(dorisTable, session, metadata);
        try {
            metadata.truncateTable(session, handle, partitions);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        long updateTime = System.currentTimeMillis();
        // Cache refresh + edit log use the LOCAL db/table names for follower-replay parity (only the
        // connector-bound handle is remote-resolved), mirroring base ExternalCatalog.truncateTable.
        Env.getCurrentEnv().getRefreshManager().refreshTableInternal(db, dorisTable, updateTime);
        Env.getCurrentEnv().getEditLog().logTruncateTable(
                new TruncateTableInfo(getName(), dbName, tableName, partitions, updateTime));
        LOG.info("finished to truncate table {}.{}.{}", getName(), dbName, tableName);
    }

    /**
     * Refreshes the local table cache on edit-log replay of a connector-driven truncate. The base
     * {@link ExternalCatalog#replayTruncateTable} delegates to {@code metadataOps.afterTruncateTable}, which is a
     * no-op for PluginDriven ({@code metadataOps == null}); this override re-resolves the cached table by the
     * replayed LOCAL names and runs {@code refreshTableInternal} (the same effect the master path applied),
     * mirroring legacy {@code HiveMetadataOps.afterTruncateTable}.
     */
    @Override
    public void replayTruncateTable(TruncateTableInfo info) {
        getDbForReplay(info.getDb()).ifPresent(db ->
                db.getTableForReplay(info.getTable()).ifPresent(tbl ->
                        Env.getCurrentEnv().getRefreshManager().refreshTableInternal(db, tbl, info.getUpdateTime())));
    }

    /**
     * Propagates the coordinator {@link #dropTable} hook's connector-cache invalidation to followers/observers
     * on edit-log replay. The base {@link ExternalCatalog#replayDropTable} plugin branch only touches the FE
     * name cache ({@code unregisterTable}); without this, a follower that had queried a paimon/iceberg table
     * keeps its latest-snapshot pin (and paimon's schema memo) for the dropped name until the 24h access-TTL —
     * the coordinator-only half of the drop+recreate fix. Resolves the REMOTE names from the still-cached
     * table BEFORE the base unregisters it, keyed exactly like the coordinator (mirrors
     * {@code RefreshManager.replayRefreshTable → refreshTableInternal}'s connector hook).
     *
     * <p><b>Never force-initializes during replay:</b> {@code getConnector()} runs only inside the
     * {@code getDbForReplay}/{@code getTableForReplay} match, which is present only when this catalog is
     * already initialized on this FE (both return empty otherwise). A never-initialized catalog has no
     * connector cache to drop, so skipping it is correct — mirroring {@code HiveConnector.forEachBuiltSibling}
     * ("a never-built sibling has no cache") and preserving the base's no-force-init replay behavior.
     */
    @Override
    public void replayDropTable(String dbName, String tblName) {
        getDbForReplay(dbName).ifPresent(db ->
                db.getTableForReplay(tblName).ifPresent(tbl ->
                        getConnector().invalidateTable(db.getRemoteName(), tbl.getRemoteName())));
        super.replayDropTable(dbName, tblName);
    }

    /**
     * Replay analogue of the coordinator {@link #dropDb} hook's connector-cache invalidation — clears every
     * table's connector cache for the dropped database on followers/observers (the base
     * {@link ExternalCatalog#replayDropDb} plugin branch only unregisters the FE db). Resolves the REMOTE db
     * name BEFORE the base unregisters the database. See {@link #replayDropTable} for the no-force-init
     * rationale.
     */
    @Override
    public void replayDropDb(String dbName) {
        getDbForReplay(dbName).ifPresent(db -> getConnector().invalidateDb(db.getRemoteName()));
        super.replayDropDb(dbName);
    }

    /**
     * Replay analogue of the coordinator {@link #createTable} hook's belt-and-suspenders connector-cache
     * invalidation (uniform with the drop path). The table name is NOT remote-resolved — parity with the
     * coordinator, where a brand-new table has no local→remote mapping. See {@link #replayDropTable} for the
     * no-force-init rationale.
     */
    @Override
    public void replayCreateTable(String dbName, String tblName) {
        super.replayCreateTable(dbName, tblName);
        getDbForReplay(dbName).ifPresent(db -> getConnector().invalidateTable(db.getRemoteName(), tblName));
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
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.addColumn(session, handle, ConnectorColumnConverter.toConnectorColumn(column),
                    toConnectorPosition(position));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void addColumns(TableIf dorisTable, List<Column> columns) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.addColumns(session, handle, ConnectorColumnConverter.toConnectorColumns(columns));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void dropColumn(TableIf dorisTable, String columnName) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.dropColumn(session, handle, columnName);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void renameColumn(TableIf dorisTable, String oldName, String newName) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.renameColumn(session, handle, oldName, newName);
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void modifyColumn(TableIf dorisTable, Column column, ColumnPosition position) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.modifyColumn(session, handle, ConnectorColumnConverter.toConnectorColumn(column),
                    toConnectorPosition(position));
        } catch (DorisConnectorException e) {
            throw new DdlException(e.getMessage(), e);
        }
        afterExternalDdl(externalTable, updateTime);
    }

    @Override
    public void reorderColumns(TableIf dorisTable, List<String> newOrder) throws UserException {
        ExternalTable externalTable = checkExternalTable(dorisTable);
        ConnectorSession session = buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = resolveAlterHandle(externalTable, session, metadata);
        long updateTime = System.currentTimeMillis();
        try {
            metadata.reorderColumns(session, handle, newOrder);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
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
        ConnectorMetadata metadata = connector.getMetadata(session);
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

    /** Neutralizes the fe-catalog {@link ColumnPosition} to the SPI {@link ConnectorColumnPosition}; null-safe. */
    private static ConnectorColumnPosition toConnectorPosition(ColumnPosition position) {
        if (position == null) {
            return null;
        }
        return position.isFirst()
                ? ConnectorColumnPosition.FIRST
                : ConnectorColumnPosition.after(position.getLastCol());
    }

    /**
     * Replays the base {@link ExternalCatalog} per-op bookkeeping for a connector-driven schema change.
     *
     * <p>The base column ops only emit the editlog ({@code logRefreshExternalTable}); the actual cache
     * invalidation is delegated INTO {@code metadataOps.refreshTable -> RefreshManager.refreshTableInternal}.
     * Since PluginDrivenExternalCatalog has no {@code metadataOps}, this helper does BOTH explicitly: the
     * {@code createForRefreshTable} editlog (LOCAL names, replay-neutral) and a {@code refreshTableInternal}
     * (re-resolving the local cached table by its REMOTE names, mirroring legacy
     * {@code IcebergMetadataOps.refreshTable}). {@code refreshTableInternal} is the single source of truth for
     * the cache work ({@code unsetObjectCreated} + {@code setUpdateTime} + {@code invalidateTableCache} + the
     * connector-side per-table cache drop), so it must NOT be re-inlined here.
     */
    protected void afterExternalDdl(ExternalTable externalTable, long updateTime) {
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(
                ExternalObjectLog.createForRefreshTable(getId(),
                        externalTable.getDbName(), externalTable.getName(), updateTime));
        getDbForReplay(externalTable.getRemoteDbName()).ifPresent(db ->
                db.getTableForReplay(externalTable.getRemoteName()).ifPresent(tbl ->
                        Env.getCurrentEnv().getRefreshManager().refreshTableInternal(db, tbl, updateTime)));
    }

    /**
     * Replays the base {@link ExternalCatalog#renameTable} bookkeeping for a connector-driven rename, since
     * PluginDriven has no {@code metadataOps}: the table-name cache fix ({@code unregisterTable(old)} +
     * {@code resetMetaCacheNames()}, mirroring legacy {@code IcebergMetadataOps.afterRenameTable}), the
     * {@code constraintManager} rename, and the {@code createForRenameTable} editlog (whose replay,
     * {@code RefreshManager.replayRefreshTable}, is already metadataOps-neutral). All use LOCAL names, matching
     * the base op + the editlog payload, so followers replay consistently. Order mirrors the base op
     * (cache &rarr; constraint &rarr; editlog).
     */
    protected void afterExternalRename(String dbName, String oldTableName, String newTableName) {
        getDbForReplay(dbName).ifPresent(db -> {
            db.unregisterTable(oldTableName);
            db.resetMetaCacheNames();
        });
        Env.getCurrentEnv().getConstraintManager().renameTable(
                new TableNameInfo(getName(), dbName, oldTableName),
                new TableNameInfo(getName(), dbName, newTableName));
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(
                ExternalObjectLog.createForRenameTable(getId(), dbName, oldTableName, newTableName));
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
     * Whether the backing connector projects the querying user's delegated credential onto the remote
     * metadata source ({@link ConnectorCapability#SUPPORTS_USER_SESSION}), gating both the FE credential
     * injection above and the shared-cache bypass ({@link #shouldBypassTableNameCache}).
     */
    private boolean supportsUserSession() {
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

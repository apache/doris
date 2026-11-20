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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.hudi.HudiExternalMetaCache;
import org.apache.doris.datasource.iceberg.IcebergCatalogResourceTracker;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.datasource.property.metastore.AbstractHiveProperties;
import org.apache.doris.datasource.property.metastore.HMSBaseProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemProvider;
import org.apache.doris.fs.FileSystemProviderImpl;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.transaction.TransactionManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * External catalog for hive metastore compatible data sources.
 */
public class HMSExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(HMSExternalCatalog.class);

    public static final String FILE_LISTING_CACHE_ENTRY_MAX_BYTES = "hive.file_listing_cache_entry_max_bytes";
    public static final String FILE_LISTING_CACHE_PENDING_MAX_BYTES = "hive.file_listing_cache_pending_max_bytes";

    public static final String FILE_META_CACHE_TTL_SECOND = "file.meta.cache.ttl-second";
    public static final String PARTITION_CACHE_TTL_SECOND = "partition.cache.ttl-second";
    public static final String HIVE_STAGING_DIR = "hive.staging_dir";
    public static final String DEFAULT_STAGING_BASE_DIR = "/tmp/.doris_staging";
    // broker name for file split and query scan.
    public static final String BIND_BROKER_NAME = "broker.name";
    // Default is false, if set to true, will get table schema from "remoteTable" instead of from hive metastore.
    // This is because for some forward compatibility issue of hive metastore, there maybe
    // "storage schema reading not support" error being thrown.
    // set this to true can avoid this error.
    // But notice that if set to true, the default value of column will be ignored because we cannot get default value
    // from remoteTable object.
    public static final String GET_SCHEMA_FROM_TABLE = "get_schema_from_table";

    /**
     * Every catalog property the HMS client may take its thrift endpoint from: hive.metastore.uris, its
     * documented alias uri, and Hive 3's metastore.thrift.uris, which the client's MetastoreConf reads before
     * hive.metastore.uris. {@link HMSBaseProperties} copies only hive.-prefixed catalog properties into the
     * client's HiveConf, so the last key does not reach the client today; it is gated anyway so the gate stays
     * correct if that copy widens. The CREATE / ALTER CATALOG SSRF gate probes every endpoint under these keys.
     */
    private static final List<String> SSRF_ENDPOINT_KEYS = ImmutableList.of(
            HMSBaseProperties.HIVE_METASTORE_URIS, "uri", "metastore.thrift.uris");

    private static final int FILE_SYSTEM_EXECUTOR_THREAD_NUM = 16;
    private ThreadPoolExecutor fileSystemExecutor;

    //for "type" = "hms" , but is iceberg table.
    private IcebergMetadataOps icebergMetadataOps;
    private IcebergCatalogResourceTracker icebergResourceTracker = new IcebergCatalogResourceTracker();

    private volatile AbstractHiveProperties hmsProperties;
    private AtomicLong runtimeGeneration = new AtomicLong();

    private transient volatile String eventIncludeTableList;
    private transient volatile Map<String, List<String>> eventIncludeTableMap;

    public long getRuntimeGeneration() {
        return runtimeGeneration.get();
    }

    /** Captures the authentication context and generation used by one Hudi scan. */
    public HudiScanRuntimeContext getHudiScanRuntimeContext() {
        ExternalMetaCacheMgr cacheMgr = Env.getCurrentEnv().getExtMetaCacheMgr();
        return cacheMgr.withCatalogLifecycleLock(getId(), () -> {
            long generation;
            ExecutionAuthenticator authenticator;
            synchronized (this) {
                makeSureInitialized();
                generation = runtimeGeneration.get();
                authenticator = executionAuthenticator;
            }
            HudiExternalMetaCache hudiCache = cacheMgr.hudi(getId());
            return new HudiScanRuntimeContext(generation, authenticator,
                    hudiCache.captureFsViewGeneration(getId()));
        });
    }

    @Override
    public synchronized void modifyCatalogProps(Map<String, String> props) {
        // Fence scans before the mutable CatalogProperty is changed. super invokes resetToUninitialized while
        // this monitor is still held, so no scan can capture the new properties with the old generation.
        runtimeGeneration.incrementAndGet();
        super.modifyCatalogProps(props);
    }

    /**
     * Lazily initializes HMSProperties from catalog properties.
     * This method is thread-safe using double-checked locking.
     * <p>
     * TODO: After all metastore integrations are completed,
     * consider moving this initialization logic into the superclass constructor
     * for unified management.
     * NOTE: Alter operations are temporarily not handled here.
     * We will consider a unified solution for alter support later,
     * as it's currently not feasible to handle it in a common/shared location.
     */
    public AbstractHiveProperties getHmsProperties() {
        makeSureInitialized();
        return hmsProperties;
    }

    @VisibleForTesting
    public HMSExternalCatalog() {
        catalogProperty = new CatalogProperty(null, null);
    }

    /**
     * Default constructor for HMSExternalCatalog.
     */
    public HMSExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                              String comment) {
        super(catalogId, name, InitCatalogLog.Type.HMS, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        icebergResourceTracker = new IcebergCatalogResourceTracker();
        runtimeGeneration = new AtomicLong();
    }

    @Override
    public void checkProperties() throws DdlException {
        checkProperties(catalogProperty);
    }

    @Override
    public void checkWhenCreating() throws DdlException {
        // Reject an intranet endpoint before any real connect in super (test_connection), regardless of that
        // flag (CREATE path; never reached on replay). On CREATE every property is newly supplied, so the
        // full map is both the candidate and the touched set.
        Map<String, String> properties = catalogProperty.getProperties();
        checkEndpointsAgainstSsrf(properties, properties);
        super.checkWhenCreating();
    }

    @Override
    public boolean validatePropertiesBeforeUpdate(
            Map<String, String> currentProperties, Map<String, String> updatedProperties) throws DdlException {
        Map<String, String> candidateProperties = currentProperties == null
                ? new HashMap<>() : new HashMap<>(currentProperties);
        candidateProperties.putAll(updatedProperties);
        checkProperties(new CatalogProperty(null, candidateProperties));
        return true;
    }

    @Override
    public void validatePropertiesBeforeUpdateUnlocked(
            Map<String, String> currentProperties, Map<String, String> updatedProperties) throws DdlException {
        Map<String, String> candidateProperties = currentProperties == null
                ? new HashMap<>() : new HashMap<>(currentProperties);
        candidateProperties.putAll(updatedProperties);
        // The probe connects to the endpoint, so it runs here, before CatalogMgr takes its write lock, rather
        // than in validatePropertiesBeforeUpdate.
        checkEndpointsAgainstSsrf(candidateProperties, updatedProperties);
    }

    /**
     * Applies the operator's SSRF policy (the third-party {@link SecurityChecker} SDK) to every metastore
     * endpoint these properties would make the FE connect to, rejecting an intranet target. It runs on CREATE
     * (through {@link #checkWhenCreating()}, before the catalog is registered) and on the interactive ALTER path
     * (through {@link #validatePropertiesBeforeUpdateUnlocked}, before CatalogMgr takes its write lock) but never
     * on replay, and it is a no-op unless the operator installed an SSRF checker
     * ({@code security_checker_class_name}).
     *
     * <p>The judgment itself is delegated to the SDK — see {@link SecurityChecker#checkEndpointForSsrf}.
     * This is a point-in-time check of the endpoint supplied at CREATE/ALTER; it does not guard a later
     * runtime connection (DNS rebinding, lazy metadata access) — that is a deliberate scope limit.
     *
     * <p>An endpoint must be given literally. The HMS client reads it through Hadoop's {@code Configuration},
     * which expands a {@code ${...}} placeholder (another property, {@code ${env.X}}, a JVM system property)
     * before it parses the endpoint, so the probe would judge a different destination than the one the client
     * connects to.
     */
    private void checkEndpointsAgainstSsrf(Map<String, String> candidate, Map<String, String> updatedProperties)
            throws DdlException {
        SecurityChecker checker = SecurityChecker.getInstance();
        if (!checker.isEnabled()) {
            return;
        }
        // Checked before the touched guard below: a placeholder value was never probed, and an ALTER that
        // changes only the property it refers to repoints the catalog without touching an endpoint key.
        for (String key : SSRF_ENDPOINT_KEYS) {
            String rawValue = candidate.get(key);
            if (rawValue != null && rawValue.contains("${")) {
                throw new DdlException("Property '" + key + "' of catalog '" + name + "' must give its endpoint"
                        + " literally while an SSRF checker is configured: a ${...} placeholder is expanded"
                        + " after this check, so its destination cannot be validated");
            }
        }
        // Only an ALTER that touches an endpoint key can repoint the catalog at a new host; a stored literal
        // value was gated at its own CREATE/ALTER time. On CREATE the full property map is passed as both
        // arguments, so every supplied endpoint counts as touched and is checked.
        boolean touched = SSRF_ENDPOINT_KEYS.stream().anyMatch(updatedProperties::containsKey);
        if (!touched) {
            return;
        }
        for (String key : SSRF_ENDPOINT_KEYS) {
            String rawValue = candidate.get(key);
            if (rawValue == null || rawValue.trim().isEmpty()) {
                continue;
            }
            // A value may list several comma-separated endpoints (e.g. HA hive.metastore.uris).
            for (String endpoint : rawValue.split(",")) {
                HostAndPort hostAndPort = parseEndpoint(endpoint.trim());
                if (hostAndPort == null) {
                    continue;
                }
                try {
                    checker.checkEndpointForSsrf(hostAndPort.getHost(), hostAndPort.getPort());
                } catch (Exception e) {
                    throw new DdlException(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Parse one endpoint — {@code scheme://host:port} (e.g. {@code thrift://host:9083}) or a bare
     * {@code host:port} — into host and port for the SSRF probe. Returns null when no concrete host:port
     * can be extracted (nothing to probe); {@link HostAndPort} handles IPv6 literals in brackets natively.
     */
    static HostAndPort parseEndpoint(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            if (value.contains("://")) {
                // Match the HMS client's URI semantics: query, fragment and userinfo do not name
                // the socket destination. HostAndPort.fromParts also strips IPv6 brackets.
                URI uri = new URI(value);
                if (uri.getHost() == null || uri.getPort() < 0) {
                    return null;
                }
                return HostAndPort.fromParts(uri.getHost(), uri.getPort());
            }
            String authority = value;
            int slashIdx = authority.indexOf('/');
            if (slashIdx >= 0) {
                authority = authority.substring(0, slashIdx);
            }
            HostAndPort hostAndPort = HostAndPort.fromString(authority);
            if (!hostAndPort.hasPort() || hostAndPort.getHost().isEmpty()) {
                return null;
            }
            return hostAndPort;
        } catch (URISyntaxException | IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    protected void checkProperties(CatalogProperty property) throws DdlException {
        super.checkProperties(property);
        String hiveParquetTimeZone = property.getOrDefault(
                FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, null);
        if (hiveParquetTimeZone != null) {
            // Validate before publishing catalog properties because scan planning parses the same value later.
            FileFormatUtils.parseHiveParquetTimeZone(hiveParquetTimeZone);
        }
        for (String key : java.util.Arrays.asList(
                FILE_LISTING_CACHE_ENTRY_MAX_BYTES, FILE_LISTING_CACHE_PENDING_MAX_BYTES)) {
            String value = property.getOrDefault(key, null);
            if (value != null) {
                org.apache.doris.datasource.metacache.CacheSpec.checkLongProperty(value, 0L, key);
            }
        }
        // check file.meta.cache.ttl-second parameter
        String fileMetaCacheTtlSecond = property.getOrDefault(FILE_META_CACHE_TTL_SECOND, null);
        if (Objects.nonNull(fileMetaCacheTtlSecond) && NumberUtils.toInt(fileMetaCacheTtlSecond, CACHE_NO_TTL)
                < CACHE_TTL_DISABLE_CACHE) {
            throw new DdlException(
                    "The parameter " + FILE_META_CACHE_TTL_SECOND + " is wrong, value is " + fileMetaCacheTtlSecond);
        }

        // check partition.cache.ttl-second parameter
        String partitionCacheTtlSecond = property.getOrDefault(PARTITION_CACHE_TTL_SECOND, null);
        if (Objects.nonNull(partitionCacheTtlSecond) && NumberUtils.toInt(partitionCacheTtlSecond, CACHE_NO_TTL)
                < CACHE_TTL_DISABLE_CACHE) {
            throw new DdlException(
                    "The parameter " + PARTITION_CACHE_TTL_SECOND + " is wrong, value is " + partitionCacheTtlSecond);
        }
        property.checkMetaStoreAndStorageProperties(AbstractHiveProperties.class);
    }

    @Override
    protected synchronized void initPreExecutionAuthenticator() {
        if (executionAuthenticator == null) {
            executionAuthenticator = hmsProperties.getExecutionAuthenticator();
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
        this.hmsProperties = (AbstractHiveProperties) catalogProperty.getMetastoreProperties();
        initPreExecutionAuthenticator();
        HiveMetadataOps hiveOps = ExternalMetadataOperations.newHiveMetadataOps(hmsProperties.getHiveConf(), this);
        threadPoolWithPreAuth = ThreadPoolManager.newDaemonFixedThreadPoolWithPreAuth(
                ICEBERG_CATALOG_EXECUTOR_THREAD_NUM,
                Integer.MAX_VALUE,
                String.format("hms_iceberg_catalog_%s_executor_pool", name),
                true,
                executionAuthenticator);
        FileSystemProvider fileSystemProvider = new FileSystemProviderImpl(Env.getCurrentEnv().getExtMetaCacheMgr(),
                this.catalogProperty.getStoragePropertiesMap());
        this.fileSystemExecutor = ThreadPoolManager.newDaemonFixedThreadPool(FILE_SYSTEM_EXECUTOR_THREAD_NUM,
                Integer.MAX_VALUE, String.format("hms_committer_%s_file_system_executor_pool", name), true);
        transactionManager = TransactionManagerFactory.createHiveTransactionManager(hiveOps, fileSystemProvider,
                fileSystemExecutor);
        metadataOps = hiveOps;
    }

    @Override
    public void onClose() {
        // Retire the Iceberg cache group before synchronized cleanup to preserve
        // HMS's lifecycle-stripe -> catalog-monitor lock ordering.
        // Cached HMS-Iceberg table values still retain this tracker; retiring the
        // cache group first releases those references so the teardown callback
        // (metadata ops / catalog / executor close) runs outside the cache-removal
        // lock stack.
        ExternalMetaCacheMgr cacheMgr = Env.getCurrentEnv().getExtMetaCacheMgr();
        cacheMgr.removeCatalogByEngine(getId(), IcebergExternalMetaCache.ENGINE);

        synchronized (this) {
            ThreadPoolExecutor retiredExecutor = threadPoolWithPreAuth;
            threadPoolWithPreAuth = null;
            IcebergMetadataOps retiredIcebergMetadataOps = icebergMetadataOps;
            Catalog retiredIcebergCatalog = retiredIcebergMetadataOps == null
                    ? null : retiredIcebergMetadataOps.getCatalog();
            icebergMetadataOps = null;
            super.onClose();
            if (null != fileSystemExecutor) {
                ThreadPoolManager.shutdownExecutorService(fileSystemExecutor);
            }
            if (null != metadataOps) {
                metadataOps.close();
                metadataOps = null;
            }
            icebergResourceTracker.retireCurrent(() -> {
                if (retiredIcebergMetadataOps != null) {
                    retiredIcebergMetadataOps.close();
                }
                if (retiredIcebergCatalog instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) retiredIcebergCatalog).close();
                    } catch (Exception e) {
                        LOG.warn("Failed to close HMS Iceberg catalog: {}", getName(), e);
                    }
                }
                if (retiredExecutor != null) {
                    ThreadPoolManager.shutdownExecutorService(retiredExecutor);
                }
            });
        }
    }

    @Override
    protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
        return metadataOps.listTableNames(ClusterNamespace.getNameFromFullName(dbName));
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return metadataOps.tableExist(ClusterNamespace.getNameFromFullName(dbName), tblName);
    }

    @Override
    public boolean tableExistInLocal(String dbName, String tblName) {
        makeSureInitialized();
        HMSExternalDatabase hmsExternalDatabase = (HMSExternalDatabase) getDbNullable(dbName);
        if (hmsExternalDatabase == null) {
            return false;
        }
        return hmsExternalDatabase.getTable(ClusterNamespace.getNameFromFullName(tblName)).isPresent();
    }

    public HMSCachedClient getClient() {
        makeSureInitialized();
        return ((HiveMetadataOps) metadataOps).getClient();
    }

    @Override
    public void registerDatabase(long dbId, String dbName) {
        registerDatabaseFromEvent(dbId, dbName, acquireMetadataLoadEpoch());
    }

    public boolean registerDatabaseFromEvent(long dbId, String dbName, long metadataLoadEpoch) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("create database [{}]", dbName);
        }

        // The event caller passes the original remote spelling used by database-list filters.
        if (isDatabaseEventTargetExcluded(dbName)) {
            return true;
        }

        ExternalDatabase<? extends ExternalTable> db = buildDbForInit(dbName, null, dbId, logType, false);
        if (isInitialized()) {
            return metaCache.updateCache(db.getRemoteName(), db.getFullName(), db,
                    Util.genIdByName(name, db.getFullName()), metadataLoadEpoch);
        }
        return false;
    }

    /** HMS notifications are unfiltered; excluded targets must not change this catalog's caches. */
    public boolean isDatabaseEventTargetExcluded(String dbName) {
        return !isDatabaseAllowedByFilter(dbName);
    }

    public boolean isPartitionEventTargetExcluded(String dbName, String tableName) {
        if (isDatabaseEventTargetExcluded(dbName)) {
            return true;
        }
        List<String> includedTables = getEventIncludeTableMap().get(dbName);
        return includedTables != null && !includedTables.isEmpty()
                && includedTables.stream().noneMatch(name -> name.equalsIgnoreCase(tableName));
    }

    private Map<String, List<String>> getEventIncludeTableMap() {
        String configuredList = catalogProperty.getOrDefault(INCLUDE_TABLE_LIST, "");
        Map<String, List<String>> snapshot = eventIncludeTableMap;
        if (snapshot == null || !configuredList.equals(eventIncludeTableList)) {
            synchronized (this) {
                configuredList = catalogProperty.getOrDefault(INCLUDE_TABLE_LIST, "");
                snapshot = eventIncludeTableMap;
                if (snapshot == null || !configuredList.equals(eventIncludeTableList)) {
                    snapshot = getIncludeTableMap();
                    eventIncludeTableMap = snapshot;
                    eventIncludeTableList = configuredList;
                }
            }
        }
        return snapshot;
    }

    @Override
    public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        try {
            super.notifyPropertiesUpdated(updatedProps);
        } finally {
            // The committed ALTER already published the properties; the engine-specific groups below
            // must still be retired when the generic reset cleanup throws.
            String fileMetaCacheTtl = updatedProps.getOrDefault(FILE_META_CACHE_TTL_SECOND, null);
            String partitionCacheTtl = updatedProps.getOrDefault(PARTITION_CACHE_TTL_SECOND, null);
            if (Objects.nonNull(fileMetaCacheTtl) || Objects.nonNull(partitionCacheTtl)
                    || updatedProps.containsKey(FILE_LISTING_CACHE_ENTRY_MAX_BYTES)
                    || updatedProps.containsKey(FILE_LISTING_CACHE_PENDING_MAX_BYTES)) {
                Env.getCurrentEnv().getExtMetaCacheMgr().removeCatalogByEngine(getId(), HiveExternalMetaCache.ENGINE);
            }
            if (updatedProps.keySet().stream()
                    .anyMatch(key -> CacheSpec.isMetaCacheKeyForEngine(key, HudiExternalMetaCache.ENGINE))) {
                Env.getCurrentEnv().getExtMetaCacheMgr()
                        .removeCatalogByEngine(getId(), HudiExternalMetaCache.ENGINE);
            }
            if (updatedProps.keySet().stream()
                    .anyMatch(key -> CacheSpec.isMetaCacheKeyForEngine(key, IcebergExternalMetaCache.ENGINE))) {
                Env.getCurrentEnv().getExtMetaCacheMgr()
                        .removeCatalogByEngine(getId(), IcebergExternalMetaCache.ENGINE);
            }
        }
    }

    @Override
    public void setDefaultPropsIfMissing(boolean isReplay) {
        super.setDefaultPropsIfMissing(isReplay);
        if (ifNotSetFallbackToSimpleAuth()) {
            // always allow fallback to simple auth, so to support both kerberos and simple auth
            catalogProperty.addProperty(DFSFileSystem.PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH, "true");
        }
    }

    public synchronized IcebergMetadataOps getIcebergMetadataOps() {
        makeSureInitialized();
        if (icebergMetadataOps == null) {
            HiveCatalog icebergHiveCatalog = IcebergUtils.createIcebergHiveCatalog(this, getName());
            icebergMetadataOps = ExternalMetadataOperations.newIcebergMetadataOps(this, icebergHiveCatalog);
        }
        return icebergMetadataOps;
    }

    /** Retains the exact HMS Iceberg runtime while a direct catalog operation is in progress. */
    public synchronized IcebergCatalogResourceTracker.LoadGuard beginIcebergCatalogOperation(
            IcebergMetadataOps expectedOps) {
        makeSureInitialized();
        if (icebergMetadataOps != expectedOps) {
            throw new IllegalStateException("Iceberg catalog runtime changed before the operation started");
        }
        return icebergResourceTracker.beginOperation();
    }

    /** Resolves a database only while the expected HMS Iceberg runtime is still current. */
    public ExternalDatabase<? extends ExternalTable> getDbForIcebergCatalogOperation(
            IcebergMetadataOps expectedOps, String dbName) {
        synchronized (this) {
            if (icebergMetadataOps != expectedOps) {
                throw new IllegalStateException("Iceberg catalog runtime changed before database resolution");
            }
        }
        // Match the parent-key -> catalog order used by loading and retained lookup repair.
        ExternalDatabase<? extends ExternalTable> database = getDbNullable(dbName);
        synchronized (this) {
            if (icebergMetadataOps != expectedOps) {
                throw new IllegalStateException("Iceberg catalog runtime changed during database resolution");
            }
        }
        return database;
    }

    /** Retains the exact HMS Iceberg runtime while a table cache generation is being loaded or borrowed. */
    public synchronized IcebergTableLoadContext beginIcebergTableLoad() {
        makeSureInitialized();
        IcebergMetadataOps ops = getIcebergMetadataOps();
        return new IcebergTableLoadContext(ops, threadPoolWithPreAuth, executionAuthenticator,
                catalogProperty.getMetastoreProperties(),
                new HashMap<>(catalogProperty.getStoragePropertiesMap()),
                catalogProperty.getEnableMappingVarbinary(), catalogProperty.getEnableMappingTimestampTz(),
                icebergResourceTracker.beginLoad());
    }

    @Override
    public void resetToUninitialized(boolean invalidCache) {
        ExternalMetaCacheMgr cacheMgr = Env.getCurrentEnv().getExtMetaCacheMgr();
        cacheMgr.withCatalogLifecycleLock(getId(), () -> {
            synchronized (this) {
                resetCatalogRuntime(cacheMgr, invalidCache);
            }
            return null;
        });
    }

    private void resetCatalogRuntime(ExternalMetaCacheMgr cacheMgr, boolean invalidCache) {
        runtimeGeneration.incrementAndGet();
        cacheMgr.removeCatalogByEngine(getId(), HiveExternalMetaCache.ENGINE);
        cacheMgr.removeCatalogByEngine(getId(), HudiExternalMetaCache.ENGINE);
        cacheMgr.removeCatalogByEngine(getId(), IcebergExternalMetaCache.ENGINE);
        super.resetToUninitialized(invalidCache);
    }

    public final class IcebergTableLoadContext implements AutoCloseable {
        private final IcebergMetadataOps ops;
        private final ThreadPoolExecutor executor;
        private final ExecutionAuthenticator authenticator;
        private final MetastoreProperties metastoreProperties;
        private final Map<StorageProperties.Type, StorageProperties> storageProperties;
        private final boolean enableMappingVarbinary;
        private final boolean enableMappingTimestampTz;
        private final IcebergCatalogResourceTracker.LoadGuard guard;

        private IcebergTableLoadContext(IcebergMetadataOps ops, ThreadPoolExecutor executor,
                ExecutionAuthenticator authenticator, MetastoreProperties metastoreProperties,
                Map<StorageProperties.Type, StorageProperties> storageProperties,
                boolean enableMappingVarbinary, boolean enableMappingTimestampTz,
                IcebergCatalogResourceTracker.LoadGuard guard) {
            this.ops = ops;
            this.executor = executor;
            this.authenticator = authenticator;
            this.metastoreProperties = metastoreProperties;
            this.storageProperties = storageProperties;
            this.enableMappingVarbinary = enableMappingVarbinary;
            this.enableMappingTimestampTz = enableMappingTimestampTz;
            this.guard = guard;
        }

        public IcebergMetadataOps getOps() {
            return ops;
        }

        public ThreadPoolExecutor getExecutor() {
            return executor;
        }

        public ExecutionAuthenticator getAuthenticator() {
            return authenticator;
        }

        public MetastoreProperties getMetastoreProperties() {
            return metastoreProperties;
        }

        public Map<StorageProperties.Type, StorageProperties> getStorageProperties() {
            return storageProperties;
        }

        public boolean isEnableMappingVarbinary() {
            return enableMappingVarbinary;
        }

        public boolean isEnableMappingTimestampTz() {
            return enableMappingTimestampTz;
        }

        public Table loadTable(String dbName, String tableName) throws Exception {
            return authenticator.execute(() -> ops.loadTableWithinCatalogGeneration(dbName, tableName));
        }

        public IcebergCatalogResourceTracker.ResourceLease promote() {
            return guard.promote();
        }

        @Override
        public void close() {
            guard.close();
        }
    }

    public static final class HudiScanRuntimeContext {
        private final long generation;
        private final ExecutionAuthenticator authenticator;
        private final HudiExternalMetaCache.FsViewGeneration fsViewGeneration;

        private HudiScanRuntimeContext(long generation, ExecutionAuthenticator authenticator,
                HudiExternalMetaCache.FsViewGeneration fsViewGeneration) {
            this.generation = generation;
            this.authenticator = authenticator;
            this.fsViewGeneration = fsViewGeneration;
        }

        public long getGeneration() {
            return generation;
        }

        public ExecutionAuthenticator getAuthenticator() {
            return authenticator;
        }

        public HudiExternalMetaCache.FsViewGeneration getFsViewGeneration() {
            return fsViewGeneration;
        }
    }
}

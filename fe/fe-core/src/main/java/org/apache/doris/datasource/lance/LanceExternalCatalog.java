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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.lance.index.LancePhysicalIndexEntry;
import org.apache.doris.datasource.lance.index.LanceShowIndexInfo;
import org.apache.doris.datasource.lance.job.LanceIndexDatasetLocator;
import org.apache.doris.datasource.lance.metadata.LanceMetadataLoader;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.lance.storage.LanceStorageOptions;
import org.apache.doris.datasource.property.metastore.AbstractLanceProperties;
import org.apache.doris.datasource.property.metastore.LanceFileSystemMetastoreProperties;
import org.apache.doris.datasource.property.metastore.LanceRestMetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListTablesRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Read-only Lance Directory or REST Namespace catalog. */
public class LanceExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(LanceExternalCatalog.class);

    public static final String LANCE_CATALOG_TYPE = AbstractLanceProperties.LANCE_CATALOG_TYPE;
    public static final String LANCE_FILESYSTEM = AbstractLanceProperties.LANCE_FILESYSTEM;
    public static final String LANCE_REST = AbstractLanceProperties.LANCE_REST;
    public static final String WAREHOUSE = LanceFileSystemMetastoreProperties.WAREHOUSE;
    public static final String NAMESPACE_PARENT = AbstractLanceProperties.NAMESPACE_PARENT;
    public static final String NAMESPACE_DELIMITER = AbstractLanceProperties.NAMESPACE_DELIMITER;
    public static final String ROOT_DATABASE = AbstractLanceProperties.ROOT_DATABASE;
    public static final String REST_URI = LanceRestMetastoreProperties.REST_URI;
    public static final String REST_SECURITY_TYPE = LanceRestMetastoreProperties.REST_SECURITY_TYPE;
    public static final String REST_BEARER_TOKEN = LanceRestMetastoreProperties.REST_BEARER_TOKEN;
    public static final String REST_API_KEY = LanceRestMetastoreProperties.REST_API_KEY;
    public static final String REST_HEADER_PREFIX = LanceRestMetastoreProperties.REST_HEADER_PREFIX;

    private transient LanceCatalogClient client;

    // Local admission epoch; accessed only under CatalogMgr's lock. It need not survive restart,
    // because no admission snapshot survives restart. Bump before even a tentative identity ALTER.
    private transient long indexTargetVersion;

    public long getIndexTargetVersion() {
        return indexTargetVersion;
    }

    public void advanceIndexTargetVersion() {
        indexTargetVersion++;
    }

    /**
     * Resolves the dataset the given (db, table) names currently point at, through the
     * same table-access resolution the readers use, and returns its durable locator form
     * ({@link LanceIndexDatasetLocator#normalize}).
     *
     * <p>This exists for SHOW-authorization revalidation of persisted Lance index jobs:
     * once a job reaches a terminal state and releases its guard, a legitimate catalog
     * ALTER can repoint the same db.table names at a different dataset, and the job must
     * stop being readable through table-level SHOW on the new target. Any failure - the
     * catalog is not initialized, the provider is unreachable, credentials expired, or
     * the names no longer resolve - yields {@code null}; callers must treat null as
     * "not resolved" (the orphan, ADMIN-only visibility rule), never as an authorization
     * grant.
     *
     * @return the normalized locator of the dataset the names currently point at, or
     *         null when it cannot be resolved
     */
    public String resolveCurrentIndexJobLocator(String dbName, String tableName) {
        try {
            return withClient(current -> current.resolveCurrentIndexJobLocator(dbName, tableName));
        } catch (Exception e) {
            LOG.warn("failed to resolve the current dataset locator of {}.{} in lance catalog {}",
                    dbName, tableName, getName(), e);
            return null;
        }
    }

    /**
     * Three-valued resolution of the dataset the given names point at, for callers
     * that take a durable action on the verdict and must not fold "verified gone"
     * and "could not tell" together. {@link #resolveCurrentIndexJobLocator} returns
     * null for both on purpose (SHOW's fail-closed rule folds them); this form
     * distinguishes them.
     *
     * <p>Callers pass their LOCAL spelling of the names — no remote-cased names are
     * required or assumed. Admission persists local database and table names, which
     * can be lowercase while the namespace stores {@code Foo.Bar}, and the local
     * lookup itself can miss transiently, so a case-sensitive resolve of the local
     * spelling could false-prove absence ({@code Foo.Bar} would read as gone and a
     * durable caller would act on it). Absence is therefore proven only through
     * full case-insensitive listings: {@code VERIFIED_ABSENT} requires the whole
     * database listing to miss the database, or the found remote database's table
     * listing to miss the table. A name found in a listing is resolved under its
     * REMOTE spelling to obtain the durable locator; any listing or resolve failure
     * — provider unreachable, credentials expired, a namespace answer that
     * contradicts its own listing — is logged (the sanitized client chain already
     * masks locators and credentials) and reported as
     * {@link LanceIndexDatasetCheck.Outcome#UNRESOLVED}, so an outage can never be
     * read as "dataset gone".
     *
     * <p>The listings are paid only on the local-resolution-miss path: callers
     * reach this check after their own local db/table lookup already came back
     * empty, so the namespace round-trips buy the absence proof instead of adding
     * overhead to the resolvable common path.
     */
    public LanceIndexDatasetCheck checkIndexJobDataset(String dbName, String tableName) {
        try {
            String remoteDbName = findIgnoreCase(withClient(current -> current.listDatabaseNames()), dbName);
            if (remoteDbName == null) {
                return LanceIndexDatasetCheck.verifiedAbsent();
            }
            String remoteTableName = findIgnoreCase(
                    withClient(current -> current.listTableNames(remoteDbName)), tableName);
            if (remoteTableName == null) {
                return LanceIndexDatasetCheck.verifiedAbsent();
            }
            return LanceIndexDatasetCheck.present(
                    withClient(current -> current.resolveCurrentIndexJobLocator(remoteDbName, remoteTableName)));
        } catch (Exception e) {
            LOG.warn("failed to check the dataset of {}.{} in lance catalog {}", dbName, tableName, getName(), e);
            return LanceIndexDatasetCheck.unresolved();
        }
    }

    /** Returns the listed name that matches {@code target} ignoring case, or null when none does. */
    private static String findIgnoreCase(List<String> names, String target) {
        for (String name : names) {
            if (name.equalsIgnoreCase(target)) {
                return name;
            }
        }
        return null;
    }

    public LanceExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, InitCatalogLog.Type.LANCE, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        client = createClient();
    }

    @VisibleForTesting
    LanceCatalogClient createClient() {
        Map<String, String> namespaceOptions = Collections.emptyMap();
        List<String> secrets = catalogSecrets();
        try {
            AbstractLanceProperties properties = getLanceProperties();
            List<StorageProperties> storageProperties = catalogProperty.getOrderedStoragePropertiesList();
            namespaceOptions = LanceStorageOptions.fromDorisStorageProperties(
                    properties.getNamespaceStorageUri(), storageProperties);
            return LanceCatalogClient.create(properties, storageProperties, namespaceOptions, secrets);
        } catch (Exception e) {
            throw LanceErrorMessages.failure("Failed to initialize Lance catalog '" + getName() + "'",
                    e, null, namespaceOptions, secrets);
        }
    }

    private List<String> catalogSecrets() {
        return java.util.Arrays.asList(catalogProperty.getOrDefault(REST_BEARER_TOKEN, ""),
                catalogProperty.getOrDefault(REST_API_KEY, ""));
    }

    @Override
    public void checkWhenCreating() throws DdlException {
        checkProperties();
        boolean testConnection = Boolean.parseBoolean(
                catalogProperty.getOrDefault(TEST_CONNECTION, String.valueOf(DEFAULT_TEST_CONNECTION)));
        if (!testConnection) {
            return;
        }

        AbstractLanceProperties properties = getLanceProperties();
        Map<String, String> storageOptions = LanceStorageOptions.fromDorisStorageProperties(
                properties.getNamespaceStorageUri(),
                catalogProperty.getOrderedStoragePropertiesList());
        List<String> parent = LanceNamespaceName.parseParentNamespace(
                properties.getNamespaceParent(), properties.getNamespaceDelimiter());
        String type = properties.getLanceCatalogType();
        List<String> secrets = catalogSecrets();
        try (BufferAllocator testAllocator = new RootAllocator(LanceMetadataLoader.READ_ALLOCATOR_LIMIT)) {
            LanceNamespace testNamespace = properties.createNamespace(testAllocator, storageOptions);
            try {
                testNamespace.listTables(new ListTablesRequest().id(parent).limit(1));
                testNamespace.listNamespaces(new ListNamespacesRequest().id(parent).limit(1));
            } finally {
                closeNamespace(testNamespace);
            }
        } catch (Exception e) {
            String message = LanceErrorMessages.sanitize(e, null, storageOptions, secrets);
            throw new DdlException("Lance " + type + " catalog connectivity test failed: "
                    + message, new RuntimeException(message));
        }
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        try {
            AbstractLanceProperties properties = getLanceProperties();
            LanceNamespaceName.parseParentNamespace(
                    properties.getNamespaceParent(), properties.getNamespaceDelimiter());
        } catch (IllegalArgumentException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    private AbstractLanceProperties getLanceProperties() {
        return (AbstractLanceProperties) catalogProperty.getMetastoreProperties();
    }

    /**
     * Returns whether this catalog is configured to use the Lance REST namespace.
     *
     * <p>This deliberately reads only the normalized catalog properties and does not initialize
     * the namespace. Callers can therefore reject unsupported REST operations before resolving a
     * database or table, both of which may trigger remote metadata requests.
     */
    public boolean isRestCatalogConfigured() {
        return LANCE_REST.equals(getLanceProperties().getLanceCatalogType());
    }

    @Override
    protected List<String> listDatabaseNames() {
        return withClient(current -> current.listDatabaseNames());
    }

    @Override
    protected List<String> listTableNamesFromRemote(SessionContext context, String dbName) {
        return withClient(current -> current.listTableNames(dbName));
    }

    @Override
    public boolean tableExist(SessionContext context, String dbName, String tableName) {
        return withClient(current -> current.tableExists(dbName, tableName));
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName) {
        return loadTableMetadata(dbName, tableName, Optional.empty());
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName, Optional<TableSnapshot> snapshot) {
        return loadTableMetadata(dbName, tableName, LanceRefSelector.snapshot(snapshot));
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName, LanceRefSelector selector) {
        return withClient(current -> current.loadTableMetadata(dbName, tableName, selector));
    }

    public LanceTableMetadata loadTableMetadataForSearch(String dbName, String tableName) {
        return withClient(current -> current.loadTableMetadataForSearch(dbName, tableName));
    }

    public LanceTableMetadata loadBasicTableMetadata(String dbName, String tableName) {
        return withClient(current -> current.loadBasicTableMetadata(dbName, tableName));
    }

    public Schema loadTableSchema(String dbName, String tableName) {
        return withClient(current -> current.loadTableSchema(dbName, tableName));
    }

    public List<LanceShowIndexInfo> loadTableIndexesForShow(String dbName, String tableName) throws AnalysisException {
        if (isRestCatalogConfigured()) {
            throw new AnalysisException("SHOW INDEX is not supported for Lance REST catalogs");
        }
        return withClient(current -> current.loadTableIndexesForShow(dbName, tableName));
    }

    public List<LancePhysicalIndexEntry> loadTableIndexEntries(String dbName, String tableName)
            throws AnalysisException {
        if (isRestCatalogConfigured()) {
            throw new AnalysisException("Lance index inspection is not supported for Lance REST catalogs");
        }
        return withClient(current -> current.loadTableIndexEntries(dbName, tableName));
    }

    /** Loads the pinned admission snapshot; REST catalogs remain unsupported. */
    public LanceIndexAdmissionSnapshot loadTableIndexAdmissionSnapshot(
            String dbName, String tableName) throws Exception {
        if (isRestCatalogConfigured()) {
            throw new AnalysisException("Lance index admission is not supported for Lance REST catalogs");
        }
        try {
            return withClient(current -> current.loadTableIndexAdmissionSnapshot(dbName, tableName));
        } catch (Exception e) {
            throw LanceErrorMessages.failure("Failed to load Lance index admission snapshot for "
                    + dbName + "." + tableName, e, null, Collections.emptyMap(), catalogSecrets());
        }
    }

    public String getLanceCatalogType() {
        return getLanceProperties().getLanceCatalogType();
    }

    private <T> T withClient(Function<LanceCatalogClient, T> operation) {
        try (LanceCatalogClient.Lease lease = acquireClient()) {
            return operation.apply(lease.client());
        }
    }

    @VisibleForTesting
    synchronized LanceCatalogClient.Lease acquireClient() {
        makeSureInitialized();
        if (client == null) {
            throw new IllegalStateException("Lance catalog resources have been closed");
        }
        return client.acquire();
    }

    @Override
    public void unregisterDatabase(String dbName) {
        // Dropping a namespace is a semantic change, unlike routine local DB-object eviction.
        invalidateTableAccessCache();
        super.unregisterDatabase(dbName);
    }

    public synchronized void invalidateTableAccessCache() {
        // Invalidation must not initialize JNI resources or wait for an in-flight namespace call.
        if (client != null) {
            client.invalidateTableAccessCache();
        }
    }

    @Override
    public void onRefreshCache(boolean invalidCache) {
        if (invalidCache) {
            refreshSessionCache();
        }
        super.onRefreshCache(invalidCache);
    }

    /**
     * REFRESH CATALOG invalidates the entire Session, including same-URI dataset replacements.
     * REFRESH TABLE invalidates Doris metadata and table access, but does not rotate the native Session.
     * New reads use a cold cache; in-flight reads retain their previous generation.
     */
    @VisibleForTesting
    void refreshSessionCache() {
        LanceCatalogClient previous;
        synchronized (this) {
            previous = client;
        }
        if (previous == null) {
            return;
        }
        // Namespace/Session construction and native cleanup must not block lease acquisition.
        LanceCatalogClient replacement = createClient();
        boolean published;
        synchronized (this) {
            // A concurrent refresh, ALTER or close may already have replaced this generation.
            published = client == previous;
            if (published) {
                client = replacement;
            }
        }
        if (published) {
            previous.close();
        } else {
            replacement.close();
        }
    }

    @Override
    public void onClose() {
        LanceCatalogClient previous;
        synchronized (this) {
            previous = client;
            client = null;
        }
        try {
            super.onClose();
        } finally {
            if (previous != null) {
                previous.close();
            }
        }
    }

    private static void closeNamespace(LanceNamespace namespaceToClose) {
        if (namespaceToClose instanceof AutoCloseable) {
            try {
                ((AutoCloseable) namespaceToClose).close();
            } catch (Exception ignored) {
                // Best effort during catalog close or failed initialization.
            }
        }
    }

}

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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.lance.index.LanceIndexInspection;
import org.apache.doris.datasource.lance.index.LanceIndexInspectionExecutor;
import org.apache.doris.datasource.lance.index.LancePhysicalIndexEntry;
import org.apache.doris.datasource.lance.index.LanceShowIndexInfo;
import org.apache.doris.datasource.lance.job.LanceIndexDatasetLocator;
import org.apache.doris.datasource.lance.metadata.LanceMetadataLoader;
import org.apache.doris.datasource.lance.metadata.LanceReadOptions;
import org.apache.doris.datasource.lance.metadata.LanceSnapshotResolver;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics.Stage;
import org.apache.doris.datasource.property.metastore.AbstractLanceProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiFunction;

/**
 * One catalog generation: Namespace access, snapshot reads and native resource lifetime.
 * Callers must hold a Lease, acquired while selecting the current generation in the catalog.
 */
final class LanceCatalogClient implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(LanceCatalogClient.class);
    private static final long METADATA_CACHE_SIZE_BYTES = 64L * 1024 * 1024;
    private static final long INDEX_CACHE_SIZE_BYTES = 128L * 1024 * 1024;

    private final LanceNamespace namespace;
    private final Session session;
    private final LanceNamespaceClient namespaceClient;
    private final Map<String, String> namespaceStorageOptions;
    private final BufferAllocator namespaceAllocator;
    private final List<String> catalogSecrets;
    private int activeOperations;
    private boolean retired;

    static LanceCatalogClient create(AbstractLanceProperties properties,
            List<StorageProperties> storageProperties, Map<String, String> namespaceOptions,
            List<String> catalogSecrets) throws DdlException {
        long limit = Config.lance_catalog_arrow_memory_limit_bytes;
        if (limit <= 0) {
            throw new IllegalArgumentException("lance_catalog_arrow_memory_limit_bytes must be positive");
        }
        BufferAllocator allocator = new RootAllocator(limit);
        LanceNamespace namespace = null;
        Session session = null;
        try {
            List<String> parent = LanceNamespaceName.parseParentNamespace(
                    properties.getNamespaceParent(), properties.getNamespaceDelimiter());
            namespace = properties.createNamespace(allocator, namespaceOptions);
            session = Session.builder().metadataCacheSizeBytes(METADATA_CACHE_SIZE_BYTES)
                    .indexCacheSizeBytes(INDEX_CACHE_SIZE_BYTES).build();
            return new LanceCatalogClient(namespace, allocator, session, properties.getLanceCatalogType(),
                    properties.getRootDatabase(), parent, storageProperties, namespaceOptions, catalogSecrets,
                    properties.getTableAccessCacheTtlSeconds());
        } catch (RuntimeException | Error e) {
            closeResource(namespace);
            closeResource(session);
            closeResource(allocator);
            throw e;
        }
    }

    LanceCatalogClient(LanceNamespace namespace, BufferAllocator allocator, Session session,
            String catalogType, String rootDatabase, List<String> parentNamespace,
            List<StorageProperties> storageProperties, Map<String, String> namespaceStorageOptions,
            List<String> catalogSecrets) {
        this(namespace, allocator, session, catalogType, rootDatabase, parentNamespace,
                storageProperties, namespaceStorageOptions, catalogSecrets,
                AbstractLanceProperties.DEFAULT_TABLE_ACCESS_CACHE_TTL_SECONDS);
    }

    LanceCatalogClient(LanceNamespace namespace, BufferAllocator allocator, Session session,
            String catalogType, String rootDatabase, List<String> parentNamespace,
            List<StorageProperties> storageProperties, Map<String, String> namespaceStorageOptions,
            List<String> catalogSecrets, int tableAccessCacheTtlSeconds) {
        this.catalogSecrets = Collections.unmodifiableList(new ArrayList<>(catalogSecrets));
        this.namespace = namespace;
        this.namespaceAllocator = allocator;
        this.session = session;
        this.namespaceClient = new LanceNamespaceClient(
                namespace, catalogType, rootDatabase, parentNamespace, storageProperties,
                tableAccessCacheTtlSeconds, Ticker.systemTicker(), System::currentTimeMillis);
        this.namespaceStorageOptions = Collections.unmodifiableMap(new HashMap<>(namespaceStorageOptions));
    }

    /** Pins this generation for one operation; the lock does not cover its SDK or JNI calls. */
    synchronized Lease acquire() {
        if (retired) {
            throw new IllegalStateException("Lance catalog resources have been closed");
        }
        activeOperations++;
        return new Lease(this);
    }

    /** Retires this generation immediately; its last active operation performs resource cleanup. */
    @Override
    public void close() {
        boolean release;
        synchronized (this) {
            if (retired) {
                return;
            }
            retired = true;
            release = activeOperations == 0;
        }
        if (release) {
            closeResources();
        }
    }

    private void release() {
        boolean release;
        synchronized (this) {
            activeOperations--;
            release = retired && activeOperations == 0;
        }
        if (release) {
            closeResources();
        }
    }

    private void closeResources() {
        closeResource(namespace);
        closeResource(session);
        closeResource(namespaceAllocator);
    }

    private static void closeResource(Object resource) {
        if (resource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) resource).close();
            } catch (Exception e) {
                // Provider exception messages may contain credentials.
                LOG.warn("Failed to close a Lance catalog resource ({})", resource.getClass().getSimpleName());
            }
        }
    }

    static final class Lease implements AutoCloseable {
        private final LanceCatalogClient client;
        private boolean closed;

        private Lease(LanceCatalogClient client) {
            this.client = client;
        }

        LanceCatalogClient client() {
            return client;
        }

        @Override
        public void close() {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
            }
            client.release();
        }
    }

    void invalidateTableAccessCache() {
        namespaceClient.invalidateTableAccessCache();
    }

    List<String> listDatabaseNames() {
        return namespaceClient.listDatabaseNames();
    }

    List<String> listTableNames(String dbName) {
        return namespaceClient.listTableNames(dbName);
    }

    boolean tableExists(String dbName, String tableName) {
        return namespaceClient.tableExists(dbName, tableName);
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName) {
        return loadTableMetadata(dbName, tableName, Optional.empty());
    }

    public LanceTableMetadata loadTableMetadataForSearch(String dbName, String tableName) {
        LanceTableMetadata metadata = loadQueryMetadata(dbName, tableName, Optional.empty(),
                LanceMetadataLoader.MetadataScope.WITH_INDEXES);
        if (!metadata.getIndexMetadataState().canPlanIndexSegments()) {
            throw new IllegalArgumentException("Lance SDK cannot provide field IDs required for search planning");
        }
        return metadata;
    }

    public LanceTableMetadata loadBasicTableMetadata(String dbName, String tableName) {
        return loadQueryMetadata(dbName, tableName, Optional.empty(), LanceMetadataLoader.MetadataScope.BASIC);
    }

    public Schema loadTableSchema(String dbName, String tableName) {
        return readTableSnapshot(dbName, tableName, Optional.empty(),
                (dataset, access, metrics) -> metrics.measure(Stage.SCHEMA, dataset::getSchema));
    }

    public LanceTableMetadata loadTableMetadata(String dbName, String tableName,
            Optional<TableSnapshot> tableSnapshot) {
        return loadQueryMetadata(dbName, tableName, tableSnapshot, LanceMetadataLoader.MetadataScope.WITH_INDEXES);
    }

    private LanceTableMetadata loadQueryMetadata(String dbName, String tableName,
            Optional<TableSnapshot> tableSnapshot, LanceMetadataLoader.MetadataScope mode) {
        return readTableSnapshot(dbName, tableName, tableSnapshot,
                (dataset, access, metrics) -> LanceMetadataLoader.read(dataset, access, mode, metrics));
    }

    /** Pins one resource generation, resolved table access, and the Dataset version for the whole read. */
    private <T> T readTableSnapshot(String dbName, String tableName, Optional<TableSnapshot> tableSnapshot,
            SnapshotReader<T> reader) {
        LanceTableAccess tableAccess = null;
        LanceMetadataMetrics metrics = LanceMetadataMetrics.startMetadataRead();
        try {
            T result;
            try (BufferAllocator allocator = namespaceAllocator.newChildAllocator(
                    "lance-metadata-read", 0, namespaceAllocator.getLimit())) {
                tableAccess = metrics.measure(Stage.TABLE_ACCESS,
                        () -> namespaceClient.resolveTableAccess(dbName, tableName));
                OptionalLong version = OptionalLong.empty();
                if (tableSnapshot.isPresent()) {
                    TableSnapshot snapshot = tableSnapshot.get();
                    if (snapshot.getType() == TableSnapshot.VersionType.VERSION) {
                        version = OptionalLong.of(LanceSnapshotResolver.parseVersion(snapshot.getValue()));
                    } else {
                        long timestamp = TimeUtils.timeStringToLong(snapshot.getValue(), TimeUtils.getTimeZone());
                        if (timestamp < 0) {
                            throw new IllegalArgumentException(
                                    "Cannot parse Lance FOR TIME AS OF value '" + snapshot.getValue() + "'");
                        }
                        try (Dataset latest = openDataset(allocator, tableAccess, OptionalLong.empty(), metrics)) {
                            version = OptionalLong.of(metrics.measure(Stage.VERSION_RESOLVE,
                                    () -> LanceSnapshotResolver.getVersionAtOrBefore(latest, timestamp)));
                        }
                    }
                }
                try (Dataset dataset = openDataset(allocator, tableAccess, version, metrics)) {
                    result = reader.read(dataset, tableAccess, metrics);
                }
            }
            metrics.succeeded();
            return result;
        } catch (Exception e) {
            throw LanceErrorMessages.failure("Failed to load Lance table metadata for " + dbName + "." + tableName, e,
                    tableAccess == null ? null : tableAccess.getDatasetUri(),
                    tableAccess == null ? namespaceStorageOptions : tableAccess.getStorageOptions(), catalogSecrets);
        } finally {
            metrics.close();
        }
    }

    private Dataset openDataset(BufferAllocator allocator, LanceTableAccess access, OptionalLong version,
            LanceMetadataMetrics metrics) {
        return metrics.measure(Stage.DATASET_OPEN, () -> Dataset.open().allocator(allocator).uri(access.getDatasetUri())
                .readOptions(LanceReadOptions.forSharedSession(access.getStorageOptions(), version, session)).build());
    }

    @FunctionalInterface
    private interface SnapshotReader<T> {
        T read(Dataset dataset, LanceTableAccess access, LanceMetadataMetrics metrics);
    }

    public List<LanceShowIndexInfo> loadTableIndexesForShow(
            String dbName, String tableName) {
        return inspectTableIndexes(dbName, tableName,
                (dataset, uri) -> LanceIndexInspection.readIndexesForShow(dataset));
    }

    public List<LancePhysicalIndexEntry> loadTableIndexEntries(
            String dbName, String tableName) {
        return inspectTableIndexes(dbName, tableName,
                (dataset, uri) -> LanceIndexInspection.readPhysicalEntries(dataset));
    }

    String resolveCurrentIndexJobLocator(String dbName, String tableName) {
        return LanceIndexDatasetLocator.normalize(
                namespaceClient.resolveTableAccessUncached(dbName, tableName).getDatasetUri());
    }

    public LanceIndexAdmissionSnapshot loadTableIndexAdmissionSnapshot(String dbName, String tableName) {
        return inspectTableIndexes(dbName, tableName, LanceIndexInspection::readAdmissionSnapshot);
    }

    private <T> T inspectTableIndexes(String dbName, String tableName, BiFunction<Dataset, String, T> inspection) {
        LanceTableAccess tableAccess = null;
        try {
            // The worker owns its Dataset and Session even if the caller releases its lease on timeout.
            // Index admission must verify the current target even while query access is cached.
            tableAccess = namespaceClient.resolveTableAccessUncached(dbName, tableName);
            LanceTableAccess access = tableAccess;
            return LanceIndexInspectionExecutor.execute(() -> {
                // The caller can time out while JNI is running; the worker must own resource cleanup.
                try (BufferAllocator allocator = new RootAllocator(LanceMetadataLoader.READ_ALLOCATOR_LIMIT);
                        Dataset dataset = Dataset.open().allocator(allocator).uri(access.getDatasetUri())
                                .readOptions(LanceReadOptions.forIndependentRead(
                                        access.getStorageOptions(), OptionalLong.empty()))
                                .build()) {
                    return inspection.apply(dataset, access.getDatasetUri());
                }
            });
        } catch (Exception e) {
            throw LanceErrorMessages.failure("Failed to load Lance index metadata for " + dbName + "." + tableName, e,
                    tableAccess == null ? null : tableAccess.getDatasetUri(),
                    tableAccess == null ? namespaceStorageOptions : tableAccess.getStorageOptions(), catalogSecrets);
        }
    }

}

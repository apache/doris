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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.lance.LanceMetadataMetrics.Operation;
import org.apache.doris.connector.lance.LanceMetadataMetrics.Stage;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.filesystem.properties.StorageProperties;

import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ReadOptions;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.TableVersionNotFoundException;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListTablesRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * One catalog's namespace access, snapshot reads and native resource lifetime.
 *
 * <p>The native resources form a generation: the namespace, the Arrow allocator the reads draw on, the Lance
 * {@link Session} every dataset FE opens shares, and the namespace client with its cache of described table
 * access. A generation is opened on first use, never while the connector is built: an FE replaying the edit log
 * builds every catalog, and loading the JNI library or reaching storage there could stop it from starting. Every
 * call runs on one generation from start to end, and pins the plugin's classloader as the thread context
 * classloader, because the engine does not pin it for metadata calls and lance-core resolves classes by name.
 *
 * <p>The Session caches the manifests and the fragment, file and index metadata of the versions FE reads, so a
 * later statement planning a version already read reads it from memory. {@link #refresh} (REFRESH CATALOG)
 * retires the generation, and the next call opens a new one, whose cold Session reads afresh a dataset replaced at
 * the same URI. Closing retires it too and refuses new calls. A retired generation is released by its last call
 * in flight: the engine reclaims a replaced or dropped catalog's connector on a background thread without waiting
 * for its statements, and closing the namespace, the Session or the allocator under a running JNI call is not
 * safe.
 */
final class LanceCatalogClient implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(LanceCatalogClient.class);
    /**
     * The capacities of each generation's Session, held in native memory outside the Arrow limit: manifests and
     * fragment and file metadata, and the index metadata that planning index segments reads. They belong to the
     * Session, not to each dataset opened with it.
     */
    static final long SESSION_METADATA_CACHE_BYTES = 64L * 1024 * 1024;
    static final long SESSION_INDEX_CACHE_BYTES = 128L * 1024 * 1024;

    private final LanceCatalogProperties properties;
    private final Supplier<List<StorageProperties>> storageProperties;
    private final LongSupplier arrowMemoryLimitBytes;
    private final ResourceFactory resources;
    private final Ticker ticker;
    /** The catalog's secrets to strip from provider errors: its REST credentials and credential headers. */
    private final List<String> catalogSecrets;

    /** The generation new calls run on; none before the first call and after a refresh. */
    private Generation current;
    private boolean closed;

    /**
     * @param storageProperties the catalog's storage keys bound by the engine; read on first use, like the
     *         memory limit, so that building the connector stays free of anything that can fail
     * @param arrowMemoryLimitBytes the Arrow memory each generation's metadata reads may use
     */
    LanceCatalogClient(LanceCatalogProperties properties, Supplier<List<StorageProperties>> storageProperties,
            LongSupplier arrowMemoryLimitBytes) {
        this(properties, storageProperties, arrowMemoryLimitBytes, new LanceResourceFactory(),
                Ticker.systemTicker());
    }

    /**
     * @param resources where each generation's allocator, namespace and Session come from
     * @param ticker the clock of the table-access cache
     */
    LanceCatalogClient(LanceCatalogProperties properties, Supplier<List<StorageProperties>> storageProperties,
            LongSupplier arrowMemoryLimitBytes, ResourceFactory resources, Ticker ticker) {
        this.properties = properties;
        this.storageProperties = storageProperties;
        this.arrowMemoryLimitBytes = arrowMemoryLimitBytes;
        this.resources = resources;
        this.ticker = ticker;
        this.catalogSecrets = properties.secretValues();
    }

    List<String> listDatabaseNames() {
        return call(generation -> generation.namespaceClient.listDatabaseNames(), "list Lance namespaces");
    }

    List<String> listTableNames(String dbName) {
        return call(generation -> generation.namespaceClient.listTableNames(dbName),
                "list the tables of Lance database " + dbName);
    }

    boolean tableExists(String dbName, String tableName) {
        return call(generation -> generation.namespaceClient.tableExists(dbName, tableName),
                "check Lance table " + dbName + "." + tableName);
    }

    /**
     * Lists one table and one child namespace under the catalog's parent namespace, which is what
     * {@code test_connection} asks of a Lance catalog: the namespace answers, with the configured
     * credentials.
     */
    void testConnection() {
        call(generation -> {
            List<String> parent = properties.getParentNamespace();
            generation.namespace.listTables(new ListTablesRequest().id(new ArrayList<>(parent)).limit(1));
            generation.namespace.listNamespaces(new ListNamespacesRequest().id(new ArrayList<>(parent)).limit(1));
            return null;
        }, "connect");
    }

    /**
     * Reads the planning metadata of one table at {@code version}, or at the newest version in storage
     * when it is empty. Every dataset is read by its URI and a version, as the BE reads it.
     *
     * @param metrics the statement's totals of the table, which the read is timed into
     */
    LanceTableMetadata loadTableMetadata(String dbName, String tableName, OptionalLong version,
            LanceMetadataMetrics metrics) {
        return readTable(dbName, tableName, version, metrics, (generation, access, readAllocator, timer) -> {
            try (Dataset dataset = openDataset(generation, readAllocator, access, version, timer)) {
                return readMetadata(dataset, access, timer);
            }
        });
    }

    /**
     * Reads the field ids and the user indexes of one table at the fixed {@code version}, for planning ranges
     * over index segments.
     */
    LanceIndexMetadata loadIndexMetadata(String dbName, String tableName, long version,
            LanceMetadataMetrics metrics) {
        OptionalLong pinned = OptionalLong.of(version);
        return readTable(dbName, tableName, pinned, metrics, (generation, access, readAllocator, timer) -> {
            try (Dataset dataset = openDataset(generation, readAllocator, access, pinned, timer)) {
                return LanceIndexMetadata.read(dataset, timer);
            }
        });
    }

    /**
     * Reads the metadata of the main-chain version committed last at or before {@code timestampMillis}, by
     * the commit times the manifests record; see {@link LanceSnapshotResolver#versionAtOrBefore}.
     */
    LanceTableMetadata loadTableMetadataAtOrBefore(String dbName, String tableName, long timestampMillis,
            String requestedText, LanceMetadataMetrics metrics) {
        String what = dbName + "." + tableName;
        return readTable(dbName, tableName, OptionalLong.empty(), metrics,
                (generation, access, readAllocator, timer) -> {
                    long version;
                    try (Dataset latest = openDataset(generation, readAllocator, access, OptionalLong.empty(),
                            timer)) {
                        version = timer.measure(Stage.VERSION_RESOLVE,
                                () -> versionAtOrBefore(latest, timestampMillis, requestedText, what));
                    }
                    LOG.debug("Resolved Lance FOR TIME AS OF '{}' to version {} from manifest commit times",
                            requestedText, version);
                    try (Dataset dataset = openDataset(generation, readAllocator, access, OptionalLong.of(version),
                            timer)) {
                        return readMetadata(dataset, access, timer);
                    }
                });
    }

    /**
     * REFRESH CATALOG: retires the current generation, so that the next call opens a new namespace, allocator and
     * Session. Opens nothing and waits for nothing: calls in flight finish on the retired generation, and the
     * last of them releases it.
     */
    void refresh() {
        Generation idle = retireCurrent();
        if (idle != null) {
            idle.closeResources();
        }
    }

    /**
     * REFRESH TABLE / DATABASE: the next read describes its table again; the Session and its cached metadata
     * stay, as {@link LanceConnector#invalidateDb} explains. Opens nothing: before the first call nothing is
     * cached.
     */
    void invalidateTableAccess() {
        Generation generation;
        synchronized (this) {
            generation = current;
        }
        if (generation != null) {
            generation.namespaceClient.invalidateTableAccessCache();
        }
    }

    /** Resolves the table's access, then reads it with a child allocator that this one read owns. */
    private <T> T readTable(String dbName, String tableName, OptionalLong version, LanceMetadataMetrics metrics,
            TableRead<T> read) {
        String what = dbName + "." + tableName;
        return call(generation -> {
            LanceTableAccess access = null;
            try (LanceMetadataMetrics.Timer timer = metrics.start(Operation.METADATA_READ)) {
                T result;
                try (BufferAllocator readAllocator = generation.allocator.newChildAllocator(
                        "lance-metadata-read", 0, generation.allocator.getLimit())) {
                    access = timer.measure(Stage.TABLE_ACCESS,
                            () -> generation.namespaceClient.resolveTableAccess(dbName, tableName));
                    result = read.read(generation, access, readAllocator, timer);
                }
                // Successful only once the read's datasets and allocator are released.
                timer.succeeded();
                return result;
            } catch (UserFacingException e) {
                throw new DorisConnectorException(e.getMessage());
            } catch (DorisConnectorException e) {
                // Raised here with a message for the user, such as a table the namespace describes but this
                // connector cannot read; as in call(), it is not wrapped again.
                throw e;
            } catch (Exception e) {
                String uri = access == null ? null : access.getDatasetUri();
                Map<String, String> options = access == null ? generation.storageOptions : access.getStorageOptions();
                if (version.isPresent() && isVersionNotFound(e)) {
                    throw new DorisConnectorException("Lance version " + version.getAsLong() + " of " + what
                            + " was not found",
                            new RuntimeException(LanceErrorMessages.sanitize(e, uri, options, catalogSecrets)));
                }
                throw LanceErrorMessages.failure("Failed to load Lance table metadata for " + what, e, uri,
                        options, catalogSecrets);
            }
        }, "load Lance table metadata for " + what);
    }

    private static LanceTableMetadata readMetadata(Dataset dataset, LanceTableAccess access,
            LanceMetadataMetrics.Timer timer) {
        Schema schema = timer.measure(Stage.SCHEMA, dataset::getSchema);
        List<LanceFragmentInfo> fragments = timer.measure(Stage.FRAGMENTS, () -> {
            List<LanceFragmentInfo> infos = new ArrayList<>();
            for (Fragment fragment : dataset.getFragments()) {
                infos.add(new LanceFragmentInfo(Integer.toUnsignedLong(fragment.getId()),
                        fragment.metadata().getNumRows(), fragment.metadata().getPhysicalRows()));
            }
            return infos;
        });
        return new LanceTableMetadata(access, dataset.version(), schema, fragments);
    }

    private static long versionAtOrBefore(Dataset latest, long timestampMillis, String requestedText,
            String what) {
        try {
            return LanceSnapshotResolver.versionAtOrBefore(latest.listVersions(), timestampMillis, requestedText);
        } catch (LanceSnapshotResolver.NoVersionAtOrBeforeException e) {
            throw new UserFacingException("Lance table " + what + " has no version at or before '"
                    + requestedText + "'");
        } catch (LanceSnapshotResolver.HistoryRemovedException e) {
            throw new UserFacingException("Lance cannot resolve FOR TIME AS OF '" + requestedText
                    + "' on " + what + ": version " + e.getVersion()
                    + ", which may hold the state at that time, no longer exists");
        }
    }

    /**
     * Opens the dataset with the generation's Session: its caches, not the dataset, decide how much metadata is
     * kept, and the next open of the same version reads it from them.
     */
    private static Dataset openDataset(Generation generation, BufferAllocator readAllocator,
            LanceTableAccess access, OptionalLong version, LanceMetadataMetrics.Timer timer) {
        ReadOptions.Builder options = new ReadOptions.Builder()
                .setStorageOptions(access.getStorageOptions())
                .setSession(generation.session);
        if (version.isPresent()) {
            options.setVersion(version.getAsLong());
        }
        return timer.measure(Stage.DATASET_OPEN, () -> Dataset.open().allocator(readAllocator)
                .uri(access.getDatasetUri()).readOptions(options.build()).build());
    }

    /**
     * Whether a failed open of an explicitly requested version means that version does not exist. A
     * namespace reports it through {@link TableVersionNotFoundException}. The storage reader reports it as
     * a missing manifest under {@code _versions/} or as Lance's own version-not-found error; a missing
     * dataset or an unreachable store fails differently and keeps its message.
     */
    private static boolean isVersionNotFound(Throwable throwable) {
        if (ExceptionUtils.indexOfType(throwable, TableVersionNotFoundException.class) >= 0) {
            return true;
        }
        String rootMessage = ExceptionUtils.getRootCauseMessage(throwable);
        if (rootMessage == null) {
            return false;
        }
        String lower = rootMessage.toLowerCase(Locale.ROOT);
        return lower.contains("version not found")
                || (lower.contains("not found") && lower.contains("_versions/"));
    }

    /**
     * Runs one operation on the current generation, opening it on first use: pins the plugin classloader for
     * it, and turns a provider failure into a sanitized connector error.
     */
    private <T> T call(GenerationCall<T> body, String operation) {
        Generation generation = enter();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(LanceCatalogClient.class.getClassLoader());
            return body.call(generation);
        } catch (DorisConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw LanceErrorMessages.failure("Failed to " + operation, e, null, generation.storageOptions,
                    catalogSecrets);
        } catch (LinkageError e) {
            // Reading a schema goes through Arrow's C Data interface, whose own JNI library loads on first
            // use; a host that cannot load it fails here rather than at catalog initialization.
            throw new DorisConnectorException("Failed to " + operation + ": a native library Lance needs"
                    + " could not be loaded: " + e, e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            leave(generation);
        }
    }

    /** Counts the calling thread in on the current generation, opening one when there is none. */
    private synchronized Generation enter() {
        if (closed) {
            throw new DorisConnectorException("Lance catalog resources have been closed");
        }
        if (current == null) {
            current = openGeneration();
        }
        current.callsInFlight++;
        return current;
    }

    private Generation openGeneration() {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        BufferAllocator allocator = null;
        LanceNamespace namespace = null;
        Session session = null;
        Map<String, String> storageOptions = Collections.emptyMap();
        try {
            Thread.currentThread().setContextClassLoader(LanceCatalogClient.class.getClassLoader());
            long limit = arrowMemoryLimitBytes.getAsLong();
            List<StorageProperties> storage = storageProperties.get();
            String implementation;
            Map<String, String> namespaceProperties;
            if (properties.isRest()) {
                // A REST namespace is reached over HTTP and reads no storage itself; every table's storage
                // options come with its description.
                implementation = "rest";
                namespaceProperties = properties.restNamespaceProperties();
            } else {
                implementation = "dir";
                LanceStorageOptions.rejectOssHdfs(storage);
                String warehouse = properties.getWarehouse();
                storageOptions = LanceStorageOptions.fromDorisStorageProperties(warehouse, storage);
                namespaceProperties = new HashMap<>();
                namespaceProperties.put("root", warehouse);
                storageOptions.forEach((key, value) -> namespaceProperties.put("storage." + key, value));
            }
            allocator = resources.newAllocator(limit);
            namespace = resources.connect(implementation, namespaceProperties, allocator);
            session = resources.newSession();
            LanceNamespaceClient namespaceClient = new LanceNamespaceClient(namespace, properties.isRest(),
                    properties.getRootDatabase(), properties.getParentNamespace(), storage,
                    properties.getTableAccessCacheTtlSeconds(), ticker);
            return new Generation(allocator, namespace, session, namespaceClient, storageOptions);
        } catch (LinkageError e) {
            closeResource(namespace);
            closeResource(session);
            closeResource(allocator);
            throw new DorisConnectorException("Failed to load the Lance JNI library. FE loads it from"
                    + " lance-core in plugins/connector/lance/lib: " + e, e);
        } catch (RuntimeException e) {
            closeResource(namespace);
            closeResource(session);
            closeResource(allocator);
            // The options this attempt built, which the namespace may echo back in its error.
            throw LanceErrorMessages.failure("Failed to initialize Lance " + properties.getCatalogType()
                    + " catalog", e, null, storageOptions, catalogSecrets);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /** Counts the calling thread out; the last call to leave a retired generation releases it. */
    private void leave(Generation generation) {
        synchronized (this) {
            generation.callsInFlight--;
            if (!generation.retired || generation.callsInFlight > 0) {
                return;
            }
        }
        generation.closeResources();
    }

    /** Retires the current generation; returns it when no call runs on it, for the caller to release. */
    private synchronized Generation retireCurrent() {
        Generation retiring = current;
        current = null;
        if (retiring == null) {
            return null;
        }
        retiring.retired = true;
        return retiring.callsInFlight == 0 ? retiring : null;
    }

    /** Retires the client at once; its last call in flight, or this call when there is none, cleans up. */
    @Override
    public void close() {
        Generation idle;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            idle = retireCurrent();
        }
        if (idle != null) {
            idle.closeResources();
        }
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

    /** Creates the resources of each generation; the production factory uses Arrow's and lance-core's. */
    interface ResourceFactory {
        BufferAllocator newAllocator(long limit);

        LanceNamespace connect(String implementation, Map<String, String> properties, BufferAllocator allocator);

        Session newSession();
    }

    private static final class LanceResourceFactory implements ResourceFactory {
        @Override
        public BufferAllocator newAllocator(long limit) {
            return new RootAllocator(limit);
        }

        @Override
        public LanceNamespace connect(String implementation, Map<String, String> properties,
                BufferAllocator allocator) {
            return LanceNamespace.connect(implementation, properties, allocator);
        }

        @Override
        public Session newSession() {
            return Session.builder().metadataCacheSizeBytes(SESSION_METADATA_CACHE_BYTES)
                    .indexCacheSizeBytes(SESSION_INDEX_CACHE_BYTES).build();
        }
    }

    /** One generation's native resources, and the calls running on it; the counters are guarded by the client. */
    private static final class Generation {
        private final BufferAllocator allocator;
        private final LanceNamespace namespace;
        private final Session session;
        private final LanceNamespaceClient namespaceClient;
        /** The options a filesystem namespace reads storage with; what a failure before a describe may echo. */
        private final Map<String, String> storageOptions;
        private int callsInFlight;
        private boolean retired;

        private Generation(BufferAllocator allocator, LanceNamespace namespace, Session session,
                LanceNamespaceClient namespaceClient, Map<String, String> storageOptions) {
            this.allocator = allocator;
            this.namespace = namespace;
            this.session = session;
            this.namespaceClient = namespaceClient;
            this.storageOptions = storageOptions;
        }

        /** Closes the namespace, then the Session, then the allocator; a failure closing one skips none. */
        private void closeResources() {
            closeResource(namespace);
            closeResource(session);
            closeResource(allocator);
        }
    }

    @FunctionalInterface
    private interface GenerationCall<T> {
        T call(Generation generation) throws Exception;
    }

    @FunctionalInterface
    private interface TableRead<T> {
        T read(Generation generation, LanceTableAccess access, BufferAllocator readAllocator,
                LanceMetadataMetrics.Timer timer) throws Exception;
    }

    /** A failure whose message is already what the user should read; it is neither wrapped nor sanitized. */
    private static final class UserFacingException extends RuntimeException {
        private UserFacingException(String message) {
            super(message);
        }
    }
}

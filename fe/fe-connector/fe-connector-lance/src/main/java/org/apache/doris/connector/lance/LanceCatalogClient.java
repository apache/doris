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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ReadOptions;
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
 * <p>The namespace and its Arrow allocator are opened on first use, never while the connector is built:
 * an FE replaying the edit log builds every catalog, and loading the JNI library or reaching storage there
 * could stop it from starting. Every call pins the plugin's classloader as the thread context classloader,
 * because the engine does not pin it for metadata calls and lance-core resolves classes by name.
 *
 * <p>Closing retires the client at once; the last call still in flight releases the native resources.
 * The engine reclaims a replaced or dropped catalog's connector on a background thread without waiting
 * for its statements, and closing the namespace or the allocator under a running JNI call is not safe.
 */
final class LanceCatalogClient implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(LanceCatalogClient.class);
    /**
     * Metadata cache of each dataset FE opens. FE plans from manifests; describing indexes reads index
     * metadata, not index pages, so no index cache is kept.
     */
    private static final long METADATA_CACHE_SIZE_BYTES = 64L * 1024 * 1024;

    private final LanceCatalogProperties properties;
    private final Supplier<List<StorageProperties>> storageProperties;
    private final LongSupplier arrowMemoryLimitBytes;
    /** The catalog's secrets to strip from provider errors: its REST credentials and credential headers. */
    private final List<String> catalogSecrets;

    private BufferAllocator allocator;
    private LanceNamespace namespace;
    private LanceNamespaceClient namespaceClient;
    private Map<String, String> namespaceStorageOptions = Collections.emptyMap();
    private int callsInFlight;
    private boolean closed;

    /**
     * @param storageProperties the catalog's storage keys bound by the engine; read on first use, like the
     *         memory limit, so that building the connector stays free of anything that can fail
     * @param arrowMemoryLimitBytes the Arrow memory this catalog's metadata reads may use
     */
    LanceCatalogClient(LanceCatalogProperties properties, Supplier<List<StorageProperties>> storageProperties,
            LongSupplier arrowMemoryLimitBytes) {
        this.properties = properties;
        this.storageProperties = storageProperties;
        this.arrowMemoryLimitBytes = arrowMemoryLimitBytes;
        this.catalogSecrets = properties.secretValues();
    }

    List<String> listDatabaseNames() {
        return call(namespaceClient -> namespaceClient.listDatabaseNames(), "list Lance namespaces");
    }

    List<String> listTableNames(String dbName) {
        return call(namespaceClient -> namespaceClient.listTableNames(dbName),
                "list the tables of Lance database " + dbName);
    }

    boolean tableExists(String dbName, String tableName) {
        return call(namespaceClient -> namespaceClient.tableExists(dbName, tableName),
                "check Lance table " + dbName + "." + tableName);
    }

    /**
     * Lists one table and one child namespace under the catalog's parent namespace, which is what
     * {@code test_connection} asks of a Lance catalog: the namespace answers, with the configured
     * credentials.
     */
    void testConnection() {
        call(namespaceClient -> {
            List<String> parent = properties.getParentNamespace();
            namespace.listTables(new ListTablesRequest().id(new ArrayList<>(parent)).limit(1));
            namespace.listNamespaces(new ListNamespacesRequest().id(new ArrayList<>(parent)).limit(1));
            return null;
        }, "connect");
    }

    /**
     * Reads the planning metadata of one table at {@code version}, or at the newest version in storage
     * when it is empty. Every dataset is read by its URI and a version, as the BE reads it.
     */
    LanceTableMetadata loadTableMetadata(String dbName, String tableName, OptionalLong version) {
        return readTable(dbName, tableName, version, (access, readAllocator) -> {
            try (Dataset dataset = openDataset(readAllocator, access, version)) {
                return readMetadata(dataset, access);
            }
        });
    }

    /**
     * Reads the field ids and the user indexes of one table at the fixed {@code version}, for planning ranges
     * over index segments.
     */
    LanceIndexMetadata loadIndexMetadata(String dbName, String tableName, long version) {
        OptionalLong pinned = OptionalLong.of(version);
        return readTable(dbName, tableName, pinned, (access, readAllocator) -> {
            try (Dataset dataset = openDataset(readAllocator, access, pinned)) {
                return LanceIndexMetadata.read(dataset);
            }
        });
    }

    /**
     * Reads the metadata of the main-chain version committed last at or before {@code timestampMillis}, by
     * the commit times the manifests record; see {@link LanceSnapshotResolver#versionAtOrBefore}.
     */
    LanceTableMetadata loadTableMetadataAtOrBefore(String dbName, String tableName, long timestampMillis,
            String requestedText) {
        String what = dbName + "." + tableName;
        return readTable(dbName, tableName, OptionalLong.empty(), (access, readAllocator) -> {
            long version;
            try (Dataset latest = openDataset(readAllocator, access, OptionalLong.empty())) {
                try {
                    version = LanceSnapshotResolver.versionAtOrBefore(latest.listVersions(), timestampMillis,
                            requestedText);
                } catch (LanceSnapshotResolver.NoVersionAtOrBeforeException e) {
                    throw new UserFacingException("Lance table " + what + " has no version at or before '"
                            + requestedText + "'");
                } catch (LanceSnapshotResolver.HistoryRemovedException e) {
                    throw new UserFacingException("Lance cannot resolve FOR TIME AS OF '" + requestedText
                            + "' on " + what + ": version " + e.getVersion()
                            + ", which may hold the state at that time, no longer exists");
                }
            }
            LOG.debug("Resolved Lance FOR TIME AS OF '{}' to version {} from manifest commit times",
                    requestedText, version);
            try (Dataset dataset = openDataset(readAllocator, access, OptionalLong.of(version))) {
                return readMetadata(dataset, access);
            }
        });
    }

    /** Resolves the table's access, then reads it with a child allocator that this one read owns. */
    private <T> T readTable(String dbName, String tableName, OptionalLong version, TableRead<T> read) {
        String what = dbName + "." + tableName;
        return call(namespaceClient -> {
            LanceTableAccess access = null;
            try (BufferAllocator readAllocator = allocator.newChildAllocator(
                    "lance-metadata-read", 0, allocator.getLimit())) {
                access = namespaceClient.resolveTableAccess(dbName, tableName);
                return read.read(access, readAllocator);
            } catch (UserFacingException e) {
                throw new DorisConnectorException(e.getMessage());
            } catch (DorisConnectorException e) {
                // Raised here with a message for the user, such as a table the namespace describes but this
                // connector cannot read; as in call(), it is not wrapped again.
                throw e;
            } catch (Exception e) {
                String uri = access == null ? null : access.getDatasetUri();
                Map<String, String> options = access == null ? namespaceStorageOptions : access.getStorageOptions();
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

    private static LanceTableMetadata readMetadata(Dataset dataset, LanceTableAccess access) {
        List<LanceFragmentInfo> fragments = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            fragments.add(new LanceFragmentInfo(Integer.toUnsignedLong(fragment.getId()),
                    fragment.metadata().getNumRows(), fragment.metadata().getPhysicalRows()));
        }
        return new LanceTableMetadata(access, dataset.version(), dataset.getSchema(), fragments);
    }

    private static Dataset openDataset(BufferAllocator readAllocator, LanceTableAccess access,
            OptionalLong version) {
        ReadOptions.Builder options = new ReadOptions.Builder()
                .setStorageOptions(access.getStorageOptions())
                .setIndexCacheSizeBytes(0)
                .setMetadataCacheSizeBytes(METADATA_CACHE_SIZE_BYTES);
        if (version.isPresent()) {
            options.setVersion(version.getAsLong());
        }
        return Dataset.open().allocator(readAllocator).uri(access.getDatasetUri())
                .readOptions(options.build()).build();
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
     * Runs one operation against the namespace: counts it in, opening the namespace on first use, pins the
     * plugin classloader, and turns a provider failure into a sanitized connector error.
     */
    private <T> T call(NamespaceCall<T> body, String operation) {
        LanceNamespaceClient client = enter();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(LanceCatalogClient.class.getClassLoader());
            return body.call(client);
        } catch (DorisConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw LanceErrorMessages.failure("Failed to " + operation, e, null, namespaceStorageOptions,
                    catalogSecrets);
        } catch (LinkageError e) {
            // Reading a schema goes through Arrow's C Data interface, whose own JNI library loads on first
            // use; a host that cannot load it fails here rather than at catalog initialization.
            throw new DorisConnectorException("Failed to " + operation + ": a native library Lance needs"
                    + " could not be loaded: " + e, e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            leave();
        }
    }

    /** Counts the calling thread in, opening the namespace on first use. */
    private synchronized LanceNamespaceClient enter() {
        if (closed) {
            throw new DorisConnectorException("Lance catalog resources have been closed");
        }
        if (namespaceClient == null) {
            openNamespace();
        }
        callsInFlight++;
        return namespaceClient;
    }

    private void openNamespace() {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        BufferAllocator newAllocator = null;
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
            newAllocator = new RootAllocator(limit);
            namespace = LanceNamespace.connect(implementation, namespaceProperties, newAllocator);
            allocator = newAllocator;
            namespaceStorageOptions = storageOptions;
            namespaceClient = new LanceNamespaceClient(namespace, properties.isRest(), properties.getRootDatabase(),
                    properties.getParentNamespace(), storage);
        } catch (LinkageError e) {
            closeResource(newAllocator);
            throw new DorisConnectorException("Failed to load the Lance JNI library. FE loads it from"
                    + " lance-core in plugins/connector/lance/lib: " + e, e);
        } catch (RuntimeException e) {
            closeResource(newAllocator);
            // The options this attempt built, which the namespace may echo back in its error.
            throw LanceErrorMessages.failure("Failed to initialize Lance " + properties.getCatalogType()
                    + " catalog", e, null, storageOptions, catalogSecrets);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /** Counts the calling thread out; the last call to leave a closed client releases the resources. */
    private void leave() {
        synchronized (this) {
            callsInFlight--;
            if (!closed || callsInFlight > 0) {
                return;
            }
        }
        closeResources();
    }

    /** Retires the client at once; its last call in flight, or this call when there is none, cleans up. */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            if (callsInFlight > 0) {
                return;
            }
        }
        closeResources();
    }

    private void closeResources() {
        closeResource(namespace);
        closeResource(allocator);
        namespace = null;
        allocator = null;
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

    @FunctionalInterface
    private interface NamespaceCall<T> {
        T call(LanceNamespaceClient namespaceClient) throws Exception;
    }

    @FunctionalInterface
    private interface TableRead<T> {
        T read(LanceTableAccess access, BufferAllocator readAllocator) throws Exception;
    }

    /** A failure whose message is already what the user should read; it is neither wrapped nor sanitized. */
    private static final class UserFacingException extends RuntimeException {
        private UserFacingException(String message) {
            super(message);
        }
    }
}

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

package org.apache.doris.connector;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.EnvUtils;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorHttpSecurityHook;
import org.apache.doris.connector.spi.ConnectorMetadataAccessObserver;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.SpiSwitchingFileSystem;
import org.apache.doris.kerberos.ExecutionAuthenticator;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageBackendType;
import org.apache.doris.thrift.TTestStorageConnectivityRequest;
import org.apache.doris.thrift.TTestStorageConnectivityResponse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link ConnectorContext}.
 *
 * <p>Provides the minimal catalog-level context that connector providers need
 * during creation. Additional context fields can be added here as the SPI evolves.
 *
 * <p>It implements {@link ConnectorStorageContext} as well and hands itself back from
 * {@link #getStorageContext()}. The split exists so a connector reads only the services that apply to it;
 * on the engine side the two halves share the catalog's parsed storage properties, the cached filesystem
 * and its {@link #close()} lifecycle, so separating them into two objects would buy nothing and move code
 * that has a lock and a shutdown flag in it.
 */
public class DefaultConnectorContext implements ConnectorContext, ConnectorStorageContext, Closeable {

    private static final Logger LOG = LogManager.getLogger(DefaultConnectorContext.class);

    private static final ExecutionAuthenticator NOOP_AUTH = new ExecutionAuthenticator() {};

    private final String catalogName;
    private final long catalogId;
    private final ConnectorMetadataAccessMetrics metadataAccessMetrics;
    private final Map<String, String> environment;
    private final Supplier<ExecutionAuthenticator> authSupplier;
    // Lazily supplies the catalog's static storage-properties map for storage-URI normalization
    // (FIX-URI-NORMALIZE). Invoked at scan time only (catalog fully initialized). Empty for ctors
    // that do not wire it — those callers (non-plugin catalogs) never invoke normalizeStorageUri.
    private final Supplier<Map<StorageTypeId, StorageAdapter>> storagePropertiesSupplier;
    // Supplies the catalog's effective raw storage map (persisted props + derived defaults, empty when the
    // connector supplies vended credentials) for direct fe-filesystem binding in getStorageProperties()
    // (design S2): no fe-core StorageProperties parse on the connector storage path. Empty for ctors that do
    // not wire it (non-plugin / 2-3-4-arg) — those yield an empty storage list, correct parity.
    private final Supplier<Map<String, String>> rawStoragePropsSupplier;

    // Engine-owned, per-catalog Doris FileSystem (a scheme-routing SpiSwitchingFileSystem over the catalog's
    // storage properties), lazily built on the first getFileSystem() and closed on catalog teardown (close()).
    // Connectors BORROW it and must not close it (see ConnectorStorageContext.getFileSystem javadoc); siblings built
    // via createSiblingConnector share this same context, so there is exactly one cached FS per catalog. Guarded
    // by fsLock; the field is dropped to null on close so a post-teardown getFileSystem() returns null.
    private final Object fsLock = new Object();
    private volatile FileSystem catalogFileSystem;
    private volatile boolean closed;

    private final ConnectorHttpSecurityHook httpSecurityHook = new ConnectorHttpSecurityHook() {
        @Override
        public void beforeRequest(String url) throws Exception {
            SecurityChecker.getInstance().startSSRFChecking(url);
        }

        @Override
        public void afterRequest() {
            SecurityChecker.getInstance().stopSSRFChecking();
        }
    };

    public DefaultConnectorContext(String catalogName, long catalogId) {
        this(catalogName, catalogId, () -> NOOP_AUTH);
    }

    /**
     * Creates the lightweight pre-initialization context used by the catalog factory for CREATE validation and
     * edit-log replay. It exposes a snapshot of the raw storage properties so connector construction and
     * connectivity checks can bind credentials, but it does not provide runtime authentication,
     * connector-derived storage defaults, backend adapters, or a filesystem.
     */
    public static DefaultConnectorContext forCatalogCreationValidation(String catalogName, long catalogId,
            Map<String, String> rawStorageProperties) {
        Map<String, String> rawSnapshot = Collections.unmodifiableMap(
                new HashMap<>(Objects.requireNonNull(rawStorageProperties, "rawStorageProperties")));
        return new DefaultConnectorContext(catalogName, catalogId, () -> NOOP_AUTH,
                Collections::emptyMap, () -> new HashMap<>(rawSnapshot), false);
    }

    public DefaultConnectorContext(String catalogName, long catalogId,
            Supplier<ExecutionAuthenticator> authSupplier) {
        this(catalogName, catalogId, authSupplier, Collections::emptyMap);
    }

    public DefaultConnectorContext(String catalogName, long catalogId,
            Supplier<ExecutionAuthenticator> authSupplier,
            Supplier<Map<StorageTypeId, StorageAdapter>> storagePropertiesSupplier) {
        this(catalogName, catalogId, authSupplier, storagePropertiesSupplier, Collections::emptyMap);
    }

    public DefaultConnectorContext(String catalogName, long catalogId,
            Supplier<ExecutionAuthenticator> authSupplier,
            Supplier<Map<StorageTypeId, StorageAdapter>> storagePropertiesSupplier,
            Supplier<Map<String, String>> rawStoragePropsSupplier) {
        this(catalogName, catalogId, authSupplier, storagePropertiesSupplier, rawStoragePropsSupplier, true);
    }

    private DefaultConnectorContext(String catalogName, long catalogId,
            Supplier<ExecutionAuthenticator> authSupplier,
            Supplier<Map<StorageTypeId, StorageAdapter>> storagePropertiesSupplier,
            Supplier<Map<String, String>> rawStoragePropsSupplier, boolean collectMetadataAccessMetrics) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.catalogId = catalogId;
        this.authSupplier = Objects.requireNonNull(authSupplier, "authSupplier");
        this.storagePropertiesSupplier =
                Objects.requireNonNull(storagePropertiesSupplier, "storagePropertiesSupplier");
        this.rawStoragePropsSupplier =
                Objects.requireNonNull(rawStoragePropsSupplier, "rawStoragePropsSupplier");
        this.environment = buildEnvironment();
        this.metadataAccessMetrics = collectMetadataAccessMetrics
                ? new ConnectorMetadataAccessMetrics(catalogName, catalogId) : null;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public long getCatalogId() {
        return catalogId;
    }

    @Override
    public Map<String, String> getEnvironment() {
        return environment;
    }

    @Override
    public ConnectorHttpSecurityHook getHttpSecurityHook() {
        return httpSecurityHook;
    }

    @Override
    public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
        // Build the sibling through the SAME factory the engine uses for a top-level catalog, so the sibling's
        // concrete class is loaded by that type's own plugin classloader (child-first) — never co-packaged into
        // the gateway's plugin. Passing `this` lets the sibling reuse this catalog's id/auth/storage suppliers
        // (correct for e.g. iceberg-on-HMS, which shares the HMS catalog's metastore + storage + credentials).
        // Returns null when no provider matches the type (or the plugin manager is not initialized); the
        // gateway caller null-checks and fails loud with its own (connector-specific) message — fe-core stays
        // connector-agnostic and does no property parsing here.
        return ConnectorFactory.createConnector(catalogType, properties, this);
    }

    @Override
    public String sanitizeOutboundUrl(String url) {
        try {
            return SecurityChecker.getInstance().getSafeJdbcUrl(url);
        } catch (Exception e) {
            throw new RuntimeException("JDBC URL security check failed: " + e.getMessage(), e);
        }
    }

    @Override
    public ConnectorStorageContext getStorageContext() {
        return this;
    }

    @Override
    public ConnectorMetadataAccessObserver getMetadataAccessObserver() {
        return metadataAccessMetrics == null
                ? ConnectorMetadataAccessObserver.NOOP : metadataAccessMetrics::record;
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        return authSupplier.get().execute(task);
    }

    @Override
    public Map<String, String> vendStorageCredentials(Map<String, String> rawVendedCredentials) {
        // Map the per-table vended token to the BE-facing AWS_* properties. Fail-soft (empty) on any
        // error, matching the legacy provider, so a malformed token degrades gracefully rather than
        // killing the scan. The outer try also covers getBackendPropertiesFromStorageMap so the
        // fail-soft boundary is byte-identical to the pre-refactor method; buildVendedStorageMap shares
        // the typed-map build with normalizeStorageUri (single source of truth — no drift).
        try {
            Map<StorageTypeId, StorageAdapter> map = buildVendedStorageMap(rawVendedCredentials);
            return map == null ? Collections.emptyMap()
                    : CredentialUtils.getBackendPropertiesFromStorageMap(map);
        } catch (Exception e) {
            LOG.warn("Failed to normalize vended credentials", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Builds the vended {@link StorageAdapter} typed map from a raw per-table token: filter to
     * cloud-storage props, run {@link StorageAdapter#ofAll} (normalizes arbitrary token key
     * shapes + derives region/endpoint), then index by {@link StorageTypeId}. Mirrors the
     * legacy vended-credentials normalization tail exactly, so the BE-credential overlay
     * ({@link #vendStorageCredentials}) and the URI normalization ({@link #normalizeStorageUri(String,
     * Map)}) derive the SAME credentials from the SAME token — no drift. Returns {@code null} when the
     * token is null/empty, yields no cloud-storage props, or normalization throws — replicating the
     * legacy "return null → fall back to the base/static map" contract.
     */
    private Map<StorageTypeId, StorageAdapter> buildVendedStorageMap(
            Map<String, String> rawVendedCredentials) {
        if (rawVendedCredentials == null || rawVendedCredentials.isEmpty()) {
            return null;
        }
        try {
            Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawVendedCredentials);
            if (filtered.isEmpty()) {
                return null;
            }
            List<StorageAdapter> vended = StorageAdapter.ofAll(filtered);
            return vended.stream()
                    .collect(Collectors.toMap(StorageAdapter::getType, Function.identity()));
        } catch (Exception e) {
            LOG.warn("Failed to normalize vended credentials", e);
            return null;
        }
    }

    @Override
    public Map<String, String> getBackendStorageProperties() {
        // Mirror legacy PaimonScanNode.getLocationProperties(): translate the catalog's parsed
        // storage-adapter map into BE-canonical scan keys (AWS_* for object stores, hadoop/dfs for
        // HDFS) via the SAME CredentialUtils.getBackendPropertiesFromStorageMap legacy/iceberg/hive use
        // — single source of truth, no drift. The map is already validated at catalog creation, so this
        // does not throw; an empty map (non-plugin ctor / local-FS warehouse) yields an empty result
        // (no overlay) — correct parity, unlike normalizeStorageUri which must fail-loud on a bad path.
        return CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesSupplier.get());
    }

    /**
     * Engine side of the BE→storage probe: pick a live backend and ask it to reach the location the
     * connector resolved. This is the RPC the legacy fe-core {@code StorageConnectivityTester} owned; it
     * stays engine-side because it needs the backend registry and the client pool. It is payload-agnostic —
     * the storage type and the property map come from the connector, so no filesystem-specific knowledge
     * lives here.
     *
     * <p>No alive backend means no probe (not a failure), matching the legacy behavior: a catalog must be
     * creatable on a cluster whose backends are still starting up.
     */
    @Override
    public void testBackendStorageConnectivity(int storageBackendTypeValue,
            Map<String, String> backendProperties) throws Exception {
        TStorageBackendType storageType = TStorageBackendType.findByValue(storageBackendTypeValue);
        if (storageType == null) {
            throw new IllegalArgumentException(
                    "Unknown storage backend type value: " + storageBackendTypeValue);
        }
        List<Long> aliveBeIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
        if (aliveBeIds.isEmpty()) {
            LOG.info("Skipping BE storage connectivity test: no alive backend");
            return;
        }
        Collections.shuffle(aliveBeIds);
        Backend backend = Env.getCurrentSystemInfo().getBackend(aliveBeIds.get(0));
        if (backend == null) {
            LOG.info("Skipping BE storage connectivity test: backend vanished between listing and lookup");
            return;
        }

        TTestStorageConnectivityRequest request = new TTestStorageConnectivityRequest();
        request.setType(storageType);
        request.setProperties(backendProperties);

        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
        BackendService.Client client = null;
        boolean ok = false;
        try {
            client = ClientPool.backendPool.borrowObject(address);
            TTestStorageConnectivityResponse response = client.testStorageConnectivity(request);
            if (response.status.getStatusCode() != TStatusCode.OK) {
                String errMsg = response.status.isSetErrorMsgs() && !response.status.getErrorMsgs().isEmpty()
                        ? response.status.getErrorMsgs().get(0) : "Unknown error";
                throw new Exception(errMsg);
            }
            ok = true;
        } finally {
            if (client != null) {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }
    }

    @Override
    public List<org.apache.doris.filesystem.properties.StorageProperties> getStorageProperties() {
        // Bind the catalog's raw storage map directly via fe-filesystem (design S2): hand the connector its
        // storage as typed fe-filesystem StorageProperties (from which it derives its Hadoop/HiveConf config
        // and BE creds without importing fe-core), sourcing the raw map straight from the catalog's raw
        // storage supplier -- no fe-core StorageProperties.createAll round-trip via getOrigProps(). The raw
        // supplier already merges the catalog's derived storage defaults (warehouse -> fs.defaultFS) and
        // honors the vended gate (empty for a REST/vended catalog). An empty map (non-plugin ctor /
        // REST-vended / credential-less warehouse) yields an empty list -- no static storage, correct parity.
        Map<String, String> rawCatalogProps = rawStoragePropsSupplier.get();
        if (rawCatalogProps == null || rawCatalogProps.isEmpty()) {
            return Collections.emptyList();
        }
        return FileSystemFactory.bindAllStorageProperties(rawCatalogProps);
    }

    @Override
    public FileSystem getFileSystem(ConnectorSession session) {
        // Engine-owned, per-catalog scheme-routing FileSystem, lazily built and cached so repeated scans reuse
        // one instance (avoids rebuilding/re-authenticating per call, mirroring the legacy per-catalog
        // FileSystemCache). The session is accepted for the Trino-shaped SPI but not yet used to key a per-user
        // filesystem — the current build is catalog-level. Returns null when the catalog has no storage
        // machinery (empty storage map: non-plugin ctor / credential-less warehouse), matching the benign
        // defaults of getBackendStorageProperties() and cleanupEmptyManagedLocation().
        FileSystem fs = catalogFileSystem;
        if (fs != null) {
            return fs;
        }
        synchronized (fsLock) {
            if (closed) {
                return null;
            }
            if (catalogFileSystem == null) {
                Map<StorageTypeId, StorageAdapter> storageProps = storagePropertiesSupplier.get();
                if (storageProps == null || storageProps.isEmpty()) {
                    return null;
                }
                catalogFileSystem = buildCatalogFileSystem(storageProps);
            }
            return catalogFileSystem;
        }
    }

    /**
     * Builds the catalog's scheme-routing filesystem from its storage properties. Extracted so tests can
     * inject a recording fake without real storage/FS wiring (mirrors the {@code @VisibleForTesting} seams
     * used elsewhere in this class).
     */
    @VisibleForTesting
    FileSystem buildCatalogFileSystem(Map<StorageTypeId, StorageAdapter> storageProps) {
        return new SpiSwitchingFileSystem(storageProps);
    }

    /**
     * Closes the cached catalog filesystem, if one was built. Idempotent. Called by the engine when the
     * catalog/context is torn down (connector replacement or catalog close); connectors must never call it.
     */
    @Override
    public void close() throws IOException {
        FileSystem fs;
        synchronized (fsLock) {
            if (closed) {
                return;
            }
            closed = true;
            fs = catalogFileSystem;
            catalogFileSystem = null;
        }
        if (metadataAccessMetrics != null) {
            metadataAccessMetrics.close();
        }
        if (fs != null) {
            fs.close();
        }
    }

    @Override
    public String normalizeStorageUri(String rawUri) {
        // No vended token → normalize against the catalog's static storage map (behavior unchanged).
        return normalizeStorageUri(rawUri, null);
    }

    @Override
    public String normalizeStorageUri(String rawUri, Map<String, String> rawVendedCredentials) {
        if (Strings.isNullOrEmpty(rawUri)) {
            return rawUri;
        }
        // Mirror legacy PaimonScanNode's 2-arg LocationPath.of(path, storagePropertiesMap):
        // scheme-normalize (oss/cos/obs/s3a -> s3, OSS bucket.endpoint -> bucket) so BE's
        // scheme-dispatched S3 factory can open the file. The storage map follows the vended
        // precedence: when the connector supplies a per-table vended token
        // (REST catalogs, whose static map is empty by design) the VENDED map REPLACES the static map;
        // otherwise the catalog's static storage map is used. Fail-loud (StoragePropertiesException
        // propagates) — a path that cannot be normalized would otherwise silently corrupt reads (esp. a
        // deletion-vector path on merge-on-read). Single source of truth: the SAME LocationPath
        // normalization legacy/iceberg/hive use, so no drift.
        Map<StorageTypeId, StorageAdapter> vended = buildVendedStorageMap(rawVendedCredentials);
        Map<StorageTypeId, StorageAdapter> effective =
                vended != null ? vended : storagePropertiesSupplier.get();
        return LocationPath.ofAdapters(rawUri, effective).toStorageLocation().toString();
    }

    @Override
    public UnaryOperator<String> newStorageUriNormalizer(Map<String, String> rawVendedCredentials) {
        // PERF: the vended token is scan-invariant, so derive the effective storage map (the expensive
        // buildVendedStorageMap = StorageProperties.createAll + hadoop config build) ONCE per scan and reuse
        // it for every per-file normalize, instead of rebuilding it per data/delete file. Each application is
        // byte-identical to normalizeStorageUri(rawUri, token): the SAME empty-uri short-circuit, the SAME
        // vended-replaces-static precedence, the SAME fail-loud LocationPath. The derivation is done LAZILY on
        // the first non-empty URI (not eagerly at construction) so a scan that normalizes zero non-empty URIs
        // triggers no derivation — preserving the exact exception timing of the per-call method. The returned
        // normalizer is single-threaded per scan (the streaming pump drives one thread; the synchronous and
        // position-delete loops are single-threaded), so the memo needs no lock.
        return new UnaryOperator<String>() {
            private Map<StorageTypeId, StorageAdapter> effective;
            private boolean built;

            @Override
            public String apply(String rawUri) {
                if (Strings.isNullOrEmpty(rawUri)) {
                    return rawUri;
                }
                if (!built) {
                    Map<StorageTypeId, StorageAdapter> vended =
                            buildVendedStorageMap(rawVendedCredentials);
                    effective = vended != null ? vended : storagePropertiesSupplier.get();
                    built = true;
                }
                return LocationPath.ofAdapters(rawUri, effective).toStorageLocation().toString();
            }
        };
    }

    @Override
    public String getBackendFileType(String rawUri, Map<String, String> rawVendedCredentials) {
        // Same LocationPath build as normalizeStorageUri (vended-aware), then read the BE file type from
        // it — authoritative over the scheme-only default because it also detects a broker-backed path via
        // the storage properties. Returns the TFileType enum NAME (the SPI stays Thrift-free). Mirrors
        // legacy IcebergTableSink.bindDataSink's
        // LocationPath.of(originalLocation, storagePropertiesMap).getTFileTypeForBE().
        Map<StorageTypeId, StorageAdapter> vended = buildVendedStorageMap(rawVendedCredentials);
        Map<StorageTypeId, StorageAdapter> effective =
                vended != null ? vended : storagePropertiesSupplier.get();
        return LocationPath.ofAdapters(rawUri, effective).getTFileTypeForBE().name();
    }

    @Override
    public List<ConnectorBrokerAddress> getBrokerAddresses() {
        // Engine-side resolution of the catalog's broker backend (the connector cannot reach BrokerMgr /
        // bindBrokerName). Mirrors legacy BaseExternalTableDataSink.getBrokerAddresses: the catalog's bound
        // broker name -> getBrokers(name) (or getAllBrokers() when unbound) -> host/port, shuffled for
        // load-balance. Returns empty when none is alive; the connector turns that into a fail-loud
        // "No alive broker." for a FILE_BROKER write (this hook is only consulted for that target).
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        String bindBroker = catalog instanceof ExternalCatalog
                ? ((ExternalCatalog) catalog).bindBrokerName() : null;
        List<FsBroker> brokers = bindBroker != null
                ? Env.getCurrentEnv().getBrokerMgr().getBrokers(bindBroker)
                : Env.getCurrentEnv().getBrokerMgr().getAllBrokers();
        if (brokers == null || brokers.isEmpty()) {
            return Collections.emptyList();
        }
        Collections.shuffle(brokers);
        List<ConnectorBrokerAddress> result = new ArrayList<>(brokers.size());
        for (FsBroker broker : brokers) {
            result.add(new ConnectorBrokerAddress(broker.host, broker.port));
        }
        return result;
    }

    @Override
    public void cleanupEmptyManagedLocation(String location, List<String> tableChildDirs) {
        // Engine-side companion to a connector drop: prune the empty directory shells the connector's drop
        // leaves behind. The connector decides WHEN (e.g. iceberg HMS-only) and captures the location before
        // the drop; here we own the fe-filesystem machinery it cannot reach (SpiSwitchingFileSystem from the
        // catalog's storage properties). Best-effort: a missing storage binding or any IO failure is logged,
        // never propagated — cleanup is cosmetic and must not fail the completed drop. Conservative: a
        // directory is removed only when it contains no files (deleteEmptyDirectory aborts on the first file).
        if (Strings.isNullOrEmpty(location)) {
            return;
        }
        Map<StorageTypeId, StorageAdapter> storageProperties = storagePropertiesSupplier.get();
        if (storageProperties == null || storageProperties.isEmpty()) {
            return;
        }
        try (FileSystem fs = new SpiSwitchingFileSystem(storageProperties)) {
            boolean deleted = (tableChildDirs == null || tableChildDirs.isEmpty())
                    ? deleteEmptyDirectory(fs, Location.of(location))
                    : deleteEmptyTableLocation(fs, Location.of(location), tableChildDirs);
            if (deleted) {
                LOG.info("Cleaned empty managed location {}", location);
            } else {
                LOG.info("Skip cleaning managed location {}, it still contains files", location);
            }
        } catch (Exception e) {
            LOG.warn("Failed to clean managed location {} after drop", location, e);
        }
    }

    /**
     * Deletes the engine-format child directories ({@code tableChildDirs}, e.g. iceberg
     * {@code ["data", "metadata"]}) under {@code location} first, then {@code location} itself — each only
     * when empty. Port of legacy {@code IcebergMetadataOps.deleteEmptyTableLocation}.
     */
    @VisibleForTesting
    static boolean deleteEmptyTableLocation(FileSystem fs, Location location, List<String> tableChildDirs)
            throws IOException {
        for (String childDir : tableChildDirs) {
            if (!deleteEmptyDirectory(fs, location.resolve(childDir))) {
                return false;
            }
        }
        return deleteEmptyDirectory(fs, location);
    }

    /**
     * Recursively removes {@code location} iff it (transitively) contains no files: it aborts (returns
     * {@code false}) on the first non-directory entry, so live data is never deleted. Port of legacy
     * {@code IcebergMetadataOps.deleteEmptyDirectory}.
     */
    @VisibleForTesting
    static boolean deleteEmptyDirectory(FileSystem fs, Location location) throws IOException {
        if (!fs.exists(location)) {
            return true;
        }
        List<Location> childDirectories = new ArrayList<>();
        try (FileIterator iterator = fs.list(location)) {
            while (iterator.hasNext()) {
                FileEntry entry = iterator.next();
                if (!entry.isDirectory()) {
                    return false;
                }
                childDirectories.add(entry.location());
            }
        }
        for (Location childDirectory : childDirectories) {
            if (!deleteEmptyDirectory(fs, childDirectory)) {
                return false;
            }
        }
        return deleteEmptyDirectoryMarker(fs, location);
    }

    /** Deletes the (empty) directory marker for {@code location}. Port of legacy {@code IcebergMetadataOps}. */
    private static boolean deleteEmptyDirectoryMarker(FileSystem fs, Location location) throws IOException {
        Location directoryMarker = Location.of(withTrailingSlash(location.uri()));
        try {
            fs.delete(directoryMarker, false);
        } catch (IOException e) {
            return !fs.exists(location);
        }
        return !fs.exists(location);
    }

    private static String withTrailingSlash(String uri) {
        return uri.endsWith("/") ? uri : uri + "/";
    }

    /**
     * The fe.conf values forwarded to every connector.
     *
     * <p><b>Do not add to this.</b> A key here is an engine change per connector setting, and every one
     * of them ties a connector's key name into fe-core. A connector that needs a deployment-level
     * setting declares it in its own {@code <name>.conf} instead, which the engine reads generically —
     * see {@code ConnectorContext.getConnectorConfig()} and {@code ConnectorConf.get}. What is left
     * below is either shared by several connectors (the jdbc/hive metastore keys, kept as the fallback
     * for deployments that have not moved to the per-plugin files) or not a connector setting at all
     * ({@code doris_home}, {@code doris_version}).
     */
    private static Map<String, String> buildEnvironment() {
        Map<String, String> env = new HashMap<>();
        String dorisHome = EnvUtils.getDorisHome();
        if (dorisHome != null) {
            env.put("doris_home", dorisHome);
        }
        // HMS resources may be read before storage binding publishes this process-global FE setting.
        env.put("hadoop_config_dir", Config.hadoop_config_dir);
        env.put("jdbc_drivers_dir", Config.jdbc_drivers_dir);
        env.put("force_sqlserver_jdbc_encrypt_false",
                String.valueOf(Config.force_sqlserver_jdbc_encrypt_false));
        // HMS metastore client socket-timeout default (C4): the metastore-spi cannot read FE Config
        // (no fe-common dependency), so the FE-configured value is threaded through the environment and
        // applied by HmsMetaStoreProperties.toHiveConfOverrides when the user has not overridden it.
        env.put("hive_metastore_client_timeout_second",
                String.valueOf(Config.hive_metastore_client_timeout_second));
        // Hive CREATE TABLE defaults (P7.1): the fe-connector-hive plugin cannot read FE Config, so the two
        // FE-global CREATE TABLE toggles are threaded through the environment (not persisted into the catalog
        // property map) and applied by HiveConnectorMetadata.createTable when the user did not override them.
        // Keys must stay byte-identical to the reads in the hive plugin (HmsConf / HiveConnectorMetadata).
        env.put("hive_default_file_format", Config.hive_default_file_format);
        env.put("enable_create_hive_bucket_table", String.valueOf(Config.enable_create_hive_bucket_table));
        // Build version stamped into a created Hive table's doris.version parameter (legacy
        // ExternalCatalog.DORIS_VERSION_VALUE); the plugin cannot import fe-common Version.
        env.put("doris_version", Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
        // The trino-connector plugin runs in an isolated classloader and cannot read FE
        // Config (it would see its own bundled copy with default values). Pass the
        // configured plugin dir through the engine environment instead.
        env.put("trino_connector_plugin_dir", Config.trino_connector_plugin_dir);
        return Collections.unmodifiableMap(env);
    }
}

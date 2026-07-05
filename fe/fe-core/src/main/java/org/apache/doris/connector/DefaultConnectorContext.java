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
import org.apache.doris.common.CatalogConfigFileUtils;
import org.apache.doris.common.Config;
import org.apache.doris.common.EnvUtils;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetaInvalidator;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.SpiSwitchingFileSystem;
import org.apache.doris.kerberos.ExecutionAuthenticator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import java.util.stream.Collectors;

/**
 * Default implementation of {@link ConnectorContext}.
 *
 * <p>Provides the minimal catalog-level context that connector providers need
 * during creation. Additional context fields can be added here as the SPI evolves.
 */
public class DefaultConnectorContext implements ConnectorContext {

    private static final Logger LOG = LogManager.getLogger(DefaultConnectorContext.class);

    private static final ExecutionAuthenticator NOOP_AUTH = new ExecutionAuthenticator() {};

    private final String catalogName;
    private final long catalogId;
    private final Map<String, String> environment;
    private final Supplier<ExecutionAuthenticator> authSupplier;
    // Lazily supplies the catalog's static storage-properties map for storage-URI normalization
    // (FIX-URI-NORMALIZE). Invoked at scan time only (catalog fully initialized). Empty for ctors
    // that do not wire it — those callers (non-plugin catalogs) never invoke normalizeStorageUri.
    private final Supplier<Map<StorageProperties.Type, StorageProperties>> storagePropertiesSupplier;
    // Supplies the catalog's effective raw storage map (persisted props + derived defaults, empty when the
    // connector supplies vended credentials) for direct fe-filesystem binding in getStorageProperties()
    // (design S2): no fe-core StorageProperties parse on the connector storage path. Empty for ctors that do
    // not wire it (non-plugin / 2-3-4-arg) — those yield an empty storage list, correct parity.
    private final Supplier<Map<String, String>> rawStoragePropsSupplier;

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

    public DefaultConnectorContext(String catalogName, long catalogId,
            Supplier<ExecutionAuthenticator> authSupplier) {
        this(catalogName, catalogId, authSupplier, Collections::emptyMap);
    }

    public DefaultConnectorContext(String catalogName, long catalogId,
            Supplier<ExecutionAuthenticator> authSupplier,
            Supplier<Map<StorageProperties.Type, StorageProperties>> storagePropertiesSupplier) {
        this(catalogName, catalogId, authSupplier, storagePropertiesSupplier, Collections::emptyMap);
    }

    public DefaultConnectorContext(String catalogName, long catalogId,
            Supplier<ExecutionAuthenticator> authSupplier,
            Supplier<Map<StorageProperties.Type, StorageProperties>> storagePropertiesSupplier,
            Supplier<Map<String, String>> rawStoragePropsSupplier) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.catalogId = catalogId;
        this.authSupplier = Objects.requireNonNull(authSupplier, "authSupplier");
        this.storagePropertiesSupplier =
                Objects.requireNonNull(storagePropertiesSupplier, "storagePropertiesSupplier");
        this.rawStoragePropsSupplier =
                Objects.requireNonNull(rawStoragePropsSupplier, "rawStoragePropsSupplier");
        this.environment = buildEnvironment();
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
    public ConnectorMetaInvalidator getMetaInvalidator() {
        return new ExternalMetaCacheInvalidator(catalogId);
    }

    @Override
    public String sanitizeJdbcUrl(String jdbcUrl) {
        try {
            return SecurityChecker.getInstance().getSafeJdbcUrl(jdbcUrl);
        } catch (Exception e) {
            throw new RuntimeException("JDBC URL security check failed: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        return authSupplier.get().execute(task);
    }

    @Override
    public Map<String, String> loadHiveConfResources(String resources) {
        if (Strings.isNullOrEmpty(resources)) {
            return Collections.emptyMap();
        }
        // Reuse the EXACT legacy loader (same hadoop_config_dir base, comma-split, fail-if-missing)
        // so the file-resolution semantics are byte-identical to legacy HMSBaseProperties; only the
        // resolved key/values cross into the connector (no HiveConf/Configuration identity hazard).
        HiveConf hc = CatalogConfigFileUtils.loadHiveConfFromHiveConfDir(resources);
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, String> e : hc) {   // HiveConf IS-A Iterable<Map.Entry<String,String>>
            out.put(e.getKey(), e.getValue());
        }
        return out;
    }

    @Override
    public Map<String, String> vendStorageCredentials(Map<String, String> rawVendedCredentials) {
        // Map the per-table vended token to the BE-facing AWS_* properties. Fail-soft (empty) on any
        // error, matching the legacy provider, so a malformed token degrades gracefully rather than
        // killing the scan. The outer try also covers getBackendPropertiesFromStorageMap so the
        // fail-soft boundary is byte-identical to the pre-refactor method; buildVendedStorageMap shares
        // the typed-map build with normalizeStorageUri (single source of truth — no drift).
        try {
            Map<StorageProperties.Type, StorageProperties> map = buildVendedStorageMap(rawVendedCredentials);
            return map == null ? Collections.emptyMap()
                    : CredentialUtils.getBackendPropertiesFromStorageMap(map);
        } catch (Exception e) {
            LOG.warn("Failed to normalize vended credentials", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Builds the vended {@link StorageProperties} typed map from a raw per-table token: filter to
     * cloud-storage props, run {@link StorageProperties#createAll} (normalizes arbitrary token key
     * shapes + derives region/endpoint), then index by {@link StorageProperties.Type}. Mirrors the
     * legacy {@code AbstractVendedCredentialsProvider} tail exactly, so the BE-credential overlay
     * ({@link #vendStorageCredentials}) and the URI normalization ({@link #normalizeStorageUri(String,
     * Map)}) derive the SAME credentials from the SAME token — no drift. Returns {@code null} when the
     * token is null/empty, yields no cloud-storage props, or normalization throws — replicating the
     * legacy provider's "return null → Factory falls back to the base/static map" contract.
     */
    private Map<StorageProperties.Type, StorageProperties> buildVendedStorageMap(
            Map<String, String> rawVendedCredentials) {
        if (rawVendedCredentials == null || rawVendedCredentials.isEmpty()) {
            return null;
        }
        try {
            Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawVendedCredentials);
            if (filtered.isEmpty()) {
                return null;
            }
            List<StorageProperties> vended = StorageProperties.createAll(filtered);
            return vended.stream()
                    .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
        } catch (Exception e) {
            LOG.warn("Failed to normalize vended credentials", e);
            return null;
        }
    }

    @Override
    public Map<String, String> getBackendStorageProperties() {
        // Mirror legacy PaimonScanNode.getLocationProperties(): translate the catalog's parsed
        // StorageProperties map into BE-canonical scan keys (AWS_* for object stores, hadoop/dfs for
        // HDFS) via the SAME CredentialUtils.getBackendPropertiesFromStorageMap legacy/iceberg/hive use
        // — single source of truth, no drift. The map is already validated at catalog creation, so this
        // does not throw; an empty map (non-plugin ctor / local-FS warehouse) yields an empty result
        // (no overlay) — correct parity, unlike normalizeStorageUri which must fail-loud on a bad path.
        return CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesSupplier.get());
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
        // scheme-dispatched S3 factory can open the file. The storage map follows legacy
        // VendedCredentialsFactory precedence: when the connector supplies a per-table vended token
        // (REST catalogs, whose static map is empty by design) the VENDED map REPLACES the static map;
        // otherwise the catalog's static storage map is used. Fail-loud (StoragePropertiesException
        // propagates) — a path that cannot be normalized would otherwise silently corrupt reads (esp. a
        // deletion-vector path on merge-on-read). Single source of truth: the SAME LocationPath
        // normalization legacy/iceberg/hive use, so no drift.
        Map<StorageProperties.Type, StorageProperties> vended = buildVendedStorageMap(rawVendedCredentials);
        Map<StorageProperties.Type, StorageProperties> effective =
                vended != null ? vended : storagePropertiesSupplier.get();
        return LocationPath.of(rawUri, effective).toStorageLocation().toString();
    }

    @Override
    public String getBackendFileType(String rawUri, Map<String, String> rawVendedCredentials) {
        // Same LocationPath build as normalizeStorageUri (vended-aware), then read the BE file type from
        // it — authoritative over the scheme-only default because it also detects a broker-backed path via
        // the storage properties. Returns the TFileType enum NAME (the SPI stays Thrift-free). Mirrors
        // legacy IcebergTableSink.bindDataSink's
        // LocationPath.of(originalLocation, storagePropertiesMap).getTFileTypeForBE().
        Map<StorageProperties.Type, StorageProperties> vended = buildVendedStorageMap(rawVendedCredentials);
        Map<StorageProperties.Type, StorageProperties> effective =
                vended != null ? vended : storagePropertiesSupplier.get();
        return LocationPath.of(rawUri, effective).getTFileTypeForBE().name();
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
        Map<StorageProperties.Type, StorageProperties> storageProperties = storagePropertiesSupplier.get();
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

    private static Map<String, String> buildEnvironment() {
        Map<String, String> env = new HashMap<>();
        String dorisHome = EnvUtils.getDorisHome();
        if (dorisHome != null) {
            env.put("doris_home", dorisHome);
        }
        env.put("jdbc_drivers_dir", Config.jdbc_drivers_dir);
        env.put("force_sqlserver_jdbc_encrypt_false",
                String.valueOf(Config.force_sqlserver_jdbc_encrypt_false));
        env.put("jdbc_driver_secure_path", Config.jdbc_driver_secure_path);
        // HMS metastore client socket-timeout default (C4): the metastore-spi cannot read FE Config
        // (no fe-common dependency), so the FE-configured value is threaded through the environment and
        // applied by HmsMetaStoreProperties.toHiveConfOverrides when the user has not overridden it.
        env.put("hive_metastore_client_timeout_second",
                String.valueOf(Config.hive_metastore_client_timeout_second));
        // The trino-connector plugin runs in an isolated classloader and cannot read FE
        // Config (it would see its own bundled copy with default values). Pass the
        // configured plugin dir through the engine environment instead.
        env.put("trino_connector_plugin_dir", Config.trino_connector_plugin_dir);
        return Collections.unmodifiableMap(env);
    }
}

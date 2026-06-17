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

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.CatalogConfigFileUtils;
import org.apache.doris.common.Config;
import org.apache.doris.common.EnvUtils;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetaInvalidator;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemFactory;

import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.catalogId = catalogId;
        this.authSupplier = Objects.requireNonNull(authSupplier, "authSupplier");
        this.storagePropertiesSupplier =
                Objects.requireNonNull(storagePropertiesSupplier, "storagePropertiesSupplier");
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
        // Hand the connector the catalog's storage bound as typed fe-filesystem StorageProperties
        // (design D-003): the connector derives its Hadoop/HiveConf config and BE creds from these
        // without importing fe-core or any provider. Source the catalog raw map from the existing
        // storage supplier's getOrigProps() (every parsed StorageProperties carries the full catalog
        // map -- StorageProperties.createAll passes it through), then bind it via the live
        // plugin-loaded FileSystemPluginManager. An empty supplier (non-plugin ctor / REST-vended /
        // credential-less warehouse) yields an empty list -- no static storage, correct parity.
        Map<StorageProperties.Type, StorageProperties> typed = storagePropertiesSupplier.get();
        if (typed == null || typed.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, String> rawCatalogProps = typed.values().iterator().next().getOrigProps();
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
        // The trino-connector plugin runs in an isolated classloader and cannot read FE
        // Config (it would see its own bundled copy with default values). Pass the
        // configured plugin dir through the engine environment instead.
        env.put("trino_connector_plugin_dir", Config.trino_connector_plugin_dir);
        return Collections.unmodifiableMap(env);
    }
}

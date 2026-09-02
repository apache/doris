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

package org.apache.doris.connector.spi;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * The storage and backend-facing half of a catalog's engine services: credential normalization, URI
 * normalization, the engine-owned filesystem, broker addresses, backend probes, and managed-location
 * cleanup. Reached through {@link ConnectorContext#getStorageContext()}.
 *
 * <p>These live apart from {@link ConnectorContext} because most connectors have no storage at all — a
 * JDBC, Elasticsearch, MaxCompute or Trino-connector implementation never calls one of these methods, and
 * splitting them keeps the context a new connector author reads down to the services that apply to every
 * connector. The engine implements this; connectors consume it.
 *
 * <p><b>Add a new storage service here, not on {@link ConnectorContext}.</b> That is what keeps the count
 * of places to touch at two — this interface and the engine implementation. A decorator of
 * {@link ConnectorContext} forwards a single {@link ConnectorContext#getStorageContext()} and is
 * structurally unable to lose a storage call, however many are added.
 *
 * <p><b>Classloader pinning.</b> No method here runs plugin code — every one of them executes entirely on
 * the engine side — which is why the plugin-pinning decorators do not wrap this object at all. A future
 * method that CAN run plugin code breaks that assumption: it would need those decorators to override
 * {@link ConnectorContext#getStorageContext()} and return a pinning wrapper of their own. There is no such
 * method today, so no such wrapper exists.
 */
public interface ConnectorStorageContext {

    /**
     * The context for a catalog whose storage the engine does not manage. Every method keeps its interface
     * default, so a connector reaching a service that is not there gets the same benign answer it would get
     * from a context that simply did not override it.
     */
    ConnectorStorageContext NOOP = new ConnectorStorageContext() {
    };

    /**
     * Normalizes raw per-table vended cloud-storage credentials (the token map a REST catalog
     * returns, e.g. {@code fs.oss.accessKeyId} / {@code s3.access-key}) into the BE-facing storage
     * property map ({@code AWS_ACCESS_KEY} / {@code AWS_SECRET_KEY} / {@code AWS_TOKEN} /
     * {@code AWS_ENDPOINT} / {@code AWS_REGION}; Azure uses its provider-owned
     * {@code AZURE_*} equivalents). The connector extracts the raw token from the live
     * table (paimon SDK only); the engine performs the same {@code StorageProperties} normalization
     * it uses for static catalog credentials (the connector cannot import fe-core).
     *
     * <p>The default returns empty (no normalization machinery / empty input), so every other
     * connector is unaffected.
     *
     * @param rawVendedCredentials the raw per-table token map (may be null/empty)
     * @return the BE-facing normalized storage-property map, or empty when none
     */
    default Map<String, String> vendStorageCredentials(Map<String, String> rawVendedCredentials) {
        return Collections.emptyMap();
    }

    /**
     * Normalizes a raw storage URI a connector emits (e.g. a paimon native data-file or
     * deletion-vector path such as {@code oss://…}, {@code cos://…}, {@code obs://…}, {@code s3a://…},
     * or the OSS {@code bucket.endpoint} authority form) into BE's canonical, scheme-dispatched form
     * ({@code s3://…}) using the catalog's storage properties. BE's file factory only recognizes the
     * canonical scheme, so a connector that hands native file paths to BE MUST route them through this
     * hook; otherwise the native read fails (data file) or silently returns wrong rows (deletion
     * vector / merge-on-read). The connector cannot perform this itself (it must not import fe-core's
     * {@code LocationPath} / {@code StorageProperties}); the engine applies the same normalization it
     * uses for static catalog paths.
     *
     * <p>The default returns the input unchanged (no normalization machinery), so every other
     * connector — and any URI already in canonical form — is unaffected.
     *
     * @param rawUri the raw storage URI (null/blank is returned unchanged)
     * @return the normalized BE-facing URI
     * @throws RuntimeException if normalization fails (fail-loud, legacy parity — a wrong path would
     *         otherwise silently corrupt reads rather than surface the misconfiguration)
     */
    default String normalizeStorageUri(String rawUri) {
        return rawUri;
    }

    /**
     * Vended-credential-aware variant of {@link #normalizeStorageUri(String)}. For a REST catalog the
     * catalog's <em>static</em> storage map is empty by design (vended creds are per-table/dynamic), so
     * the single-arg form would throw on an object-store path. This overload lets the connector pass the
     * raw per-table vended token (the same map it gives {@link #vendStorageCredentials}); the engine
     * normalizes the URI against the vended credentials when present and falls back to the static map
     * otherwise (legacy {@code VendedCredentialsFactory} precedence: vended replaces static).
     *
     * <p>The default ignores the token and delegates to {@link #normalizeStorageUri(String)}, so every
     * connector that has no vended credentials — and the no-op default — is unaffected.
     *
     * @param rawUri               the raw storage URI (null/blank is returned unchanged)
     * @param rawVendedCredentials the raw per-table vended token map (may be null/empty → static path)
     * @return the normalized BE-facing URI
     * @throws RuntimeException if normalization fails (fail-loud, legacy parity)
     */
    default String normalizeStorageUri(String rawUri, Map<String, String> rawVendedCredentials) {
        return normalizeStorageUri(rawUri);
    }

    /**
     * Scan-scoped batch form of {@link #normalizeStorageUri(String, Map)}: derives the vended storage
     * configuration from the (scan-invariant) per-table token ONCE and returns a normalizer that applies
     * it to many raw URIs cheaply. A vended-credentials scan normalizes O(N_files + N_deletes) paths but
     * the token→storage-config derivation ({@code StorageProperties.createAll} + a hadoop config build) is
     * a pure function of the token, so hoisting it out of the per-file loop turns O(N) heavy derivations
     * into one. The connector builds the normalizer once (where it extracts the token) and reuses it for
     * every data/delete/position-delete path in the scan.
     *
     * <p>The default returns a normalizer that delegates per call to {@link #normalizeStorageUri(String,
     * Map)} — behavior-identical, no hoist — so a connector with no engine context (offline unit tests)
     * and any connector that does not override the engine side are unaffected. The engine
     * ({@code DefaultConnectorContext}) overrides this to perform the actual once-per-scan derivation.
     *
     * @param rawVendedCredentials the raw per-table vended token map (may be null/empty → static path)
     * @return a URI normalizer for this scan; each application is byte-identical to
     *         {@link #normalizeStorageUri(String, Map)} with the same token
     */
    default UnaryOperator<String> newStorageUriNormalizer(Map<String, String> rawVendedCredentials) {
        return rawUri -> normalizeStorageUri(rawUri, rawVendedCredentials);
    }

    /**
     * Resolves the BE-facing file type (a {@code TFileType} enum name, e.g. {@code "FILE_S3"}) for a raw
     * storage URI a connector emits (e.g. an iceberg write output path). A write-side analogue of
     * {@link #normalizeStorageUri(String, Map)}: a connector that hands an output location to a BE table
     * sink must tell BE which file-system family to open it with, and that decision (object store vs HDFS
     * vs local vs broker) lives in the engine's {@code LocationPath} together with the catalog's storage
     * properties — which the connector must not import. The result is the enum <em>name</em> (a plain
     * String) so this SPI stays Thrift-free, exactly like {@link #normalizeStorageUri}; the connector,
     * which has the Thrift types, maps it back. The engine resolves it the same way it does for a legacy
     * external-table sink.
     *
     * <p>The default derives the type from the URI scheme alone (object-store schemes → {@code FILE_S3},
     * {@code hdfs}/{@code viewfs} → {@code FILE_HDFS}, {@code file} or no scheme → {@code FILE_LOCAL}); it
     * has no storage-property machinery and so cannot detect a broker-backed path — the engine override
     * does. Mirrors the vended-aware normalization: the same raw per-table vended token is accepted so a
     * REST catalog (empty static map) still resolves.
     *
     * @param rawUri               the raw storage URI
     * @param rawVendedCredentials the raw per-table vended token map (may be null/empty → static path)
     * @return the BE file type enum name for the URI
     */
    default String getBackendFileType(String rawUri, Map<String, String> rawVendedCredentials) {
        if (rawUri == null) {
            return "FILE_LOCAL";
        }
        int schemeEnd = rawUri.indexOf("://");
        if (schemeEnd < 0) {
            return "FILE_LOCAL";
        }
        String scheme = rawUri.substring(0, schemeEnd).toLowerCase();
        if ("hdfs".equals(scheme) || "viewfs".equals(scheme)) {
            return "FILE_HDFS";
        }
        if ("file".equals(scheme)) {
            return "FILE_LOCAL";
        }
        return "FILE_S3";
    }

    /**
     * Resolves the broker backend addresses bound to this catalog (host + port), for a write whose
     * {@link #getBackendFileType} resolved to {@code FILE_BROKER} (e.g. an {@code ofs://} / {@code gfs://}
     * iceberg write). A write-side companion to {@link #getBackendFileType}: a connector that hands a
     * broker-backed output location to a BE table sink must also tell BE which brokers to open it through,
     * and the broker registry (alive instances) + the catalog's bound broker name live in the engine, which
     * the connector must not import. Returns neutral {@link ConnectorBrokerAddress} host/port pairs so this
     * SPI stays Thrift-free — the connector, which has the Thrift types, maps them to {@code TNetworkAddress},
     * exactly like it maps the {@link #getBackendFileType} String back to {@code TFileType}.
     *
     * <p>The engine override resolves the catalog's bound broker (or any alive broker when none is bound) and
     * shuffles for load-balance, mirroring legacy write planning ({@code BaseExternalTableDataSink}); the
     * connector applies these only for a {@code FILE_BROKER} target and fails loud when the resolved set is
     * empty. The default returns empty (no broker machinery), so every non-broker write — and every other
     * connector — is unaffected.
     *
     * @return the catalog's broker backend addresses, or empty when none
     */
    default List<ConnectorBrokerAddress> getBrokerAddresses() {
        return Collections.emptyList();
    }

    /**
     * Returns the catalog's static storage credentials/config normalized to BE-canonical scan
     * properties: object-store creds as {@code AWS_ACCESS_KEY} / {@code AWS_SECRET_KEY} /
     * {@code AWS_TOKEN} / {@code AWS_ENDPOINT} / {@code AWS_REGION} (Azure uses provider-owned
     * {@code AZURE_*} keys), and HDFS config as the resolved
     * {@code hadoop.*} / {@code dfs.*} keys (user overrides plus the legacy-derived defaults). The
     * engine runs the same {@code CredentialUtils.getBackendPropertiesFromStorageMap} that legacy /
     * iceberg / hive use over the catalog's parsed {@code StorageProperties} map — the single source of
     * truth — so there is no re-ported normalization that could drift.
     *
     * <p>BE's native (FILE_S3) reader understands ONLY these canonical keys. A connector that copies
     * the raw catalog aliases ({@code s3.access_key}, {@code oss.access_key}, …) to BE hands the native
     * reader no usable credentials → 403 on a private bucket. A connector that emits static storage
     * props to BE MUST source them from this hook.
     *
     * <p>The default returns empty (no normalization machinery / no storage map), so every other
     * connector — and any credential-less (e.g. local-filesystem) warehouse — is unaffected.
     *
     * @return the BE-facing normalized storage-property map, or empty when none
     */
    default Map<String, String> getBackendStorageProperties() {
        return Collections.emptyMap();
    }

    /**
     * Asks one alive backend to reach the given storage location, so a {@code test_connection=true}
     * CREATE CATALOG fails on a warehouse that FE can read but BE cannot (a different network, a
     * different credential set). The FE-side probe a connector runs itself cannot catch that.
     *
     * <p>The engine owns the round-trip (picking a live backend, the RPC, the status check) because it
     * needs the backend registry and the client pool, which no plugin can see. It does not interpret the
     * payload: {@code storageBackendTypeValue} is the connector's own {@code TStorageBackendType} enum
     * value and {@code backendProperties} the BE-facing property map, sourced from
     * {@link #getBackendStorageProperties()} / {@link #getStorageProperties()}. Callers targeting S3 must
     * include a {@code test_location} entry — BE requires it.
     *
     * <p>The default does nothing (no backend fleet, e.g. in connector unit tests), matching the legacy
     * behavior of skipping the probe when no backend is alive.
     *
     * @param storageBackendTypeValue the {@code TStorageBackendType} value BE should probe with
     * @param backendProperties       BE-facing storage properties (credentials, endpoint, test_location)
     * @throws Exception if the backend reports the storage unreachable
     */
    default void testBackendStorageConnectivity(int storageBackendTypeValue,
            Map<String, String> backendProperties) throws Exception {
        // Default: no backend fleet to ask -> skip.
    }

    /**
     * Returns the catalog's static storage configuration as a list of typed, already-bound
     * {@link StorageProperties} (the fe-filesystem API contract). fe-core binds the catalog's raw
     * properties against the registered filesystem providers and hands the result down here, so a
     * connector can derive both its Hadoop/{@code HiveConf} config
     * ({@code toHadoopProperties().toHadoopConfigurationMap()}) and its BE-facing credentials
     * ({@code toBackendProperties().toMap()}) without importing fe-core or any storage provider —
     * it sees only the {@code fe-filesystem-api} interface.
     *
     * <p>One entry per configured backend (e.g. an object store, plus HDFS when present), mirroring
     * the engine's parsed storage list. HDFS has a typed model and contributes its
     * {@code hadoop.config.resources} XML + HA + auth keys via {@code toHadoopProperties()} (C2);
     * backends with no typed model (broker/local) are absent and the connector handles those via its own
     * raw {@code fs.}/{@code dfs.}/{@code hadoop.} passthrough.
     *
     * <p>The default returns an empty list (no storage machinery), so every other connector — and any
     * credential-less warehouse — is unaffected.
     *
     * @return the catalog's typed storage properties, or an empty list when none
     */
    default List<StorageProperties> getStorageProperties() {
        return Collections.emptyList();
    }

    /**
     * Returns the engine's {@link FileSystem} for this catalog — a scheme-routing handle backed by the
     * catalog's parsed {@link #getStorageProperties() storage properties} and the registered fe-filesystem
     * providers (hdfs/s3/oss/cos/obs/azure/http/local/broker). A connector uses it to list, read, and write
     * table data without bundling any Hadoop {@code FileSystem} implementation itself; the engine owns scheme
     * routing and per-scheme classloader pinning, exactly as Trino's {@code TrinoFileSystemFactory.create(session)}
     * hands the connector a {@code TrinoFileSystem}.
     *
     * <p><b>Ownership.</b> The returned filesystem is <em>engine-owned and connector-borrowed</em>: the engine
     * builds and caches it per catalog and closes it when the catalog/context is torn down. A connector MUST NOT
     * call {@link FileSystem#close()} on it.
     *
     * <p><b>Identity.</b> The {@code session} parameter mirrors Trino's {@code create(ConnectorSession)} shape and
     * reserves per-user identity via {@link ConnectorSession#getUser()}. The current implementation resolves the
     * filesystem at catalog granularity (the session is not yet used to key a per-user filesystem); when per-user
     * identity lands, the engine will key the cache by identity.
     *
     * <p>The default returns {@code null} (no engine-managed filesystem), so connectors that do not use it — and
     * the no-op default context — are unaffected, matching the benign default of
     * {@link #getBackendStorageProperties()}.
     *
     * @param session the query/connector session (reserved for per-user identity; may be null for catalog-level use)
     * @return the catalog's engine-owned {@link FileSystem}, or {@code null} when the engine manages no storage
     */
    default FileSystem getFileSystem(ConnectorSession session) {
        return null;
    }

    /**
     * Best-effort removal of the EMPTY directory shells left behind after a connector drops a managed
     * table or database. The data + metadata FILES are already deleted by the connector's own drop (e.g.
     * iceberg {@code dropTable(purge=true)}); this only prunes now-empty directories (the parent table /
     * database location, descending {@code tableChildDirs} first). A directory is removed ONLY when it
     * contains no files — never recursively deleting live data.
     *
     * <p>The connector decides WHEN to call this (e.g. iceberg only for HMS-managed locations) and captures
     * {@code location} BEFORE the drop; the engine owns the {@code fe-filesystem} machinery to build a
     * {@code FileSystem} from the catalog's storage properties (which the connector cannot construct). Any
     * failure is swallowed (logged) — cleanup is cosmetic and must never fail the drop.
     *
     * <p>The default is a no-op, so connectors whose engine does not manage storage cleanup are unaffected.
     *
     * @param location       the table/database root location to prune (no-op when blank)
     * @param tableChildDirs engine-format child directories to descend first (e.g. iceberg
     *                       {@code ["data", "metadata"]}); empty/{@code null} for a database/namespace root
     */
    default void cleanupEmptyManagedLocation(String location, List<String> tableChildDirs) {
        // no-op: the engine that manages storage overrides this.
    }
}

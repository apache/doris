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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.UnaryOperator;

/**
 * Runtime context provided by fe-core to connector implementations.
 * Provides access to engine-level services.
 */
public interface ConnectorContext {

    /** Returns the catalog name. */
    String getCatalogName();

    /** Returns the catalog ID. */
    long getCatalogId();

    /**
     * Returns engine-level environment properties that connectors may need.
     * These are system configurations from the FE, not catalog properties.
     *
     * <p>Known keys include:
     * <ul>
     *   <li>{@code doris_home} — the DORIS_HOME path</li>
     *   <li>{@code jdbc_drivers_dir} — the configured JDBC drivers directory</li>
     * </ul>
     */
    default Map<String, String> getEnvironment() {
        return Collections.emptyMap();
    }

    /**
     * Returns the HTTP security hook for SSRF protection.
     * Connectors making outbound HTTP requests should call this hook
     * before and after each request.
     */
    default ConnectorHttpSecurityHook getHttpSecurityHook() {
        return ConnectorHttpSecurityHook.NOOP;
    }

    /**
     * Sanitizes a JDBC URL according to engine-level security policies.
     * The engine may reject URLs that target internal networks, contain
     * banned parameters, or otherwise violate security rules.
     *
     * <p>Connectors MUST call this method before using any JDBC URL
     * to establish a database connection.
     *
     * @param jdbcUrl the raw JDBC URL
     * @return the sanitized URL (may be the same string if no changes needed)
     * @throws RuntimeException if the URL violates security policies
     */
    default String sanitizeJdbcUrl(String jdbcUrl) {
        return jdbcUrl;
    }

    /**
     * Executes a task within the catalog's authentication context.
     * For secured deployments (e.g., Kerberos), this wraps the call
     * with the appropriate UGI.doAs() or equivalent mechanism.
     *
     * <p>Connectors accessing secured external systems (e.g., Hive Metastore
     * with Kerberos) MUST use this method to wrap their external calls.</p>
     *
     * <p>The default implementation simply executes the task directly (simple auth).</p>
     *
     * @param task the task to execute within the authentication context
     * @param <T>  the return type of the task
     * @return the result of the task
     * @throws Exception if the task execution or authentication fails
     */
    default <T> T executeAuthenticated(Callable<T> task) throws Exception {
        return task.call();
    }

    /**
     * Returns the meta invalidator the connector can call to notify the engine
     * of external metadata changes (e.g. from HMS notification events).
     *
     * <p>Connectors that have no external change notifications can ignore this;
     * the default returns {@link ConnectorMetaInvalidator#NOOP}.</p>
     */
    default ConnectorMetaInvalidator getMetaInvalidator() {
        return ConnectorMetaInvalidator.NOOP;
    }

    /**
     * Builds a <em>sibling</em> connector of another catalog type on top of this same catalog's context, for a
     * heterogeneous "gateway" connector that serves more than one table format from a single catalog and must
     * delegate some tables to another format's connector (e.g. a Hive-metastore catalog whose Iceberg-registered
     * tables are served by the Iceberg connector).
     *
     * <p>The engine builds the sibling through the same connector factory it uses for a top-level catalog, so the
     * sibling's concrete class is loaded by <em>that type's own plugin classloader</em> — never co-packaged into
     * the caller's plugin (a duplicate native stack, e.g. a second AWS SDK, would poison shared JVM state). The
     * returned connector shares THIS context (same catalog id, authentication, and storage), so the sibling reuses
     * the caller's metastore/storage/credentials without re-deriving them.
     *
     * <p>fe-core stays connector-agnostic: this is a generic "give me a connector of type {@code catalogType} with
     * these {@code properties}" factory. The caller (the gateway connector) is responsible for synthesizing the
     * sibling's {@code properties} — the engine does not parse or translate them.
     *
     * <p><b>Cross-plugin type safety.</b> Because the sibling lives in a different (child-first) classloader, it is
     * type-compatible with the caller ONLY through the parent-first SPI interfaces ({@link Connector},
     * {@code ConnectorMetadata}, {@code ConnectorTableHandle}, …). The caller MUST hold the result as the bare
     * {@link Connector} interface and MUST NOT cast it — or any object it produces — to a concrete connector type,
     * or it will {@code ClassCastException} across the loader split.
     *
     * <p><b>Lifecycle.</b> The engine tracks and closes only a catalog's <em>primary</em> connector; a sibling built
     * here is owned by the caller, which MUST forward {@link Connector#close()} to it from its own {@code close()}.
     *
     * <p>The default returns {@code null} (no sibling support), so every connector that is not a gateway — and the
     * no-op default context — is unaffected.
     *
     * @param catalogType the sibling connector's type (e.g. {@code "iceberg"}); resolved by the same provider set
     *                    the engine uses for top-level catalogs
     * @param properties  the sibling connector's fully-synthesized catalog properties (caller-owned)
     * @return the sibling connector, or {@code null} when no provider matches {@code catalogType} (or the engine has
     *         no connector factory wired — e.g. the default context)
     */
    default Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
        return null;
    }

    /**
     * Normalizes raw per-table vended cloud-storage credentials (the token map a REST catalog
     * returns, e.g. {@code fs.oss.accessKeyId} / {@code s3.access-key}) into the BE-facing storage
     * property map ({@code AWS_ACCESS_KEY} / {@code AWS_SECRET_KEY} / {@code AWS_TOKEN} /
     * {@code AWS_ENDPOINT} / {@code AWS_REGION}). The connector extracts the raw token from the live
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
     * {@code AWS_TOKEN} / {@code AWS_ENDPOINT} / {@code AWS_REGION}, and HDFS config as the resolved
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

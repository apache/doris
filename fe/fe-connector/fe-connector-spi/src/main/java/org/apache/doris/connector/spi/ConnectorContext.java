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

import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

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
     * Resolves the catalog's {@code hive.conf.resources} (comma-separated hive-site.xml file names
     * under the FE's {@code hadoop_config_dir}) into a flat key-&gt;value map the connector can
     * overlay onto its {@code HiveConf}. The connector cannot perform this filesystem/Config-dir
     * resolution itself (it must not import fe-core/fe-common); the engine context loads the files
     * via {@code CatalogConfigFileUtils}, matching legacy HMS behavior.
     *
     * <p>The default returns empty (no external file support), so connectors that do not use it —
     * and every other connector — are unaffected.
     *
     * @param resources the raw {@code hive.conf.resources} value (may be null/blank)
     * @return a flat map of the resolved hive-site.xml key/values, or empty when none
     * @throws RuntimeException if a referenced file is missing/unreadable (fail-loud, legacy parity)
     */
    default Map<String, String> loadHiveConfResources(String resources) {
        return Collections.emptyMap();
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

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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Runtime context provided by fe-core to connector implementations.
 * Provides access to engine-level services.
 *
 * <p>Every service here applies to any connector, whatever it reads. The storage and backend-facing
 * services — which most connectors never touch — live on {@link ConnectorStorageContext}, reached through
 * {@link #getStorageContext()}. A new engine service belongs on whichever of the two its audience matches;
 * putting a storage service here would put it back in front of every connector that has no storage.
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
     * Resolves and materializes a JDBC driver through engine-owned storage services.
     *
     * <p>The checksum identifies immutable driver content. An engine implementation may return a
     * checksum-versioned local URL so lazy connector creation after replay or FE promotion does not
     * depend on a file left by the FE that originally created the catalog. The default returns
     * {@code null}; standalone connector tests and engines without this service retain the connector's
     * local directory resolution.
     */
    default String resolveJdbcDriverUrl(String driverUrl, String checksum) {
        return null;
    }

    /**
     * The contents of {@code <name>.conf} in this connector's own plugin directory, keys and values
     * verbatim, immutable. {@code <name>} is this connector's {@link ConnectorProvider#name()}.
     *
     * <p>This is a connector's <b>deployment-level</b> configuration channel: one per FE process,
     * maintained by an administrator in the plugin directory, and not settable by a user in
     * {@code CREATE CATALOG}. A value that varies per catalog belongs in the property map handed to
     * {@code ConnectorProvider.create}; a value that varies per query belongs in
     * {@code ConnectorSession.getSessionProperties()}.
     *
     * <p>Unlike {@link #getEnvironment()}, adding a key here costs the engine nothing: the file is named
     * after the plugin and parsed generically, so no key name of yours ever appears in {@code fe-core}.
     * Read it through {@link ConnectorConf#get}, which layers this map over {@code getEnvironment()} for
     * keys that predate this channel.
     *
     * <p>Never null. Returns an empty map when: the file does not exist; the file could not be read (the
     * engine has already logged an ERROR); or the connector was not loaded from a plugin directory at all
     * (a classpath built-in, or a provider registered by a test).
     *
     * <p>Engine side: {@code ConnectorPluginManager.loadPlugins} reads the file once, right after the
     * provider is admitted, and {@code ConnectorPluginManager.createConnector} attaches it to the context
     * it hands {@code ConnectorProvider.create}. Editing the file needs an FE restart, same as fe.conf.
     */
    default Map<String, String> getConnectorConfig() {
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
     * Sanitizes an outbound URL according to engine-level security policies. The engine may reject URLs
     * that target internal networks, contain banned parameters, or otherwise violate security rules. The
     * check is protocol-neutral; a JDBC URL is simply the case that exists today.
     *
     * <p><b>Scope.</b> A connector MUST route a URL through this hook when the connector itself opens the
     * connection. It cannot cover a connection opened <em>inside</em> a third-party SDK the connector hands
     * a user-supplied address to — an Iceberg JDBC metastore or a Paimon JDBC catalog builds its own
     * connection with no hook point the connector can reach — so those addresses do not pass through here.
     * Read this as "the engine's check applies where a connector connects", not as "every outbound address
     * in FE is checked".
     *
     * @param url the raw outbound URL
     * @return the sanitized URL (may be the same string if no changes needed)
     * @throws RuntimeException if the URL violates security policies
     */
    default String sanitizeOutboundUrl(String url) {
        return url;
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
     * This catalog's storage and backend-facing services. Never {@code null}: a catalog whose storage the
     * engine does not manage answers {@link ConnectorStorageContext#NOOP}, whose every method keeps its
     * interface default.
     *
     * <p>The returned object is stable for the life of the catalog, so a connector may take it once at
     * construction and hold it.
     */
    default ConnectorStorageContext getStorageContext() {
        return ConnectorStorageContext.NOOP;
    }
}

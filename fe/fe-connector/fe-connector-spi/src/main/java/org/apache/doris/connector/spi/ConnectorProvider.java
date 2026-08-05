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

import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SPI interface for connector provider discovery via Java ServiceLoader.
 *
 * <p>Extends {@link PluginFactory} to allow
 * {@link org.apache.doris.extension.loader.DirectoryPluginRuntimeManager}
 * to load connector providers from plugin directories at runtime.
 *
 * <p>Implementations must:
 * <ol>
 *   <li>Have a public no-arg constructor.</li>
 *   <li>Register in META-INF/services/org.apache.doris.connector.spi.ConnectorProvider.</li>
 *   <li>Have NO dependency on fe-core, fe-common, or fe-catalog.</li>
 * </ol>
 */
public interface ConnectorProvider extends PluginFactory {

    /**
     * Returns the connector type name (e.g., "hms", "iceberg", "es").
     * Corresponds to the {@code type} property in CREATE CATALOG.
     *
     * <p><b>Contract.</b> The name must be globally unique across all installed connectors (compared
     * case-insensitively) and must not be a catalog type the engine implements itself. fe-core enforces both
     * when a provider is discovered: one whose type name is already claimed, or which claims an engine
     * built-in type name, is refused — on the classpath that is a build error and fails loud, in a plugin
     * directory it is logged and skipped so that one bad plugin cannot stop FE from starting.
     *
     * <p>Uniqueness is not cosmetic. It is what {@code CREATE CATALOG} routes on, and it is what makes
     * source-prefixed namespaces distinct <em>by construction</em> (see {@code ConnectorStatementScopes},
     * which relies on this method being a connector's unique identity).
     */
    String getType();

    /**
     * Returns true if this provider can handle the given catalog type and properties.
     * Must be cheap (no network calls) and deterministic.
     */
    default boolean supports(String catalogType, Map<String, String> properties) {
        return getType().equalsIgnoreCase(catalogType);
    }

    /**
     * Returns true if this connector may appear as a standalone catalog, i.e. whether {@link #getType()} is a
     * type name a user can write in {@code CREATE CATALOG ... ("type" = ...)}. Default {@code true}.
     *
     * <p>Return {@code false} for a connector that exists only as an <em>embedded sibling</em> of another one
     * (built and owned by that connector through {@code ConnectorContext.createSiblingConnector}, for a table
     * format that is parasitic on the other connector's metastore). Such a connector still registers normally
     * and stays fully reachable for sibling lookup — the engine only declines to build a catalog around it,
     * because there would be no catalog semantics on the engine side to back it.
     *
     * <p>This is the only thing that decides whether a registered connector can be reached by
     * {@code CREATE CATALOG}: the engine keeps no list of accepted catalog types.
     */
    default boolean isStandaloneCatalogType() {
        return true;
    }

    /**
     * Creates a Connector instance for a catalog.
     * Called once per catalog lifecycle.
     *
     * @param properties catalog configuration properties
     * @param context runtime context provided by fe-core
     * @return a ready-to-use Connector
     */
    Connector create(Map<String, String> properties, ConnectorContext context);

    /**
     * Validates catalog properties before creation.
     * Called during CREATE CATALOG to fail fast on invalid configuration.
     * Default implementation does nothing (all properties accepted).
     *
     * @param properties catalog configuration properties
     * @throws IllegalArgumentException if required properties are missing or invalid
     */
    default void validateProperties(Map<String, String> properties) {
        // no-op by default
    }

    /**
     * Validates an ALTER CATALOG candidate without publishing it to the live catalog.
     * Connectors with legacy-property compatibility rules may override this method.
     */
    default void validatePropertiesForUpdate(
            Map<String, String> currentProperties, Map<String, String> updatedProperties) {
        Map<String, String> candidate = currentProperties == null
                ? new HashMap<>() : new HashMap<>(currentProperties);
        candidate.putAll(updatedProperties);
        validateProperties(candidate);
    }

    /**
     * The JDBC driver jar URLs that the given catalog properties would make this connector load into the
     * FE JVM — raw (not resolved to a full URL), alias-resolved, and flavor-aware, so a property that is
     * dead config for the resolved flavor must not be reported. Default: none.
     *
     * <p><b>Why the engine asks.</b> Whether a driver jar location is allowed at all is an operator
     * decision expressed in fe.conf ({@code jdbc_driver_secure_path}, {@code jdbc_driver_url_white_list}),
     * so only the engine can apply it. On CREATE the engine applies it through
     * {@link Connector#preCreateValidation}. ALTER CATALOG never reaches that hook — it validates through
     * {@link #validatePropertiesForUpdate} alone — so without this declaration an operator's allow-list
     * would be enforced at CREATE and then silently bypassable by a follow-up ALTER that repoints
     * {@code driver_url} at any URL. Any connector that hands a property value to a class loader MUST
     * declare it here.
     *
     * <p>This is only the operator-configurable gate. The mandatory, non-configurable rule
     * ({@link JdbcDriverUrlSecurity}) is the connector's own responsibility and belongs in
     * {@link #validateProperties}, which the engine runs on both CREATE and ALTER.
     */
    default List<String> driverUrlsToValidate(Map<String, String> properties) {
        return Collections.emptyList();
    }

    /**
     * Whether connectors of this type expose an incremental metadata-change source through
     * {@code Connector#getEventSource()}.
     *
     * <p>The engine's event driver uses this to decide whether an <b>uninitialized</b> catalog is worth
     * force-initializing once, so that a catalog nobody has queried on this FE still seeds its event cursor
     * (each FE keeps its own cursor, and a follower normally receives no queries). It is declared here rather
     * than on {@code Connector} precisely because that decision is made before the connector exists: asking
     * the connector would force-initialize every idle catalog, which is what this flag exists to avoid.</p>
     *
     * <p>Must agree with whether {@code Connector#getEventSource()} returns non-null — that method stays the
     * single source of truth, and the engine still skips a connector whose source turns out to be null, so a
     * {@code true} here costs at most one wasted initialization. Default {@code false}: a connector without
     * an event source is never force-initialized.</p>
     */
    default boolean providesEventSource() {
        return false;
    }

    /**
     * The database a session should land in when it switches to a catalog of this type, or empty (the default)
     * to leave the session's database alone.
     *
     * <p>For data sources whose metadata model has no database layer, where Doris invents a single fixed
     * database name. Answered from the provider because the switch may happen before anything has touched the
     * connector.</p>
     */
    default Optional<String> defaultDatabaseOnUse() {
        return Optional.empty();
    }

    /**
     * The legacy engine names this connector accepts in {@code CREATE TABLE ... ENGINE=<name>}, or empty
     * (the default) if it accepts none.
     *
     * <p>{@code ENGINE=} predates catalogs and is optional: omitting it is always legal and lets the target
     * catalog decide. The clause survives only because existing SQL writes it, so the engine remains a name
     * the connector owns — the engine does not keep a table of which name belongs to which data source, and
     * never puts one of these names in a message. It uses them for exactly two things: deciding whether an
     * explicitly written name is accepted here, and refusing at registration time a plugin that claims a name
     * another plugin already claims.</p>
     *
     * <p>Note this is NOT the name a table displays: an HMS catalog accepts {@code ENGINE=hive} while
     * displaying {@code hms}. Declaring a name also says nothing about which clauses the connector supports —
     * {@code PARTITION BY} and {@code DISTRIBUTED BY} are validated inside
     * {@code ConnectorTableDdlOps#createTable}, by the connector that has to honor them.</p>
     *
     * <p>Answered from the provider, not the connector, because the question is asked while analyzing a
     * statement, before anything should force a catalog to initialize.</p>
     */
    default Set<String> acceptedCreateTableEngineNames() {
        return Collections.emptySet();
    }

    /**
     * The name this connector's tables display as their engine. Defaults to {@link #getType()}; override only
     * to spell it differently from the catalog type.
     *
     * <p>It is what a user reads in the {@code ENGINE} column of {@code SHOW TABLE STATUS} and
     * {@code information_schema.tables}, and after {@code ENGINE=} in {@code SHOW CREATE TABLE}. Both places
     * show the same string.</p>
     *
     * <p>Do not confuse it with {@link #acceptedCreateTableEngineNames()}: that one is the name a user may
     * <em>write</em>, this one is the name Doris <em>displays</em>, and a connector may legitimately have both
     * and have them differ — an HMS catalog accepts {@code ENGINE=hive} while displaying {@code hms}. The
     * engine never routes, matches or validates anything on the displayed name; it only prints it, and it
     * keeps no table of which name belongs to which data source.</p>
     */
    default String displayEngineName() {
        return getType();
    }

    /**
     * This plugin's identity to the engine. Defaults to {@link #getType()}; two loaded plugins may not
     * share one ({@code ConnectorPluginManager.loadPlugins} skips the second).
     *
     * <p>It is also the name of this connector's own configuration file: the engine reads
     * {@code <pluginDir>/<name>.conf} and serves it back through
     * {@link ConnectorContext#getConnectorConfig()}. So a connector that ships a conf template must name
     * it {@code <name>.conf.template}, and <b>changing this method renames that file</b> — the plugin
     * directory name has no say in it, and need not match ({@code hive/hms.conf} is a shipped example).
     * Guard it with a test that the template resource exists under {@code name() + ".conf.template"}.
     *
     * <p>The engine builds a file name out of this string, which is safe because
     * {@code PluginNames.validate} has already confined it to {@code [a-zA-Z0-9._-]} — no separator can
     * reach a path. Do not re-validate it.
     */
    @Override
    default String name() {
        return getType();
    }

    /**
     * Not used by DirectoryPluginRuntimeManager for connectors.
     * Provided to satisfy {@link PluginFactory} contract.
     */
    @Override
    default Plugin create() {
        throw new UnsupportedOperationException(
                "ConnectorProvider does not support no-arg create(). "
                + "Use create(Map, ConnectorContext) instead.");
    }
}

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
import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;

import java.util.Map;

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
     * source-prefixed namespaces distinct <em>by construction</em> (see {@code ConnectorStatementScopes} in
     * fe-connector-api, which relies on this method being a connector's unique identity).
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

    /** API version for compatibility checking. Major version change = incompatible. */
    default int apiVersion() {
        return 1;
    }

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

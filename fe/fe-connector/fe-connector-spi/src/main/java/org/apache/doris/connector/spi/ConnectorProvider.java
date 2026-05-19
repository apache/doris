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
     * Returns the connector type name (e.g., "hive", "iceberg", "es").
     * Corresponds to the {@code type} property in CREATE CATALOG.
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

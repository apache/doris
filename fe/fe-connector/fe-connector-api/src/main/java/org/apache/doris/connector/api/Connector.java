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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Main entry point for a connector implementation.
 *
 * <p>A {@code Connector} instance is created once per catalog and provides
 * access to metadata, scan planning, and optional write operations.</p>
 */
public interface Connector extends Closeable {

    /** Returns the metadata interface for the given session. */
    ConnectorMetadata getMetadata(ConnectorSession session);

    /** Returns the scan plan provider for split generation. */
    default ConnectorScanPlanProvider getScanPlanProvider() {
        return null;
    }

    /**
     * Returns the write plan provider for sink ({@code TDataSink}) generation,
     * or {@code null} if this connector does not support writes.
     */
    default ConnectorWritePlanProvider getWritePlanProvider() {
        return null;
    }

    /** Returns the set of capabilities this connector supports. */
    default Set<ConnectorCapability> getCapabilities() {
        return Collections.emptySet();
    }

    /** Returns the table-level property descriptors. */
    default List<ConnectorPropertyMetadata<?>> getTableProperties() {
        return Collections.emptyList();
    }

    /** Returns the session-level property descriptors. */
    default List<ConnectorPropertyMetadata<?>> getSessionProperties() {
        return Collections.emptyList();
    }

    /**
     * Returns whether connectivity testing should be enabled by default when
     * the user does not explicitly set the {@code test_connection} property.
     *
     * <p>Connectors that rely on external drivers or remote connectivity
     * (e.g., JDBC) should return {@code true} so that bad configurations
     * are caught at CREATE CATALOG time rather than at first query.</p>
     *
     * @return {@code true} if test_connection defaults to enabled
     */
    default boolean defaultTestConnection() {
        return false;
    }

    /**
     * Performs connector-specific validation during CREATE CATALOG.
     *
     * <p>Called before {@link #testConnection(ConnectorSession)}. Connectors
     * may override this to validate driver security, compute checksums,
     * test BE connectivity, or perform any other pre-creation checks.</p>
     *
     * <p>The engine provides infrastructure services through the
     * {@link ConnectorValidationContext}; each connector calls only the
     * services relevant to its validation needs.</p>
     *
     * @param context engine services for validation
     * @throws Exception if validation fails
     */
    default void preCreateValidation(ConnectorValidationContext context) throws Exception {
        // No-op by default
    }

    /**
     * Tests connectivity to the underlying data source.
     *
     * <p>Connectors should override this to verify they can reach the
     * metastore, storage, and any other required services.</p>
     *
     * @return the test result; default returns success (no-op test)
     */
    default ConnectorTestResult testConnection(ConnectorSession session) {
        return ConnectorTestResult.success();
    }

    @Override
    default void close() throws IOException {
    }

    /**
     * Execute a REST passthrough request against the underlying data source.
     *
     * <p>Connectors that expose HTTP endpoints (e.g., Elasticsearch) can
     * override this to proxy REST requests from FE REST APIs.</p>
     *
     * @param path the relative URL path (e.g., "index_name/_search")
     * @param body the request body (may be null for GET-style requests)
     * @return the response body as a JSON string
     * @throws UnsupportedOperationException if the connector doesn't support REST
     */
    default String executeRestRequest(String path, String body) {
        throw new UnsupportedOperationException("REST passthrough not supported by this connector");
    }

    /**
     * Invalidates any connector-side per-table cache (e.g. a latest-snapshot/version cache) so a subsequent
     * read reflects the latest external state. Called by the engine on {@code REFRESH TABLE}. The names are
     * the REMOTE db/table names (as seen by the connector). Default no-op for connectors that cache nothing.
     */
    default void invalidateTable(String dbName, String tableName) {
    }

    /** Invalidates all connector-side per-table caches. Default no-op. */
    default void invalidateAll() {
    }

    /**
     * Optional per-connector override of the catalog's schema-cache TTL (in seconds), consulted generically by
     * the engine when sizing the schema meta-cache. Semantics match {@code schema.cache.ttl-second}:
     * {@code 0} disables schema caching (always read fresh), {@code -1} = no expiration, {@code > 0} = TTL.
     * Lets a connector make its own cache knob also govern schema freshness (e.g. paimon's
     * {@code meta.cache.paimon.table.ttl-second}, which legacy used for the whole table cache). An explicit
     * user {@code schema.cache.ttl-second} always wins over this. Default: no override.
     */
    default OptionalLong schemaCacheTtlSecondOverride() {
        return OptionalLong.empty();
    }
}

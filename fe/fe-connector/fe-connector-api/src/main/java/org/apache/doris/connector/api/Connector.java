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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
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
     * Returns the scan plan provider for the given table, allowing one connector to select a
     * different provider <b>per table</b>.
     *
     * <p>The selection MUST happen here, at provider-acquisition time — not inside a single
     * dispatching provider — because {@link ConnectorScanPlanProvider} has methods that do not
     * carry the handle (e.g. {@code appendExplainInfo}) and providers are built fresh/stateless
     * per call, so a provider returned here must already be bound to the correct backing scanner
     * for {@code handle}. This is the seam a heterogeneous gateway connector (one catalog serving
     * multiple table formats) overrides to delegate to per-format sub-providers by the concrete
     * (connector-defined) handle type; the engine never inspects the format.</p>
     *
     * <p>The default ignores {@code handle} and returns the connector-level
     * {@link #getScanPlanProvider()}, so every single-format connector is unaffected.</p>
     */
    default ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle handle) {
        return getScanPlanProvider();
    }

    /**
     * Returns the write plan provider for sink ({@code TDataSink}) generation,
     * or {@code null} if this connector does not support writes.
     */
    default ConnectorWritePlanProvider getWritePlanProvider() {
        return null;
    }

    /**
     * Returns the write plan provider for the given table, allowing one connector to select a different
     * provider <b>per table</b> — the write-side analogue of {@link #getScanPlanProvider(ConnectorTableHandle)}.
     *
     * <p>The default ignores {@code handle} and returns the connector-level {@link #getWritePlanProvider()}, so
     * every single-format connector is unaffected. A heterogeneous gateway connector (one catalog serving
     * multiple table formats) overrides this to delegate to a per-format sub-provider by the concrete
     * (connector-defined) handle type; the engine never inspects the format.</p>
     */
    default ConnectorWritePlanProvider getWritePlanProvider(ConnectorTableHandle handle) {
        return getWritePlanProvider();
    }

    /**
     * The write operations the engine may perform on this connector — the single admission source. Reads the
     * write provider's {@link ConnectorWritePlanProvider#supportedOperations()}; no provider ⇒ empty set ⇒ all
     * writes rejected. The engine consults this instead of {@code getWritePlanProvider() != null}.
     */
    default Set<WriteOperation> supportedWriteOperations() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p == null ? EnumSet.noneOf(WriteOperation.class) : p.supportedOperations();
    }

    /**
     * Per-table view of {@link #supportedWriteOperations()}: derives from {@link #getWritePlanProvider(
     * ConnectorTableHandle)} so a heterogeneous gateway admits the right operations for {@code handle} (e.g. an
     * iceberg-on-HMS table admits DELETE/MERGE that the hive provider does not). The default routes through the
     * per-handle provider, so every single-format connector is unaffected.
     */
    default Set<WriteOperation> supportedWriteOperations(ConnectorTableHandle handle) {
        ConnectorWritePlanProvider p = getWritePlanProvider(handle);
        return p == null ? EnumSet.noneOf(WriteOperation.class) : p.supportedOperations();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#supportsWriteBranch()}. No provider ⇒ false. */
    default boolean supportsWriteBranch() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.supportsWriteBranch();
    }

    /** Per-table view of {@link #supportsWriteBranch()} (derives from the per-handle provider). */
    default boolean supportsWriteBranch(ConnectorTableHandle handle) {
        ConnectorWritePlanProvider p = getWritePlanProvider(handle);
        return p != null && p.supportsWriteBranch();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#requiresParallelWrite()}. No provider ⇒ false. */
    default boolean requiresParallelWrite() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.requiresParallelWrite();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#requiresFullSchemaWriteOrder()}. No provider ⇒ false. */
    default boolean requiresFullSchemaWriteOrder() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.requiresFullSchemaWriteOrder();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#requiresPartitionLocalSort()}. No provider ⇒ false. */
    default boolean requiresPartitionLocalSort() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.requiresPartitionLocalSort();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#requiresPartitionHashWrite()}. No provider ⇒ false. */
    default boolean requiresPartitionHashWrite() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.requiresPartitionHashWrite();
    }

    /** Per-table view of {@link #requiresPartitionHashWrite()} (derives from the per-handle provider). */
    default boolean requiresPartitionHashWrite(ConnectorTableHandle handle) {
        ConnectorWritePlanProvider p = getWritePlanProvider(handle);
        return p != null && p.requiresPartitionHashWrite();
    }

    /**
     * Null-safe view of {@link ConnectorWritePlanProvider#requiresMaterializeStaticPartitionValues()}. No
     * provider ⇒ false.
     */
    default boolean requiresMaterializeStaticPartitionValues() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.requiresMaterializeStaticPartitionValues();
    }

    /** Per-table view of {@link #requiresMaterializeStaticPartitionValues()} (derives from the per-handle provider). */
    default boolean requiresMaterializeStaticPartitionValues(ConnectorTableHandle handle) {
        ConnectorWritePlanProvider p = getWritePlanProvider(handle);
        return p != null && p.requiresMaterializeStaticPartitionValues();
    }

    /**
     * Returns the procedure ops for {@code ALTER TABLE EXECUTE} dispatch, or {@code null} if this
     * connector exposes no table procedures. Procedure-side analogue of {@link #getWritePlanProvider()}.
     */
    default ConnectorProcedureOps getProcedureOps() {
        return null;
    }

    /** Returns the set of capabilities this connector supports. */
    default Set<ConnectorCapability> getCapabilities() {
        return Collections.emptySet();
    }

    /**
     * Storage-configuration defaults this connector derives from its own catalog properties, which the raw
     * catalog map does not already supply. Design S8: storage-property derivation is owned by the connector —
     * fe-core does not parse metastore properties. fe-core folds the returned map into the catalog's storage
     * properties as DEFAULTS (an explicit user key always wins via {@code putIfAbsent}), and does so BEFORE
     * both the fe-filesystem bind ({@code ConnectorContext.getStorageProperties()}) and the BE storage map
     * ({@code getBackendStorageProperties()}), so the FE bind and the BE scan see the same derived storage.
     *
     * <p>The default is empty (no derivation), so every connector that does not need it is unaffected. The
     * iceberg connector overrides this to bridge a hadoop-catalog {@code warehouse=hdfs://<ns>/path} into
     * {@code fs.defaultFS=hdfs://<ns>}, which the shared HDFS detection never derives from {@code warehouse}.</p>
     *
     * @param rawCatalogProps the catalog's current persisted properties
     * @return extra storage-property defaults; an empty map when there is nothing to derive
     */
    default Map<String, String> deriveStorageProperties(Map<String, String> rawCatalogProps) {
        return Collections.emptyMap();
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

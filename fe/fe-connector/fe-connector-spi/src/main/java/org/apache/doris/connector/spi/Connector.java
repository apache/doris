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

import org.apache.doris.connector.spi.event.ConnectorEventSource;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.spi.rest.ConnectorRestPassthrough;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Main entry point for a connector implementation.
 *
 * <p>A {@code Connector} instance is created once per catalog and provides
 * access to metadata, scan planning, and optional write operations.</p>
 *
 * <p><b>This interface does not mirror any provider's switches, and must not start.</b> A subsystem trait
 * (write operations, parallel write, partition-hash write, ...) is declared on the provider that owns it and
 * read from there; a forwarding copy here would be a second overridable answer to one question, and a
 * connector overriding the copy while leaving the provider at its default would produce two divergent
 * answers with no compile error and no failing test. The engine reaches a trait by fetching the provider
 * ({@link #getWritePlanProvider(ConnectorTableHandle)} for a per-table answer, {@link #getWritePlanProvider()}
 * for a connector-wide one) and asking it, treating a {@code null} provider as "not supported".</p>
 *
 * <p>The getters that return {@code null} ({@code getScanPlanProvider}, {@code getWritePlanProvider},
 * {@code getProcedureOps}, {@code getEventSource}, {@code getRestPassthrough}) are the opposite case: those
 * ARE the declaration points for "this subsystem exists". See the {@code org.apache.doris.connector.spi}
 * package documentation for the full rule.</p>
 */
public interface Connector extends Closeable {

    /**
     * Returns the metadata interface for the given session. The engine calls this exactly once per catalog per
     * statement through its own single entry point and closes the result when the statement ends, so an
     * implementation may return a fresh, statement-scoped object; see {@link ConnectorMetadata} for the
     * lifecycle contract.
     */
    ConnectorMetadata getMetadata(ConnectorSession session);

    /**
     * Whether {@code handle} is one of THIS connector's own concrete {@link ConnectorTableHandle} subclasses.
     *
     * <p>A heterogeneous gateway connector that serves several table formats through embedded <em>sibling</em>
     * connectors uses this to route a foreign handle to the sibling that produced it: the sibling's concrete
     * handle type is invisible across the plugin classloader split, so the gateway cannot {@code instanceof} it
     * directly — it asks each sibling, and the sibling tests its OWN in-loader type. The default returns
     * {@code false} (a connector owns no handle it did not define), so every non-gateway connector is
     * unaffected.</p>
     *
     * <p>fe-core NEVER calls this — it is a connector-to-sibling routing predicate only, so the engine stays
     * format-agnostic (it discriminates handles solely by the gateway's own handle type, never by asking a
     * connector to classify one).</p>
     */
    default boolean ownsHandle(ConnectorTableHandle handle) {
        return false;
    }

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
     * Returns the procedure ops for {@code ALTER TABLE EXECUTE} dispatch, or {@code null} if this
     * connector exposes no table procedures. Procedure-side analogue of {@link #getWritePlanProvider()}.
     */
    default ConnectorProcedureOps getProcedureOps() {
        return null;
    }

    /**
     * Returns the procedure ops for the given table, allowing one connector to select a different set of
     * procedures <b>per table</b> — the procedure-side analogue of {@link #getScanPlanProvider(
     * ConnectorTableHandle)} / {@link #getWritePlanProvider(ConnectorTableHandle)}.
     *
     * <p>The default ignores {@code handle} and returns the connector-level {@link #getProcedureOps()}, so every
     * single-format connector is unaffected. A heterogeneous gateway connector (one catalog serving multiple
     * table formats) overrides this to delegate a foreign (e.g. iceberg-on-HMS) handle to a sibling connector's
     * procedure ops by the concrete (connector-defined) handle type; the engine never inspects the format.</p>
     */
    default ConnectorProcedureOps getProcedureOps(ConnectorTableHandle handle) {
        return getProcedureOps();
    }

    /** Returns the set of capabilities this connector supports. */
    default Set<ConnectorCapability> getCapabilities() {
        return Collections.emptySet();
    }

    /**
     * Storage-configuration defaults this connector derives from its own catalog properties, which the raw
     * catalog map does not already supply. Storage-property derivation is owned by the connector —
     * fe-core does not parse metastore properties. fe-core folds the returned map into the catalog's storage
     * properties as DEFAULTS (an explicit user key always wins via {@code putIfAbsent}), and does so BEFORE
     * both the fe-filesystem bind ({@code ConnectorStorageContext.getStorageProperties()}) and the BE storage map
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
     * Invalidates any connector-side per-table cache (e.g. a latest-snapshot/version cache) so a subsequent
     * read reflects the latest external state. Called by the engine on {@code REFRESH TABLE} and before applying
     * structural table events (create, drop, rename, and same-name view recreation). The names are the REMOTE
     * db/table names (as seen by the connector). Default no-op for connectors that cache nothing.
     */
    default void invalidateTable(String dbName, String tableName) {
    }

    /** Invalidates all connector-side per-table caches. Default no-op. */
    default void invalidateAll() {
    }

    /**
     * Invalidates the connector-side caches for every table in one database. Called by the engine on
     * {@code REFRESH DATABASE} and before applying structural database events (create, drop, and rename).
     * The name is the REMOTE db name (as seen by the connector). Default no-op for connectors that cache nothing.
     */
    default void invalidateDb(String dbName) {
    }

    /**
     * Invalidates the connector-side caches for specific partitions of a table so a subsequent read
     * reflects the latest external state. Driven by the engine's metastore-event sync when partitions
     * are added/dropped/altered. The names are the REMOTE db/table names and canonical partition names
     * ({@code "col=val/.../colN=valN"}); an empty/whole-table drop is expressed by
     * {@link #invalidateTable(String, String)}. A connector whose partition cache cannot target a single
     * name may degrade to invalidating the whole table's partition caches (correctness-safe when the
     * cache re-lists on miss). Default no-op for connectors that cache nothing.
     */
    default void invalidatePartition(String dbName, String tableName, List<String> partitionNames) {
    }

    /**
     * Returns this connector's incremental metadata-change source, or {@code null} if it has none.
     * A capability-probe getter (mirrors {@link #getScanPlanProvider()} / {@link #getProcedureOps()}):
     * the engine's single, connector-agnostic, role-aware event driver iterates catalogs and calls
     * {@link ConnectorEventSource#pollOnce} only on connectors that expose a source, never via
     * {@code instanceof}. The default returns {@code null}, so every connector without a metastore-event
     * feed is unaffected.
     */
    default ConnectorEventSource getEventSource() {
        return null;
    }

    /**
     * Returns this connector's HTTP passthrough capability, or {@code null} if it has none. A capability-probe
     * getter with the same shape as {@link #getEventSource()}: the caller probes for {@code null}, never via
     * {@code instanceof}. Consumed by FE HTTP endpoints that speak one source's HTTP dialect (today
     * {@code ESCatalogAction}), which narrow to that catalog type first and only then ask for the capability.
     * The default returns {@code null}, so no connector inherits an entry point it cannot serve.
     */
    default ConnectorRestPassthrough getRestPassthrough() {
        return null;
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

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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fluss connector: one instance per catalog, owning that catalog's single fluss {@link Connection}.
 *
 * <p>The connection is the expensive, thread-safe, long-lived object (it carries the RPC client,
 * metadata updater and their threads); {@code Admin} handles taken from it are cheap and memoized by
 * the connection itself. So the connection is created once, lazily, and closed when the catalog goes
 * away, while each statement gets its own {@link ConnectorMetadata} over a fresh
 * {@link ConnectionBackedFlussAdminOps} view of it.
 */
public class FlussConnector implements Connector {

    /**
     * The sibling connector type a fluss lake table ({@code tbl$lake}) is delegated to. A string literal,
     * not the paimon plugin's own constant: that plugin loads child-first and none of its classes are
     * visible from here. Matches the type {@code PaimonConnectorProvider} registers.
     */
    private static final String PAIMON_CONNECTOR_TYPE = "paimon";

    private final FlussCatalogProperties properties;
    private final String catalogName;
    private final ConnectorContext context;

    private volatile Connection connection;
    private volatile boolean closed;

    // The embedded paimon SIBLING connector this catalog delegates its lake tables to. Built lazily in the
    // PAIMON plugin's OWN child-first classloader via context.createSiblingConnector, never co-packaged
    // into the fluss zip (a second copy of paimon in one JVM). Held ONLY as the parent-first Connector
    // interface and NEVER cast: the concrete type is invisible to the fluss loader, so a cast would CCE
    // across the loader split.
    //
    // Exactly ONE, and that is a correctness constraint rather than thrift. Handle routing asks each
    // sibling "is this handle yours?", and two paimon siblings answer that question with the SAME class
    // test — so a second one could never be told apart from the first and a table would silently read the
    // wrong warehouse. One fluss cluster injects one lake configuration into all of its datalake tables
    // (LakeCatalogDynamicLoader), so one is also all a healthy catalog ever needs; a cluster reconfigured
    // under a live catalog gets a loud error instead (see getOrCreateLakeSibling).
    private volatile Connector lakeSibling;

    // The configuration lakeSibling was built from, for the "reconfigured under a live catalog" check.
    // Written BEFORE the volatile lakeSibling publishes it, so a reader that sees the sibling sees this.
    private Map<String, String> lakeSiblingProperties;

    // Cluster-reported storage defaults become known only after the first lake table is resolved. Publish
    // them before constructing the sibling: the shared context lazily asks deriveStorageProperties() while
    // that sibling binds its FE filesystem and BE storage map.
    private volatile Map<String, String> lakeStorageProperties = Collections.emptyMap();

    // Source Fluss identity -> physical lake identity. The latter may be overridden independently for the
    // database and table, and targeted invalidation must follow the same path as lookup.
    private final Map<TablePath, TablePath> lakeTablePaths = new LinkedHashMap<>();

    public FlussConnector(FlussCatalogProperties properties, ConnectorContext context) {
        this.properties = properties;
        this.catalogName = context.getCatalogName();
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new FlussConnectorMetadata(adminOps(), properties.getTypeMappingOptions(),
                properties.getRawCatalogProperties(), properties.getLakeOverrides(),
                this::getOrCreateLakeSibling, this::lakeSiblingOwning, this::rememberLakePath);
    }

    /**
     * A fresh provider per call, which is what the engine wants: it memoizes one instance per scan node
     * and that instance keeps the just-planned range counts for the node's EXPLAIN line.
     */
    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new FlussScanPlanProvider(adminOps(), properties, this::getOrCreateLakeSibling);
    }

    /**
     * Routes scan planning by handle, so a lake table's scan is planned by the paimon sibling that owns
     * its handle and reads as a plain paimon table (native readers, its own statistics and time travel).
     * A fluss handle keeps this connector's own provider.
     */
    @Override
    public ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle handle) {
        Connector owner = lakeSiblingOwning(handle);
        return owner == null ? getScanPlanProvider() : owner.getScanPlanProvider(handle);
    }

    /**
     * Only fluss's own handles. The engine asks this to route a handle back to the connector that made it,
     * and a lake table's handle was made by the paimon sibling, which answers for itself.
     */
    @Override
    public boolean ownsHandle(ConnectorTableHandle handle) {
        return handle instanceof FlussTableHandle;
    }

    /**
     * Nested-column pruning, and only that. The scanner honours a pruned nested type by remapping the
     * requested sub-fields onto the fluss row while decoding — fluss projects top-level fields only, so
     * there is nothing to push down — and the lake half of a union read is served by the paimon sibling,
     * which pushes the same shape down for real. NOT SUPPORTS_FIELD_ID_ACCESS_PATH: this connector puts no
     * field id on the Doris column tree, so rewriting the access paths to ids would make every segment
     * "-1", which BE matches neither as an id nor as a name.
     */
    @Override
    public Set<ConnectorCapability> getCapabilities() {
        return EnumSet.of(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE);
    }

    /**
     * The already-built lake sibling when it owns {@code handle}, else null (i.e. a fluss handle). Asks the
     * sibling's {@code ownsHandle} because the sibling tests its OWN in-loader handle type, which this side
     * cannot {@code instanceof} across the plugin split.
     *
     * <p>Deliberately a peek, never a build: a query that never touches a lake table must not construct a
     * paimon catalog (nor fail when the paimon plugin is absent).
     */
    private synchronized Connector lakeSiblingOwning(ConnectorTableHandle handle) {
        throwIfClosed();
        Connector sibling = lakeSibling;
        return sibling != null && sibling.ownsHandle(handle) ? sibling : null;
    }

    /**
     * Builds (once) and returns this catalog's paimon sibling. Fails loud when no paimon provider is
     * available, e.g. the plugin is not installed; that failure is NOT memoized (the field stays unset), so
     * a later-installed plugin recovers on the next access.
     *
     * <p>Also fails loud when a second, DIFFERENT lake configuration shows up — see the field comment: a
     * second paimon sibling could not be routed apart from the first, so serving both would mean reading
     * one warehouse under the other's name. The message asks for catalog recreation, which rebuilds this
     * connector and picks up the new configuration. It deliberately does not print either configuration:
     * they carry storage credentials.
     *
     * <p>Package-private (not private) so a unit test can drive the sibling wiring without
     * {@link #getMetadata} first opening a real fluss connection.
     */
    synchronized Connector getOrCreateLakeSibling(Map<String, String> siblingProperties) {
        throwIfClosed();
        if (lakeSibling == null) {
            lakeStorageProperties = Collections.unmodifiableMap(
                    LakeStorageOptions.toStorageProperties(siblingProperties));
            Map<String, String> metadataProperties = new HashMap<>(siblingProperties);
            metadataProperties.keySet().removeIf(option -> LakeStorageOptions.isStorageOption(option)
                    && !LakeStorageOptions.isSiblingAuthenticationGate(option));
            Connector sibling =
                    context.createSiblingConnector(PAIMON_CONNECTOR_TYPE, metadataProperties);
            if (sibling == null) {
                throw new DorisConnectorException(
                        "Cannot read the lake table of fluss catalog '" + catalogName
                                + "': the paimon connector plugin is not available");
            }
            lakeSiblingProperties = Collections.unmodifiableMap(new HashMap<>(siblingProperties));
            lakeSibling = sibling;
        }
        if (!lakeSiblingProperties.equals(siblingProperties)) {
            throw new DorisConnectorException(
                    "Fluss catalog '" + catalogName + "' is already serving lake tables with a different"
                            + " paimon configuration than this table's. Its fluss cluster was reconfigured;"
                            + " recreate the catalog after applying the new lake configuration");
        }
        return lakeSibling;
    }

    /**
     * The storage the lake sits on: cluster-reported defaults learned from a lake table plus the storage
     * half of the catalog's {@code fluss.lake.paimon.*} settings, translated into Doris names.
     *
     * <p>This is the only route that reaches both halves of a scan. The engine folds what is returned here
     * into the catalog's storage properties before the FE binds a filesystem and before the BE-side storage
     * map is built, so the FE reads the lake's manifests and the BE reads its data files through the same
     * configuration. Anything given to the paimon sibling instead would reach at most the FE.
     *
     * <p>Reads the argument rather than this connector's own bound properties: the engine calls this with
     * the catalog's current persisted properties, which is what an {@code ALTER CATALOG} updates first.
     */
    @Override
    public Map<String, String> deriveStorageProperties(Map<String, String> rawCatalogProps) {
        Map<String, String> storage = new HashMap<>(lakeStorageProperties);
        storage.putAll(LakeStorageOptions.toStorageProperties(
                FlussCatalogProperties.extractLakeOverrides(rawCatalogProps)));
        return storage;
    }

    synchronized void rememberLakePath(TablePath sourcePath, TablePath lakePath) {
        lakeTablePaths.put(sourcePath, lakePath);
    }

    @Override
    public synchronized void invalidateTable(String dbName, String tableName) {
        if (lakeSibling != null) {
            // Keep the mapping until the table is resolved again (which replaces it) or the whole catalog
            // is invalidated. A single engine refresh may emit more than one targeted invalidation, and a
            // second one must not fall back to the source name merely because the first already ran.
            TablePath lakePath = lakeTablePaths.get(TablePath.of(dbName, tableName));
            lakeSibling.invalidateTable(
                    lakePath == null ? dbName : lakePath.getDatabaseName(),
                    lakePath == null ? tableName : lakePath.getTableName());
        }
    }

    @Override
    public synchronized void invalidateDb(String dbName) {
        if (lakeSibling != null) {
            Set<String> lakeDatabases = new LinkedHashSet<>();
            // An unresolved table cannot have populated the sibling's table cache, but the sibling may
            // still hold database-level metadata for the source namespace. Invalidating it as well as every
            // known physical override is conservative and prevents one overridden table from accidentally
            // suppressing invalidation of ordinary lake tables in the same Fluss database.
            lakeDatabases.add(dbName);
            lakeTablePaths.forEach((source, lake) -> {
                if (source.getDatabaseName().equals(dbName)) {
                    lakeDatabases.add(lake.getDatabaseName());
                }
            });
            lakeDatabases.forEach(lakeSibling::invalidateDb);
        }
    }

    @Override
    public synchronized void invalidateAll() {
        lakeTablePaths.clear();
        if (lakeSibling != null) {
            lakeSibling.invalidateAll();
        }
    }

    @Override
    public synchronized void invalidatePartition(
            String dbName, String tableName, List<String> partitionNames) {
        if (lakeSibling != null) {
            TablePath lakePath = lakeTablePaths.get(TablePath.of(dbName, tableName));
            lakeSibling.invalidatePartition(
                    lakePath == null ? dbName : lakePath.getDatabaseName(),
                    lakePath == null ? tableName : lakePath.getTableName(), partitionNames);
        }
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            adminOps().listDatabases();
            return ConnectorTestResult.success();
        } catch (Exception e) {
            return ConnectorTestResult.failure("Fluss connectivity test failed: " + e.getMessage());
        }
    }

    /**
     * Closes this catalog's fluss connection and its lake sibling. The engine closes only a catalog's
     * PRIMARY connector, so the sibling's lifecycle is this connector's to own — an unclosed one leaks its
     * paimon catalog (file handles, client pools) for the life of the FE. The sibling is closed first and
     * its failure is held back, so it cannot leak the fluss connection.
     */
    @Override
    public void close() throws IOException {
        Connector sibling;
        Connection toClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            sibling = lakeSibling;
            lakeSibling = null;
            lakeSiblingProperties = null;
            lakeStorageProperties = Collections.emptyMap();
            lakeTablePaths.clear();
            toClose = connection;
            connection = null;
        }

        IOException siblingFailure = null;
        if (sibling != null) {
            try {
                sibling.close();
            } catch (Exception e) {
                siblingFailure = new IOException(
                        "Failed to close the paimon lake sibling of catalog '" + catalogName + "'", e);
            }
        }

        if (toClose != null) {
            try {
                toClose.close();
            } catch (Exception e) {
                throw new IOException(
                        "Failed to close the fluss connection of catalog '" + catalogName + "'", e);
            }
        }
        if (siblingFailure != null) {
            throw siblingFailure;
        }
    }

    private FlussAdminOps adminOps() {
        return new ConnectionBackedFlussAdminOps(getOrCreateConnection(), catalogName,
                properties.getBootstrapServers());
    }

    private synchronized Connection getOrCreateConnection() {
        throwIfClosed();
        if (connection == null) {
            connection = createConnection();
        }
        return connection;
    }

    private void throwIfClosed() {
        if (closed) {
            throw new DorisConnectorException(
                    "Fluss catalog '" + catalogName + "' is already closed");
        }
    }

    private Connection createConnection() {
        Configuration config = new Configuration();
        properties.getFlussClientConfig().forEach(config::setString);

        // TCCL pin, and this is the locus that matters: creating the connection is what spawns the
        // fluss client's own threads (netty IO, metadata updater), and a thread inherits the context
        // classloader of whoever created it. Started under the engine's loader they would resolve
        // fluss classes against fe-core instead of this child-first plugin, where fluss does not exist
        // at all. Pinning here fixes every thread the connection owns for its whole life, which is why
        // the per-call admin path needs no pin of its own.
        ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        try {
            return ConnectionFactory.createConnection(config);
        } catch (RuntimeException e) {
            throw new DorisConnectorException("Failed to connect fluss catalog '" + catalogName + "' ("
                    + FlussCatalogProperties.BOOTSTRAP_SERVERS + "="
                    + properties.getBootstrapServers() + ")", e);
        } finally {
            Thread.currentThread().setContextClassLoader(callerLoader);
        }
    }
}

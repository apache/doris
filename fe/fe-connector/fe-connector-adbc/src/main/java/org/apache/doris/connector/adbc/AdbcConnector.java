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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.ConnectorValidationContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * ADBC connector implementation. Created once per catalog lifecycle.
 */
public class AdbcConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(AdbcConnector.class);

    private final AdbcCatalogProperties props;
    private final ConnectorContext context;
    private final AdbcSchemaStrategy schemaStrategy = new AdbcSchemaStrategy();
    private final AdbcPartitionedReadSupport partitionedRead = new AdbcPartitionedReadSupport();
    private final AdbcDialectSelector dialectSelector;
    private final AdbcMetadataCache metadataCache;

    private volatile AdbcClient client;
    private volatile boolean closed;

    /**
     * Binding the properties here is what makes them checked on every path that builds a connector,
     * including the ones no validator runs on -- a replayed edit log, a mirror loaded on first access.
     * An invalid catalog then fails when it is built rather than serving a query with a property nobody
     * could read.
     */
    public AdbcConnector(Map<String, String> properties, ConnectorContext context) {
        this.props = AdbcCatalogProperties.of(properties);
        this.context = context;
        // Both read properties only; the driver is not touched here (see getOrCreateClient).
        this.dialectSelector = new AdbcDialectSelector(props.getSqlDialect());
        // The raw map, because the cache knobs are the shared framework's keys rather than this
        // connector's: CacheSpec owns their names, and mirroring them as fields here would give the
        // validator and the reader two things to drift apart.
        this.metadataCache = new AdbcMetadataCache(props.getRaw());
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new AdbcConnectorMetadata(getOrCreateClient(), schemaStrategy, props.getRaw(),
                () -> dialectSelector.select(this::getOrCreateClient), metadataCache);
    }

    /**
     * The catalog's metadata memory, shared by every statement's {@link AdbcConnectorMetadata} and dropped
     * by the REFRESH hooks below.
     */
    AdbcMetadataCache metadataCache() {
        return metadataCache;
    }

    /** {@code REFRESH TABLE}. The names are the remote ones, which for ADBC are the ones Doris shows. */
    @Override
    public void invalidateTable(String dbName, String tableName) {
        metadataCache.invalidateTable(dbName, tableName);
    }

    /** {@code REFRESH DATABASE}. */
    @Override
    public void invalidateDb(String dbName) {
        metadataCache.invalidateDb(dbName);
    }

    /**
     * {@code REFRESH CATALOG}. Note that this does NOT rebuild the connector, so without dropping the cache
     * here the catalog would keep serving what it remembered until the TTL ran out -- the statement a user
     * reaches for when metadata looks wrong would be the one statement that changed nothing.
     */
    @Override
    public void invalidateAll() {
        metadataCache.invalidateAll();
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            List<String> databases = getMetadata(session).listDatabaseNames(session);
            return ConnectorTestResult.success(
                    "Connected successfully, found " + databases.size() + " databases");
        } catch (Exception e) {
            LOG.warn("ADBC connection test failed", e);
            return ConnectorTestResult.failure("ADBC connection failed: " + e.getMessage());
        }
    }

    /**
     * Scan planning, built per call because it holds no state -- everything worth keeping across queries
     * (the driver's dialect, the connection, whether the driver partitions) lives on the connector.
     */
    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new AdbcScanPlanProvider(props, resolveDriverPath(), dialectSelector,
                this::getOrCreateClient, partitionedRead);
    }

    /**
     * Runs on CREATE and ALTER CATALOG only -- never on edit-log replay or at query time -- so it is the
     * one place that may reach the filesystem and the remote source without risking FE startup.
     */
    @Override
    public void preCreateValidation(ConnectorValidationContext validationContext) throws Exception {
        // Checked here rather than through the context's own checksum service: that one resolves against
        // jdbc_drivers_dir and a .jar grammar, neither of which applies to an ADBC driver library.
        AdbcDriverPathResolver.checkChecksum(resolveDriverPath(),
                props.getDriverChecksum(), props.getDriverUrl());
        // Before the remote check, because a misspelled dialect is answerable without a source and its
        // error should not be preceded by a connection failure the user cannot act on.
        dialectSelector.validateConfiguredName();
        checkRemoteCatalogIsPinned();
    }

    /**
     * Verifies that {@code uri} pins a remote catalog, which is what lets ADBC's three naming levels be
     * shown as Doris's two without concatenating anything (see {@link AdbcNamespace}).
     *
     * <p><b>A driver that cannot answer is trusted, not refused.</b> {@code getCurrentCatalog} is optional
     * in ADBC and drivers implement these context getters one by one -- the SQLite driver answers
     * {@code getCurrentCatalog} but rejects {@code getCurrentDbSchema} with {@code NOT_FOUND}, so the
     * failure status does not even reliably say "not implemented". Refusing on any error would therefore
     * lock out drivers over a missing self-check rather than a misconfiguration, and a genuinely unpinned
     * uri still surfaces clearly at the first {@code SHOW DATABASES}. Only an answer that arrives and is
     * empty is treated as a real "not pinned".
     */
    private void checkRemoteCatalogIsPinned() {
        String currentCatalog;
        try {
            currentCatalog = getOrCreateClient().withConnection(
                    connection -> connection.getCurrentCatalog());
        } catch (DorisConnectorException e) {
            // AdbcClient already funnels every driver-side failure (any AdbcException, whatever its status)
            // into this one type, so catching it covers "the driver could not answer" in full.
            LOG.info("ADBC driver cannot report its current catalog, so the '{}' property is accepted"
                            + " as written: {}", AdbcCatalogProperties.URI, e.toString());
            return;
        }
        if (currentCatalog != null && currentCatalog.isEmpty()) {
            throw new DorisConnectorException("The ADBC source reports no current catalog, so '"
                    + AdbcCatalogProperties.URI + "' does not pin one. Name the remote catalog in the"
                    + " uri (for example postgresql://host:5432/mydb) or through a driver option, because"
                    + " a Doris catalog maps exactly one remote catalog.");
        }
    }

    private Path resolveDriverPath() {
        Path driverPath = AdbcDriverPathResolver.resolve(props.getDriverUrl(),
                AdbcConf.driversDir(context), AdbcConf.driverSecurePath(context));
        AdbcDriverPathResolver.checkExists(driverPath, props.getDriverUrl());
        return driverPath;
    }

    private AdbcClient getOrCreateClient() {
        if (closed) {
            throw new DorisConnectorException("AdbcConnector has been closed");
        }
        AdbcClient existing = client;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (closed) {
                throw new DorisConnectorException("AdbcConnector has been closed");
            }
            if (client == null) {
                // Deliberately NOT in the constructor: an FE follower replaying the edit log builds every
                // catalog, and its filesystem layout need not match the leader's, so resolving the driver
                // here would let a missing file stop FE from starting.
                client = new AdbcClient(resolveDriverPath(),
                        props.getDriverUrl(),
                        props.getDriverEntrypoint(),
                        props.getUri(),
                        props.getUser(),
                        props.getPassword(),
                        props.getDriverOptions());
            }
            return client;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (client != null) {
            client.close();
            client = null;
        }
    }
}

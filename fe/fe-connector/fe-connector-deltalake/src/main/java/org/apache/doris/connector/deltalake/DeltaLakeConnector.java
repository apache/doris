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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final class DeltaLakeConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeConnector.class);
    private static final ExecutorService RESOURCE_CLOSER = resourceCloser();

    private final DeltaLakeCatalogProperties properties;
    private final ConnectorContext context;
    private final CatalogMetaCache metaCache;
    private final DeltaLakeSnapshotCache snapshotCache;
    private DeltaLakeRuntime runtime;
    private boolean closed;

    DeltaLakeConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = DeltaLakeCatalogProperties.of(properties);
        this.context = context;
        metaCache = CatalogMetaCache.managed(context.getCatalogId(), "deltalake", properties);
        snapshotCache = new DeltaLakeSnapshotCache(metaCache, properties);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new DeltaLakeConnectorMetadata(runtime(), properties, snapshotCache);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new DeltaLakeScanPlanProvider(runtime());
    }

    private synchronized DeltaLakeRuntime runtime() {
        if (closed) {
            throw new DorisConnectorException("Delta catalog is closed: " + context.getCatalogName());
        }
        if (runtime == null) {
            // Resource construction sets up local configuration and lazy clients; network reads start
            // after this monitor has been released, in metadata and scan operations.
            runtime = new DeltaLakeRuntime(properties, context);
        }
        return runtime;
    }

    @Override
    public boolean ownsHandle(ConnectorTableHandle handle) {
        return handle instanceof DeltaLakeTableHandle;
    }

    @Override
    public Set<ConnectorCapability> getCapabilities() {
        return Collections.singleton(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT);
    }

    @Override
    public OptionalLong schemaCacheTtlSecondOverride() {
        // Schema is derived from the statement's Delta snapshot, never a separately cached latest schema.
        return OptionalLong.of(0);
    }

    @Override
    public void invalidateTable(String database, String table) {
        metaCache.invalidateTable(database, table);
    }

    @Override
    public void invalidateDb(String database) {
        metaCache.invalidateDatabase(database);
    }

    @Override
    public void invalidateAll() {
        metaCache.invalidateCatalog();
    }

    @Override
    public void close() {
        DeltaLakeRuntime owned;
        synchronized (this) {
            closed = true;
            owned = runtime;
            runtime = null;
        }
        metaCache.close();
        if (owned != null) {
            // ALTER/DROP may hold the catalog write lock. S3/HDFS client shutdown can wait for
            // networking threads; perform it outside that lock, as the Hudi connector does.
            RESOURCE_CLOSER.execute(() -> {
                try {
                    owned.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close Delta catalog resources for {}", context.getCatalogName(), e);
                }
            });
        }
    }

    private static ExecutorService resourceCloser() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), action -> {
                    Thread thread = new Thread(action, "deltalake-resource-closer");
                    thread.setDaemon(true);
                    thread.setContextClassLoader(DeltaLakeConnector.class.getClassLoader());
                    return thread;
                });
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }
}

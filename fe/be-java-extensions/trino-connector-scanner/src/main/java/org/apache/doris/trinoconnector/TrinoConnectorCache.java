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

package org.apache.doris.trinoconnector;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.node.NodeInfo;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.connector.CatalogFactory;
import io.trino.connector.ConnectorName;
import io.trino.connector.DefaultCatalogFactory;
import io.trino.connector.LazyCatalogFactory;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.TypeRegistry;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.testing.TestingAccessControlManager;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.type.InternalTypeManager;
import io.trino.util.EmbedVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used to cache connector
 */
public class TrinoConnectorCache {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorCache.class);

    // Max cache num of trino connector
    public static final long max_external_schema_cache_num = 100;
    // 24 hour
    private static final int CACHE_EXPIRATION_MINUTES = 1440;

    private static LoadingCache<TrinoConnectorCacheKey, TrinoConnectorCacheValue> connectorCache
            = CacheBuilder.newBuilder()
            .maximumSize(max_external_schema_cache_num)
            .expireAfterAccess(CACHE_EXPIRATION_MINUTES, TimeUnit.MINUTES)
            .removalListener(new RemovalListener<TrinoConnectorCacheKey, TrinoConnectorCacheValue>() {
                @Override
                public void onRemoval(RemovalNotification<TrinoConnectorCacheKey,
                        TrinoConnectorCacheValue> notification) {
                    invalidateTableCache(notification.getKey(), notification.getValue());
                }
            })
            .build(new CacheLoader<TrinoConnectorCacheKey, TrinoConnectorCacheValue>() {
                @Override
                public TrinoConnectorCacheValue load(TrinoConnectorCacheKey key) {
                    return loadCache(key);
                }
            });

    public static TrinoConnectorCacheValue getConnector(TrinoConnectorCacheKey key) {
        try {
            LOG.info("Connector cache size is : " + connectorCache.size());
            return connectorCache.get(key);
        } catch (Exception e) {
            throw new RuntimeException("failed to get connector for:" + key);
        }
    }

    private static TrinoConnectorCacheValue loadCache(TrinoConnectorCacheKey key) {
        try {
            LOG.info("load connector:{}", key);
            HandleResolver handleResolver = key.trinoConnectorPluginManager.getHandleResolver();
            ConnectorName connectorName = new ConnectorName(key.connectorName);

            // RecordReader will use ProcessBuilder to start a hotspot process, which may be stuck,
            // so use another process to kill this stuck process.
            AtomicBoolean isKilled = new AtomicBoolean(false);
            ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
            executorService.scheduleAtFixedRate(() -> {
                if (!isKilled.get()) {
                    List<Long> pids = ProcessUtils.getChildProcessIds(
                            ProcessUtils.getCurrentProcId());
                    for (long pid : pids) {
                        String cmd = ProcessUtils.getCommandLine(pid);
                        if (cmd != null && cmd.contains("org.openjdk.jol.vm.sa.AttachMain")) {
                            ProcessUtils.killProcess(pid);
                            isKilled.set(true);
                        }
                    }
                }
            }, 100, 1000, TimeUnit.MILLISECONDS);

            // create CatalogHandle
            CatalogHandle catalogHandle = CatalogHandle.createRootCatalogHandle(key.catalogName,
                    new CatalogVersion("test"));

            isKilled.set(true);
            executorService.shutdownNow();

            CatalogFactory catalogFactory = createCatalogFactory(key.trinoConnectorPluginManager.getTypeRegistry(),
                    key.trinoConnectorPluginManager.getConnectorFactories().get(connectorName));
            TrinoConnectorServicesProvider trinoConnectorServicesProvider = new TrinoConnectorServicesProvider(
                    key.catalogName, key.connectorName, catalogFactory, key.properties, MoreExecutors.directExecutor());
            trinoConnectorServicesProvider.loadInitialCatalogs();

            Connector connector = trinoConnectorServicesProvider.getConnectorServices(catalogHandle).getConnector();
            return new TrinoConnectorCacheValue(catalogHandle, connector,
                    handleResolver, trinoConnectorServicesProvider);
        } catch (Exception e) {
            LOG.warn("failed to create trino connector", e);
            throw new RuntimeException(e);
        }
    }

    public static void invalidateTableCache(TrinoConnectorCacheKey key, TrinoConnectorCacheValue value) {
        try {
            LOG.info("remove connector:{}", key);

            Connector connector = value.getConnector();
            if (connector != null) {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
                        connector.getClass().getClassLoader())) {
                    connector.shutdown();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to invalidate connector for: " + key);
        }
    }

    private static CatalogFactory createCatalogFactory(TypeRegistry typeRegistry, ConnectorFactory connectorFactory) {
        LazyCatalogFactory catalogFactory = new LazyCatalogFactory();
        NoOpTransactionManager noOpTransactionManager = new NoOpTransactionManager();
        TestingAccessControlManager accessControl = new TestingAccessControlManager(noOpTransactionManager,
                new EventListenerManager(new EventListenerConfig()));
        accessControl.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        catalogFactory.setCatalogFactory(new DefaultCatalogFactory(
                MetadataManager.createTestMetadataManager(),
                accessControl,
                new InMemoryNodeManager(),
                new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                new GroupByHashPageIndexerFactory(new JoinCompiler(typeRegistry.getTypeOperators())),
                new NodeInfo("test"),
                EmbedVersion.testingVersionEmbedder(),
                OpenTelemetry.noop(),
                noOpTransactionManager,
                new InternalTypeManager(typeRegistry),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new OptimizerConfig()));
        catalogFactory.addConnectorFactory(connectorFactory);
        return catalogFactory;
    }

    public static class TrinoConnectorCacheKey {
        private String catalogName;
        private String connectorName;
        private String createTime;
        // other properties
        private Map<String, String> properties;
        private TrinoConnectorPluginManager trinoConnectorPluginManager;

        public TrinoConnectorCacheKey(String catalogName, String connectorName, String createTime) {
            this.catalogName = catalogName;
            this.connectorName = connectorName;
            this.createTime = createTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TrinoConnectorCacheKey that = (TrinoConnectorCacheKey) o;
            return Objects.equals(catalogName, that.catalogName) && Objects.equals(connectorName, that.connectorName)
                    && Objects.equals(createTime, that.createTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, connectorName, createTime);
        }

        @Override
        public String toString() {
            return "TrinoConnectorCacheKey{" + "catalogName='" + catalogName + '\'' + ", connectorName='"
                    + connectorName + '\'' + ", createTime='" + createTime + '\'' + '}';
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }

        public void setTrinoConnectorPluginManager(TrinoConnectorPluginManager trinoConnectorPluginManager) {
            this.trinoConnectorPluginManager = trinoConnectorPluginManager;
        }
    }

    public static class TrinoConnectorCacheValue {
        private CatalogHandle catalogHandle;
        private Connector connector;
        private HandleResolver handleResolver;
        private TrinoConnectorServicesProvider trinoConnectorServicesProvider;

        public TrinoConnectorCacheValue(CatalogHandle catalogHandle, Connector connector, HandleResolver handleResolver,
                TrinoConnectorServicesProvider trinoConnectorServicesProvider) {
            this.catalogHandle = catalogHandle;
            this.connector = connector;
            this.handleResolver = handleResolver;
            this.trinoConnectorServicesProvider = trinoConnectorServicesProvider;
        }

        public CatalogHandle getCatalogHandle() {
            return catalogHandle;
        }

        public Connector getConnector() {
            return connector;
        }

        public HandleResolver getHandleResolver() {
            return handleResolver;
        }

        public TrinoConnectorServicesProvider getTrinoConnectorServicesProvider() {
            return trinoConnectorServicesProvider;
        }
    }
}

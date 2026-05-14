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

package org.apache.doris.connector.trino;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.node.NodeInfo;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.client.ClientCapabilities;
import io.trino.connector.CatalogServiceProviderModule;
import io.trino.connector.ConnectorName;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.DefaultCatalogFactory;
import io.trino.connector.LazyCatalogFactory;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TypeRegistry;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.server.ServerPluginsProvider;
import io.trino.server.ServerPluginsProviderConfig;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.testing.TestingAccessControlManager;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.type.InternalTypeManager;
import io.trino.util.EmbedVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

/**
 * Manages Trino Connector bootstrapping within the Doris connector plugin.
 *
 * <p>Combines the singleton plugin-loading logic from {@code TrinoConnectorPluginLoader}
 * and the per-catalog connector creation logic from
 * {@code TrinoConnectorExternalCatalog.initLocalObjectsImpl()}.</p>
 *
 * <p>Thread-safe: plugin initialization is done once (double-checked locking),
 * and per-catalog creation is synchronized on the instance level.</p>
 */
public class TrinoBootstrap {

    private static final Logger LOG = LogManager.getLogger(TrinoBootstrap.class);

    private static final Object INIT_LOCK = new Object();
    private static volatile TrinoBootstrap instance;

    // Singleton Trino infrastructure (loaded once across all catalogs)
    private final FeaturesConfig featuresConfig;
    private final TypeOperators typeOperators;
    private final HandleResolver handleResolver;
    private final TypeRegistry typeRegistry;
    private final TrinoPluginManager pluginManager;

    private TrinoBootstrap(String pluginDir) {
        System.setProperty("jdk.attach.allowAttachSelf", "true");
        String osName = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
        if (osName.contains("mac") || osName.contains("darwin")) {
            System.setProperty("jol.skipHotspotSAAttach", "true");
        }

        configureJulLogging();

        this.featuresConfig = new FeaturesConfig();
        this.typeOperators = new TypeOperators();
        this.handleResolver = new HandleResolver();
        this.typeRegistry = new TypeRegistry(typeOperators, featuresConfig);

        ServerPluginsProviderConfig config = new ServerPluginsProviderConfig()
                .setInstalledPluginsDir(new File(pluginDir));
        ServerPluginsProvider serverPluginsProvider = new ServerPluginsProvider(
                config, MoreExecutors.directExecutor());
        this.pluginManager = new TrinoPluginManager(
                serverPluginsProvider, typeRegistry, handleResolver);
        pluginManager.loadPlugins();

        LOG.info("TrinoBootstrap initialized, plugin dir: {}", pluginDir);
    }

    /**
     * Get or initialize the singleton TrinoBootstrap with the given plugin directory.
     */
    public static TrinoBootstrap getInstance(String pluginDir) {
        if (instance == null) {
            synchronized (INIT_LOCK) {
                if (instance == null) {
                    instance = new TrinoBootstrap(pluginDir);
                }
            }
        }
        return instance;
    }

    /**
     * Returns the HandleResolver for JSON serialization of Trino SPI handles.
     */
    public HandleResolver getHandleResolver() {
        return handleResolver;
    }

    /**
     * Returns the TypeRegistry for JSON serialization of Trino types/blocks.
     */
    public TypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    /**
     * Create a Trino Connector + Session for a specific Doris catalog.
     *
     * @param catalogName        Doris catalog name
     * @param connectorNameStr   Trino connector name (e.g., "hive", "iceberg")
     * @param trinoProperties    Trino connector properties (already stripped of "trino." prefix)
     * @return bootstrap result containing Connector, Session, CatalogHandle, ConnectorName
     */
    public TrinoConnectionResult createConnection(String catalogName, String connectorNameStr,
            Map<String, String> trinoProperties) {
        ConnectorName connectorName = new ConnectorName(connectorNameStr);

        // 1. Create CatalogFactory with Trino internal machinery
        LazyCatalogFactory catalogFactory = new LazyCatalogFactory();
        NoOpTransactionManager noOpTransactionManager = new NoOpTransactionManager();
        TestingAccessControlManager accessControl = new TestingAccessControlManager(
                noOpTransactionManager,
                new EventListenerManager(new EventListenerConfig()));
        accessControl.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        catalogFactory.setCatalogFactory(new DefaultCatalogFactory(
                MetadataManager.createTestMetadataManager(),
                accessControl,
                new InMemoryNodeManager(),
                new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                new GroupByHashPageIndexerFactory(new JoinCompiler(typeOperators)),
                new NodeInfo("test"),
                EmbedVersion.testingVersionEmbedder(),
                OpenTelemetry.noop(),
                noOpTransactionManager,
                new InternalTypeManager(typeRegistry),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new OptimizerConfig()));

        // 2. Find and register ConnectorFactory
        Optional<ConnectorFactory> connectorFactory = Optional.ofNullable(
                pluginManager.getConnectorFactories().get(connectorName));
        if (!connectorFactory.isPresent()) {
            throw new RuntimeException(
                    "Cannot find Trino ConnectorFactory for '" + connectorNameStr
                    + "'. Ensure Trino plugin is installed in the plugin directory.");
        }
        catalogFactory.addConnectorFactory(connectorFactory.get());

        // 3. Create services provider and load catalog
        CatalogHandle trinoCatalogHandle = CatalogHandle.createRootCatalogHandle(
                catalogName, new CatalogVersion("test"));
        Map<String, String> connectorProps = new HashMap<>(trinoProperties);
        connectorProps.remove("connector.name");

        TrinoServicesProvider servicesProvider = new TrinoServicesProvider(
                catalogName, connectorNameStr, catalogFactory,
                connectorProps, MoreExecutors.directExecutor());
        servicesProvider.loadInitialCatalogs();

        Connector connector = servicesProvider.getConnectorServices(trinoCatalogHandle).getConnector();

        // 4. Build Trino Session
        SessionPropertyManager sessionPropertyManager = createSessionPropertyManager(servicesProvider);
        QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
        Session trinoSession = Session.builder(sessionPropertyManager)
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(Identity.ofUser("user"))
                .setOriginalIdentity(Identity.ofUser("user"))
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(ZoneId.systemDefault().toString()))
                .setLocale(Locale.ENGLISH)
                .setClientCapabilities(Arrays.stream(ClientCapabilities.values())
                        .map(Enum::name)
                        .collect(ImmutableSet.toImmutableSet()))
                .setRemoteUserAddress("address")
                .setUserAgent("agent")
                .build();

        return new TrinoConnectionResult(connector, trinoSession, trinoCatalogHandle, connectorName);
    }

    private SessionPropertyManager createSessionPropertyManager(
            ConnectorServicesProvider servicesProvider) {
        Set<SystemSessionPropertiesProvider> systemSessionProperties =
                ImmutableSet.<SystemSessionPropertiesProvider>builder()
                        .add(new SystemSessionProperties(
                                new QueryManagerConfig(),
                                new TaskManagerConfig(),
                                new MemoryManagerConfig(),
                                featuresConfig,
                                new OptimizerConfig(),
                                new NodeMemoryConfig(),
                                new DynamicFilterConfig(),
                                new NodeSchedulerConfig()))
                        .build();
        return CatalogServiceProviderModule.createSessionPropertyManager(
                systemSessionProperties, servicesProvider);
    }

    private static void configureJulLogging() {
        try {
            System.setProperty("java.util.logging.SimpleFormatter.format",
                    "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s: %5$s%6$s%n");
            java.util.logging.Logger logger = java.util.logging.Logger.getLogger("");
            logger.setUseParentHandlers(false);

            String dorisHome = System.getenv("DORIS_HOME");
            if (dorisHome != null) {
                FileHandler fileHandler = new FileHandler(
                        dorisHome + "/log/trinoconnector%g.log",
                        500000000, 10, true);
                fileHandler.setLevel(Level.INFO);
                fileHandler.setFormatter(new SimpleFormatter());
                logger.addHandler(fileHandler);
                java.util.logging.LogManager.getLogManager().addLogger(logger);
            }
        } catch (Exception e) {
            LOG.warn("Failed to configure JUL logging for Trino connector", e);
        }
    }

    /**
     * Resolves the Trino plugin directory from catalog properties.
     * Falls back to DORIS_HOME/plugins/connectors and DORIS_HOME/connectors.
     */
    public static String resolvePluginDir(Map<String, String> properties) {
        String explicitDir = properties.get("trino.plugin.dir");
        if (explicitDir != null && !explicitDir.isEmpty()) {
            return explicitDir;
        }

        String dorisHome = System.getenv("DORIS_HOME");
        if (dorisHome == null) {
            dorisHome = ".";
        }

        String defaultDir = dorisHome + "/plugins/connectors";
        String oldDir = dorisHome + "/connectors";
        File oldDirFile = new File(oldDir);
        if (oldDirFile.exists() && oldDirFile.isDirectory()) {
            String[] contents = oldDirFile.list();
            if (contents != null && contents.length > 0) {
                return oldDir;
            }
        }
        return defaultDir;
    }

    /**
     * Result of creating a Trino connection for a Doris catalog.
     */
    public static class TrinoConnectionResult {
        private final Connector connector;
        private final Session session;
        private final CatalogHandle catalogHandle;
        private final ConnectorName connectorName;

        public TrinoConnectionResult(Connector connector, Session session,
                CatalogHandle catalogHandle, ConnectorName connectorName) {
            this.connector = Objects.requireNonNull(connector);
            this.session = Objects.requireNonNull(session);
            this.catalogHandle = Objects.requireNonNull(catalogHandle);
            this.connectorName = Objects.requireNonNull(connectorName);
        }

        public Connector getConnector() {
            return connector;
        }

        public Session getSession() {
            return session;
        }

        public CatalogHandle getCatalogHandle() {
            return catalogHandle;
        }

        public ConnectorName getConnectorName() {
            return connectorName;
        }
    }
}

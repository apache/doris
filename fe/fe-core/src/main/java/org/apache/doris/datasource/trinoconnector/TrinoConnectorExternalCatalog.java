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

package org.apache.doris.datasource.trinoconnector;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog.Type;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.trinoconnector.TrinoConnectorServicesProvider;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.node.NodeInfo;
import io.opentelemetry.api.OpenTelemetry;
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
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SessionPropertyManager;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.testing.TestingAccessControlManager;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.type.InternalTypeManager;
import io.trino.util.EmbedVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TrinoConnectorExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorExternalCatalog.class);
    private static final String TRINO_CONNECTOR_PROPERTIES_PREFIX = "trino.";
    public static final String TRINO_CONNECTOR_NAME = "trino.connector.name";
    private static final String APPLICATION_SHUTDOWN_HOOKS_CLASS = "java.lang.ApplicationShutdownHooks";
    private static final String HDFS_CLASSLOADER_CLASS = "io.trino.filesystem.manager.HdfsClassLoader";

    private static final List<String> TRINO_CONNECTOR_REQUIRED_PROPERTIES = ImmutableList.of(
            TRINO_CONNECTOR_NAME
    );

    private CatalogHandle trinoCatalogHandle;
    private Connector connector;
    private ConnectorName connectorName;
    private Session trinoSession;
    private ImmutableMap<String, String> trinoProperties;

    public TrinoConnectorExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, Type.TRINO_CONNECTOR, comment);
        Objects.requireNonNull(name, "catalogName is null");
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public void onClose() {
        super.onClose();
        try {
            if (connector != null) {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
                        connector.getClass().getClassLoader())) {
                    connector.shutdown();
                }
            }
        } finally {
            removeTrinoHdfsShutdownHooks();
        }
    }

    /**
     * Each Trino HDFS classloader may register one Hadoop shutdown hook thread.
     * These hooks are JVM GC roots and can pin HdfsClassLoader instances after catalog drop.
     */
    private void removeTrinoHdfsShutdownHooks() {
        List<Thread> hooksToRemove = new ArrayList<>();
        List<String> matchedHookSamples = new ArrayList<>();
        int totalHooks = 0;
        int matchedHooks = 0;
        Class<?> shutdownHooksClass;
        Field hooksField;
        try {
            shutdownHooksClass = Class.forName(APPLICATION_SHUTDOWN_HOOKS_CLASS);
            hooksField = shutdownHooksClass.getDeclaredField("hooks");
            hooksField.setAccessible(true);
            Object hookMapObj = hooksField.get(null);
            if (!(hookMapObj instanceof Map<?, ?>)) {
                LOG.info("Skip HDFS shutdown hook cleanup for trino catalog {} because hooks map type is {}",
                        name, hookMapObj == null ? "null" : hookMapObj.getClass().getName());
                return;
            }
            Map<?, ?> hookMap = (Map<?, ?>) hookMapObj;
            synchronized (shutdownHooksClass) {
                totalHooks = hookMap.size();
                for (Object hookObj : hookMap.keySet()) {
                    if (!(hookObj instanceof Thread)) {
                        continue;
                    }
                    Thread hook = (Thread) hookObj;
                    ClassLoader contextClassLoader = hook.getContextClassLoader();
                    if (isTrinoHdfsClassLoader(contextClassLoader)) {
                        hooksToRemove.add(hook);
                        matchedHooks++;
                        if (matchedHookSamples.size() < 5) {
                            matchedHookSamples.add(describeHook(hook));
                        }
                    }
                }
            }
        } catch (ReflectiveOperationException e) {
            LOG.warn("Failed to inspect shutdown hooks when closing trino catalog {}", name, e);
            return;
        }

        int removed = 0;
        int removeMiss = 0;
        List<String> removeMissSamples = new ArrayList<>();
        for (Thread hook : hooksToRemove) {
            try {
                if (Runtime.getRuntime().removeShutdownHook(hook)) {
                    removed++;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Removed HDFS shutdown hook for trino catalog {}: {}", name, describeHook(hook));
                    }
                } else {
                    removeMiss++;
                    if (removeMissSamples.size() < 5) {
                        removeMissSamples.add(describeHook(hook));
                    }
                }
            } catch (IllegalStateException e) {
                LOG.warn("Shutdown in progress when removing hooks for trino catalog {}", name, e);
                return;
            }
        }

        int remainingMatchedHooks = countRemainingMatchedHooks(shutdownHooksClass, hooksField);
        LOG.info("Trino catalog {} shutdown-hook cleanup summary: totalHooksBefore={}, matchedBefore={}, "
                        + "removed={}, removeMiss={}, remainingMatched={}, matchedSamples={}, removeMissSamples={}",
                name, totalHooks, matchedHooks, removed, removeMiss, remainingMatchedHooks,
                matchedHookSamples, removeMissSamples);
    }

    private int countRemainingMatchedHooks(Class<?> shutdownHooksClass, Field hooksField) {
        try {
            Object hookMapObj = hooksField.get(null);
            if (!(hookMapObj instanceof Map<?, ?>)) {
                return -1;
            }
            Map<?, ?> hookMap = (Map<?, ?>) hookMapObj;
            int remaining = 0;
            synchronized (shutdownHooksClass) {
                for (Object hookObj : hookMap.keySet()) {
                    if (!(hookObj instanceof Thread)) {
                        continue;
                    }
                    Thread hook = (Thread) hookObj;
                    if (isTrinoHdfsClassLoader(hook.getContextClassLoader())) {
                        remaining++;
                    }
                }
            }
            return remaining;
        } catch (IllegalAccessException e) {
            LOG.warn("Failed to count remaining HDFS shutdown hooks for trino catalog {}", name, e);
            return -1;
        }
    }

    private boolean isTrinoHdfsClassLoader(ClassLoader contextClassLoader) {
        return contextClassLoader != null
                && HDFS_CLASSLOADER_CLASS.equals(contextClassLoader.getClass().getName());
    }

    private String describeHook(Thread hook) {
        ClassLoader contextClassLoader = hook.getContextClassLoader();
        String classLoaderName = contextClassLoader == null ? "null" : contextClassLoader.getClass().getName();
        String classLoaderId = contextClassLoader == null ? "null"
                : Integer.toHexString(System.identityHashCode(contextClassLoader));
        return "thread=" + hook.getName()
                + ",id=" + hook.getId()
                + ",alive=" + hook.isAlive()
                + ",state=" + hook.getState()
                + ",cl=" + classLoaderName + "@" + classLoaderId;
    }

    @Override
    protected void initLocalObjectsImpl() {
        this.trinoCatalogHandle = CatalogHandle.createRootCatalogHandle(name, new CatalogVersion("test"));
        // All properties obtained by this method are used by the trino-connector.
        // We should not modify this map
        trinoProperties = ImmutableMap.copyOf(catalogProperty.getProperties().entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(TRINO_CONNECTOR_PROPERTIES_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(TRINO_CONNECTOR_PROPERTIES_PREFIX.length()),
                                kv1 -> kv1.getValue())));

        ConnectorServicesProvider connectorServicesProvider = createConnectorServicesProvider();

        this.connector = connectorServicesProvider.getConnectorServices(trinoCatalogHandle).getConnector();
        SessionPropertyManager sessionPropertyManager = createTrinoSessionPropertyManager(connectorServicesProvider);

        QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
        this.trinoSession = Session.builder(sessionPropertyManager)
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(Identity.ofUser("user"))
                .setOriginalIdentity(Identity.ofUser("user"))
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(ZoneId.systemDefault().toString()))
                .setLocale(Locale.ENGLISH)
                .setClientCapabilities(Arrays.stream(ClientCapabilities.values()).map(Enum::name)
                        .collect(ImmutableSet.toImmutableSet()))
                .setRemoteUserAddress("address")
                .setUserAgent("agent")
                .build();
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        for (String requiredProperty : TRINO_CONNECTOR_REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
    }

    @Override
    protected List<String> listDatabaseNames() {
        ConnectorSession connectorSession = trinoSession.toConnectorSession(trinoCatalogHandle);
        ConnectorTransactionHandle connectorTransactionHandle = this.connector.beginTransaction(
                IsolationLevel.READ_UNCOMMITTED, true, true);
        ConnectorMetadata connectorMetadata = this.connector.getMetadata(connectorSession, connectorTransactionHandle);
        return connectorMetadata.listSchemaNames(connectorSession);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return getTrinoConnectorTable(dbName, tblName).isPresent();
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        QualifiedTablePrefix qualifiedTablePrefix = new QualifiedTablePrefix(trinoCatalogHandle.getCatalogName(),
                dbName);
        List<QualifiedObjectName> tables = trinoListTables(qualifiedTablePrefix);
        return tables.stream().map(field -> field.getObjectName()).collect(Collectors.toList());
    }

    private ConnectorServicesProvider createConnectorServicesProvider() {
        // 1. check and create ConnectorName
        if (!trinoProperties.containsKey("connector.name")) {
            throw new RuntimeException("Can not find trino.connector.name property, please specify a connector name.");
        }
        Map<String, String> trinoConnectorProperties = new HashMap<>();
        trinoConnectorProperties.putAll(trinoProperties);
        String connectorNameString = trinoConnectorProperties.remove("connector.name");
        Objects.requireNonNull(connectorNameString, "connectorName is null");
        if (connectorNameString.indexOf('-') >= 0) {
            String deprecatedConnectorName = connectorNameString;
            connectorNameString = connectorNameString.replace('-', '_');
            LOG.warn("You are using the deprecated connector name '{}'. The correct connector name is '{}'",
                    deprecatedConnectorName, connectorNameString);
        }

        this.connectorName = new ConnectorName(connectorNameString);

        // 2. create CatalogFactory
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
                new GroupByHashPageIndexerFactory(new JoinCompiler(TrinoConnectorPluginLoader.getTypeOperators())),
                new NodeInfo("test"),
                EmbedVersion.testingVersionEmbedder(),
                OpenTelemetry.noop(),
                noOpTransactionManager,
                new InternalTypeManager(TrinoConnectorPluginLoader.getTypeRegistry()),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new OptimizerConfig()));

        Optional<ConnectorFactory> connectorFactory = Optional.ofNullable(
                TrinoConnectorPluginLoader.getTrinoConnectorPluginManager().getConnectorFactories().get(connectorName));
        if (!connectorFactory.isPresent()) {
            throw new RuntimeException("Can not find connectorFactory, did you forget to install plugins?");
        }
        catalogFactory.addConnectorFactory(connectorFactory.get());

        // 3. create TrinoConnectorServicesProvider
        TrinoConnectorServicesProvider trinoConnectorServicesProvider = new TrinoConnectorServicesProvider(
                trinoCatalogHandle.getCatalogName(), connectorNameString, catalogFactory,
                trinoConnectorProperties, MoreExecutors.directExecutor());
        trinoConnectorServicesProvider.loadInitialCatalogs();
        return trinoConnectorServicesProvider;
    }

    private SessionPropertyManager createTrinoSessionPropertyManager(
            ConnectorServicesProvider trinoConnectorServicesProvider) {
        Set<SystemSessionPropertiesProvider> extraSessionProperties = ImmutableSet.of();
        Set<SystemSessionPropertiesProvider> systemSessionProperties =
                ImmutableSet.<SystemSessionPropertiesProvider>builder()
                        .addAll(Objects.requireNonNull(extraSessionProperties, "extraSessionProperties is null"))
                        .add(new SystemSessionProperties(
                                new QueryManagerConfig(),
                                new TaskManagerConfig(),
                                new MemoryManagerConfig(),
                                TrinoConnectorPluginLoader.getFeaturesConfig(),
                                new OptimizerConfig(),
                                new NodeMemoryConfig(),
                                new DynamicFilterConfig(),
                                new NodeSchedulerConfig()))
                        .build();

        return CatalogServiceProviderModule.createSessionPropertyManager(systemSessionProperties,
                trinoConnectorServicesProvider);
    }

    private List<QualifiedObjectName> trinoListTables(QualifiedTablePrefix prefix) {
        Objects.requireNonNull(prefix, "prefix can not be null");

        Set<QualifiedObjectName> tables = new LinkedHashSet();
        ConnectorSession connectorSession = trinoSession.toConnectorSession(trinoCatalogHandle);
        ConnectorTransactionHandle connectorTransactionHandle = this.connector.beginTransaction(
                IsolationLevel.READ_UNCOMMITTED, true, true);
        ConnectorMetadata connectorMetadata = this.connector.getMetadata(connectorSession, connectorTransactionHandle);
        List<SchemaTableName> schemaTableNames = connectorMetadata.listTables(connectorSession, prefix.getSchemaName());
        List<QualifiedObjectName> tmpTables = new ArrayList<>();
        for (SchemaTableName schemaTableName : schemaTableNames) {
            QualifiedObjectName objName = QualifiedObjectName.convertFromSchemaTableName(prefix.getCatalogName())
                    .apply(schemaTableName);
            tmpTables.add(objName);
        }
        Objects.requireNonNull(tables);
        tmpTables.stream().filter(prefix::matches).forEach(tables::add);
        return ImmutableList.copyOf(tables);
    }

    public Optional<ConnectorTableHandle> getTrinoConnectorTable(String dbName, String tblName) {
        makeSureInitialized();
        QualifiedObjectName tableName = new QualifiedObjectName(trinoCatalogHandle.getCatalogName(), dbName, tblName);

        if (!tableName.getCatalogName().isEmpty()
                && !tableName.getSchemaName().isEmpty()
                && !tableName.getObjectName().isEmpty()) {
            ConnectorSession connectorSession = trinoSession.toConnectorSession(trinoCatalogHandle);
            ConnectorTransactionHandle connectorTransactionHandle = this.connector.beginTransaction(
                    IsolationLevel.READ_UNCOMMITTED, true, true);
            return Optional.ofNullable(
                    this.connector.getMetadata(connectorSession, connectorTransactionHandle)
                            .getTableHandle(connectorSession, tableName.asSchemaTableName(),
                                    Optional.empty(), Optional.empty()));
        }
        return Optional.empty();
    }

    // BE need create_time key
    public Map<String, String> getTrinoConnectorPropertiesWithCreateTime() {
        Map<String, String> trinoPropertiesWithCreateTime = new HashMap<>();
        trinoPropertiesWithCreateTime.putAll(trinoProperties);
        trinoPropertiesWithCreateTime.put("create_time", catalogProperty.getProperties().get("create_time"));
        return trinoPropertiesWithCreateTime;
    }

    public Connector getConnector() {
        return connector;
    }

    public ConnectorName getConnectorName() {
        return connectorName;
    }

    public CatalogHandle getTrinoCatalogHandle() {
        return trinoCatalogHandle;
    }

    public Session getTrinoSession() {
        return trinoSession;
    }
}

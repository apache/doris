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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog.Type;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.constants.TrinoConnectorProperties;
import org.apache.doris.datasource.shade.TrinoConnectorServicesProvider;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.node.NodeInfo;
import io.airlift.tracing.Tracing;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.client.ClientCapabilities;
import io.trino.connector.CatalogName;
import io.trino.connector.CatalogServiceProviderModule;
import io.trino.connector.ConnectorAwareNodeManager;
import io.trino.connector.ConnectorContextInstance;
import io.trino.connector.ConnectorName;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.DefaultCatalogFactory;
import io.trino.connector.InternalMetadataProvider;
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
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingSession;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.type.InternalTypeManager;
import io.trino.util.EmbedVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
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
    public static final String TRINO_CONNECTOR_HMS = "hms";
    public static final String TRINO_CONNECTOR_FILESYSTEM = "filesystem";

    private static final List<String> TRINO_HIVE_REQUIRED_PROPERTIES = ImmutableList.of(
            TrinoConnectorProperties.TRINO_CONNECTOR_NAME
    );

    private String catalogType = TRINO_CONNECTOR_HMS;
    private final CatalogName trinoCatalogName;
    private final CatalogHandle trinoCatalogHandle;
    private Session trinoSession;
    private Connector connector;
    private final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    private ConnectorServicesProvider trinoConnectorServicesProvider;

    public TrinoConnectorExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, Type.TRINO_CONNECTOR, comment);
        Objects.requireNonNull(name, "catalogName is null");
        catalogProperty = new CatalogProperty(resource, props);
        trinoCatalogName = new CatalogName(name);
        trinoCatalogHandle = createRootCatalogHandle(name, new CatalogVersion("test"));
    }

    private void initConnector() {
        Map<String, String> trinoConnectorProperties = getTrinoConnectorProperties();
        String connectorNameString = trinoConnectorProperties.remove("connector.name");
        Objects.requireNonNull(connectorNameString, "connectorName is null");
        if (connectorNameString.indexOf('-') >= 0) {
            String deprecatedConnectorName = connectorNameString;
            connectorNameString = connectorNameString.replace('-', '_');
            LOG.warn("You are using the deprecated connector name '{}'. The correct connector name is '{}'",
                    deprecatedConnectorName, connectorNameString);
        }
        ConnectorName connectorName = new ConnectorName(connectorNameString);

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
                new GroupByHashPageIndexerFactory(new JoinCompiler(
                        Env.getCurrentEnv().getTypeRegistry().getTypeOperators())),
                new NodeInfo("test"),
                EmbedVersion.testingVersionEmbedder(),
                OpenTelemetry.noop(),
                noOpTransactionManager,
                new InternalTypeManager(Env.getCurrentEnv().getTypeRegistry()),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new OptimizerConfig()));
        catalogFactory.addConnectorFactory(Env.getCurrentEnv().getPluginManager()
                .getConnectorFactories().get(connectorName));

        trinoConnectorServicesProvider = new TrinoConnectorServicesProvider(trinoCatalogName.getCatalogName(),
                connectorNameString, catalogFactory, trinoConnectorProperties, MoreExecutors.directExecutor());
        trinoConnectorServicesProvider.loadInitialCatalogs();
        this.connector = trinoConnectorServicesProvider.getConnectorServices(trinoCatalogHandle).getConnector();

        // solution 2: not need ConnectorServicesProvider, desc table has bug
        // CatalogProperties catalogProperties = new CatalogProperties(
        //         createRootCatalogHandle(trinoCatalogName.getCatalogName(), new CatalogVersion("default")),
        //         connectorName, ImmutableMap.copyOf(trinoConnectorProperties));
        // LOG.info("-- Loading catalog {} --", trinoCatalogName.getCatalogName());
        // CatalogConnector trinoCatalog = catalogFactory.createCatalog(catalogProperties);
        // LOG.info("-- Added catalog {} using connector {} --",
        //         trinoCatalogName.getCatalogName(), trinoCatalog.getConnectorName());
        // this.connector = trinoCatalog.getMaterializedConnector(trinoCatalogHandle.getType()).getConnector();
    }

    private SessionPropertyManager initTrinoSessionPropertyManager() {
        // solution 1: need ConnectorServicesProvider
        Set<SystemSessionPropertiesProvider> extraSessionProperties = ImmutableSet.of();
        Set<SystemSessionPropertiesProvider> systemSessionProperties =
                ImmutableSet.<SystemSessionPropertiesProvider>builder()
                        .addAll(Objects.requireNonNull(extraSessionProperties, "extraSessionProperties is null"))
                        .add(new SystemSessionProperties(
                                new QueryManagerConfig(),
                                new TaskManagerConfig(),
                                new MemoryManagerConfig(),
                                Env.getCurrentEnv().getFeaturesConfig(),
                                new OptimizerConfig(),
                                new NodeMemoryConfig(),
                                new DynamicFilterConfig(),
                                new NodeSchedulerConfig()))
                        .build();

        return CatalogServiceProviderModule.createSessionPropertyManager(systemSessionProperties,
                trinoConnectorServicesProvider);

        // solution 2: not need ConnectorServicesProvider, desc table has bug
        // SystemSessionPropertiesProvider systemSessionProperties = new SystemSessionProperties(
        //                 new QueryManagerConfig(),
        //                 new TaskManagerConfig(),
        //                 new MemoryManagerConfig(),
        //                 Env.getCurrentEnv().getFeaturesConfig(),
        //                 new OptimizerConfig(),
        //                 new NodeMemoryConfig(),
        //                 new DynamicFilterConfig(),
        //                 new NodeSchedulerConfig());
        // return new SessionPropertyManager(systemSessionProperties);
    }

    @Override
    protected void init() {
        super.init();
    }

    public String getCatalogType() {
        makeSureInitialized();
        return catalogType;
    }

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
        QualifiedTablePrefix qualifiedTablePrefix = new QualifiedTablePrefix(trinoCatalogName.getCatalogName(), dbName);
        List<QualifiedObjectName> tables = trinoListTables(qualifiedTablePrefix);
        return tables.stream().map(field -> field.getObjectName()).collect(Collectors.toList());
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
        QualifiedObjectName tableName = new QualifiedObjectName(trinoCatalogName.getCatalogName(), dbName, tblName);

        if (!tableName.getCatalogName().isEmpty()
                && !tableName.getSchemaName().isEmpty()
                && !tableName.getObjectName().isEmpty()) {
            ConnectorSession connectorSession = trinoSession.toConnectorSession(trinoCatalogHandle);
            ConnectorTransactionHandle connectorTransactionHandle = this.connector.beginTransaction(
                    IsolationLevel.READ_UNCOMMITTED, true, true);
            return Optional.ofNullable(
                    this.connector.getMetadata(connectorSession, connectorTransactionHandle)
                            .getTableHandle(connectorSession, tableName.asSchemaTableName()));
        }
        return Optional.empty();
    }



    private Connector createConnector(CatalogName catalogName, ConnectorFactory connectorFactory,
            Map<String, String> properties) {
        InMemoryNodeManager inMemoryNodeManager = new InMemoryNodeManager();
        // inMemoryNodeManager.addCurrentNodeConnector(catalogName);
        TypeManager typeManager = new InternalTypeManager(Env.getCurrentEnv().getTypeRegistry());
        TypeOperators typeOperators = Env.getCurrentEnv().getTypeRegistry().getTypeOperators();

        ConnectorContext context = new ConnectorContextInstance(
                trinoCatalogHandle,
                OpenTelemetry.noop(),
                Tracing.noopTracer(),
                new ConnectorAwareNodeManager(inMemoryNodeManager, "testenv", trinoCatalogHandle, true),
                EmbedVersion.testingVersionEmbedder(),
                typeManager,
                new InternalMetadataProvider(MetadataManager.createTestMetadataManager(), typeManager),
                new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                new GroupByHashPageIndexerFactory(new JoinCompiler(typeOperators)));

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
                connectorFactory.getClass().getClassLoader())) {
            return connectorFactory.create(catalogName.getCatalogName(), properties, context);
        }
    }

    public Map<String, String> getTrinoConnectorProperties() {
        Map<String, String> properties = catalogProperty.getProperties();
        properties.remove("create_time");
        properties.remove("type");
        return properties;
    }

    public Connector getConnector() {
        return connector;
    }

    public CatalogName getTrinoCatalogName() {
        return trinoCatalogName;
    }

    public CatalogHandle getTrinoCatalogHandle() {
        return trinoCatalogHandle;
    }

    @Override
    protected void initLocalObjectsImpl() {
        initConnector();

        SessionPropertyManager sessionPropertyManager = initTrinoSessionPropertyManager();
        this.trinoSession = Session.builder(sessionPropertyManager)
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(Identity.ofUser("user"))
                .setOriginalIdentity(Identity.ofUser("user"))
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setTimeZoneKey(TestingSession.DEFAULT_TIME_ZONE_KEY)
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
        for (String requiredProperty : TRINO_HIVE_REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
    }

    @Override
    public void onClose() {
        super.onClose();
        if (connector != null) {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
                    connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
        }
    }

    public Session getTrinoSession() {
        return trinoSession;
    }
}

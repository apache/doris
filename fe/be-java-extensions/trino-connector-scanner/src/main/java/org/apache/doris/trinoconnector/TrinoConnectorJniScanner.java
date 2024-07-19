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

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.trinoconnector.TrinoConnectorCache.TrinoConnectorCacheKey;
import org.apache.doris.trinoconnector.TrinoConnectorCache.TrinoConnectorCacheValue;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.json.ObjectMapperProvider;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.client.ClientCapabilities;
import io.trino.connector.CatalogServiceProviderModule;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.split.RecordPageSourceProvider;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.type.InternalTypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TrinoConnectorJniScanner extends JniScanner {
    private static volatile int physicalProcessorCount = -1;
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorJniScanner.class);
    private static final String TRINO_CONNECTOR_PROPERTIES_PREFIX = "trino.";
    private final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();


    private final String catalogNameString;
    private final String catalogCreateTime;

    // these need to be deserialized
    private final String connectorSplitString;
    private final String connectorTableHandleString;
    private final String connectorColumnHandleString;
    private final String connectorColumnMetadataString;
    private final String connectorPredicateString;
    private final String connectorTrascationHandleString;

    // trinoConnectorOptionParams saves the properties that Trino needs to use
    private final Map<String, String> trinoConnectorOptionParams;


    private CatalogHandle catalogHandle;
    private Connector connector;
    private HandleResolver handleResolver;
    private Session session;
    private ObjectMapperProvider objectMapperProvider;


    private ConnectorPageSourceProvider pageSourceProvider;
    private ConnectorPageSource source;
    private ConnectorSplit connectorSplit;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private ConnectorTableHandle connectorTableHandle;
    private List<ColumnHandle> columns;
    private List<TrinoColumnMetadata> columnMetadataList = Lists.newArrayList();
    private DynamicFilter dynamicFilter = DynamicFilter.EMPTY;
    private List<Type> trinoTypeList;
    private long[] appendDataTimeNs;


    private final TrinoConnectorColumnValue columnValue = new TrinoConnectorColumnValue();
    private List<String> trinoConnectorAllFieldNames;


    public TrinoConnectorJniScanner(int batchSize, Map<String, String> params) {
        String[] requiredFields = params.get("required_fields").split(",");
        String[] requiredTypes = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        initTableInfo(columnTypes, requiredFields, batchSize);
        appendDataTimeNs = new long[fields.length];

        catalogNameString = params.get("catalog_name");
        connectorSplitString = params.get("trino_connector_split");
        connectorTableHandleString = params.get("trino_connector_table_handle");
        connectorColumnHandleString = params.get("trino_connector_column_handles");
        connectorColumnMetadataString = params.get("trino_connector_column_metadata");
        connectorPredicateString = params.get("trino_connector_predicate");
        connectorTrascationHandleString = params.get("trino_connector_trascation_handle");
        if (LOG.isDebugEnabled()) {
            LOG.debug("TrinoConnectorJniScanner connectorSplitString = " + connectorSplitString
                    + " ; connectorTableHandleString = " + connectorTableHandleString
                    + " ; connectorColumnHandleString = " + connectorColumnHandleString
                    + " ; connectorColumnMetadataString = " + connectorColumnMetadataString
                    + " ; connectorPredicateString = " + connectorPredicateString
                    + " ; connectorTrascationHandleString = " + connectorTrascationHandleString);
        }


        trinoConnectorOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(TRINO_CONNECTOR_PROPERTIES_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(TRINO_CONNECTOR_PROPERTIES_PREFIX.length()),
                                kv1 -> kv1.getValue()));
        catalogCreateTime = trinoConnectorOptionParams.remove("create_time");
    }

    @Override
    public void open() throws IOException {
        initConnector();
        this.pageSourceProvider = getConnectorPageSourceProvider();
        // mock ObjectMapperProvider
        this.objectMapperProvider = generateObjectMapperProvider();
        initTrinoTableMetadata();
        parseRequiredTypes();

        source = pageSourceProvider.createPageSource(connectorTransactionHandle,
                session.toConnectorSession(catalogHandle),
                connectorSplit, connectorTableHandle, columns, dynamicFilter);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    protected int getNext() throws IOException {
        int rows = 0;
        if (connectorSplit == null || source == null) {
            return rows;
        }

        try {
            while (!source.isFinished()) {
                Page page = source.getNextPage();
                if (page == null) {
                    // used for RecordPageSource
                    // because RecordPageSource will be null even if source is not isFinished.
                    if (!source.isFinished()) {
                        continue;
                    } else {
                        break;
                    }
                }

                // assure the page is in memory before handing to another operator
                page = page.getLoadedPage();
                if (page.getPositionCount() == 0) {
                    break;
                }
                for (int i = 0; i < page.getChannelCount(); ++i) {
                    long startTime = System.nanoTime();
                    Block block = page.getBlock(i);
                    columnValue.setBlock(block);
                    columnValue.setColumnType(types[i]);
                    columnValue.setTrinoType(trinoTypeList.get(i));
                    columnValue.setConnectorSession(session.toConnectorSession(catalogHandle));
                    for (int j = 0; j < page.getPositionCount(); ++j) {
                        columnValue.setPosition(j);
                        appendData(i, columnValue);
                    }
                    appendDataTimeNs[i] += System.nanoTime() - startTime;
                }
                rows += page.getPositionCount();
                if (rows >= batchSize) {
                    return rows;
                }
            }
        } catch (Exception e) {
            printException(e);
            throw e;
        }
        return rows;
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        // do nothing
        return null;
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> mp = new HashMap<>();
        for (int i = 0; i < appendDataTimeNs.length; ++i) {
            mp.put("timer:AppendDataTime[" + i + "]",  String.valueOf(appendDataTimeNs[i]));
        }
        return mp;
    }

    private ConnectorPageSourceProvider getConnectorPageSourceProvider() {
        ConnectorPageSourceProvider connectorPageSourceProvider = null;
        try {
            connectorPageSourceProvider = connector.getPageSourceProvider();
            Objects.requireNonNull(connectorPageSourceProvider,
                    String.format("Connector '%s' returned a null page source provider", catalogNameString));
        } catch (UnsupportedOperationException ignored) {
            LOG.debug("exception when getPageSourceProvider: " + ignored.getMessage());
        }

        try {
            ConnectorRecordSetProvider connectorRecordSetProvider = connector.getRecordSetProvider();
            Objects.requireNonNull(connectorRecordSetProvider,
                    String.format("Connector '%s' returned a null record set provider", catalogNameString));
            if (connectorPageSourceProvider != null) {
                throw new RuntimeException(String.format(
                        "Connector '%s' returned both page source and record set providers", catalogNameString));
            }
            connectorPageSourceProvider = new RecordPageSourceProvider(connectorRecordSetProvider);
        } catch (UnsupportedOperationException ignored) {
            LOG.debug("exception when getRecordSetProvider: " + ignored.getMessage());
        }

        return connectorPageSourceProvider;
    }

    private ObjectMapperProvider generateObjectMapperProvider() {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Set<Module> modules = new HashSet<Module>();
        modules.add(HandleJsonModule.tableHandleModule(handleResolver));
        modules.add(HandleJsonModule.columnHandleModule(handleResolver));
        modules.add(HandleJsonModule.splitModule(handleResolver));
        modules.add(HandleJsonModule.transactionHandleModule(handleResolver));
        // modules.add(HandleJsonModule.outputTableHandleModule(handleResolver));
        // modules.add(HandleJsonModule.insertTableHandleModule(handleResolver));
        // modules.add(HandleJsonModule.tableExecuteHandleModule(handleResolver));
        // modules.add(HandleJsonModule.indexHandleModule(handleResolver));
        // modules.add(HandleJsonModule.partitioningHandleModule(handleResolver));
        objectMapperProvider.setModules(modules);

        // set json deserializers
        TypeManager typeManager = new InternalTypeManager(
                TrinoConnectorPluginLoader.getTrinoConnectorPluginManager().getTypeRegistry());
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(),
                typeManager);
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(
                io.trino.spi.type.Type.class, new TypeDeserializer(typeManager),
                Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde)));
        return objectMapperProvider;
    }

    private void initConnector() {
        String connectorName = trinoConnectorOptionParams.remove("connector.name");

        TrinoConnectorCacheKey cacheKey = new TrinoConnectorCacheKey(catalogNameString, connectorName,
                catalogCreateTime);
        cacheKey.setProperties(this.trinoConnectorOptionParams);
        cacheKey.setTrinoConnectorPluginManager(TrinoConnectorPluginLoader.getTrinoConnectorPluginManager());

        TrinoConnectorCacheValue connectorCacheValue = TrinoConnectorCache.getConnector(cacheKey);
        this.catalogHandle = connectorCacheValue.getCatalogHandle();
        this.connector = connectorCacheValue.getConnector();
        this.handleResolver = connectorCacheValue.getHandleResolver();

        // create session
        this.session = createSession(connectorCacheValue.getTrinoConnectorServicesProvider());
    }

    private void initTrinoTableMetadata() {
        try {
            // TODO(ftw): This deserialization takes a lot of time
            connectorTransactionHandle = TrinoConnectorScannerUtils.decodeStringToObject(
                    connectorTrascationHandleString,
                    ConnectorTransactionHandle.class, this.objectMapperProvider);

            connectorSplit = TrinoConnectorScannerUtils.decodeStringToObject(connectorSplitString,
                    ConnectorSplit.class, this.objectMapperProvider);

            connectorTableHandle = TrinoConnectorScannerUtils.decodeStringToObject(connectorTableHandleString,
                    ConnectorTableHandle.class, this.objectMapperProvider);

            columns = TrinoConnectorScannerUtils.decodeStringToList(connectorColumnHandleString,
                    ColumnHandle.class, this.objectMapperProvider);

            columnMetadataList = TrinoConnectorScannerUtils.decodeStringToList(connectorColumnMetadataString,
                    TrinoColumnMetadata.class, this.objectMapperProvider);
            trinoConnectorAllFieldNames = columnMetadataList.stream().map(columnMetadata -> columnMetadata.getName())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error("get exception: " + e.getMessage());
            throw e;
        }
    }

    private void parseRequiredTypes() {
        appendDataTimeNs = new long[fields.length];
        trinoTypeList = Lists.newArrayList();
        for (int i = 0; i < fields.length; i++) {
            int index = trinoConnectorAllFieldNames.indexOf(fields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        fields[i], trinoConnectorAllFieldNames));
            }
            trinoTypeList.add(columnMetadataList.get(index).getType());
        }
    }

    private Session createSession(TrinoConnectorServicesProvider trinoConnectorServicesProvider) {
        // create trino session
        Set<SystemSessionPropertiesProvider> systemSessionProperties =
                ImmutableSet.<SystemSessionPropertiesProvider>builder()
                        .add(new SystemSessionProperties(
                                new QueryManagerConfig(),
                                new TaskManagerConfig().setTaskConcurrency(4),
                                new MemoryManagerConfig(),
                                TrinoConnectorPluginLoader.getFeaturesConfig(),
                                new OptimizerConfig(),
                                new NodeMemoryConfig(),
                                new DynamicFilterConfig(),
                                new NodeSchedulerConfig()))
                        .build();
        SessionPropertyManager sessionPropertyManager = CatalogServiceProviderModule.createSessionPropertyManager(
                systemSessionProperties, trinoConnectorServicesProvider);

        return Session.builder(sessionPropertyManager)
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

    private void printException(Exception e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        LOG.error("exception when get next: " + stringWriter);
    }
}

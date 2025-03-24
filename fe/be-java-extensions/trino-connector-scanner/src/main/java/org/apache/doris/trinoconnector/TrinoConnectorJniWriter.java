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
import org.apache.doris.common.jni.vec.VectorTable;
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
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
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


/**
 * TrinoConnectorJniWriter is used to write data to Trino connector.
 * It extends JniScanner to reuse the JNI infrastructure.
 */
public class TrinoConnectorJniWriter extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorJniWriter.class);
    private static final String TRINO_CONNECTOR_PROPERTIES_PREFIX = "trino.";
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    private final String catalogNameString;
    private final String catalogCreateTime;

    private final String tableHandleString;
    private final String columnHandlesString;
    private final String columnMetadataString;
    private final String transactionHandleString;
    private final Map<String, String> trinoConnectorOptionParams;

    private CatalogHandle catalogHandle;
    private Connector connector;
    private HandleResolver handleResolver;
    private Session session;
    private ObjectMapperProvider objectMapperProvider;


    private ConnectorPageSinkProvider pageSinkProvider;
    private ConnectorPageSink sink;
    private ConnectorInsertTableHandle insertTableHandle;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private ConnectorTableHandle connectorTableHandle;
    private List<ColumnHandle> columns;
    private List<TrinoColumnMetadata> columnMetadataList = Lists.newArrayList();
    private List<Type> trinoTypeList;
    private long[] appendDataTimeNs;

    private List<String> trinoConnectorAllFieldNames;

    private ConnectorMetadata metadata;

    public TrinoConnectorJniWriter(int batchSize, Map<String, String> params) {
        catalogNameString = params.get("catalog_name");
        tableHandleString = params.get("trino_connector_table_handle");
        columnHandlesString = params.get("trino_connector_column_handles");
        columnMetadataString = params.get("trino_connector_column_metadata");
        transactionHandleString = params.get("trino_connector_transaction_handle");

        trinoConnectorOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(TRINO_CONNECTOR_PROPERTIES_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(TRINO_CONNECTOR_PROPERTIES_PREFIX.length()),
                                kv1 -> kv1.getValue()));
        catalogCreateTime = trinoConnectorOptionParams.remove("create_time");

        String requiredFieldsStr = params.get("required_fields");
        if (requiredFieldsStr != null && !requiredFieldsStr.isEmpty()) {
            this.fields = requiredFieldsStr.split(",");
        }
    }

    @Override
    public void open() throws IOException {
        try {
            initConnector();
            this.pageSinkProvider = getConnectorPageSinkProvider();

            if (pageSinkProvider == null) {
                throw new IOException("Failed to get ConnectorPageSinkProvider from connector");
            }

            this.objectMapperProvider = generateObjectMapperProvider();

            try {
                initTrinoTableMetadata();
            } catch (Exception e) {
                printException(e);
            }

            if (fields == null || fields.length == 0 && trinoConnectorAllFieldNames != null) {
                fields = trinoConnectorAllFieldNames.toArray(new String[0]);
            }

            if (fields == null || fields.length == 0) {
                fields = new String[]{"id", "name"};
                LOG.info("Using default fields: id, name");
            }

            try {
                parseRequiredTypes();
            } catch (Exception e) {
                printException(e);
                trinoTypeList = Arrays.asList(
                        io.trino.spi.type.IntegerType.INTEGER,
                        io.trino.spi.type.VarcharType.VARCHAR
                );
                LOG.info("Using default types: INTEGER, VARCHAR");
            }

            LOG.info("Creating InsertTableHandle using connector metadata");

            try {
                connectorTransactionHandle = connector.beginTransaction(
                        io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED,
                        true,
                        true);
                LOG.info("Created new transaction handle: {}", connectorTransactionHandle);

                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                metadata = connector.getMetadata(connectorSession, connectorTransactionHandle);
                if (metadata == null) {
                    throw new IOException("Failed to get connector metadata");
                }

                insertTableHandle = metadata.beginInsert(
                        connectorSession,
                        connectorTableHandle,
                        columns,
                        io.trino.spi.connector.RetryMode.NO_RETRIES);

                if (insertTableHandle == null) {
                    throw new IOException("Failed to begin insert operation, received null InsertTableHandle");
                }

                this.metadata = metadata;

                LOG.info("Successfully created InsertTableHandle: {}", insertTableHandle.getClass().getName());
            } catch (Exception e) {
                LOG.error("Failed to create InsertTableHandle", e);
                printException(e);
                throw new IOException("Failed to create InsertTableHandle: " + e.getMessage(), e);
            }

            try {
                sink = pageSinkProvider.createPageSink(
                        connectorTransactionHandle,
                        session.toConnectorSession(catalogHandle),
                        insertTableHandle,
                        new ConnectorPageSinkId() {
                            @Override
                            public long getId() {
                                return 0;
                            }
                        });
            } catch (Exception e) {
                printException(e);
                throw e;
            }

            LOG.info("Successfully created ConnectorPageSink for catalog: {}", catalogNameString);
        } catch (Exception e) {
            LOG.error("Failed to create ConnectorPageSink", e);
            printException(e);
            throw new IOException("Failed to create ConnectorPageSink: " + e.getMessage(), e);
        }
    }

    private ConnectorPageSinkProvider getConnectorPageSinkProvider() {
        ConnectorPageSinkProvider connectorPageSinkProvider = null;
        try {
            connectorPageSinkProvider = connector.getPageSinkProvider();
            Objects.requireNonNull(connectorPageSinkProvider,
                    String.format("Connector '%s' returned a null page sink provider", catalogNameString));
        } catch (UnsupportedOperationException e) {
            LOG.debug("exception when getPageSinkProvider: " + e.getMessage());
        }
        return connectorPageSinkProvider;
    }

    private void parseRequiredTypes() {
        if (fields == null || fields.length == 0) {
            fields = trinoConnectorAllFieldNames.toArray(new String[0]);
        }

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

    public void writeData(Map<String, String> params) throws Exception {
        if (sink == null) {
            throw new RuntimeException("ConnectorPageSink is not initialized");
        }

        if (!params.containsKey("meta_address")) {
            throw new RuntimeException("Missing meta_address in params");
        }

        if (!params.containsKey("required_fields") || !params.containsKey("columns_types")) {
            StringBuilder requiredFields = new StringBuilder();
            StringBuilder columnsTypes = new StringBuilder();

            for (int i = 0; i < fields.length; i++) {
                if (i > 0) {
                    requiredFields.append(",");
                    columnsTypes.append("#");
                }
                requiredFields.append(fields[i]);

                Type trinoType = trinoTypeList.get(i);
                String typeName = trinoType.getDisplayName();
                String dorisType = mapTrinoTypeToDorisType(typeName);
                columnsTypes.append(dorisType);
            }

            params.put("required_fields", requiredFields.toString());
            params.put("columns_types", columnsTypes.toString());
        }

        VectorTable table = VectorTable.createReadableTable(params);
        int numRows = table.getNumRows();
        if (numRows == 0) {
            LOG.info("No data to write");
            return;
        }

        LOG.info("Writing {} rows to Trino connector", numRows);

        try {
            if (fields == null || fields.length == 0) {
                fields = trinoConnectorAllFieldNames.toArray(new String[0]);
                parseRequiredTypes();
            }

            PageBuilder pageBuilder = new PageBuilder(trinoTypeList);

            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                pageBuilder.declarePosition();

                for (int colIdx = 0; colIdx < fields.length; colIdx++) {
                    Object value = getValueFromVectorTable(table, colIdx, rowIdx);

                    appendValueToPageBuilder(pageBuilder, colIdx, value);
                }
            }

            Page page = pageBuilder.build();
            sink.appendPage(page);

            LOG.info("Successfully wrote {} rows to Trino connector", numRows);
        } catch (Exception e) {
            LOG.error("Failed to write data to Trino connector", e);
            printException(e);
            throw e;
        }
    }

    public void finishWrite() throws Exception {
        if (sink == null) {
            LOG.warn("ConnectorPageSink is not initialized, nothing to finish");
            return;
        }

        try {
            java.util.concurrent.CompletableFuture<java.util.Collection<io.airlift.slice.Slice>> futureFragments
                    = sink.finish();
            java.util.Collection<io.airlift.slice.Slice> fragments = futureFragments.get();
            LOG.info("Successfully finished writing to Trino connector, fragments: {}", fragments.size());

            if (metadata != null) {
                try {
                    ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                    java.util.Optional<io.trino.spi.connector.ConnectorOutputMetadata> outputMetadata =
                            metadata.finishInsert(
                                    connectorSession,
                                    insertTableHandle,
                                    fragments,
                                    java.util.Collections.emptyList());

                    if (outputMetadata.isPresent()) {
                        LOG.info("Successfully finished insert operation with metadata: {}",
                                outputMetadata.get());
                    } else {
                        LOG.info("Successfully finished insert operation without metadata");
                    }
                } catch (Exception e) {
                    LOG.error("Failed to finish insert operation", e);
                    printException(e);

                    try {
                        connector.rollback(connectorTransactionHandle);
                        LOG.info("Rolled back transaction after finishInsert failure");
                    } catch (Exception rollbackEx) {
                        LOG.error("Failed to rollback transaction after finishInsert failure", rollbackEx);
                        printException(rollbackEx);
                    }

                    throw e;
                }
            } else {
                LOG.warn("Cannot finish insert: metadata is null");
            }
        } catch (Exception e) {
            LOG.error("Failed to finish writing to Trino connector", e);
            printException(e);

            if (sink != null) {
                try {
                    sink.abort();
                } catch (Exception abortEx) {
                    LOG.warn("Failed to abort sink during exception handling", abortEx);
                }
            }

            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        if (sink != null) {
            try {
                sink.abort();
            } catch (Exception e) {
                LOG.warn("Failed to abort ConnectorPageSink", e);
            }
        }

        if (connector != null && connectorTransactionHandle != null) {
            try {
                LOG.info("Attempting to rollback transaction to clean up temporary tables");

                connector.rollback(connectorTransactionHandle);

                LOG.info("Successfully rolled back transaction and cleaned up resources");
            } catch (Exception e) {
                LOG.error("Failed to rollback transaction", e);
                printException(e);
            }
        }
    }

    private Object getValueFromVectorTable(VectorTable table, int colIdx, int rowIdx) {
        if (table.getColumn(colIdx).isNullAt(rowIdx)) {
            return null;
        }

        ColumnType.Type dorisType = table.getColumnType(colIdx).getType();
        switch (dorisType) {
            case BOOLEAN:
                return table.getColumn(colIdx).getBoolean(rowIdx);
            case TINYINT:
                return table.getColumn(colIdx).getByte(rowIdx);
            case SMALLINT:
                return table.getColumn(colIdx).getShort(rowIdx);
            case INT:
                return table.getColumn(colIdx).getInt(rowIdx);
            case BIGINT:
                return table.getColumn(colIdx).getLong(rowIdx);
            case LARGEINT:
                return table.getColumn(colIdx).getBigInteger(rowIdx);
            case FLOAT:
                return table.getColumn(colIdx).getFloat(rowIdx);
            case DOUBLE:
                return table.getColumn(colIdx).getDouble(rowIdx);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return table.getColumn(colIdx).getDecimal(rowIdx);
            case DATEV2:
                return table.getColumn(colIdx).getDate(rowIdx);
            case DATETIMEV2:
                return table.getColumn(colIdx).getDateTime(rowIdx);
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
                return table.getColumn(colIdx).getStringWithOffset(rowIdx);
            default:
                throw new RuntimeException("Unsupported type: " + dorisType);
        }
    }

    private void appendValueToPageBuilder(PageBuilder pageBuilder, int colIdx, Object value) {
        if (value == null) {
            pageBuilder.getBlockBuilder(colIdx).appendNull();
            return;
        }

        Type trinoType = trinoTypeList.get(colIdx);
        String typeName = trinoType.getDisplayName();

        if (typeName.equals("boolean")) {
            trinoType.writeBoolean(pageBuilder.getBlockBuilder(colIdx), (Boolean) value);
        } else if (typeName.equals("tinyint")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.equals("smallint")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.equals("integer")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.equals("bigint")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.startsWith("decimal")) {
            trinoType.writeObject(pageBuilder.getBlockBuilder(colIdx), value);
        } else if (typeName.equals("real")) {
            trinoType.writeLong(pageBuilder.getBlockBuilder(colIdx), ((Number) value).longValue());
        } else if (typeName.equals("double")) {
            trinoType.writeDouble(pageBuilder.getBlockBuilder(colIdx), ((Number) value).doubleValue());
        } else if (typeName.equals("date")) {
            trinoType.writeObject(pageBuilder.getBlockBuilder(colIdx), value);
        } else if (typeName.startsWith("timestamp")) {
            trinoType.writeObject(pageBuilder.getBlockBuilder(colIdx), value);
        } else if (typeName.startsWith("varchar") || typeName.startsWith("char")) {
            io.airlift.slice.Slice slice = io.airlift.slice.Slices.utf8Slice(value.toString());
            trinoType.writeSlice(pageBuilder.getBlockBuilder(colIdx), slice);
        } else if (typeName.startsWith("varbinary")) {
            if (value instanceof byte[]) {
                io.airlift.slice.Slice slice = io.airlift.slice.Slices.wrappedBuffer((byte[]) value);
                trinoType.writeSlice(pageBuilder.getBlockBuilder(colIdx), slice);
            } else if (value instanceof String) {
                io.airlift.slice.Slice slice = io.airlift.slice.Slices.utf8Slice((String) value);
                trinoType.writeSlice(pageBuilder.getBlockBuilder(colIdx), slice);
            } else {
                throw new RuntimeException("Unsupported value type for VARBINARY: " + value.getClass().getName());
            }
        } else {
            throw new RuntimeException("Unsupported Trino type: " + typeName);
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> statistics = new HashMap<>();
        statistics.put("writer_catalog", catalogNameString);
        return statistics;
    }

    @Override
    protected int getNext() throws IOException {
        return 0;
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        return null;
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

        this.session = createSession(connectorCacheValue.getTrinoConnectorServicesProvider());
    }

    private ObjectMapperProvider generateObjectMapperProvider() {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Set<Module> modules = new HashSet<Module>();
        modules.add(HandleJsonModule.tableHandleModule(handleResolver));
        modules.add(HandleJsonModule.columnHandleModule(handleResolver));
        modules.add(HandleJsonModule.transactionHandleModule(handleResolver));
        modules.add(HandleJsonModule.insertTableHandleModule(handleResolver));
        objectMapperProvider.setModules(modules);

        TypeManager typeManager = new InternalTypeManager(
                TrinoConnectorPluginLoader.getTrinoConnectorPluginManager().getTypeRegistry());
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(),
                typeManager);
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(
                io.trino.spi.type.Type.class, new TypeDeserializer(typeManager),
                Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde)));
        return objectMapperProvider;
    }

    private void initTrinoTableMetadata() {
        try {
            connectorTableHandle = TrinoConnectorScannerUtils.decodeStringToObject(
                    tableHandleString, ConnectorTableHandle.class, this.objectMapperProvider);

            columns = TrinoConnectorScannerUtils.decodeStringToList(
                    columnHandlesString, ColumnHandle.class, this.objectMapperProvider);

            columnMetadataList = TrinoConnectorScannerUtils.decodeStringToList(
                    columnMetadataString, TrinoColumnMetadata.class, this.objectMapperProvider);

            trinoTypeList = columnMetadataList.stream()
                    .map(TrinoColumnMetadata::getType)
                    .collect(Collectors.toList());
            trinoConnectorAllFieldNames = columnMetadataList.stream().map(columnMetadata -> columnMetadata.getName())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error("Failed to initialize Trino table metadata", e);
            throw new RuntimeException("Failed to initialize Trino table metadata", e);
        }
    }

    private Session createSession(TrinoConnectorServicesProvider trinoConnectorServicesProvider) {
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
        LOG.error("Exception: " + stringWriter);
    }

    private String mapTrinoTypeToDorisType(String trinoType) {
        if (trinoType.equals("boolean")) {
            return "BOOLEAN";
        } else if (trinoType.equals("tinyint")) {
            return "TINYINT";
        } else if (trinoType.equals("smallint")) {
            return "SMALLINT";
        } else if (trinoType.equals("integer")) {
            return "INT";
        } else if (trinoType.equals("bigint")) {
            return "BIGINT";
        } else if (trinoType.startsWith("decimal")) {
            return "DECIMALV2";
        } else if (trinoType.equals("real")) {
            return "FLOAT";
        } else if (trinoType.equals("double")) {
            return "DOUBLE";
        } else if (trinoType.equals("date")) {
            return "DATEV2";
        } else if (trinoType.startsWith("timestamp")) {
            return "DATETIMEV2";
        } else if (trinoType.startsWith("varchar") || trinoType.startsWith("char")) {
            return "VARCHAR";
        } else if (trinoType.startsWith("varbinary")) {
            return "BINARY";
        } else {
            return "VARCHAR";
        }
    }
}

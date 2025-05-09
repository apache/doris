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

package org.apache.doris.datasource.trinoconnector.sink;


import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalCatalog;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorPluginLoader;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.planner.BaseExternalTableDataSink;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TTrinoConnnectorTableSink;
import org.apache.doris.trinoconnector.TrinoColumnMetadata;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TypeManager;
import io.trino.type.InternalTypeManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TrinoConnectorTableSink extends BaseExternalTableDataSink {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorTableSink.class);
    private final TrinoConnectorExternalTable targetTable;
    private ObjectMapperProvider objectMapperProvider;
    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_JNI);
        }};

    public TrinoConnectorTableSink(TrinoConnectorExternalTable targetTable) {
        this.targetTable = targetTable;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("TRINO CONNECTOR TABLE SINK\n");
        sb.append(prefix).append("  CATALOG: ").append(targetTable.getCatalog().getName()).append("\n");
        sb.append(prefix).append("  DATABASE: ").append(targetTable.getDbName()).append("\n");
        sb.append(prefix).append("  TABLE: ").append(targetTable.getName()).append("\n");
        return sb.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx) throws AnalysisException {
        objectMapperProvider = createObjectMapperProvider();
        TTrinoConnnectorTableSink tTrinoConnectorTableSink = new TTrinoConnnectorTableSink();

        // Set basic table information
        tTrinoConnectorTableSink.setCatalogName(targetTable.getCatalog().getName());
        tTrinoConnectorTableSink.setDbName(targetTable.getDbName());
        tTrinoConnectorTableSink.setTableName(targetTable.getName());

        TrinoConnectorExternalCatalog catalog = (TrinoConnectorExternalCatalog) targetTable.getCatalog();

        // Set Trino connector options
        tTrinoConnectorTableSink.setTrinoConnectorOptions(catalog.getTrinoConnectorPropertiesWithCreateTime());

        tTrinoConnectorTableSink.setTrinoConnectorTableHandle(encodeObjectToString(
                targetTable.getConnectorTableHandle(), objectMapperProvider
        ));

        Map<String, ColumnHandle> columnHandleMap = targetTable.getColumnHandleMap();
        Map<String, ColumnMetadata> columnMetadataMap = targetTable.getColumnMetadataMap();
        List<ColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        for (Map.Entry<String, ColumnHandle> entry : columnHandleMap.entrySet()) {
            columnHandles.add(entry.getValue());
        }
        for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
            columnMetadataList.add(entry.getValue());
        }

        tTrinoConnectorTableSink.setTrinoConnectorColumnHandles(
                encodeObjectToString(columnHandles, objectMapperProvider));
        tTrinoConnectorTableSink.setTrinoConnectorColumnMetadata(encodeObjectToString(columnMetadataList.stream().map(
                        filed -> new TrinoColumnMetadata(filed.getName(), filed.getType(), filed.isNullable(),
                                filed.getComment(),
                                filed.getExtraInfo(), filed.isHidden(), filed.getProperties()))
                .collect(java.util.stream.Collectors.toList()), objectMapperProvider));

        tTrinoConnectorTableSink.setTrinoConnectorTransactionHandle(encodeObjectToString(
                targetTable.getConnectorTransactionHandle(), objectMapperProvider
        ));


        tDataSink = new TDataSink(TDataSinkType.TRINO_CONNECTOR_TABLE_SINK);
        tDataSink.setTrinoConnectorTableSink(tTrinoConnectorTableSink);
    }

    private <T> String encodeObjectToString(T t, ObjectMapperProvider objectMapperProvider) {
        try {
            io.airlift.json.JsonCodec<T> jsonCodec = (io.airlift.json.JsonCodec<T>) new JsonCodecFactory(
                    objectMapperProvider).jsonCodec(t.getClass());
            return jsonCodec.toJson(t);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ObjectMapperProvider createObjectMapperProvider() {
        // mock ObjectMapperProvider
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Set<Module> modules = new HashSet<Module>();
        HandleResolver handleResolver = TrinoConnectorPluginLoader.getHandleResolver();
        modules.add(HandleJsonModule.tableHandleModule(handleResolver));
        modules.add(HandleJsonModule.columnHandleModule(handleResolver));
        modules.add(HandleJsonModule.splitModule(handleResolver));
        modules.add(HandleJsonModule.transactionHandleModule(handleResolver));
        modules.add(HandleJsonModule.insertTableHandleModule(handleResolver));
        objectMapperProvider.setModules(modules);

        // set json deserializers
        TypeManager typeManager = new InternalTypeManager(TrinoConnectorPluginLoader.getTypeRegistry());
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(),
                typeManager);
        objectMapperProvider.setJsonSerializers(ImmutableMap.of(Block.class,
                new BlockJsonSerde.Serializer(blockEncodingSerde)));
        return objectMapperProvider;
    }
}

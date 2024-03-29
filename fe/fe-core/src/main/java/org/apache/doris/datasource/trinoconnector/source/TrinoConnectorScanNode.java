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

package org.apache.doris.datasource.trinoconnector.source;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorPluginLoader;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTrinoConnectorFileDesc;
import org.apache.doris.trinoconnector.TrinoColumnMetadata;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.MoreFutures;
import io.airlift.concurrent.Threads;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TypeManager;
import io.trino.split.BufferingSplitSource;
import io.trino.split.ConnectorAwareSplitSource;
import io.trino.split.SplitSource;
import io.trino.type.InternalTypeManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TrinoConnectorScanNode extends FileQueryScanNode {
    private static final int minScheduleSplitBatchSize = 10;
    private TrinoConnectorSource source = null;
    private ObjectMapperProvider objectMapperProvider;

    // private static List<Predicate> predicates;

    public TrinoConnectorScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "TRINO_CONNECTOR_SCAN_NODE", StatisticalType.TRINO_CONNECTOR_SCAN_NODE, needCheckColumnPriv);
    }

    @Override
    protected void doInitialize() throws UserException {
        TrinoConnectorExternalTable table = (TrinoConnectorExternalTable) desc.getTable();
        if (table.isView()) {
            throw new AnalysisException(
                    String.format("Querying external view '%s.%s' is not supported", table.getDbName(),
                            table.getName()));
        }

        source = new TrinoConnectorSource(desc, table);

        computeColumnsFilter();
        initBackendPolicy();
        initSchemaParams();
    }

    @Override
    public List<Split> getSplits() throws UserException {
        // 1. Get necessary objects
        Connector connector = source.getConnector();
        ConnectorTransactionHandle connectorTransactionHandle = connector.beginTransaction(
                IsolationLevel.READ_UNCOMMITTED, true, true);
        source.setConnectorTransactionHandle(connectorTransactionHandle);
        ConnectorSession connectorSession = source.getTrinoSession().toConnectorSession(source.getCatalogHandle());
        ConnectorMetadata connectorMetadata = connector.getMetadata(connectorSession, connectorTransactionHandle);

        // 2. Begin query
        connectorMetadata.beginQuery(connectorSession);

        // 3. get splitSource
        SplitSource splitSource = getTrinoSplitSource(connector, source.getTrinoSession(), connectorTransactionHandle,
                source.getTrinoConnectorExtTableHandle(),
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());
        // 4. get trino.Splits and convert it to doris.Splits
        List<Split> splits = Lists.newArrayList();
        while (!splitSource.isFinished()) {
            for (io.trino.metadata.Split split : getNextSplitBatch(splitSource)) {
                splits.add(new TrinoConnectorSplit(split.getConnectorSplit(), source.getConnectorName()));
            }
        }
        return splits;
    }

    private SplitSource getTrinoSplitSource(Connector connector, Session session,
            ConnectorTransactionHandle connectorTransactionHandle, ConnectorTableHandle table,
            DynamicFilter dynamicFilter, Constraint constraint) {
        ConnectorSplitManager splitManager = connector.getSplitManager();

        if (!SystemSessionProperties.isAllowPushdownIntoConnectors(session)) {
            dynamicFilter = DynamicFilter.EMPTY;
        }

        ConnectorSession connectorSession = session.toConnectorSession(source.getCatalogHandle());
        // TODO(ftw): here can not use table.getTransactionHandle
        ConnectorSplitSource connectorSplitSource = splitManager.getSplits(connectorTransactionHandle, connectorSession,
                table, dynamicFilter, constraint);

        SplitSource splitSource = new ConnectorAwareSplitSource(source.getCatalogHandle(), connectorSplitSource);
        if (this.minScheduleSplitBatchSize > 1) {
            ExecutorService executorService = Executors.newCachedThreadPool(
                    Threads.daemonThreadsNamed(TrinoConnectorScanNode.class.getSimpleName() + "-%s"));
            splitSource = new BufferingSplitSource(splitSource,
                    new BoundedExecutor(executorService, 10), this.minScheduleSplitBatchSize);
        }
        return splitSource;
    }

    private List<io.trino.metadata.Split> getNextSplitBatch(SplitSource splitSource) {
        return MoreFutures.getFutureValue(splitSource.getNextBatch(1000)).getSplits();
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof TrinoConnectorSplit) {
            setTrinoConnectorParams(rangeDesc, (TrinoConnectorSplit) split);
        }
    }

    public void setTrinoConnectorParams(TFileRangeDesc rangeDesc, TrinoConnectorSplit trinoConnectorSplit) {
        // mock ObjectMapperProvider
        objectMapperProvider = createObjectMapperProvider();

        // set TTrinoConnectorFileDesc
        TTrinoConnectorFileDesc fileDesc = new TTrinoConnectorFileDesc();
        fileDesc.setTrinoConnectorSplit(encodeObjectToString(trinoConnectorSplit.getSplit(), objectMapperProvider));
        fileDesc.setCatalogName(source.getCatalog().getName());
        fileDesc.setDbName(source.getTargetTable().getDbName());
        fileDesc.setTrinoConnectorOptions(source.getCatalog().getTrinoConnectorProperties());
        fileDesc.setTableName(source.getTargetTable().getName());
        fileDesc.setTrinoConnectorTableHandle(
                encodeObjectToString(source.getTargetTable().getConnectorTableHandle(), objectMapperProvider));

        Map<String, ColumnHandle> columnHandleMap = source.getTargetTable().getColumnHandleMap();
        Map<String, ColumnMetadata> columnMetadataMap = source.getTargetTable().getColumnMetadataMap();
        List<ColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        for (SlotDescriptor slotDescriptor : source.getDesc().getSlots()) {
            if (!slotDescriptor.isMaterialized()) {
                continue;
            }
            String colName = slotDescriptor.getColumn().getName();
            if (columnMetadataMap.containsKey(colName)) {
                columnMetadataList.add(columnMetadataMap.get(colName));
                columnHandles.add(columnHandleMap.get(colName));
            }
        }
        fileDesc.setTrinoConnectorColumnHandles(encodeObjectToString(columnHandles, objectMapperProvider));
        fileDesc.setTrinoConnectorTrascationHandle(
                encodeObjectToString(source.getConnectorTransactionHandle(), objectMapperProvider));
        fileDesc.setTrinoConnectorColumnMetadata(encodeObjectToString(columnMetadataList.stream().map(
                        filed -> new TrinoColumnMetadata(filed.getName(), filed.getType(), filed.isNullable(),
                                filed.getComment(),
                                filed.getExtraInfo(), filed.isHidden(), filed.getProperties()))
                .collect(Collectors.toList()), objectMapperProvider));

        // set TTableFormatFileDesc
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTrinoConnectorParams(fileDesc);
        tableFormatFileDesc.setTableFormatType(TableFormatType.TRINO_CONNECTOR.value());

        // set TFileRangeDesc
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    private ObjectMapperProvider createObjectMapperProvider() {
        // mock ObjectMapperProvider
        TypeManager typeManager = new InternalTypeManager(TrinoConnectorPluginLoader.getTypeRegistry());
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Set<Module> modules = new HashSet<Module>();
        HandleResolver handleResolver = TrinoConnectorPluginLoader.getHandleResolver();
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
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.of(io.trino.spi.type.Type.class, new TypeDeserializer(typeManager)));
        return objectMapperProvider;
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

    // When calling 'setTrinoConnectorParams' and 'getSplits', the column trimming has not been performed yet,
    // Therefore, trino_connector_column_names is temporarily reset here
    @Override
    public void updateRequiredSlots(PlanTranslatorContext planTranslatorContext,
            Set<SlotId> requiredByProjectSlotIdSet) throws UserException {
        super.updateRequiredSlots(planTranslatorContext, requiredByProjectSlotIdSet);
        Map<String, ColumnMetadata> columnMetadataMap = source.getTargetTable().getColumnMetadataMap();
        Map<String, ColumnHandle> columnHandleMap = source.getTargetTable().getColumnHandleMap();
        List<ColumnHandle> columnHandles = new ArrayList<>();
        for (SlotDescriptor slotDescriptor : desc.getSlots()) {
            String colName = slotDescriptor.getColumn().getName();
            if (columnMetadataMap.containsKey(colName)) {
                columnHandles.add(columnHandleMap.get(colName));
            }
        }

        for (TScanRangeLocations tScanRangeLocations : scanRangeLocations) {
            List<TFileRangeDesc> ranges = tScanRangeLocations.scan_range.ext_scan_range.file_scan_range.ranges;
            for (TFileRangeDesc tFileRangeDesc : ranges) {
                tFileRangeDesc.table_format_params.trino_connector_params.setTrinoConnectorColumnHandles(
                        encodeObjectToString(columnHandles, objectMapperProvider));
            }
        }
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        return getLocationType("");
    }

    @Override
    public TFileType getLocationType(String location) throws DdlException, MetaNotFoundException {
        // todo: no use
        return TFileType.FILE_S3;
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return new ArrayList<>();
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return source.getFileAttributes();
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return source.getCatalog().getCatalogProperty().getHadoopProperties();
    }
}

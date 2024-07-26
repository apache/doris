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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorPluginLoader;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTrinoConnectorFileDesc;
import org.apache.doris.trinoconnector.TrinoColumnMetadata;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.MoreFutures;
import io.airlift.concurrent.Threads;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import io.trino.split.BufferingSplitSource;
import io.trino.split.ConnectorAwareSplitSource;
import io.trino.split.SplitSource;
import io.trino.type.InternalTypeManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TrinoConnectorScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorScanNode.class);
    private static final int minScheduleSplitBatchSize = 10;
    private TrinoConnectorSource source = null;
    private ObjectMapperProvider objectMapperProvider;

    private ConnectorMetadata connectorMetadata;
    private Constraint constraint;

    public TrinoConnectorScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "TRINO_CONNECTOR_SCAN_NODE", StatisticalType.TRINO_CONNECTOR_SCAN_NODE, needCheckColumnPriv);
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        source = new TrinoConnectorSource(desc);
        convertPredicate();
    }

    protected void convertPredicate() throws UserException {
        if (conjuncts.isEmpty()) {
            constraint = Constraint.alwaysTrue();
        }
        TupleDomain<ColumnHandle> summary = TupleDomain.all();
        TrinoConnectorPredicateConverter trinoConnectorPredicateConverter = new TrinoConnectorPredicateConverter(
                source.getTargetTable().getColumnHandleMap(),
                source.getTargetTable().getColumnMetadataMap());
        try {
            for (int i = 0; i < conjuncts.size(); ++i) {
                summary = summary.intersect(
                        trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(conjuncts.get(i)));
            }
        } catch (AnalysisException e) {
            LOG.warn("Can not convert Expr to trino tuple domain, exception: {}", e.getMessage());
            summary = TupleDomain.all();
        }
        constraint = new Constraint(summary);
    }

    @Override
    public List<Split> getSplits() throws UserException {
        // 1. Get necessary objects
        Connector connector = source.getConnector();
        connectorMetadata = source.getConnectorMetadata();
        ConnectorSession connectorSession = source.getTrinoSession().toConnectorSession(source.getCatalogHandle());

        List<Split> splits = Lists.newArrayList();
        try {
            connectorMetadata.beginQuery(connectorSession);
            applyPushDown(connectorSession);

            // 3. get splitSource
            try (SplitSource splitSource = getTrinoSplitSource(connector, source.getTrinoSession(),
                    source.getTrinoConnectorTableHandle(), DynamicFilter.EMPTY)) {
                // 4. get trino.Splits and convert it to doris.Splits
                while (!splitSource.isFinished()) {
                    for (io.trino.metadata.Split split : getNextSplitBatch(splitSource)) {
                        splits.add(new TrinoConnectorSplit(split.getConnectorSplit(), source.getConnectorName()));
                    }
                }
            }
        } finally {
            // 4. Clear query
            connectorMetadata.cleanupQuery(connectorSession);
        }
        return splits;
    }

    private void applyPushDown(ConnectorSession connectorSession) {
        // push down predicate/filter
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> filterResult
                = connectorMetadata.applyFilter(connectorSession, source.getTrinoConnectorTableHandle(), constraint);
        if (filterResult.isPresent()) {
            source.setTrinoConnectorTableHandle(filterResult.get().getHandle());
        }

        // push down limit
        if (hasLimit()) {
            long limit = getLimit();
            Optional<LimitApplicationResult<ConnectorTableHandle>> limitResult
                    = connectorMetadata.applyLimit(connectorSession, source.getTrinoConnectorTableHandle(), limit);
            if (limitResult.isPresent()) {
                source.setTrinoConnectorTableHandle(limitResult.get().getHandle());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("The TrinoConnectorTableHandle is " + source.getTrinoConnectorTableHandle()
                    + " after pushing down.");
        }

        // push down projection
        Map<String, ColumnHandle> columnHandleMap = source.getTargetTable().getColumnHandleMap();
        Map<String, ColumnMetadata> columnMetadataMap = source.getTargetTable().getColumnMetadataMap();
        Map<String, ColumnHandle> assignments = Maps.newLinkedHashMap();
        List<ConnectorExpression> projections = Lists.newArrayList();
        for (SlotDescriptor slotDescriptor : desc.getSlots()) {
            String colName = slotDescriptor.getColumn().getName();
            assignments.put(colName, columnHandleMap.get(colName));
            projections.add(new Variable(colName, columnMetadataMap.get(colName).getType()));
        }
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> projectionResult
                = connectorMetadata.applyProjection(connectorSession, source.getTrinoConnectorTableHandle(),
                projections, assignments);
        if (projectionResult.isPresent()) {
            source.setTrinoConnectorTableHandle(projectionResult.get().getHandle());
        }
    }

    private SplitSource getTrinoSplitSource(Connector connector, Session session, ConnectorTableHandle table,
            DynamicFilter dynamicFilter) {
        ConnectorSplitManager splitManager = connector.getSplitManager();

        if (!SystemSessionProperties.isAllowPushdownIntoConnectors(session)) {
            dynamicFilter = DynamicFilter.EMPTY;
        }

        ConnectorSession connectorSession = session.toConnectorSession(source.getCatalogHandle());
        // Constraint is not used by Hive/BigQuery Connector
        ConnectorSplitSource connectorSplitSource = splitManager.getSplits(source.getConnectorTransactionHandle(),
                connectorSession, table, dynamicFilter, constraint);

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
        fileDesc.setTrinoConnectorOptions(source.getCatalog().getTrinoConnectorPropertiesWithCreateTime());
        fileDesc.setTableName(source.getTargetTable().getName());
        fileDesc.setTrinoConnectorTableHandle(encodeObjectToString(
                source.getTrinoConnectorTableHandle(), objectMapperProvider));

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
        // modules.add(HandleJsonModule.tableFunctionHandleModule(handleResolver));
        objectMapperProvider.setModules(modules);

        // set json deserializers
        TypeManager typeManager = new InternalTypeManager(TrinoConnectorPluginLoader.getTypeRegistry());
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(),
                typeManager);
        objectMapperProvider.setJsonSerializers(ImmutableMap.of(Block.class,
                new BlockJsonSerde.Serializer(blockEncodingSerde)));
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
        // can not use `source.getTargetTable()`
        // because source is null when called getTargetTable
        return desc.getTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return source.getCatalog().getCatalogProperty().getHadoopProperties();
    }
}

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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.trinoconnector.TrinoColumnMetadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.MoreFutures;
import io.airlift.concurrent.Threads;
import io.trino.Session;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.split.BufferingSplitSource;
import io.trino.split.ConnectorAwareSplitSource;
import io.trino.split.SplitSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Scan plan provider for Trino connectors.
 *
 * <p>Delegates to the Trino connector's own {@link ConnectorSplitManager} to obtain
 * splits, applies filter/limit/projection pushdown via Trino's ConnectorMetadata,
 * and serializes all Trino SPI objects to JSON for BE consumption.</p>
 *
 * <p>Ported from {@code TrinoConnectorScanNode.getSplits()} and
 * {@code setScanParams()} in fe-core.</p>
 */
public class TrinoScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(TrinoScanPlanProvider.class);
    private static final int SPLIT_BATCH_SIZE = 1000;
    private static final int MIN_SCHEDULE_SPLIT_BATCH_SIZE = 10;

    private final TrinoDorisConnector dorisConnector;

    public TrinoScanPlanProvider(TrinoDorisConnector dorisConnector) {
        this.dorisConnector = dorisConnector;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        return planScan(session, handle, columns, filter, -1);
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit) {
        TrinoTableHandle trinoHandle = (TrinoTableHandle) handle;

        Connector connector = dorisConnector.getTrinoConnector();
        Session trinoSession = dorisConnector.getTrinoSession();
        io.trino.spi.connector.CatalogHandle catalogHandle = dorisConnector.getTrinoCatalogHandle();

        io.trino.spi.connector.ConnectorSession connSession =
                trinoSession.toConnectorSession(catalogHandle);

        io.trino.spi.connector.ConnectorTransactionHandle txnHandle =
                connector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true, true);
        ConnectorMetadata metadata = connector.getMetadata(connSession, txnHandle);

        io.trino.spi.connector.ConnectorTableHandle currentTrinoHandle =
                trinoHandle.getTrinoTableHandle();

        try {
            metadata.beginQuery(connSession);

            // Build constraint from filter expression
            Constraint constraint = buildConstraint(filter, trinoHandle);

            // Apply filter pushdown
            Optional<ConstraintApplicationResult<io.trino.spi.connector.ConnectorTableHandle>>
                    filterResult = metadata.applyFilter(connSession, currentTrinoHandle, constraint);
            if (filterResult.isPresent()) {
                currentTrinoHandle = filterResult.get().getHandle();
            }

            // Apply limit pushdown
            if (limit > 0) {
                Optional<LimitApplicationResult<io.trino.spi.connector.ConnectorTableHandle>>
                        limitResult = metadata.applyLimit(connSession, currentTrinoHandle, limit);
                if (limitResult.isPresent()) {
                    currentTrinoHandle = limitResult.get().getHandle();
                }
            }

            // Apply projection pushdown
            applyProjection(metadata, connSession, trinoHandle, currentTrinoHandle, columns);

            // Get splits
            return getSplitsFromTrino(
                    connector, trinoSession, catalogHandle, connSession,
                    txnHandle, metadata, currentTrinoHandle, constraint,
                    trinoHandle, session);
        } finally {
            metadata.cleanupQuery(connSession);
        }
    }

    private io.trino.spi.connector.ConnectorTableHandle applyProjection(
            ConnectorMetadata metadata,
            io.trino.spi.connector.ConnectorSession connSession,
            TrinoTableHandle trinoHandle,
            io.trino.spi.connector.ConnectorTableHandle currentHandle,
            List<ConnectorColumnHandle> columns) {
        Map<String, ColumnHandle> colHandleMap = trinoHandle.getColumnHandleMap();
        Map<String, ColumnMetadata> colMetaMap = trinoHandle.getColumnMetadataMap();

        Map<String, ColumnHandle> assignments = new HashMap<>();
        List<io.trino.spi.expression.ConnectorExpression> projections = new ArrayList<>();

        for (ConnectorColumnHandle col : columns) {
            TrinoColumnHandle trinoCol = (TrinoColumnHandle) col;
            String colName = trinoCol.getColumnName();
            if (colHandleMap.containsKey(colName) && colMetaMap.containsKey(colName)) {
                assignments.put(colName, colHandleMap.get(colName));
                projections.add(new io.trino.spi.expression.Variable(
                        colName, colMetaMap.get(colName).getType()));
            }
        }

        Optional<ProjectionApplicationResult<io.trino.spi.connector.ConnectorTableHandle>>
                result = metadata.applyProjection(connSession, currentHandle, projections, assignments);
        if (result.isPresent()) {
            return result.get().getHandle();
        }
        return currentHandle;
    }

    private List<ConnectorScanRange> getSplitsFromTrino(
            Connector connector,
            Session trinoSession,
            io.trino.spi.connector.CatalogHandle catalogHandle,
            io.trino.spi.connector.ConnectorSession connSession,
            io.trino.spi.connector.ConnectorTransactionHandle txnHandle,
            ConnectorMetadata metadata,
            io.trino.spi.connector.ConnectorTableHandle tableHandle,
            Constraint constraint,
            TrinoTableHandle dorisHandle,
            ConnectorSession dorisSession) {

        ConnectorSplitManager splitManager = connector.getSplitManager();
        ConnectorSplitSource connSplitSource = splitManager.getSplits(
                txnHandle, connSession, tableHandle,
                DynamicFilter.EMPTY, constraint);

        SplitSource splitSource = new ConnectorAwareSplitSource(catalogHandle, connSplitSource);
        if (MIN_SCHEDULE_SPLIT_BATCH_SIZE > 1) {
            ExecutorService executor = Executors.newCachedThreadPool(
                    Threads.daemonThreadsNamed("TrinoScanPlanProvider-%s"));
            splitSource = new BufferingSplitSource(splitSource,
                    new BoundedExecutor(executor, 10), MIN_SCHEDULE_SPLIT_BATCH_SIZE);
        }

        // Prepare JSON serializer
        TrinoBootstrap bootstrap = TrinoBootstrap.getInstance(
                TrinoBootstrap.resolvePluginDir(dorisConnector.getTrinoProperties()));
        TrinoJsonSerializer serializer = new TrinoJsonSerializer(
                bootstrap.getHandleResolver(), bootstrap.getTypeRegistry());

        // Pre-serialize shared fields (same for all splits)
        String tableHandleJson = serializer.toJson(tableHandle);
        String txnHandleJson = serializer.toJson(txnHandle);
        String columnHandlesJson = serializeColumnHandles(dorisHandle, serializer);
        String columnMetadataJson = serializeColumnMetadata(dorisHandle, serializer);
        String optionsJson = serializeOptions(dorisSession);
        String catalogName = dorisSession.getCatalogName();

        List<ConnectorScanRange> ranges = new ArrayList<>();
        try (SplitSource ss = splitSource) {
            while (!ss.isFinished()) {
                List<io.trino.metadata.Split> batch =
                        MoreFutures.getFutureValue(ss.getNextBatch(SPLIT_BATCH_SIZE))
                                .getSplits();
                for (io.trino.metadata.Split split : batch) {
                    io.trino.spi.connector.ConnectorSplit connSplit = split.getConnectorSplit();
                    String splitJson = serializer.toJson(connSplit);

                    List<String> hosts = connSplit.getAddresses().stream()
                            .map(HostAddress::getHostText)
                            .collect(Collectors.toList());

                    TrinoScanRange range = new TrinoScanRange.Builder()
                            .split(splitJson)
                            .catalogName(catalogName)
                            .dbName(dorisHandle.getDbName())
                            .tableName(dorisHandle.getTableName())
                            .options(optionsJson)
                            .tableHandle(tableHandleJson)
                            .columnHandles(columnHandlesJson)
                            .columnMetadata(columnMetadataJson)
                            .transactionHandle(txnHandleJson)
                            .hosts(hosts)
                            .build();
                    ranges.add(range);
                }
            }
        }
        return ranges;
    }

    private Constraint buildConstraint(Optional<ConnectorExpression> filter,
            TrinoTableHandle trinoHandle) {
        if (!filter.isPresent()) {
            return Constraint.alwaysTrue();
        }
        TrinoPredicateConverter converter = new TrinoPredicateConverter(
                trinoHandle.getColumnHandleMap(),
                trinoHandle.getColumnMetadataMap());
        TupleDomain<ColumnHandle> tupleDomain = converter.convert(filter.get());
        return new Constraint(tupleDomain);
    }

    private String serializeColumnHandles(TrinoTableHandle handle,
            TrinoJsonSerializer serializer) {
        List<ColumnHandle> handles = new ArrayList<>(handle.getColumnHandleMap().values());
        return serializer.toJson(handles);
    }

    private String serializeColumnMetadata(TrinoTableHandle handle,
            TrinoJsonSerializer serializer) {
        List<TrinoColumnMetadata> metadataList = handle.getColumnMetadataMap().values().stream()
                .map(m -> new TrinoColumnMetadata(
                        m.getName(), m.getType(), m.isNullable(),
                        m.getComment(), m.getExtraInfo(), m.isHidden(),
                        m.getProperties()))
                .collect(Collectors.toList());
        return serializer.toJson(metadataList);
    }

    private String serializeOptions(ConnectorSession session) {
        Map<String, String> props = new HashMap<>(session.getCatalogProperties());
        if (!props.containsKey("create_time")) {
            props.put("create_time", String.valueOf(System.currentTimeMillis() / 1000));
        }
        try {
            return new ObjectMapper().writeValueAsString(props);
        } catch (JsonProcessingException e) {
            LOG.warn("Failed to serialize Trino connector options", e);
            return "{}";
        }
    }
}

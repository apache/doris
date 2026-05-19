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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Scan plan provider for MaxCompute (ODPS).
 *
 * <p>Creates ODPS {@link TableBatchReadSession}, generates splits via
 * {@link InputSplitAssigner}, and returns {@link MaxComputeScanRange}s that
 * carry the serialized session and split parameters for BE consumption.</p>
 *
 * <p>Ported from {@code MaxComputeScanNode.getSplits()} / {@code getSplitByTableSession()}
 * / {@code getSplitsWithLimitOptimization()} in fe-core.</p>
 */
public class MaxComputeScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(MaxComputeScanPlanProvider.class);

    private final MaxComputeDorisConnector connector;

    // These are initialized lazily from connector properties
    private EnvironmentSettings settings;
    private SplitOptions splitOptions;
    private String splitStrategy;
    private long splitByteSize;
    private long splitRowCount;
    private boolean splitCrossPartition;
    private int connectTimeout;
    private int readTimeout;
    private int retryTimes;
    private boolean dateTimePushDown;

    private volatile boolean initialized;

    public MaxComputeScanPlanProvider(MaxComputeDorisConnector connector) {
        this.connector = connector;
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    initFromProperties();
                    initialized = true;
                }
            }
        }
    }

    private void initFromProperties() {
        Map<String, String> props = connector.getProperties();

        connectTimeout = Integer.parseInt(
                props.getOrDefault(MCConnectorProperties.CONNECT_TIMEOUT,
                        MCConnectorProperties.DEFAULT_CONNECT_TIMEOUT));
        readTimeout = Integer.parseInt(
                props.getOrDefault(MCConnectorProperties.READ_TIMEOUT,
                        MCConnectorProperties.DEFAULT_READ_TIMEOUT));
        retryTimes = Integer.parseInt(
                props.getOrDefault(MCConnectorProperties.RETRY_COUNT,
                        MCConnectorProperties.DEFAULT_RETRY_COUNT));

        dateTimePushDown = Boolean.parseBoolean(
                props.getOrDefault(MCConnectorProperties.DATETIME_PREDICATE_PUSH_DOWN,
                        MCConnectorProperties.DEFAULT_DATETIME_PREDICATE_PUSH_DOWN));

        splitCrossPartition = Boolean.parseBoolean(
                props.getOrDefault(MCConnectorProperties.SPLIT_CROSS_PARTITION,
                        MCConnectorProperties.DEFAULT_SPLIT_CROSS_PARTITION));

        splitStrategy = props.getOrDefault(MCConnectorProperties.SPLIT_STRATEGY,
                MCConnectorProperties.DEFAULT_SPLIT_STRATEGY);
        if (splitStrategy.equals(MCConnectorProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {
            splitByteSize = Long.parseLong(props.getOrDefault(
                    MCConnectorProperties.SPLIT_BYTE_SIZE,
                    MCConnectorProperties.DEFAULT_SPLIT_BYTE_SIZE));
            splitOptions = SplitOptions.newBuilder()
                    .SplitByByteSize(splitByteSize)
                    .withCrossPartition(splitCrossPartition)
                    .build();
        } else {
            splitRowCount = Long.parseLong(props.getOrDefault(
                    MCConnectorProperties.SPLIT_ROW_COUNT,
                    MCConnectorProperties.DEFAULT_SPLIT_ROW_COUNT));
            splitOptions = SplitOptions.newBuilder()
                    .SplitByRowOffset()
                    .withCrossPartition(splitCrossPartition)
                    .build();
        }

        RestOptions restOptions = RestOptions.newBuilder()
                .withConnectTimeout(connectTimeout)
                .withReadTimeout(readTimeout)
                .withRetryTimes(retryTimes)
                .build();

        Credentials credentials = Credentials.newBuilder()
                .withAccount(connector.getClient().getAccount())
                .withAppAccount(connector.getClient().getAppAccount())
                .build();

        settings = EnvironmentSettings.newBuilder()
                .withCredentials(credentials)
                .withServiceEndpoint(connector.getClient().getEndpoint())
                .withQuotaName(connector.getQuota())
                .withRestOptions(restOptions)
                .build();
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        return planScan(session, handle, columns, filter, -1);
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter, long limit) {
        ensureInitialized();
        MaxComputeTableHandle mcHandle = (MaxComputeTableHandle) handle;
        Table odpsTable = mcHandle.getOdpsTable();

        if (odpsTable.getFileNum() <= 0 || columns.isEmpty()) {
            return Collections.emptyList();
        }

        // Separate partition columns from data columns
        Set<String> partitionColumnNames = odpsTable.getSchema().getPartitionColumns()
                .stream().map(Column::getName).collect(Collectors.toSet());

        List<String> requiredPartitionCols = new ArrayList<>();
        List<String> requiredDataCols = new ArrayList<>();
        for (ConnectorColumnHandle colHandle : columns) {
            MaxComputeColumnHandle mcCol = (MaxComputeColumnHandle) colHandle;
            if (partitionColumnNames.contains(mcCol.getColumnName())) {
                requiredPartitionCols.add(mcCol.getColumnName());
            } else {
                requiredDataCols.add(mcCol.getColumnName());
            }
        }

        // Convert filter to ODPS predicate
        Predicate filterPredicate = convertFilter(filter, odpsTable);

        // Check limit optimization eligibility
        boolean onlyPartitionEquality = filter.isPresent()
                && checkOnlyPartitionEquality(filter.get(), partitionColumnNames);
        boolean useLimitOpt = limit > 0 && (onlyPartitionEquality || !filter.isPresent());

        try {
            if (useLimitOpt) {
                return planScanWithLimitOptimization(mcHandle.getTableIdentifier(),
                        requiredPartitionCols, requiredDataCols,
                        filterPredicate, limit, odpsTable);
            }

            TableBatchReadSession readSession = createReadSession(
                    mcHandle.getTableIdentifier(),
                    requiredPartitionCols, requiredDataCols,
                    filterPredicate, Collections.emptyList(), splitOptions);
            return buildSplitsFromSession(readSession, odpsTable);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create MaxCompute read session", e);
        }
    }

    private Predicate convertFilter(Optional<ConnectorExpression> filter, Table odpsTable) {
        if (!filter.isPresent()) {
            return Predicate.NO_PREDICATE;
        }

        Map<String, OdpsType> columnTypeMap = new HashMap<>();
        for (Column col : odpsTable.getSchema().getColumns()) {
            columnTypeMap.put(col.getName(), col.getType());
        }
        for (Column col : odpsTable.getSchema().getPartitionColumns()) {
            columnTypeMap.put(col.getName(), col.getType());
        }

        ZoneId sourceZone = resolveProjectTimeZone();
        MaxComputePredicateConverter converter = new MaxComputePredicateConverter(
                columnTypeMap, dateTimePushDown, sourceZone);
        return converter.convert(filter.get());
    }

    private ZoneId resolveProjectTimeZone() {
        return MCConnectorEndpoint.resolveProjectTimeZone(connector.getEndpoint());
    }

    private TableBatchReadSession createReadSession(
            TableIdentifier tableId,
            List<String> partitionCols, List<String> dataCols,
            Predicate filterPredicate,
            List<com.aliyun.odps.PartitionSpec> requiredPartitions,
            SplitOptions options) throws IOException {
        return new TableReadSessionBuilder()
                .identifier(tableId)
                .withSettings(settings)
                .withSplitOptions(options)
                .requiredPartitionColumns(partitionCols)
                .requiredDataColumns(dataCols)
                .withFilterPredicate(filterPredicate)
                .requiredPartitions(requiredPartitions)
                .withArrowOptions(
                        ArrowOptions.newBuilder()
                                .withDatetimeUnit(TimestampUnit.MILLI)
                                .withTimestampUnit(TimestampUnit.MICRO)
                                .build())
                .buildBatchReadSession();
    }

    private List<ConnectorScanRange> buildSplitsFromSession(
            TableBatchReadSession readSession, Table odpsTable) throws IOException {
        long t0 = System.currentTimeMillis();
        String serialized = serializeSession(readSession);
        long t1 = System.currentTimeMillis();
        LOG.info("MaxCompute serializeSession cost {} ms, size {} bytes",
                t1 - t0, serialized.length());

        InputSplitAssigner assigner = readSession.getInputSplitAssigner();
        List<ConnectorScanRange> result = new ArrayList<>();

        if (splitStrategy.equals(MCConnectorProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {
            for (com.aliyun.odps.table.read.split.InputSplit split : assigner.getAllSplits()) {
                result.add(MaxComputeScanRange.builder()
                        .start(((IndexedInputSplit) split).getSplitIndex())
                        .length(splitByteSize)
                        .scanSerialize(serialized)
                        .sessionId(split.getSessionId())
                        .splitType(MaxComputeScanRange.SPLIT_TYPE_BYTE_SIZE)
                        .readTimeout(readTimeout)
                        .connectTimeout(connectTimeout)
                        .retryTimes(retryTimes)
                        .build());
            }
        } else {
            long totalRowCount = assigner.getTotalRowCount();
            long rowsPerSplit = splitRowCount;
            for (long offset = 0; offset < totalRowCount; offset += rowsPerSplit) {
                long count = Math.min(rowsPerSplit, totalRowCount - offset);
                com.aliyun.odps.table.read.split.InputSplit split =
                        assigner.getSplitByRowOffset(offset, count);
                result.add(MaxComputeScanRange.builder()
                        .start(offset)
                        .length(count)
                        .scanSerialize(serialized)
                        .sessionId(split.getSessionId())
                        .splitType(MaxComputeScanRange.SPLIT_TYPE_ROW_OFFSET)
                        .readTimeout(readTimeout)
                        .connectTimeout(connectTimeout)
                        .retryTimes(retryTimes)
                        .build());
            }
        }

        LOG.info("MaxCompute planScan: {} splits generated in {} ms",
                result.size(), System.currentTimeMillis() - t0);
        return result;
    }

    private List<ConnectorScanRange> planScanWithLimitOptimization(
            TableIdentifier tableId,
            List<String> partitionCols, List<String> dataCols,
            Predicate filterPredicate, long limit,
            Table odpsTable) throws IOException {
        long t0 = System.currentTimeMillis();

        SplitOptions rowOffsetOptions = SplitOptions.newBuilder()
                .SplitByRowOffset()
                .withCrossPartition(false)
                .build();

        TableBatchReadSession readSession = createReadSession(
                tableId, partitionCols, dataCols,
                filterPredicate, Collections.emptyList(), rowOffsetOptions);

        String serialized = serializeSession(readSession);
        InputSplitAssigner assigner = readSession.getInputSplitAssigner();
        long totalRowCount = assigner.getTotalRowCount();

        LOG.info("MaxCompute limit optimization: totalRowCount={}, limit={}", totalRowCount, limit);

        if (totalRowCount <= 0) {
            return Collections.emptyList();
        }

        long rowsToRead = Math.min(limit, totalRowCount);
        com.aliyun.odps.table.read.split.InputSplit split =
                assigner.getSplitByRowOffset(0, rowsToRead);

        ConnectorScanRange range = MaxComputeScanRange.builder()
                .start(0)
                .length(rowsToRead)
                .scanSerialize(serialized)
                .sessionId(split.getSessionId())
                .splitType(MaxComputeScanRange.SPLIT_TYPE_ROW_OFFSET)
                .readTimeout(readTimeout)
                .connectTimeout(connectTimeout)
                .retryTimes(retryTimes)
                .build();

        LOG.info("MaxCompute limit optimization: 1 split with {} rows, cost {} ms",
                rowsToRead, System.currentTimeMillis() - t0);
        return Collections.singletonList(range);
    }

    /**
     * Check if all filter predicates are partition-column equality predicates.
     * This enables the limit optimization path.
     */
    private boolean checkOnlyPartitionEquality(ConnectorExpression expr,
            Set<String> partitionColumnNames) {
        // Conservative: return false to disable limit optimization when filter is complex.
        // The full check would walk the expression tree to verify all leaves are
        // partition_col = literal or partition_col IN (literal, ...).
        // For the first iteration, we keep it simple and always return false.
        return false;
    }

    private static String serializeSession(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
}

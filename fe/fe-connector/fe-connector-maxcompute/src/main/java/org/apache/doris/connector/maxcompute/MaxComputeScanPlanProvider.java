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
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit;
import com.aliyun.odps.table.configuration.SplitOptions;
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

    /**
     * FE session variable name gating the LIMIT-split optimization (default OFF). Hardcoded
     * here because the connector must not depend on fe-core's {@code SessionVariable} constant;
     * it is read from {@link ConnectorSession#getSessionProperties()} (same pattern the JDBC
     * connector uses for its session vars). Must stay byte-identical to
     * {@code SessionVariable.ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION}.
     */
    private static final String ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION =
            "enable_mc_limit_split_optimization";

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

        // EnvironmentSettings is built once on the connector and shared by both
        // the scan and write plan providers (mirrors legacy catalog.getSettings()).
        settings = connector.getSettings();
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
        return planScan(session, handle, columns, filter, limit, null);
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter, long limit, List<String> requiredPartitions) {
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
        Predicate filterPredicate = convertFilter(filter, odpsTable, session);

        // Partition pruning: restrict the read session to the pruned partitions when present.
        // null/empty => not pruned => scan all (mirrors legacy MaxComputeScanNode's empty
        // requiredPartitionSpecs). The "pruned to zero" case is short-circuited upstream in
        // PluginDrivenScanNode.getSplits, so it never reaches here.
        List<com.aliyun.odps.PartitionSpec> requiredPartitionSpecs = toPartitionSpecs(requiredPartitions);

        // Check limit optimization eligibility. Mirrors legacy MaxComputeScanNode's three-gate
        // (sessionVariable.enableMcLimitSplitOptimization && onlyPartitionEqualityPredicate
        // && hasLimit()), default OFF: the optimization fires only when the user enabled the
        // session var AND (there is no filter OR every conjunct is partition-column equality).
        boolean limitOptEnabled = isLimitOptEnabled(session.getSessionProperties());
        boolean useLimitOpt = shouldUseLimitOptimization(
                limitOptEnabled, limit, filter, partitionColumnNames);

        try {
            if (useLimitOpt) {
                return planScanWithLimitOptimization(mcHandle.getTableIdentifier(),
                        requiredPartitionCols, requiredDataCols,
                        filterPredicate, limit, requiredPartitionSpecs, odpsTable);
            }

            TableBatchReadSession readSession = createReadSession(
                    mcHandle.getTableIdentifier(),
                    requiredPartitionCols, requiredDataCols,
                    filterPredicate, requiredPartitionSpecs, splitOptions);
            return buildSplitsFromSession(readSession, odpsTable);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create MaxCompute read session", e);
        }
    }

    /**
     * Mirrors legacy {@code MaxComputeScanNode.isBatchMode()}'s {@code odpsTable.getFileNum() > 0}
     * gate. The partition-count / non-empty-slots / session-var gates live in the generic scan
     * node ({@code PluginDrivenScanNode.isBatchMode}); this method only answers the
     * connector-specific "does this table have files to read in batches" question.
     *
     * <p>{@code planScanForPartitionBatch} is intentionally NOT overridden: the SPI default
     * delegates to the 6-arg {@link #planScan}, which already builds one read session over the
     * given partition subset — exactly the per-batch behaviour legacy {@code startSplit} got from
     * {@code createTableBatchReadSession}.</p>
     */
    @Override
    public boolean supportsBatchScan(ConnectorSession session, ConnectorTableHandle handle) {
        return ((MaxComputeTableHandle) handle).getOdpsTable().getFileNum() > 0;
    }

    /**
     * Converts pruned partition spec strings (the keys of the Nereids selected-partition map,
     * e.g. {@code "pt=1,region=cn"}) into ODPS {@link com.aliyun.odps.PartitionSpec}s.
     * Mirrors legacy {@code MaxComputeScanNode}'s {@code new PartitionSpec(key)} conversion.
     *
     * <p>{@code null} or empty input returns an empty list, which the ODPS read session
     * builder treats as "read all partitions" — preserving the pre-pruning behavior.</p>
     */
    static List<com.aliyun.odps.PartitionSpec> toPartitionSpecs(List<String> requiredPartitions) {
        if (requiredPartitions == null || requiredPartitions.isEmpty()) {
            return Collections.emptyList();
        }
        List<com.aliyun.odps.PartitionSpec> specs = new ArrayList<>(requiredPartitions.size());
        for (String name : requiredPartitions) {
            specs.add(new com.aliyun.odps.PartitionSpec(name));
        }
        return specs;
    }

    private Predicate convertFilter(Optional<ConnectorExpression> filter, Table odpsTable,
            ConnectorSession session) {
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

        // Source time zone = the session time zone, mirroring legacy
        // MaxComputeScanNode.convertDateTimezone's DateUtils.getTimeZone() (= the session var).
        // ConnectorSession.getTimeZone() is populated from ctx.getSessionVariable().getTimeZone()
        // by ConnectorSessionBuilder.from(ctx), so this is the same source as legacy. (The earlier
        // project-region TZ from the endpoint was wrong: Doris interprets datetime literals in the
        // session TZ, so converting from any other zone shifts the pushed-down UTC literal.) The id
        // is passed raw and parsed lazily inside the converter, so a Doris-valid-but-ZoneId-invalid
        // value (e.g. "CST") degrades the datetime predicate instead of failing the query.
        MaxComputePredicateConverter converter = new MaxComputePredicateConverter(
                columnTypeMap, dateTimePushDown, session.getTimeZone());
        return converter.convert(filter.get());
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
                        // -1 is the BE sentinel that distinguishes BYTE_SIZE from ROW_OFFSET
                        // splits (MaxComputeJniScanner: split_size == -1 => BYTE_SIZE). The real
                        // byte size lives in the session, not the range; mirrors legacy
                        // MaxComputeScanNode's MaxComputeSplit(..., length=-1, ...).
                        .length(-1L)
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
            List<com.aliyun.odps.PartitionSpec> requiredPartitions,
            Table odpsTable) throws IOException {
        long t0 = System.currentTimeMillis();

        SplitOptions rowOffsetOptions = SplitOptions.newBuilder()
                .SplitByRowOffset()
                .withCrossPartition(false)
                .build();

        TableBatchReadSession readSession = createReadSession(
                tableId, partitionCols, dataCols,
                filterPredicate, requiredPartitions, rowOffsetOptions);

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
     * Gate (1): reads the {@code enable_mc_limit_split_optimization} session variable
     * (default {@code false}). Map-typed for direct unit testing without a live session.
     */
    static boolean isLimitOptEnabled(Map<String, String> sessionProperties) {
        return Boolean.parseBoolean(
                sessionProperties.getOrDefault(ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION, "false"));
    }

    /**
     * Whether the LIMIT-split optimization is eligible, mirroring legacy
     * {@code MaxComputeScanNode}'s {@code enableMcLimitSplitOptimization
     * && onlyPartitionEqualityPredicate && hasLimit()} (default OFF). Pure → unit-testable.
     *
     * @param limitOptEnabled gate (1): the session var value
     * @param limit gate (3): {@code > 0} means a LIMIT is present
     * @param filter the pushed-down filter; empty means no predicate
     * @param partitionColumnNames the table's partition column names
     */
    static boolean shouldUseLimitOptimization(boolean limitOptEnabled, long limit,
            Optional<ConnectorExpression> filter, Set<String> partitionColumnNames) {
        if (!limitOptEnabled || limit <= 0) {
            return false;
        }
        if (!filter.isPresent()) {
            // No predicate: every row qualifies, so the first min(limit, total) rows are correct.
            return true;
        }
        return checkOnlyPartitionEquality(filter.get(), partitionColumnNames);
    }

    /**
     * Gate (2): true iff every conjunct in {@code expr} is a partition-column equality
     * ({@code partcol = literal}) or partition-column IN-list ({@code partcol IN (literal, ...)}).
     * Mirrors legacy {@code MaxComputeScanNode.checkOnlyPartitionEqualityPredicate()}: when this
     * holds, every row in the (pruned) partitions qualifies, so reading the first {@code limit}
     * rows by row offset is correct.
     *
     * <p>The empty-filter case is handled upstream in {@link #shouldUseLimitOptimization}
     * (legacy treats empty conjuncts as eligible).</p>
     */
    static boolean checkOnlyPartitionEquality(ConnectorExpression expr,
            Set<String> partitionColumnNames) {
        if (expr instanceof ConnectorAnd) {
            for (ConnectorExpression conjunct : ((ConnectorAnd) expr).getConjuncts()) {
                if (!isPartitionEqualityLeaf(conjunct, partitionColumnNames)) {
                    return false;
                }
            }
            return true;
        }
        return isPartitionEqualityLeaf(expr, partitionColumnNames);
    }

    private static boolean isPartitionEqualityLeaf(ConnectorExpression expr,
            Set<String> partitionColumnNames) {
        // partcol = literal (mirror legacy: column on the LEFT, literal on the RIGHT, EQ only).
        if (expr instanceof ConnectorComparison) {
            ConnectorComparison cmp = (ConnectorComparison) expr;
            return cmp.getOperator() == ConnectorComparison.Operator.EQ
                    && isPartitionColumnRef(cmp.getLeft(), partitionColumnNames)
                    && cmp.getRight() instanceof ConnectorLiteral;
        }
        // partcol IN (literal, ...) (not NOT-IN; all list elements must be literals).
        if (expr instanceof ConnectorIn) {
            ConnectorIn in = (ConnectorIn) expr;
            if (in.isNegated() || !isPartitionColumnRef(in.getValue(), partitionColumnNames)) {
                return false;
            }
            for (ConnectorExpression item : in.getInList()) {
                if (!(item instanceof ConnectorLiteral)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static boolean isPartitionColumnRef(ConnectorExpression expr,
            Set<String> partitionColumnNames) {
        return expr instanceof ConnectorColumnRef
                && partitionColumnNames.contains(((ConnectorColumnRef) expr).getColumnName());
    }

    private static String serializeSession(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
}

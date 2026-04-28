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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergMetadataPlanningExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.ResultRow;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FE-side distributed planning producer.
 *
 * <p>The producer queries the logical system table {@code tbl$metadata_planning},
 * reconstructs file scan tasks from the result rows, and then reuses the normal
 * {@code FileScanTask -> IcebergSplit} path in {@link IcebergScanNode}.
 */
public class DistributedPlanningSplitProducer implements PlanningSplitProducer {
    private static final Logger LOG = LogManager.getLogger(DistributedPlanningSplitProducer.class);
    private static final Type LONG_LIST_TYPE = new TypeToken<List<Long>>() { }.getType();
    // Pre-built index map for O(1) column lookup; avoids O(n) linear search per field per row.
    private static final Map<String, Integer> COL_INDEX;

    static {
        COL_INDEX = new HashMap<>();
        List<String> cols = IcebergMetadataPlanningExternalTable.OUTPUT_COLUMN_NAMES;
        for (int i = 0; i < cols.size(); i++) {
            COL_INDEX.put(cols.get(i).toLowerCase(), i);
        }
    }

    private final PlanningSplitProducer.Context context;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicReference<CompletableFuture<Void>> runningTask = new AtomicReference<>();

    public DistributedPlanningSplitProducer(PlanningSplitProducer.Context context) {
        this.context = Preconditions.checkNotNull(context, "planning context is null");
    }

    @Override
    public void start(int numBackends, SplitSink sink) throws UserException {
        stopped.set(false);
        String planningQuerySql;
        ConnectContext parentContext;
        try {
            TableScan scan = context.getExecutionAuthenticator().execute(new Callable<TableScan>() {
                @Override
                public TableScan call() throws Exception {
                    return context.createTableScan();
                }
            });
            planningQuerySql = buildMetadataPlanningQuerySql(scan);
            parentContext = capturePlanningParentContext();
        } catch (Exception e) {
            Optional<NotSupportedException> opt = context.checkNotSupportedException(e);
            if (opt.isPresent()) {
                throw new UserException(opt.get().getMessage(), opt.get());
            } else if (e instanceof UserException) {
                throw (UserException) e;
            }
            throw new UserException(e.getMessage(), e);
        }
        final String sql = planningQuerySql;
        final ConnectContext planningParentContext = parentContext;
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                context.getExecutionAuthenticator().execute(() -> {
                    ConnectContext queryContext = createPlanningQueryContext(planningParentContext, sql);
                    try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(queryContext);
                            StmtExecutor.ResultRowBatchIterator resultBatches =
                                    createPlanningQueryIterator(queryContext, sql)) {
                        while (!stopped.get() && sink.needMore()) {
                            List<ResultRow> batchRows = resultBatches.getNextBatch();
                            if (batchRows == null) {
                                break;
                            }
                            List<FileScanTask> planningTasks = new ArrayList<>(batchRows.size());
                            for (ResultRow row : batchRows) {
                                if (stopped.get() || !sink.needMore()) {
                                    break;
                                }
                                planningTasks.add(createPlanningTask(row));
                            }
                            try (CloseableIterable<FileScanTask> splitTasks =
                                         context.splitPlanningTasks(planningTasks)) {
                                CloseableIterator<FileScanTask> iterator = splitTasks.iterator();
                                while (!stopped.get() && sink.needMore() && iterator.hasNext()) {
                                    sink.addBatch(Lists.newArrayList(context.createPlanningSplit(iterator.next())));
                                }
                            }
                        }
                    }
                    return null;
                });
                if (!stopped.get()) {
                    sink.finish();
                    LOG.info("Iceberg distributed metadata planning executed query [{}] "
                            + "and converted rows to split tasks", sql);
                }
            } catch (Exception e) {
                if (stopped.get()) {
                    LOG.debug("Iceberg distributed metadata planning is stopped");
                    return;
                }
                Optional<NotSupportedException> opt = context.checkNotSupportedException(e);
                if (opt.isPresent()) {
                    sink.fail(new UserException(opt.get().getMessage(), opt.get()));
                } else if (e instanceof UserException) {
                    sink.fail((UserException) e);
                } else {
                    LOG.warn("Unexpected error during iceberg distributed metadata planning", e);
                    sink.fail(new UserException(e.getMessage(), e));
                }
            }
        }, context.getScheduleExecutor());
        runningTask.set(future);
    }

    @Override
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture<Void> task = runningTask.get();
        if (task != null) {
            task.cancel(true);
        }
    }

    private String buildMetadataPlanningQuerySql(TableScan scan) {
        List<String> qualifier = context.getSourceTableQualifier();
        String qualifiedTable = quoteIdentifier(qualifier.get(0))
                + "." + quoteIdentifier(qualifier.get(1))
                + "." + quoteIdentifier(qualifier.get(2)
                + "$" + org.apache.doris.datasource.systable.IcebergSysTable.METADATA_PLANNING);
        String projection = String.join(", ", IcebergMetadataPlanningExternalTable.OUTPUT_COLUMN_NAMES);
        long snapshotId = Preconditions.checkNotNull(scan.snapshot(),
                "Iceberg distributed planning snapshot is null").snapshotId();
        String serializedPredicate = SerializationUtil.serializeToBase64(scan.filter());
        return "SELECT " + projection + " FROM " + qualifiedTable
                + " FOR VERSION AS OF " + snapshotId
                + " WHERE " + quoteIdentifier(IcebergMetadataPlanningExternalTable.CONTROL_PREDICATE_COLUMN)
                + " = " + quoteStringLiteral(serializedPredicate);
    }

    private ConnectContext capturePlanningParentContext() {
        ConnectContext parentContext = Preconditions.checkNotNull(ConnectContext.get(), "ConnectContext is null");
        ConnectContext contextSnapshot = parentContext.cloneContext();
        contextSnapshot.setSessionVariable(VariableMgr.cloneSessionVariable(parentContext.getSessionVariable()));
        contextSnapshot.setRemoteIP(parentContext.getRemoteIP());
        return contextSnapshot;
    }

    private ConnectContext createPlanningQueryContext(ConnectContext parentContext, String planningQuerySql) {
        ConnectContext queryContext = parentContext.cloneContext();
        queryContext.setSessionVariable(VariableMgr.cloneSessionVariable(parentContext.getSessionVariable()));
        queryContext.setRemoteIP(parentContext.getRemoteIP());
        queryContext.resetQueryId();
        queryContext.setStartTime();
        StatementContext statementContext = new StatementContext(queryContext,
                new OriginStatement(planningQuerySql, 0));
        queryContext.setStatementContext(statementContext);
        return queryContext;
    }

    private StmtExecutor.ResultRowBatchIterator createPlanningQueryIterator(
            ConnectContext queryContext, String planningQuerySql) {
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(planningQuerySql);
        LogicalPlanAdapter parsedStmt = new LogicalPlanAdapter(logicalPlan, queryContext.getStatementContext());
        parsedStmt.setOrigStmt(new OriginStatement(planningQuerySql, 0));
        StmtExecutor stmtExecutor = new StmtExecutor(queryContext, parsedStmt);
        return stmtExecutor.executeInternalQueryLazy();
    }

    private FileScanTask createPlanningTask(ResultRow row) throws UserException {
        int content = parseRequiredIntField(row, "content");
        Preconditions.checkState(content == ManifestContent.DATA.id(),
                "Only data file planning rows are supported, but got content %s", content);
        int specId = parseRequiredIntField(row, "spec_id");
        PartitionSpec partitionSpec = context.getPlanningPartitionSpec(specId);
        Preconditions.checkNotNull(partitionSpec, "Partition spec with specId %s not found", specId);
        DataFile dataFile = createPlanningDataFile(row, partitionSpec);
        return new BaseFileScanTask(
                dataFile,
                new DeleteFile[0],
                context.getPlanningTableSchemaJson(),
                PartitionSpecParser.toJson(partitionSpec),
                ResidualEvaluator.of(partitionSpec, Expressions.alwaysTrue(), true));
    }

    private DataFile createPlanningDataFile(ResultRow row, PartitionSpec partitionSpec) {
        String originalPath = getRequiredStringField(row, "file_path");
        long fileSizeInBytes = parseRequiredLongField(row, "file_size_in_bytes");
        long recordCount = parseRequiredLongField(row, "record_count");
        String fileFormat = getRequiredStringField(row, "file_format");
        String partitionDataJson = getOptionalStringField(row, "partition_data");
        List<Long> splitOffsets = parseSplitOffsets(getOptionalStringField(row, "split_offsets"));
        Long firstRowId = parseOptionalLongField(row, "first_row_id");
        Long fileSequenceNumber = parseOptionalLongField(row, "file_sequence_number");

        DataFiles.Builder builder = DataFiles.builder(partitionSpec)
                .withPath(originalPath)
                .withFileSizeInBytes(fileSizeInBytes)
                .withRecordCount(recordCount)
                .withFormat(FileFormat.fromString(fileFormat))
                .withSplitOffsets(splitOffsets)
                .withFirstRowId(firstRowId);
        if (partitionDataJson != null) {
            builder.withPartition(IcebergUtils.parsePartitionDataJson(partitionDataJson, partitionSpec));
        }
        return new PlanningDataFile(builder.build(), fileSequenceNumber);
    }

    private static int getRequiredColumnIndex(String columnName) {
        Integer index = COL_INDEX.get(columnName.toLowerCase());
        if (index == null) {
            throw new IllegalStateException("Required metadata planning column is missing: " + columnName);
        }
        return index;
    }

    private String getRequiredStringField(ResultRow row, String columnName) {
        String value = row.get(getRequiredColumnIndex(columnName));
        Preconditions.checkNotNull(value, "Metadata planning result column %s is null", columnName);
        return value;
    }

    private String getOptionalStringField(ResultRow row, String columnName) {
        return row.get(getRequiredColumnIndex(columnName));
    }

    private int parseRequiredIntField(ResultRow row, String columnName) {
        return Integer.parseInt(getRequiredStringField(row, columnName));
    }

    private long parseRequiredLongField(ResultRow row, String columnName) {
        return Long.parseLong(getRequiredStringField(row, columnName));
    }

    private Long parseOptionalLongField(ResultRow row, String columnName) {
        String value = getOptionalStringField(row, columnName);
        return value == null ? null : Long.valueOf(value);
    }

    private List<Long> parseSplitOffsets(String splitOffsets) {
        if (splitOffsets == null || splitOffsets.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            List<Long> offsets = org.apache.doris.persist.gson.GsonUtils.GSON.fromJson(splitOffsets, LONG_LIST_TYPE);
            return offsets == null ? Collections.emptyList() : offsets;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse split offsets: " + splitOffsets, e);
        }
    }

    private String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private String quoteStringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static class PlanningDataFile implements DataFile {
        private final DataFile delegate;
        private final Long fileSequenceNumber;

        private PlanningDataFile(DataFile delegate, Long fileSequenceNumber) {
            this.delegate = delegate;
            this.fileSequenceNumber = fileSequenceNumber;
        }

        @Override
        public Long pos() {
            return delegate.pos();
        }

        @Override
        public int specId() {
            return delegate.specId();
        }

        @Override
        public FileContent content() {
            return delegate.content();
        }

        @Override
        public CharSequence path() {
            return delegate.path();
        }

        @Override
        public FileFormat format() {
            return delegate.format();
        }

        @Override
        public StructLike partition() {
            return delegate.partition();
        }

        @Override
        public long recordCount() {
            return delegate.recordCount();
        }

        @Override
        public long fileSizeInBytes() {
            return delegate.fileSizeInBytes();
        }

        @Override
        public Map<Integer, Long> columnSizes() {
            return delegate.columnSizes();
        }

        @Override
        public Map<Integer, Long> valueCounts() {
            return delegate.valueCounts();
        }

        @Override
        public Map<Integer, Long> nullValueCounts() {
            return delegate.nullValueCounts();
        }

        @Override
        public Map<Integer, Long> nanValueCounts() {
            return delegate.nanValueCounts();
        }

        @Override
        public Map<Integer, ByteBuffer> lowerBounds() {
            return delegate.lowerBounds();
        }

        @Override
        public Map<Integer, ByteBuffer> upperBounds() {
            return delegate.upperBounds();
        }

        @Override
        public ByteBuffer keyMetadata() {
            return delegate.keyMetadata();
        }

        @Override
        public List<Long> splitOffsets() {
            return delegate.splitOffsets();
        }

        @Override
        public List<Integer> equalityFieldIds() {
            return delegate.equalityFieldIds();
        }

        @Override
        public Integer sortOrderId() {
            return delegate.sortOrderId();
        }

        @Override
        public Long dataSequenceNumber() {
            return delegate.dataSequenceNumber();
        }

        @Override
        public Long fileSequenceNumber() {
            // DataFiles.Builder can restore first_row_id, split_offsets and partition values, but it does not
            // expose a setter for file_sequence_number. Keep a thin wrapper here so the distributed path can
            // feed the same row-lineage metadata into IcebergScanNode#createIcebergSplit(FileScanTask) as the
            // local planning path.
            return fileSequenceNumber;
        }

        @Override
        public Long firstRowId() {
            return delegate.firstRowId();
        }

        @Override
        public DataFile copy() {
            return new PlanningDataFile(delegate.copy(), fileSequenceNumber);
        }

        @Override
        public DataFile copyWithoutStats() {
            return new PlanningDataFile(delegate.copyWithoutStats(), fileSequenceNumber);
        }

        @Override
        public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
            return new PlanningDataFile(delegate.copyWithStats(requestedColumnIds), fileSequenceNumber);
        }
    }
}

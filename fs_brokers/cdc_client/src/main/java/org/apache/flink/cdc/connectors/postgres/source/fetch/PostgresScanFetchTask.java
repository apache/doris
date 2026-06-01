/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetUtils;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresQueryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.debezium.connector.postgresql.PostgresObjectUtils.waitForReplicationSlotReady;
import static io.debezium.connector.postgresql.Utils.refreshSchema;

/**
 * Copied from Flink Cdc 3.6.0
 *
 * <p>Line 333~336: modified createDataEventsForTable to fix FLINK-39748.
 */
public class PostgresScanFetchTask extends AbstractScanFetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresScanFetchTask.class);

    public PostgresScanFetchTask(SnapshotSplit split) {
        super(split);
    }

    @Override
    public void execute(Context context) throws Exception {

        PostgresSourceFetchTaskContext ctx = (PostgresSourceFetchTaskContext) context;
        PostgresSourceConfig sourceConfig = (PostgresSourceConfig) context.getSourceConfig();
        try {
            // create slot here,  because a slot can only read wal-log after its own creation.
            // if skip backfill, no need to create slot here
            maybeCreateSlotForBackFillReadTask(
                    ctx.getConnection(),
                    ctx.getReplicationConnection(),
                    sourceConfig.getSlotNameForBackfillTask(),
                    ctx.getPluginName(),
                    sourceConfig.isSkipSnapshotBackfill());
            super.execute(context);
        } finally {
            // remove slot after snapshot slit finish
            maybeDropSlotForBackFillReadTask(
                    (PostgresReplicationConnection) ctx.getReplicationConnection(),
                    sourceConfig.isSkipSnapshotBackfill());
        }
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        PostgresSourceFetchTaskContext ctx = (PostgresSourceFetchTaskContext) context;

        PostgresSnapshotSplitReadTask snapshotSplitReadTask =
                new PostgresSnapshotSplitReadTask(
                        ctx.getConnection(),
                        ctx.getDbzConnectorConfig(),
                        ctx.getDatabaseSchema(),
                        ctx.getOffsetContext(),
                        ctx.getEventDispatcher(),
                        ctx.getSnapshotChangeEventSourceMetrics(),
                        snapshotSplit);

        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        SnapshotResult<PostgresOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext, ctx.getPartition(), ctx.getOffsetContext());

        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for postgres split %s fail", snapshotResult));
        }
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        PostgresSourceFetchTaskContext ctx = (PostgresSourceFetchTaskContext) context;

        final PostgresOffsetContext.Loader loader =
                new PostgresOffsetContext.Loader(ctx.getDbzConnectorConfig());
        final PostgresOffsetContext postgresOffsetContext =
                PostgresOffsetUtils.getPostgresOffsetContext(
                        loader, backfillStreamSplit.getStartingOffset());

        final PostgresStreamFetchTask.StreamSplitReadTask backfillReadTask =
                new PostgresStreamFetchTask.StreamSplitReadTask(
                        ctx.getDbzConnectorConfig(),
                        ctx.getSnapShotter(),
                        ctx.getConnection(),
                        ctx.getEventDispatcher(),
                        ctx.getWaterMarkDispatcher(),
                        ctx.getErrorHandler(),
                        ctx.getTaskContext().getClock(),
                        ctx.getDatabaseSchema(),
                        ctx.getTaskContext(),
                        ctx.getReplicationConnection(),
                        backfillStreamSplit);
        LOG.info(
                "Execute backfillReadTask for split {} with slot name {}",
                snapshotSplit,
                ((PostgresSourceConfig) ctx.getSourceConfig()).getSlotNameForBackfillTask());
        backfillReadTask.execute(
                new StoppableChangeEventSourceContext(), ctx.getPartition(), postgresOffsetContext);
    }

    /**
     * Create a slot before snapshot reading so that the slot can track the WAL log during the
     * snapshot reading phase.
     */
    private void maybeCreateSlotForBackFillReadTask(
            PostgresConnection jdbcConnection,
            ReplicationConnection replicationConnection,
            String slotName,
            String pluginName,
            boolean skipSnapshotBackfill) {
        // if skip backfill, no need to create slot here
        if (skipSnapshotBackfill) {
            return;
        }

        try {
            SlotState slotInfo = null;
            try {
                slotInfo = jdbcConnection.getReplicationSlotState(slotName, pluginName);
            } catch (SQLException e) {
                LOG.info("Unable to load info of replication slot, will try to create the slot");
            }
            if (slotInfo == null) {
                try {
                    replicationConnection.createReplicationSlot().orElse(null);
                } catch (SQLException ex) {
                    String message = "Creation of replication slot failed";
                    if (ex.getMessage().contains("already exists")) {
                        message +=
                                "; when setting up multiple connectors for the same database host, please make sure to use a distinct replication slot name for each.";
                    }
                    throw new FlinkRuntimeException(message, ex);
                }
            }
            waitForReplicationSlotReady(30, jdbcConnection, slotName, pluginName);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    /** Drop slot for backfill task and close replication connection. */
    private void maybeDropSlotForBackFillReadTask(
            PostgresReplicationConnection replicationConnection, boolean skipSnapshotBackfill) {
        // if skip backfill, no need to create slot here
        if (skipSnapshotBackfill) {
            return;
        }

        try {
            replicationConnection.close(true);
        } catch (Throwable t) {
            LOG.error("Unexpected error while dropping replication slot", t);
            throw new FlinkRuntimeException(t);
        }
    }

    /** A SnapshotChangeEventSource implementation for Postgres to read snapshot split. */
    public static class PostgresSnapshotSplitReadTask
            extends AbstractSnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> {
        private static final Logger LOG =
                LoggerFactory.getLogger(PostgresSnapshotSplitReadTask.class);

        private final PostgresConnection jdbcConnection;
        private final PostgresConnectorConfig connectorConfig;
        private final PostgresEventDispatcher<TableId> eventDispatcher;
        private final SnapshotSplit snapshotSplit;
        private final PostgresOffsetContext offsetContext;
        private final PostgresSchema databaseSchema;
        private final SnapshotProgressListener<PostgresPartition> snapshotProgressListener;
        private final Clock clock;

        public PostgresSnapshotSplitReadTask(
                PostgresConnection jdbcConnection,
                PostgresConnectorConfig connectorConfig,
                PostgresSchema databaseSchema,
                PostgresOffsetContext previousOffset,
                PostgresEventDispatcher<TableId> eventDispatcher,
                SnapshotProgressListener snapshotProgressListener,
                SnapshotSplit snapshotSplit) {
            super(connectorConfig, snapshotProgressListener);
            this.jdbcConnection = jdbcConnection;
            this.connectorConfig = connectorConfig;
            this.snapshotProgressListener = snapshotProgressListener;
            this.databaseSchema = databaseSchema;
            this.eventDispatcher = eventDispatcher;
            this.snapshotSplit = snapshotSplit;
            this.offsetContext = previousOffset;
            this.clock = Clock.SYSTEM;
        }

        @Override
        protected SnapshotResult<PostgresOffsetContext> doExecute(
                ChangeEventSourceContext context,
                PostgresOffsetContext previousOffset,
                SnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final PostgresSnapshotContext ctx = (PostgresSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;

            refreshSchema(databaseSchema, jdbcConnection, true);
            createDataEvents(ctx, snapshotSplit.getTableId());

            return SnapshotResult.completed(ctx.offset);
        }

        private void createDataEvents(PostgresSnapshotContext snapshotContext, TableId tableId)
                throws InterruptedException {
            EventDispatcher.SnapshotReceiver<PostgresPartition> snapshotReceiver =
                    eventDispatcher.getSnapshotChangeEventReceiver();
            LOG.info("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext,
                    snapshotReceiver,
                    Objects.requireNonNull(databaseSchema.tableFor(tableId)));
            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                PostgresSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver<PostgresPartition> snapshotReceiver,
                Table table)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}",
                    snapshotSplit.splitId(),
                    table.id());

            List<String> uuidFields =
                    snapshotSplit.getSplitKeyType().getFieldNames().stream()
                            .filter(field -> table.columnWithName(field).typeName().equals("uuid"))
                            .collect(Collectors.toList());

            List<String> columnNames =
                    table.columns().stream()
                            .map(column -> jdbcConnection.quotedColumnIdString(column.name()))
                            .collect(Collectors.toList());
            final String selectSql =
                    PostgresQueryUtils.buildSplitScanQuery(
                            snapshotSplit.getTableId(),
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null,
                            columnNames,
                            uuidFields);
            LOG.debug(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    table.id(),
                    selectSql);

            try (PreparedStatement selectStatement =
                            PostgresQueryUtils.readTableSplitDataStatement(
                                    jdbcConnection,
                                    selectSql,
                                    snapshotSplit.getSplitStart() == null,
                                    snapshotSplit.getSplitEnd() == null,
                                    snapshotSplit.getSplitStart(),
                                    snapshotSplit.getSplitEnd(),
                                    snapshotSplit.getSplitKeyType().getFieldCount(),
                                    connectorConfig.getSnapshotFetchSize());
                    ResultSet rs = selectStatement.executeQuery()) {

                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();

                while (rs.next()) {
                    rows++;
                    final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                    for (int i = 0; i < columnArray.getColumns().length; i++) {
                        Column col = columnArray.getColumns()[i];
                        row[col.position() - 1] =
                                jdbcConnection.getColumnValue(
                                        rs, i + 1, col, table, databaseSchema);
                    }
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(stop - exportStart));
                        snapshotProgressListener.rowsScanned(
                                snapshotContext.partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    snapshotContext.offset.event(table.id(), clock.currentTime());
                    SnapshotChangeRecordEmitter<PostgresPartition> emitter =
                            new SnapshotChangeRecordEmitter<>(
                                    snapshotContext.partition, snapshotContext.offset, row, clock);
                    eventDispatcher.dispatchSnapshotEvent(
                            snapshotContext.partition, table.id(), emitter, snapshotReceiver);
                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
            } catch (SQLException e) {
                throw new FlinkRuntimeException(
                        "Snapshotting of table " + table.id() + " failed", e);
            }
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(
                PostgresPartition partition, PostgresOffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected PostgresSnapshotContext prepare(PostgresPartition partition) throws Exception {
            return new PostgresSnapshotContext(partition);
        }

        private static class PostgresSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                        PostgresPartition, PostgresOffsetContext> {

            public PostgresSnapshotContext(PostgresPartition partition) throws SQLException {
                super(partition, "");
            }
        }
    }
}

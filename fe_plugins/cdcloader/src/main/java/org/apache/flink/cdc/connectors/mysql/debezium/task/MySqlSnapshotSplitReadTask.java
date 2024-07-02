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

package org.apache.flink.cdc.connectors.mysql.debezium.task;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl;
import org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.hooks.SnapshotPhaseHooks;

import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.Calendar;

/**
 * copy flinkcdc from https://github.com/apache/flink-cdc/blob/release-3.1.1/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc/src/main/java/org/apache/flink/cdc/connectors/mysql/debezium/task/MySqlSnapshotSplitReadTask.java
 * add 160 and 194
 * */
public class MySqlSnapshotSplitReadTask
    extends AbstractSnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSnapshotSplitReadTask.class);

    /** Interval for showing a log statement with the progress while scanning a single table. */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final MySqlSourceConfig sourceConfig;
    private final MySqlDatabaseSchema databaseSchema;
    private final MySqlConnection jdbcConnection;
    private final EventDispatcherImpl<TableId> dispatcher;
    private final Clock clock;
    private final MySqlSnapshotSplit snapshotSplit;
    private final TopicSelector<TableId> topicSelector;
    private final EventDispatcher.SnapshotReceiver<MySqlPartition> snapshotReceiver;
    private final SnapshotChangeEventSourceMetrics<MySqlPartition> snapshotChangeEventSourceMetrics;

    private final SnapshotPhaseHooks hooks;
    private final boolean isBackfillSkipped;

    public MySqlSnapshotSplitReadTask(
        MySqlSourceConfig sourceConfig,
        MySqlConnectorConfig connectorConfig,
        SnapshotChangeEventSourceMetrics<MySqlPartition> snapshotChangeEventSourceMetrics,
        MySqlDatabaseSchema databaseSchema,
        MySqlConnection jdbcConnection,
        EventDispatcherImpl<TableId> dispatcher,
        TopicSelector<TableId> topicSelector,
        EventDispatcher.SnapshotReceiver<MySqlPartition> snapshotReceiver,
        Clock clock,
        MySqlSnapshotSplit snapshotSplit,
        SnapshotPhaseHooks hooks,
        boolean isBackfillSkipped) {
        super(connectorConfig, snapshotChangeEventSourceMetrics);
        this.sourceConfig = sourceConfig;
        this.databaseSchema = databaseSchema;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotSplit = snapshotSplit;
        this.topicSelector = topicSelector;
        this.snapshotReceiver = snapshotReceiver;
        this.snapshotChangeEventSourceMetrics = snapshotChangeEventSourceMetrics;
        this.hooks = hooks;
        this.isBackfillSkipped = isBackfillSkipped;
    }

    @Override
    public SnapshotResult<MySqlOffsetContext> execute(
        ChangeEventSourceContext context,
        MySqlPartition partition,
        MySqlOffsetContext previousOffset)
        throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
        final SnapshotContext<MySqlPartition, MySqlOffsetContext> ctx;
        try {
            ctx = prepare(partition);
        } catch (Exception e) {
            LOG.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }
        try {
            return doExecute(context, previousOffset, ctx, snapshottingTask);
        } catch (InterruptedException e) {
            LOG.warn("Snapshot was interrupted before completion");
            throw e;
        } catch (Exception t) {
            throw new DebeziumException(t);
        }
    }

    @Override
    protected SnapshotResult<MySqlOffsetContext> doExecute(
        ChangeEventSourceContext context,
        MySqlOffsetContext previousOffset,
        SnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext,
        SnapshottingTask snapshottingTask)
        throws Exception {
        final MySqlSnapshotContext ctx = (MySqlSnapshotContext) snapshotContext;
        ctx.offset = previousOffset;
        final SignalEventDispatcher signalEventDispatcher =
            new SignalEventDispatcher(
                previousOffset.getOffset(),
                topicSelector.topicNameFor(snapshotSplit.getTableId()),
                dispatcher.getQueue());

        if (hooks.getPreLowWatermarkAction() != null) {
            hooks.getPreLowWatermarkAction().accept(jdbcConnection, snapshotSplit);
        }
        final BinlogOffset lowWatermark = DebeziumUtils.currentBinlogOffset(jdbcConnection);
        lowWatermark.getOffset().put(BinlogOffset.TIMESTAMP_KEY, String.valueOf(clock.currentTime().getEpochSecond()));
        LOG.info(
            "Snapshot step 1 - Determining low watermark {} for split {}",
            lowWatermark,
            snapshotSplit);
        ((SnapshotSplitReader.SnapshotSplitChangeEventSourceContextImpl) (context))
            .setLowWatermark(lowWatermark);
        signalEventDispatcher.dispatchWatermarkEvent(
            snapshotSplit, lowWatermark, SignalEventDispatcher.WatermarkKind.LOW);

        if (hooks.getPostLowWatermarkAction() != null) {
            hooks.getPostLowWatermarkAction().accept(jdbcConnection, snapshotSplit);
        }

        LOG.info("Snapshot step 2 - Snapshotting data");
        createDataEvents(ctx, snapshotSplit.getTableId());

        if (hooks.getPreHighWatermarkAction() != null) {
            hooks.getPreHighWatermarkAction().accept(jdbcConnection, snapshotSplit);
        }

        BinlogOffset highWatermark;
        if (isBackfillSkipped) {
            // Directly set HW = LW if backfill is skipped. Binlog events created during snapshot
            // phase could be processed later in binlog reading phase.
            //
            // Note that this behaviour downgrades the delivery guarantee to at-least-once. We can't
            // promise that the snapshot is exactly the view of the table at low watermark moment,
            // so binlog events created during snapshot might be replayed later in binlog reading
            // phase.
            highWatermark = lowWatermark;
        } else {
            // Get the current binlog offset as HW
            highWatermark = DebeziumUtils.currentBinlogOffset(jdbcConnection);
            highWatermark.getOffset().put(BinlogOffset.TIMESTAMP_KEY, String.valueOf(clock.currentTime().getEpochSecond()));
        }

        LOG.info(
            "Snapshot step 3 - Determining high watermark {} for split {}",
            highWatermark,
            snapshotSplit);
        signalEventDispatcher.dispatchWatermarkEvent(
            snapshotSplit, highWatermark, SignalEventDispatcher.WatermarkKind.HIGH);
        ((SnapshotSplitReader.SnapshotSplitChangeEventSourceContextImpl) (context))
            .setHighWatermark(highWatermark);

        if (hooks.getPostHighWatermarkAction() != null) {
            hooks.getPostHighWatermarkAction().accept(jdbcConnection, snapshotSplit);
        }
        return SnapshotResult.completed(ctx.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(
        MySqlPartition partition, MySqlOffsetContext previousOffset) {
        return new SnapshottingTask(false, true);
    }

    @Override
    protected MySqlSnapshotContext prepare(MySqlPartition partition) throws Exception {
        return new MySqlSnapshotContext(partition);
    }

    private static class MySqlSnapshotContext
        extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
        MySqlPartition, MySqlOffsetContext> {

        public MySqlSnapshotContext(MySqlPartition partition) throws SQLException {
            super(partition, "");
        }
    }

    private void createDataEvents(MySqlSnapshotContext snapshotContext, TableId tableId)
        throws Exception {
        LOG.debug("Snapshotting table {}", tableId);
        createDataEventsForTable(
            snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
        snapshotReceiver.completeSnapshot();
    }

    /** Dispatches the data change events for the records of a single table. */
    private void createDataEventsForTable(
        MySqlSnapshotContext snapshotContext,
        EventDispatcher.SnapshotReceiver<MySqlPartition> snapshotReceiver,
        Table table)
        throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOG.info("Exporting data from split '{}' of table {}", snapshotSplit.splitId(), table.id());

        final String selectSql =
            StatementUtils.buildSplitScanQuery(
                snapshotSplit.getTableId(),
                snapshotSplit.getSplitKeyType(),
                snapshotSplit.getSplitStart() == null,
                snapshotSplit.getSplitEnd() == null);
        LOG.info(
            "For split '{}' of table {} using select statement: '{}'",
            snapshotSplit.splitId(),
            table.id(),
            selectSql);

        try (PreparedStatement selectStatement =
                 StatementUtils.readTableSplitDataStatement(
                     jdbcConnection,
                     selectSql,
                     snapshotSplit.getSplitStart() == null,
                     snapshotSplit.getSplitEnd() == null,
                     snapshotSplit.getSplitStart(),
                     snapshotSplit.getSplitEnd(),
                     snapshotSplit.getSplitKeyType().getFieldCount(),
                     sourceConfig.getFetchSize());
             ResultSet rs = selectStatement.executeQuery()) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Threads.Timer logTimer = getTableScanLogTimer();

            while (rs.next()) {
                rows++;
                final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                for (int i = 0; i < columnArray.getColumns().length; i++) {
                    Column actualColumn = table.columns().get(i);
                    row[columnArray.getColumns()[i].position() - 1] =
                        readField(rs, i + 1, actualColumn, table);
                }
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    LOG.info(
                        "Exported {} records for split '{}' after {}",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(stop - exportStart));
                    snapshotChangeEventSourceMetrics.rowsScanned(
                        snapshotContext.partition, table.id(), rows);
                    logTimer = getTableScanLogTimer();
                }
                dispatcher.dispatchSnapshotEvent(
                    (MySqlPartition) snapshotContext.partition,
                    table.id(),
                    getChangeRecordEmitter(snapshotContext, table.id(), row),
                    snapshotReceiver);
            }
            LOG.info(
                "Finished exporting {} records for split '{}', total duration '{}'",
                rows,
                snapshotSplit.splitId(),
                Strings.duration(clock.currentTimeInMillis() - exportStart));
        } catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    protected ChangeRecordEmitter<MySqlPartition> getChangeRecordEmitter(
        MySqlSnapshotContext snapshotContext, TableId tableId, Object[] row) {
        snapshotContext.offset.event(tableId, clock.currentTime());
        return new SnapshotChangeRecordEmitter<>(
            snapshotContext.partition, snapshotContext.offset, row, clock);
    }

    private Threads.Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    /**
     * Read JDBC return value and deal special type like time, timestamp.
     *
     * <p>Note https://issues.redhat.com/browse/DBZ-3238 has fixed this issue, please remove this
     * method once we bump Debezium version to 1.6
     */
    private Object readField(ResultSet rs, int fieldNo, Column actualColumn, Table actualTable)
        throws SQLException {
        if (actualColumn.jdbcType() == Types.TIME) {
            return readTimeField(rs, fieldNo);
        } else if (actualColumn.jdbcType() == Types.DATE) {
            return readDateField(rs, fieldNo, actualColumn, actualTable);
        }
        // This is for DATETIME columns (a logical date + time without time zone)
        // by reading them with a calendar based on the default time zone, we make sure that the
        // value
        // is constructed correctly using the database's (or connection's) time zone
        else if (actualColumn.jdbcType() == Types.TIMESTAMP) {
            return readTimestampField(rs, fieldNo, actualColumn, actualTable);
        }
        // JDBC's rs.GetObject() will return a Boolean for all TINYINT(1) columns.
        // TINYINT columns are reprtoed as SMALLINT by JDBC driver
        else if (actualColumn.jdbcType() == Types.TINYINT
            || actualColumn.jdbcType() == Types.SMALLINT) {
            // It seems that rs.wasNull() returns false when default value is set and NULL is
            // inserted
            // We thus need to use getObject() to identify if the value was provided and if yes then
            // read it again to get correct scale
            return rs.getObject(fieldNo) == null ? null : rs.getInt(fieldNo);
        } else {
            return rs.getObject(fieldNo);
        }
    }

    /**
     * As MySQL connector/J implementation is broken for MySQL type "TIME" we have to use a
     * binary-ish workaround. https://issues.jboss.org/browse/DBZ-342
     */
    private Object readTimeField(ResultSet rs, int fieldNo) throws SQLException {
        Blob b = rs.getBlob(fieldNo);
        if (b == null) {
            return null; // Don't continue parsing time field if it is null
        }

        try {
            return MySqlValueConverters.stringToDuration(
                new String(b.getBytes(1, (int) (b.length())), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            LOG.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    /**
     * In non-string mode the date field can contain zero in any of the date part which we need to
     * handle as all-zero.
     */
    private Object readDateField(ResultSet rs, int fieldNo, Column column, Table table)
        throws SQLException {
        Blob b = rs.getBlob(fieldNo);
        if (b == null) {
            return null; // Don't continue parsing date field if it is null
        }

        try {
            return MySqlValueConverters.stringToLocalDate(
                new String(b.getBytes(1, (int) (b.length())), "UTF-8"), column, table);
        } catch (UnsupportedEncodingException e) {
            LOG.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    /**
     * In non-string mode the time field can contain zero in any of the date part which we need to
     * handle as all-zero.
     */
    private Object readTimestampField(ResultSet rs, int fieldNo, Column column, Table table)
        throws SQLException {
        Blob b = rs.getBlob(fieldNo);
        if (b == null) {
            return null; // Don't continue parsing timestamp field if it is null
        }

        try {
            return MySqlValueConverters.containsZeroValuesInDatePart(
                (new String(b.getBytes(1, (int) (b.length())), "UTF-8")), column, table)
                ? null
                : rs.getTimestamp(fieldNo, Calendar.getInstance());
        } catch (UnsupportedEncodingException e) {
            LOG.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }
}

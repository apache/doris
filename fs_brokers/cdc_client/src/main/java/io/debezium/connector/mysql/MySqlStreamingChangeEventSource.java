/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializationException;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import com.github.shyiko.mysql.binlog.network.DefaultSSLSocketFactory;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;
import com.github.shyiko.mysql.binlog.network.ServerException;
import io.debezium.DebeziumException;
import io.debezium.annotation.SingleThreadAccess;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.GtidNewChannelPosition;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.util.ErrorMessageUtils;
import io.debezium.data.Envelope.Operation;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static io.debezium.util.Strings.isNullOrEmpty;

/**
 * Copied from FlinkCDC project(3.5.0).
 *
 * <p>Line 924 : change Log Level info to debug.
 */
public class MySqlStreamingChangeEventSource
        implements StreamingChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MySqlStreamingChangeEventSource.class);

    private static final String KEEPALIVE_THREAD_NAME = "blc-keepalive";

    private final EnumMap<EventType, BlockingConsumer<Event>> eventHandlers =
            new EnumMap<>(EventType.class);
    private final BinaryLogClient client;
    private final MySqlStreamingChangeEventSourceMetrics metrics;
    private final Clock clock;
    private final EventProcessingFailureHandlingMode eventDeserializationFailureHandlingMode;
    private final EventProcessingFailureHandlingMode inconsistentSchemaHandlingMode;

    private int startingRowNumber = 0;
    private long initialEventsToSkip = 0L;
    private boolean skipEvent = false;
    private boolean ignoreDmlEventByGtidSource = false;
    private final Predicate<String> gtidDmlSourceFilter;
    private final AtomicLong totalRecordCounter = new AtomicLong();
    private volatile Map<String, ?> lastOffset = null;
    private com.github.shyiko.mysql.binlog.GtidSet gtidSet;
    private final float heartbeatIntervalFactor = 0.8f;
    private final Map<String, Thread> binaryLogClientThreads = new ConcurrentHashMap<>(4);
    private final MySqlTaskContext taskContext;
    private final MySqlConnectorConfig connectorConfig;
    private final MySqlConnection connection;
    private final EventDispatcher<MySqlPartition, TableId> eventDispatcher;
    private final ErrorHandler errorHandler;

    @SingleThreadAccess("binlog client thread")
    private Instant eventTimestamp;

    /** Describe binlog position. */
    public static class BinlogPosition {
        final String filename;
        final long position;

        public BinlogPosition(String filename, long position) {
            assert filename != null;

            this.filename = filename;
            this.position = position;
        }

        public String getFilename() {
            return filename;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public String toString() {
            return filename + "/" + position;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + filename.hashCode();
            result = prime * result + (int) (position ^ (position >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BinlogPosition other = (BinlogPosition) obj;
            if (!filename.equals(other.filename)) {
                return false;
            }
            if (position != other.position) {
                return false;
            }
            return true;
        }
    }

    @FunctionalInterface
    private interface BinlogChangeEmitter<T> {
        void emit(TableId tableId, T data) throws InterruptedException;
    }

    public MySqlStreamingChangeEventSource(
            MySqlConnectorConfig connectorConfig,
            MySqlConnection connection,
            EventDispatcher<MySqlPartition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            MySqlTaskContext taskContext,
            MySqlStreamingChangeEventSourceMetrics metrics) {

        this.taskContext = taskContext;
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.clock = clock;
        this.eventDispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.metrics = metrics;

        eventDeserializationFailureHandlingMode =
                connectorConfig.getEventProcessingFailureHandlingMode();
        inconsistentSchemaHandlingMode = connectorConfig.inconsistentSchemaFailureHandlingMode();

        // Set up the log reader ...
        client = taskContext.getBinaryLogClient();
        // BinaryLogClient will overwrite thread names later
        client.setThreadFactory(
                Threads.threadFactory(
                        MySqlConnector.class,
                        connectorConfig.getLogicalName(),
                        "binlog-client",
                        false,
                        false,
                        x -> binaryLogClientThreads.put(x.getName(), x)));
        client.setServerId(connectorConfig.serverId());
        client.setSSLMode(sslModeFor(connectorConfig.sslMode()));
        if (connectorConfig.sslModeEnabled()) {
            SSLSocketFactory sslSocketFactory =
                    getBinlogSslSocketFactory(connectorConfig, connection);
            if (sslSocketFactory != null) {
                client.setSslSocketFactory(sslSocketFactory);
            }
        }
        Configuration configuration = connectorConfig.getConfig();
        client.setKeepAlive(configuration.getBoolean(MySqlConnectorConfig.KEEP_ALIVE));
        final long keepAliveInterval =
                configuration.getLong(MySqlConnectorConfig.KEEP_ALIVE_INTERVAL_MS);
        client.setKeepAliveInterval(keepAliveInterval);
        // Considering heartbeatInterval should be less than keepAliveInterval, we use the
        // heartbeatIntervalFactor
        // multiply by keepAliveInterval and set the result value to heartbeatInterval.The default
        // value of heartbeatIntervalFactor
        // is 0.8, and we believe the left time (0.2 * keepAliveInterval) is enough to process the
        // packet received from the MySQL server.
        client.setHeartbeatInterval((long) (keepAliveInterval * heartbeatIntervalFactor));

        boolean filterDmlEventsByGtidSource =
                configuration.getBoolean(MySqlConnectorConfig.GTID_SOURCE_FILTER_DML_EVENTS);
        gtidDmlSourceFilter =
                filterDmlEventsByGtidSource ? connectorConfig.gtidSourceFilter() : null;

        // Set up the event deserializer with additional type(s) ...
        final Map<Long, TableMapEventData> tableMapEventByTableId =
                new HashMap<Long, TableMapEventData>();
        EventDeserializer eventDeserializer =
                new EventDeserializer() {
                    @Override
                    public Event nextEvent(ByteArrayInputStream inputStream) throws IOException {
                        try {
                            // Delegate to the superclass ...
                            Event event = super.nextEvent(inputStream);

                            // We have to record the most recent TableMapEventData for each table
                            // number for our custom deserializers ...
                            if (event.getHeader().getEventType() == EventType.TABLE_MAP) {
                                TableMapEventData tableMapEvent = event.getData();
                                tableMapEventByTableId.put(
                                        tableMapEvent.getTableId(), tableMapEvent);
                            }

                            // DBZ-5126 Clean cache on rotate event to prevent it from growing
                            // indefinitely.
                            if (event.getHeader().getEventType() == EventType.ROTATE
                                    && event.getHeader().getTimestamp() != 0) {
                                tableMapEventByTableId.clear();
                            }
                            return event;
                        }
                        // DBZ-217 In case an event couldn't be read we create a pseudo-event for
                        // the sake of logging
                        catch (EventDataDeserializationException edde) {
                            // DBZ-3095 As of Java 15, when reaching EOF in the binlog stream, the
                            // polling loop in
                            // BinaryLogClient#listenForEventPackets() keeps returning values != -1
                            // from peek();
                            // this causes the loop to never finish
                            // Propagating the exception (either EOF or socket closed) causes the
                            // loop to be aborted
                            // in this case
                            if (edde.getCause() instanceof IOException) {
                                throw edde;
                            }

                            EventHeaderV4 header = new EventHeaderV4();
                            header.setEventType(EventType.INCIDENT);
                            header.setTimestamp(edde.getEventHeader().getTimestamp());
                            header.setServerId(edde.getEventHeader().getServerId());

                            if (edde.getEventHeader() instanceof EventHeaderV4) {
                                header.setEventLength(
                                        ((EventHeaderV4) edde.getEventHeader()).getEventLength());
                                header.setNextPosition(
                                        ((EventHeaderV4) edde.getEventHeader()).getNextPosition());
                                header.setFlags(((EventHeaderV4) edde.getEventHeader()).getFlags());
                            }

                            EventData data = new EventDataDeserializationExceptionData(edde);
                            return new Event(header, data);
                        }
                    }
                };

        // Add our custom deserializers ...
        eventDeserializer.setEventDataDeserializer(EventType.STOP, new StopEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.GTID, new GtidEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(
                EventType.WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(tableMapEventByTableId));
        eventDeserializer.setEventDataDeserializer(
                EventType.UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(tableMapEventByTableId));
        eventDeserializer.setEventDataDeserializer(
                EventType.DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(tableMapEventByTableId));
        eventDeserializer.setEventDataDeserializer(
                EventType.EXT_WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(tableMapEventByTableId)
                        .setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(
                EventType.EXT_UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(tableMapEventByTableId)
                        .setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(
                EventType.EXT_DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(tableMapEventByTableId)
                        .setMayContainExtraInformation(true));
        client.setEventDeserializer(eventDeserializer);
    }

    protected void onEvent(MySqlOffsetContext offsetContext, Event event) {
        long ts = 0;

        if (event.getHeader().getEventType() == EventType.HEARTBEAT) {
            // HEARTBEAT events have no timestamp but are fired only when
            // there is no traffic on the connection which means we are caught-up
            // https://dev.mysql.com/doc/internals/en/heartbeat-event.html
            metrics.setMilliSecondsBehindSource(ts);
            return;
        }

        // MySQL has seconds resolution but mysql-binlog-connector-java returns
        // a value in milliseconds
        long eventTs = event.getHeader().getTimestamp();

        if (eventTs == 0) {
            LOGGER.trace("Received unexpected event with 0 timestamp: {}", event);
            return;
        }

        ts = clock.currentTimeInMillis() - eventTs;
        LOGGER.trace("Current milliseconds behind source: {} ms", ts);
        metrics.setMilliSecondsBehindSource(ts);
    }

    protected void ignoreEvent(MySqlOffsetContext offsetContext, Event event) {
        LOGGER.trace("Ignoring event due to missing handler: {}", event);
    }

    protected void handleEvent(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event) {
        if (event == null) {
            return;
        }

        final EventHeader eventHeader = event.getHeader();
        // Update the source offset info. Note that the client returns the value in *milliseconds*,
        // even though the binlog
        // contains only *seconds* precision ...
        // HEARTBEAT events have no timestamp; only set the timestamp if the event is not a
        // HEARTBEAT
        eventTimestamp =
                !eventHeader.getEventType().equals(EventType.HEARTBEAT)
                        ? Instant.ofEpochMilli(eventHeader.getTimestamp())
                        : null;
        offsetContext.setBinlogServerId(eventHeader.getServerId());

        final EventType eventType = eventHeader.getEventType();
        if (eventType == EventType.ROTATE) {
            EventData eventData = event.getData();
            RotateEventData rotateEventData;
            if (eventData instanceof EventDeserializer.EventDataWrapper) {
                rotateEventData =
                        (RotateEventData)
                                ((EventDeserializer.EventDataWrapper) eventData).getInternal();
            } else {
                rotateEventData = (RotateEventData) eventData;
            }
            offsetContext.setBinlogStartPoint(
                    rotateEventData.getBinlogFilename(), rotateEventData.getBinlogPosition());
        } else if (eventHeader instanceof EventHeaderV4) {
            EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
            offsetContext.setEventPosition(
                    trackableEventHeader.getPosition(), trackableEventHeader.getEventLength());
        }

        // If there is a handler for this event, forward the event to it ...
        try {
            // Forward the event to the handler ...
            eventHandlers
                    .getOrDefault(eventType, (e) -> ignoreEvent(offsetContext, e))
                    .accept(event);

            // Generate heartbeat message if the time is right
            eventDispatcher.dispatchHeartbeatEvent(partition, offsetContext);

            // Capture that we've completed another event ...
            offsetContext.completeEvent();

            // update last offset used for logging
            lastOffset = offsetContext.getOffset();

            if (skipEvent) {
                // We're in the mode of skipping events and we just skipped this one, so decrement
                // our skip count ...
                --initialEventsToSkip;
                skipEvent = initialEventsToSkip > 0;
            }
        } catch (RuntimeException e) {
            // There was an error in the event handler, so propagate the failure to Kafka Connect
            // ...
            logStreamingSourceState();
            errorHandler.setProducerThrowable(
                    new DebeziumException("Error processing binlog event", e));
            // Do not stop the client, since Kafka Connect should stop the connector on it's own
            // (and doing it here may cause problems the second time it is stopped).
            // We can clear the listeners though so that we ignore all future events ...
            eventHandlers.clear();
            LOGGER.info(
                    "Error processing binlog event, and propagating to Kafka Connect so it stops this connector. Future binlog events read before connector is shutdown will be ignored.");
        } catch (InterruptedException e) {
            // Most likely because this reader was stopped and our thread was interrupted ...
            Thread.currentThread().interrupt();
            eventHandlers.clear();
            LOGGER.info("Stopped processing binlog events due to thread interruption");
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends EventData> T unwrapData(Event event) {
        EventData eventData = event.getData();
        if (eventData instanceof EventDeserializer.EventDataWrapper) {
            eventData = ((EventDeserializer.EventDataWrapper) eventData).getInternal();
        }
        return (T) eventData;
    }

    /**
     * Handle the supplied event that signals that mysqld has stopped.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerStop(MySqlOffsetContext offsetContext, Event event) {
        LOGGER.debug("Server stopped: {}", event);
    }

    /**
     * Handle the supplied event that is sent by a primary to a replica to let the replica know that
     * the primary is still alive. Not written to a binary log.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerHeartbeat(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event)
            throws InterruptedException {
        LOGGER.trace("Server heartbeat: {}", event);
        eventDispatcher.dispatchServerHeartbeatEvent(partition, offsetContext);
    }

    /**
     * Handle the supplied event that signals that an out of the ordinary event that occurred on the
     * master. It notifies the replica that something happened on the primary that might cause data
     * to be in an inconsistent state.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerIncident(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event) {
        if (event.getData() instanceof EventDataDeserializationExceptionData) {
            metrics.onErroneousEvent(partition, "source = " + event);
            EventDataDeserializationExceptionData data = event.getData();

            EventHeaderV4 eventHeader =
                    (EventHeaderV4)
                            data.getCause()
                                    .getEventHeader(); // safe cast, instantiated that ourselves

            // logging some additional context but not the exception itself, this will happen in
            // handleEvent()
            if (eventDeserializationFailureHandlingMode
                    == EventProcessingFailureHandlingMode.FAIL) {
                LOGGER.error(
                        "Error while deserializing binlog event at offset {}.{}"
                                + "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        offsetContext.getOffset(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        offsetContext.getSource().binlogFilename());

                throw new RuntimeException(data.getCause());
            } else if (eventDeserializationFailureHandlingMode
                    == EventProcessingFailureHandlingMode.WARN) {
                LOGGER.warn(
                        "Error while deserializing binlog event at offset {}.{}"
                                + "This exception will be ignored and the event be skipped.{}"
                                + "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        offsetContext.getOffset(),
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        offsetContext.getSource().binlogFilename(),
                        data.getCause());
            }
        } else {
            LOGGER.error("Server incident: {}", event);
        }
    }

    /**
     * Handle the supplied event with a {@link RotateEventData} that signals the logs are being
     * rotated. This means that either the server was restarted, or the binlog has transitioned to a
     * new file. In either case, subsequent table numbers will be different than those seen to this
     * point.
     *
     * @param event the database change data event to be processed; may not be null
     */
    protected void handleRotateLogsEvent(MySqlOffsetContext offsetContext, Event event) {
        LOGGER.debug("Rotating logs: {}", event);
        RotateEventData command = unwrapData(event);
        assert command != null;
        taskContext.getSchema().clearTableMappings();
    }

    /**
     * Handle the supplied event with a {@link GtidEventData} that signals the beginning of a GTID
     * transaction. We don't yet know whether this transaction contains any events we're interested
     * in, but we have to record it so that we know the position of this event and know we've
     * processed the binlog to this point.
     *
     * <p>Note that this captures the current GTID and complete GTID set, regardless of whether the
     * connector is {@link MySqlTaskContext#gtidSourceFilter() filtering} the GTID set upon
     * connection. We do this because we actually want to capture all GTID set values found in the
     * binlog, whether or not we process them. However, only when we connect do we actually want to
     * pass to MySQL only those GTID ranges that are applicable per the configuration.
     *
     * @param event the GTID event to be processed; may not be null
     */
    protected void handleGtidEvent(MySqlOffsetContext offsetContext, Event event) {
        LOGGER.debug("GTID transaction: {}", event);
        GtidEventData gtidEvent = unwrapData(event);
        String gtid = gtidEvent.getGtid();
        gtidSet.add(gtid);
        offsetContext.startGtid(gtid, gtidSet.toString()); // rather than use the client's GTID set
        ignoreDmlEventByGtidSource = false;
        if (gtidDmlSourceFilter != null && gtid != null) {
            String uuid = gtid.trim().substring(0, gtid.indexOf(":"));
            if (!gtidDmlSourceFilter.test(uuid)) {
                ignoreDmlEventByGtidSource = true;
            }
        }
        metrics.onGtidChange(gtid);
    }

    /**
     * Handle the supplied event with an {@link RowsQueryEventData} by recording the original SQL
     * query that generated the event.
     *
     * @param event the database change data event to be processed; may not be null
     */
    protected void handleRowsQuery(MySqlOffsetContext offsetContext, Event event) {
        // Unwrap the RowsQueryEvent
        final RowsQueryEventData lastRowsQueryEventData = unwrapData(event);

        // Set the query on the source
        offsetContext.setQuery(lastRowsQueryEventData.getQuery());
    }

    /**
     * Handle the supplied event with an {@link QueryEventData} by possibly recording the DDL
     * statements as changes in the MySQL schemas.
     *
     * @param partition the partition in which the even occurred
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while recording the DDL statements
     */
    protected void handleQueryEvent(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event)
            throws InterruptedException {
        QueryEventData command = unwrapData(event);
        LOGGER.debug("Received query command: {}", event);
        String sql = command.getSql().trim();
        if (sql.equalsIgnoreCase("BEGIN")) {
            // We are starting a new transaction ...
            offsetContext.startNextTransaction();
            eventDispatcher.dispatchTransactionStartedEvent(
                    partition, offsetContext.getTransactionId(), offsetContext);
            offsetContext.setBinlogThread(command.getThreadId());
            if (initialEventsToSkip != 0) {
                LOGGER.debug(
                        "Restarting partially-processed transaction; change events will not be created for the first {} events plus {} more rows in the next event",
                        initialEventsToSkip,
                        startingRowNumber);
                // We are restarting, so we need to skip the events in this transaction that we
                // processed previously...
                skipEvent = true;
            }
            return;
        }
        if (sql.equalsIgnoreCase("COMMIT")) {
            handleTransactionCompletion(partition, offsetContext, event);
            return;
        }

        String upperCasedStatementBegin = Strings.getBegin(sql, 7).toUpperCase();

        if (upperCasedStatementBegin.startsWith("XA ")) {
            // This is an XA transaction, and we currently ignore these and do nothing ...
            return;
        }
        if (connectorConfig.getDdlFilter().test(sql)) {
            LOGGER.debug("DDL '{}' was filtered out of processing", sql);
            return;
        }
        if (upperCasedStatementBegin.equals("INSERT ")
                || upperCasedStatementBegin.equals("UPDATE ")
                || upperCasedStatementBegin.equals("DELETE ")) {
            LOGGER.warn(
                    "Received DML '"
                            + sql
                            + "' for processing, binlog probably contains events generated with statement or mixed based replication format");
            return;
        }
        if (sql.equalsIgnoreCase("ROLLBACK")) {
            // We have hit a ROLLBACK which is not supported
            LOGGER.warn(
                    "Rollback statements cannot be handled without binlog buffering, the connector will fail. Please check '{}' to see how to enable buffering",
                    MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER.name());
        }

        final List<SchemaChangeEvent> schemaChangeEvents =
                taskContext
                        .getSchema()
                        .parseStreamingDdl(
                                partition,
                                sql,
                                command.getDatabase(),
                                offsetContext,
                                clock.currentTimeAsInstant());
        try {
            for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
                if (taskContext.getSchema().skipSchemaChangeEvent(schemaChangeEvent)) {
                    continue;
                }

                final TableId tableId =
                        schemaChangeEvent.getTables().isEmpty()
                                ? null
                                : schemaChangeEvent.getTables().iterator().next().id();
                eventDispatcher.dispatchSchemaChangeEvent(
                        partition,
                        tableId,
                        (receiver) -> {
                            try {
                                receiver.schemaChangeEvent(schemaChangeEvent);
                            } catch (Exception e) {
                                throw new DebeziumException(e);
                            }
                        });
            }
        } catch (InterruptedException e) {
            LOGGER.info("Processing interrupted");
        }
    }

    private void handleTransactionCompletion(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event)
            throws InterruptedException {
        // We are completing the transaction ...
        eventDispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
        offsetContext.commitTransaction();
        offsetContext.setBinlogThread(-1L);
        skipEvent = false;
        ignoreDmlEventByGtidSource = false;
    }

    /**
     * Handle a change in the table metadata.
     *
     * <p>This method should be called whenever we consume a TABLE_MAP event, and every transaction
     * in the log should include one of these for each table affected by the transaction. Each table
     * map event includes a monotonically-increasing numeric identifier, and this identifier is used
     * within subsequent events within the same transaction. This table identifier can change when:
     *
     * <ol>
     *   <li>the table structure is modified (e.g., via an {@code ALTER TABLE ...} command); or
     *   <li>MySQL rotates to a new binary log file, even if the table structure does not change.
     * </ol>
     *
     * @param event the update event; never null
     */
    protected void handleUpdateTableMetadata(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event)
            throws InterruptedException {
        TableMapEventData metadata = unwrapData(event);
        long tableNumber = metadata.getTableId();
        String databaseName = metadata.getDatabase();
        String tableName = metadata.getTable();
        TableId tableId = new TableId(databaseName, null, tableName);
        if (taskContext.getSchema().assignTableNumber(tableNumber, tableId)) {
            LOGGER.debug("Received update table metadata event: {}", event);
        } else {
            informAboutUnknownTableIfRequired(partition, offsetContext, event, tableId);
        }
    }

    /**
     * If we receive an event for a table that is monitored but whose metadata we don't know, either
     * ignore that event or raise a warning or error as per the {@link
     * MySqlConnectorConfig#INCONSISTENT_SCHEMA_HANDLING_MODE} configuration.
     */
    private void informAboutUnknownTableIfRequired(
            MySqlPartition partition,
            MySqlOffsetContext offsetContext,
            Event event,
            TableId tableId,
            Operation operation)
            throws InterruptedException {
        if (tableId != null
                && connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            metrics.onErroneousEvent(
                    partition, "source = " + tableId + ", event " + event, operation);
            EventHeaderV4 eventHeader = event.getHeader();

            if (inconsistentSchemaHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                LOGGER.error(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database history topic. Take a new snapshot in this case.{}"
                                + "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event,
                        offsetContext.getOffset(),
                        tableId,
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        offsetContext.getSource().binlogFilename());
                throw new DebeziumException(
                        "Encountered change event for table "
                                + tableId
                                + " whose schema isn't known to this connector");
            } else if (inconsistentSchemaHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                LOGGER.warn(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database history topic. Take a new snapshot in this case.{}"
                                + "The event will be ignored.{}"
                                + "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event,
                        offsetContext.getOffset(),
                        tableId,
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        offsetContext.getSource().binlogFilename());
            } else {
                LOGGER.debug(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database history topic. Take a new snapshot in this case.{}"
                                + "The event will be ignored.{}"
                                + "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event,
                        offsetContext.getOffset(),
                        tableId,
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        offsetContext.getSource().binlogFilename());
            }
        } else {
            if (tableId == null) {
                EventData eventData = unwrapData(event);
                if (eventData instanceof WriteRowsEventData) {
                    tableId =
                            taskContext
                                    .getSchema()
                                    .getExcludeTableId(
                                            ((WriteRowsEventData) eventData).getTableId());
                } else if (eventData instanceof UpdateRowsEventData) {
                    tableId =
                            taskContext
                                    .getSchema()
                                    .getExcludeTableId(
                                            ((UpdateRowsEventData) eventData).getTableId());
                } else if (eventData instanceof DeleteRowsEventData) {
                    tableId =
                            taskContext
                                    .getSchema()
                                    .getExcludeTableId(
                                            ((DeleteRowsEventData) eventData).getTableId());
                }
            }
            LOGGER.trace("Filtered {} event for {}", event.getHeader().getEventType(), tableId);
            metrics.onFilteredEvent(partition, "source = " + tableId, operation);
            eventDispatcher.dispatchFilteredEvent(partition, offsetContext);
        }
    }

    private void informAboutUnknownTableIfRequired(
            MySqlPartition partition,
            MySqlOffsetContext offsetContext,
            Event event,
            TableId tableId)
            throws InterruptedException {
        informAboutUnknownTableIfRequired(partition, offsetContext, event, tableId, null);
    }

    /**
     * Generate source records for the supplied event with an {@link WriteRowsEventData}.
     *
     * @param partition the partition in which the even occurred
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleInsert(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event)
            throws InterruptedException {
        handleChange(
                partition,
                offsetContext,
                event,
                Operation.CREATE,
                WriteRowsEventData.class,
                x -> taskContext.getSchema().getTableId(x.getTableId()),
                WriteRowsEventData::getRows,
                (tableId, row) ->
                        eventDispatcher.dispatchDataChangeEvent(
                                partition,
                                tableId,
                                new MySqlChangeRecordEmitter(
                                        partition,
                                        offsetContext,
                                        clock,
                                        Operation.CREATE,
                                        null,
                                        row)));
    }

    /**
     * Generate source records for the supplied event with an {@link UpdateRowsEventData}.
     *
     * @param partition the partition in which the even occurred
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleUpdate(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event)
            throws InterruptedException {
        handleChange(
                partition,
                offsetContext,
                event,
                Operation.UPDATE,
                UpdateRowsEventData.class,
                x -> taskContext.getSchema().getTableId(x.getTableId()),
                UpdateRowsEventData::getRows,
                (tableId, row) ->
                        eventDispatcher.dispatchDataChangeEvent(
                                partition,
                                tableId,
                                new MySqlChangeRecordEmitter(
                                        partition,
                                        offsetContext,
                                        clock,
                                        Operation.UPDATE,
                                        row.getKey(),
                                        row.getValue())));
    }

    /**
     * Generate source records for the supplied event with an {@link DeleteRowsEventData}.
     *
     * @param partition the partition in which the even occurred
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleDelete(
            MySqlPartition partition, MySqlOffsetContext offsetContext, Event event)
            throws InterruptedException {
        handleChange(
                partition,
                offsetContext,
                event,
                Operation.DELETE,
                DeleteRowsEventData.class,
                x -> taskContext.getSchema().getTableId(x.getTableId()),
                DeleteRowsEventData::getRows,
                (tableId, row) ->
                        eventDispatcher.dispatchDataChangeEvent(
                                partition,
                                tableId,
                                new MySqlChangeRecordEmitter(
                                        partition,
                                        offsetContext,
                                        clock,
                                        Operation.DELETE,
                                        row,
                                        null)));
    }

    private <T extends EventData, U> void handleChange(
            MySqlPartition partition,
            MySqlOffsetContext offsetContext,
            Event event,
            Operation operation,
            Class<T> eventDataClass,
            TableIdProvider<T> tableIdProvider,
            RowsProvider<T, U> rowsProvider,
            BinlogChangeEmitter<U> changeEmitter)
            throws InterruptedException {
        if (skipEvent) {
            // We can skip this because we should already be at least this far ...
            LOGGER.debug("Skipping previously processed row event: {}", event);
            return;
        }
        if (ignoreDmlEventByGtidSource) {
            LOGGER.debug("Skipping DML event because this GTID source is filtered: {}", event);
            return;
        }
        final T data = unwrapData(event);
        final TableId tableId = tableIdProvider.getTableId(data);
        final List<U> rows = rowsProvider.getRows(data);
        String changeType = operation.name();

        if (tableId != null && taskContext.getSchema().schemaFor(tableId) != null) {
            int count = 0;
            int numRows = rows.size();
            if (startingRowNumber < numRows) {
                for (int row = startingRowNumber; row != numRows; ++row) {
                    offsetContext.setRowNumber(row, numRows);
                    offsetContext.event(tableId, eventTimestamp);
                    changeEmitter.emit(tableId, rows.get(row));
                    count++;
                }
                if (LOGGER.isDebugEnabled()) {
                    if (startingRowNumber != 0) {
                        LOGGER.debug(
                                "Emitted {} {} record(s) for last {} row(s) in event: {}",
                                count,
                                changeType,
                                numRows - startingRowNumber,
                                event);
                    } else {
                        LOGGER.debug(
                                "Emitted {} {} record(s) for event: {}", count, changeType, event);
                    }
                }
                offsetContext.changeEventCompleted();
            } else {
                // All rows were previously processed ...
                LOGGER.debug("Skipping previously processed {} event: {}", changeType, event);
            }
        } else {
            informAboutUnknownTableIfRequired(partition, offsetContext, event, tableId, operation);
        }
        startingRowNumber = 0;
    }

    /**
     * Handle a {@link EventType#VIEW_CHANGE} event.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void viewChange(MySqlOffsetContext offsetContext, Event event)
            throws InterruptedException {
        LOGGER.debug("View Change event: {}", event);
        // do nothing
    }

    /**
     * Handle a {@link EventType#XA_PREPARE} event.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void prepareTransaction(MySqlOffsetContext offsetContext, Event event)
            throws InterruptedException {
        LOGGER.debug("XA Prepare event: {}", event);
        // do nothing
    }

    private SSLMode sslModeFor(SecureConnectionMode mode) {
        switch (mode) {
            case DISABLED:
                return SSLMode.DISABLED;
            case PREFERRED:
                return SSLMode.PREFERRED;
            case REQUIRED:
                return SSLMode.REQUIRED;
            case VERIFY_CA:
                return SSLMode.VERIFY_CA;
            case VERIFY_IDENTITY:
                return SSLMode.VERIFY_IDENTITY;
        }
        return null;
    }

    @Override
    public void execute(
            ChangeEventSourceContext context,
            MySqlPartition partition,
            MySqlOffsetContext offsetContext)
            throws InterruptedException {
        if (!connectorConfig.getSnapshotMode().shouldStream()) {
            LOGGER.info(
                    "Streaming is disabled for snapshot mode {}",
                    connectorConfig.getSnapshotMode());
            return;
        }
        if (connectorConfig.getSnapshotMode() != MySqlConnectorConfig.SnapshotMode.NEVER) {
            taskContext.getSchema().assureNonEmptySchema();
        }
        final Set<Operation> skippedOperations = connectorConfig.getSkippedOperations();

        final MySqlOffsetContext effectiveOffsetContext =
                offsetContext != null ? offsetContext : MySqlOffsetContext.initial(connectorConfig);

        // Register our event handlers ...
        eventHandlers.put(
                EventType.STOP, (event) -> handleServerStop(effectiveOffsetContext, event));
        eventHandlers.put(
                EventType.HEARTBEAT,
                (event) -> handleServerHeartbeat(partition, effectiveOffsetContext, event));
        eventHandlers.put(
                EventType.INCIDENT,
                (event) -> handleServerIncident(partition, effectiveOffsetContext, event));
        eventHandlers.put(
                EventType.ROTATE, (event) -> handleRotateLogsEvent(effectiveOffsetContext, event));
        eventHandlers.put(
                EventType.TABLE_MAP,
                (event) -> handleUpdateTableMetadata(partition, effectiveOffsetContext, event));
        eventHandlers.put(
                EventType.QUERY,
                (event) -> handleQueryEvent(partition, effectiveOffsetContext, event));

        if (!skippedOperations.contains(Operation.CREATE)) {
            eventHandlers.put(
                    EventType.WRITE_ROWS,
                    (event) -> handleInsert(partition, effectiveOffsetContext, event));
            eventHandlers.put(
                    EventType.EXT_WRITE_ROWS,
                    (event) -> handleInsert(partition, effectiveOffsetContext, event));
        }

        if (!skippedOperations.contains(Operation.UPDATE)) {
            eventHandlers.put(
                    EventType.UPDATE_ROWS,
                    (event) -> handleUpdate(partition, effectiveOffsetContext, event));
            eventHandlers.put(
                    EventType.EXT_UPDATE_ROWS,
                    (event) -> handleUpdate(partition, effectiveOffsetContext, event));
        }

        if (!skippedOperations.contains(Operation.DELETE)) {
            eventHandlers.put(
                    EventType.DELETE_ROWS,
                    (event) -> handleDelete(partition, effectiveOffsetContext, event));
            eventHandlers.put(
                    EventType.EXT_DELETE_ROWS,
                    (event) -> handleDelete(partition, effectiveOffsetContext, event));
        }

        eventHandlers.put(
                EventType.VIEW_CHANGE, (event) -> viewChange(effectiveOffsetContext, event));
        eventHandlers.put(
                EventType.XA_PREPARE, (event) -> prepareTransaction(effectiveOffsetContext, event));
        eventHandlers.put(
                EventType.XID,
                (event) -> handleTransactionCompletion(partition, effectiveOffsetContext, event));

        // Conditionally register ROWS_QUERY handler to parse SQL statements.
        if (connectorConfig.includeSqlQuery()) {
            eventHandlers.put(
                    EventType.ROWS_QUERY,
                    (event) -> handleRowsQuery(effectiveOffsetContext, event));
        }

        BinaryLogClient.EventListener listener;
        if (connectorConfig.bufferSizeForStreamingChangeEventSource() == 0) {
            listener = (event) -> handleEvent(partition, effectiveOffsetContext, event);
        } else {
            EventBuffer buffer =
                    new EventBuffer(
                            connectorConfig.bufferSizeForStreamingChangeEventSource(),
                            this,
                            context);
            listener = (event) -> buffer.add(partition, effectiveOffsetContext, event);
        }
        client.registerEventListener(listener);

        client.registerLifecycleListener(new ReaderThreadLifecycleListener(effectiveOffsetContext));
        client.registerEventListener((event) -> onEvent(effectiveOffsetContext, event));
        if (LOGGER.isDebugEnabled()) {
            client.registerEventListener((event) -> logEvent(effectiveOffsetContext, event));
        }

        final boolean isGtidModeEnabled = connection.isGtidModeEnabled();
        metrics.setIsGtidModeEnabled(isGtidModeEnabled);

        // Get the current GtidSet from MySQL so we can get a filtered/merged GtidSet based off of
        // the last Debezium checkpoint.
        String availableServerGtidStr = connection.knownGtidSet();
        if (isGtidModeEnabled) {
            // The server is using GTIDs, so enable the handler ...
            eventHandlers.put(
                    EventType.GTID, (event) -> handleGtidEvent(effectiveOffsetContext, event));

            // Now look at the GTID set from the server and what we've previously seen ...
            GtidSet availableServerGtidSet = new GtidSet(availableServerGtidStr);

            // also take into account purged GTID logs
            GtidSet purgedServerGtidSet = connection.purgedGtidSet();
            LOGGER.info("GTID set purged on server: {}", purgedServerGtidSet);

            GtidSet filteredGtidSet =
                    filterGtidSet(
                            effectiveOffsetContext, availableServerGtidSet, purgedServerGtidSet);
            if (filteredGtidSet != null) {
                // We've seen at least some GTIDs, so start reading from the filtered GTID set ...
                LOGGER.info("Registering binlog reader with GTID set: {}", filteredGtidSet);
                String filteredGtidSetStr = filteredGtidSet.toString();
                client.setGtidSet(filteredGtidSetStr);
                effectiveOffsetContext.setCompletedGtidSet(filteredGtidSetStr);
                gtidSet = new com.github.shyiko.mysql.binlog.GtidSet(filteredGtidSetStr);
            } else {
                // We've not yet seen any GTIDs, so that means we have to start reading the binlog
                // from the beginning ...
                client.setBinlogFilename(effectiveOffsetContext.getSource().binlogFilename());
                client.setBinlogPosition(effectiveOffsetContext.getSource().binlogPosition());
                gtidSet = new com.github.shyiko.mysql.binlog.GtidSet("");
            }
        } else {
            // The server is not using GTIDs, so start reading the binlog based upon where we last
            // left off ...
            client.setBinlogFilename(effectiveOffsetContext.getSource().binlogFilename());
            client.setBinlogPosition(effectiveOffsetContext.getSource().binlogPosition());
        }

        // We may be restarting in the middle of a transaction, so see how far into the transaction
        // we have already processed...
        initialEventsToSkip = effectiveOffsetContext.eventsToSkipUponRestart();
        LOGGER.info("Skip {} events on streaming start", initialEventsToSkip);

        // Set the starting row number, which is the next row number to be read ...
        startingRowNumber = effectiveOffsetContext.rowsToSkipUponRestart();
        LOGGER.info("Skip {} rows on streaming start", startingRowNumber);

        // Only when we reach the first BEGIN event will we start to skip events ...
        skipEvent = false;

        try {
            // Start the log reader, which starts background threads ...
            if (context.isRunning()) {
                long timeout = connectorConfig.getConnectionTimeout().toMillis();
                long started = clock.currentTimeInMillis();
                try {
                    LOGGER.debug(
                            "Attempting to establish binlog reader connection with timeout of {} ms",
                            timeout);
                    client.connect(timeout);
                    // Need to wait for keepalive thread to be running, otherwise it can be left
                    // orphaned
                    // The problem is with timing. When the close is called too early after connect
                    // then
                    // the keepalive thread is not terminated
                    if (client.isKeepAlive()) {
                        LOGGER.info("Waiting for keepalive thread to start");
                        final Metronome metronome = Metronome.parker(Duration.ofMillis(100), clock);
                        int waitAttempts = 50;
                        boolean keepAliveThreadRunning = false;
                        while (!keepAliveThreadRunning && waitAttempts-- > 0) {
                            for (Thread t : binaryLogClientThreads.values()) {
                                if (t.getName().startsWith(KEEPALIVE_THREAD_NAME) && t.isAlive()) {
                                    LOGGER.info("Keepalive thread is running");
                                    keepAliveThreadRunning = true;
                                }
                            }
                            metronome.pause();
                        }
                    }
                } catch (TimeoutException e) {
                    // If the client thread is interrupted *before* the client could connect, the
                    // client throws a timeout exception
                    // The only way we can distinguish this is if we get the timeout exception
                    // before the specified timeout has
                    // elapsed, so we simply check this (within 10%) ...
                    long duration = clock.currentTimeInMillis() - started;
                    if (duration > (0.9 * timeout)) {
                        double actualSeconds = TimeUnit.MILLISECONDS.toSeconds(duration);
                        throw new DebeziumException(
                                "Timed out after "
                                        + actualSeconds
                                        + " seconds while waiting to connect to MySQL at "
                                        + connectorConfig.hostname()
                                        + ":"
                                        + connectorConfig.port()
                                        + " with user '"
                                        + connectorConfig.username()
                                        + "'",
                                e);
                    }
                    // Otherwise, we were told to shutdown, so we don't care about the timeout
                    // exception
                } catch (AuthenticationException e) {
                    throw new DebeziumException(
                            "Failed to authenticate to the MySQL database at "
                                    + connectorConfig.hostname()
                                    + ":"
                                    + connectorConfig.port()
                                    + " with user '"
                                    + connectorConfig.username()
                                    + "'",
                            e);
                } catch (Throwable e) {
                    throw new DebeziumException(
                            "Unable to connect to the MySQL database at "
                                    + connectorConfig.hostname()
                                    + ":"
                                    + connectorConfig.port()
                                    + " with user '"
                                    + connectorConfig.username()
                                    + "': "
                                    + e.getMessage(),
                            e);
                }
            }
            while (context.isRunning()) {
                Thread.sleep(100);
            }
        } finally {
            try {
                client.disconnect();
            } catch (Exception e) {
                LOGGER.info("Exception while stopping binary log client", e);
            }
        }
    }

    private SSLSocketFactory getBinlogSslSocketFactory(
            MySqlConnectorConfig connectorConfig, MySqlConnection connection) {
        String acceptedTlsVersion = connection.getSessionVariableForSslVersion();
        if (!isNullOrEmpty(acceptedTlsVersion)) {
            SSLMode sslMode = sslModeFor(connectorConfig.sslMode());
            LOGGER.info(
                    "Enable ssl "
                            + sslMode
                            + " mode for connector "
                            + connectorConfig.getLogicalName());

            final char[] keyPasswordArray = connection.connectionConfig().sslKeyStorePassword();
            final String keyFilename = connection.connectionConfig().sslKeyStore();
            final char[] trustPasswordArray = connection.connectionConfig().sslTrustStorePassword();
            final String trustFilename = connection.connectionConfig().sslTrustStore();
            KeyManager[] keyManagers = null;
            if (keyFilename != null) {
                try {
                    KeyStore ks = connection.loadKeyStore(keyFilename, keyPasswordArray);

                    KeyManagerFactory kmf = KeyManagerFactory.getInstance("NewSunX509");
                    kmf.init(ks, keyPasswordArray);

                    keyManagers = kmf.getKeyManagers();
                } catch (KeyStoreException
                         | NoSuchAlgorithmException
                         | UnrecoverableKeyException e) {
                    throw new DebeziumException("Could not load keystore", e);
                }
            }
            TrustManager[] trustManagers;
            try {
                KeyStore ks = null;
                if (trustFilename != null) {
                    ks = connection.loadKeyStore(trustFilename, trustPasswordArray);
                }

                if (ks == null && (sslMode == SSLMode.PREFERRED || sslMode == SSLMode.REQUIRED)) {
                    trustManagers =
                            new TrustManager[] {
                                    new X509TrustManager() {

                                        @Override
                                        public void checkClientTrusted(
                                                X509Certificate[] x509Certificates, String s)
                                                throws CertificateException {}

                                        @Override
                                        public void checkServerTrusted(
                                                X509Certificate[] x509Certificates, String s)
                                                throws CertificateException {}

                                        @Override
                                        public X509Certificate[] getAcceptedIssuers() {
                                            return new X509Certificate[0];
                                        }
                                    }
                            };
                } else {
                    TrustManagerFactory tmf =
                            TrustManagerFactory.getInstance(
                                    TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(ks);
                    trustManagers = tmf.getTrustManagers();
                }
            } catch (KeyStoreException | NoSuchAlgorithmException e) {
                throw new DebeziumException("Could not load truststore", e);
            }
            // DBZ-1208 Resembles the logic from the upstream BinaryLogClient, only that
            // the accepted TLS version is passed to the constructed factory
            final KeyManager[] finalKMS = keyManagers;
            return new DefaultSSLSocketFactory(acceptedTlsVersion) {

                @Override
                protected void initSSLContext(SSLContext sc) throws GeneralSecurityException {
                    sc.init(finalKMS, trustManagers, null);
                }
            };
        }

        return null;
    }

    private void logStreamingSourceState() {
        logStreamingSourceState(Level.ERROR);
    }

    protected void logEvent(MySqlOffsetContext offsetContext, Event event) {
        LOGGER.trace("Received event: {}", event);
    }

    private void logStreamingSourceState(Level severity) {
        final Object position =
                client == null
                        ? "N/A"
                        : client.getBinlogFilename() + "/" + client.getBinlogPosition();
        final String message =
                "Error during binlog processing. Last offset stored = {}, binlog reader near position = {}";
        switch (severity) {
            case WARN:
                LOGGER.warn(message, lastOffset, position);
                break;
            case DEBUG:
                LOGGER.debug(message, lastOffset, position);
                break;
            default:
                LOGGER.error(message, lastOffset, position);
        }
    }

    /**
     * Apply the include/exclude GTID source filters to the current {@link #source() GTID set} and
     * merge them onto the currently available GTID set from a MySQL server.
     *
     * <p>The merging behavior of this method might seem a bit strange at first. It's required in
     * order for Debezium to consume a MySQL binlog that has multi-source replication enabled, if a
     * failover has to occur. In such a case, the server that Debezium is failed over to might have
     * a different set of sources, but still include the sources required for Debezium to continue
     * to function. MySQL does not allow downstream replicas to connect if the GTID set does not
     * contain GTIDs for all channels that the server is replicating from, even if the server does
     * have the data needed by the client. To get around this, we can have Debezium merge its GTID
     * set with whatever is on the server, so that MySQL will allow it to connect. See <a
     * href="https://issues.jboss.org/browse/DBZ-143">DBZ-143</a> for details.
     *
     * <p>This method does not mutate any state in the context.
     *
     * @param availableServerGtidSet the GTID set currently available in the MySQL server
     * @param purgedServerGtid the GTID set already purged by the MySQL server
     * @return A GTID set meant for consuming from a MySQL binlog; may return null if the SourceInfo
     *     has no GTIDs and therefore none were filtered
     */
    public GtidSet filterGtidSet(
            MySqlOffsetContext offsetContext,
            GtidSet availableServerGtidSet,
            GtidSet purgedServerGtid) {
        String gtidStr = offsetContext.gtidSet();
        if (gtidStr == null) {
            return null;
        }
        LOGGER.info("Attempting to generate a filtered GTID set");
        LOGGER.info("GTID set from previous recorded offset: {}", gtidStr);
        GtidSet filteredGtidSet = new GtidSet(gtidStr);
        Predicate<String> gtidSourceFilter = connectorConfig.gtidSourceFilter();
        if (gtidSourceFilter != null) {
            filteredGtidSet = filteredGtidSet.retainAll(gtidSourceFilter);
            LOGGER.info(
                    "GTID set after applying GTID source includes/excludes to previous recorded offset: {}",
                    filteredGtidSet);
        }
        LOGGER.info("GTID set available on server: {}", availableServerGtidSet);

        GtidSet mergedGtidSet;

        if (connectorConfig.gtidNewChannelPosition() == GtidNewChannelPosition.EARLIEST) {
            final GtidSet knownGtidSet = filteredGtidSet;
            LOGGER.info("Using first available positions for new GTID channels");
            final GtidSet relevantAvailableServerGtidSet =
                    (gtidSourceFilter != null)
                            ? availableServerGtidSet.retainAll(gtidSourceFilter)
                            : availableServerGtidSet;
            LOGGER.info(
                    "Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);

            // Since the GTID recorded in the checkpoint represents the CDC-executed records, in
            // certain scenarios
            // (such as when the startup mode is earliest/timestamp/binlogfile), the recorded GTID
            // may not start from
            // the beginning. For example, A:300-500. However, during job recovery, we usually only
            // need to focus on
            // the last consumed point instead of consuming A:1-299. Therefore, some adjustments
            // need to be made to the
            // recorded offset in the checkpoint, and the available GTID for other MySQL instances
            // should be completed.
            mergedGtidSet =
                    GtidUtils.fixRestoredGtidSet(
                            GtidUtils.mergeGtidSetInto(
                                    relevantAvailableServerGtidSet.retainAll(
                                            uuid -> knownGtidSet.forServerWithId(uuid) != null),
                                    purgedServerGtid),
                            filteredGtidSet);
        } else {
            mergedGtidSet = availableServerGtidSet.with(filteredGtidSet);
        }

        LOGGER.info("Final merged GTID set to use when connecting to MySQL: {}", mergedGtidSet);
        return mergedGtidSet;
    }

    MySqlStreamingChangeEventSourceMetrics getMetrics() {
        return metrics;
    }

    void rewindBinaryLogClient(ChangeEventSourceContext context, BinlogPosition position) {
        try {
            if (context.isRunning()) {
                LOGGER.debug("Rewinding binlog to position {}", position);
                client.disconnect();
                client.setBinlogFilename(position.getFilename());
                client.setBinlogPosition(position.getPosition());
                client.connect();
            }
        } catch (IOException e) {
            LOGGER.error("Unexpected error when re-connecting to the MySQL binary log reader", e);
        }
    }

    BinlogPosition getCurrentBinlogPosition() {
        return new BinlogPosition(client.getBinlogFilename(), client.getBinlogPosition());
    }

    /**
     * Wraps the specified exception in a {@link DebeziumException}, ensuring that all useful state
     * is captured inside the new exception's message.
     *
     * @param error the exception; may not be null
     * @return the wrapped Kafka Connect exception
     */
    protected DebeziumException wrap(Throwable error) {
        assert error != null;
        String msg = error.getMessage();
        if (error instanceof ServerException) {
            ServerException e = (ServerException) error;
            msg = msg + " Error code: " + e.getErrorCode() + "; SQLSTATE: " + e.getSqlState() + ".";
        } else if (error instanceof SQLException) {
            SQLException e = (SQLException) error;
            msg =
                    e.getMessage()
                            + " Error code: "
                            + e.getErrorCode()
                            + "; SQLSTATE: "
                            + e.getSQLState()
                            + ".";
        }
        msg = ErrorMessageUtils.optimizeErrorMessage(msg);
        return new DebeziumException(msg, error);
    }

    /** LifecycleListener for Reader Thread. */
    protected final class ReaderThreadLifecycleListener implements LifecycleListener {
        private final MySqlOffsetContext offsetContext;

        ReaderThreadLifecycleListener(MySqlOffsetContext offsetContext) {
            this.offsetContext = offsetContext;
        }

        @Override
        public void onDisconnect(BinaryLogClient client) {
            if (LOGGER.isInfoEnabled()) {
                taskContext.temporaryLoggingContext(
                        connectorConfig,
                        "binlog",
                        () -> {
                            Map<String, ?> offset = lastOffset;
                            if (offset != null) {
                                LOGGER.info(
                                        "Stopped reading binlog after {} events, last recorded offset: {}",
                                        totalRecordCounter,
                                        offset);
                            } else {
                                LOGGER.info(
                                        "Stopped reading binlog after {} events, no new offset was recorded",
                                        totalRecordCounter);
                            }
                        });
            }
        }

        @Override
        public void onConnect(BinaryLogClient client) {
            // Set up the MDC logging context for this thread ...
            taskContext.configureLoggingContext("binlog");

            // The event row number will be used when processing the first event ...
            LOGGER.info(
                    "Connected to MySQL binlog at {}:{}, starting at {}",
                    connectorConfig.hostname(),
                    connectorConfig.port(),
                    offsetContext);
        }

        @Override
        public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
            LOGGER.debug("A communication failure event arrived", ex);
            logStreamingSourceState();
            try {
                // Stop BinaryLogClient background threads
                client.disconnect();
            } catch (final Exception e) {
                LOGGER.debug("Exception while closing client", e);
            }
            errorHandler.setProducerThrowable(wrap(ex));
        }

        @Override
        public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
            if (eventDeserializationFailureHandlingMode
                    == EventProcessingFailureHandlingMode.FAIL) {
                LOGGER.debug("A deserialization failure event arrived", ex);
                logStreamingSourceState();
                errorHandler.setProducerThrowable(wrap(ex));
            } else if (eventDeserializationFailureHandlingMode
                    == EventProcessingFailureHandlingMode.WARN) {
                LOGGER.warn("A deserialization failure event arrived", ex);
                logStreamingSourceState(Level.WARN);
            } else {
                LOGGER.debug("A deserialization failure event arrived", ex);
                logStreamingSourceState(Level.DEBUG);
            }
        }
    }

    @FunctionalInterface
    private interface TableIdProvider<E extends EventData> {
        TableId getTableId(E data);
    }

    @FunctionalInterface
    private interface RowsProvider<E extends EventData, U> {
        List<U> getRows(E data);
    }
}

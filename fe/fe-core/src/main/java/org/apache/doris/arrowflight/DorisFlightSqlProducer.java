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
// This file is copied from
// https://github.com/apache/arrow/blob/main/java/flight/flight-sql/src/test/java/org/apache/arrow/flight/sql/example/FlightSqlExample.java
// and modified by Doris

package org.apache.doris.arrowflight;

import org.apache.doris.arrowflight.FlightSqlQuerySchema.UndiscoverableSchemaException;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter;
import org.apache.doris.arrowflight.protocol.FlightProtocolAdapter.PreparedQuery;
import org.apache.doris.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.arrowflight.results.FlightSqlResultCacheEntry;
import org.apache.doris.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.common.IncrWindowNotReadyException;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.GetSessionOptionsRequest;
import org.apache.arrow.flight.GetSessionOptionsResult;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetXdbcTypeInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedCaseSensitivity;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implementation of Arrow Flight SQL service
 * <p>
 * All methods must catch all possible Exceptions, print and throw CallStatus,
 * otherwise error message will be discarded.
 */
public class DorisFlightSqlProducer implements FlightSqlProducer, AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(DorisFlightSqlProducer.class);
    /** What ListActions answers: the actions this producer implements, see {@link #listActions}. */
    public static final List<ActionType> SUPPORTED_ACTIONS = ImmutableList.of(
            FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT,
            FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT,
            FlightConstants.SET_SESSION_OPTIONS,
            FlightConstants.GET_SESSION_OPTIONS,
            FlightConstants.CLOSE_SESSION);
    private final Location location;
    private final BufferAllocator rootAllocator = new RootAllocator();
    private final SqlInfoBuilder sqlInfoBuilder;
    private final FlightSessionsManager flightSessionsManager;
    private final ExecutorService executorService = Executors.newFixedThreadPool(100);
    // What Prepare advertises for a statement whose result schema it cannot tell: a client takes a
    // prepared statement with an empty dataset schema for an update (arrow#38911).
    private static final Schema PLACEHOLDER_DATASET_SCHEMA = new Schema(
            Collections.singletonList(Field.nullable("ResultMeta", new ArrowType.Utf8())));

    public DorisFlightSqlProducer(final Location location, FlightSessionsManager flightSessionsManager) {
        this.location = location;
        this.flightSessionsManager = flightSessionsManager;
        sqlInfoBuilder = new SqlInfoBuilder();
        sqlInfoBuilder.withFlightSqlServerName("DorisFE").withFlightSqlServerVersion("1.0")
                .withFlightSqlServerArrowVersion("18.2.0").withFlightSqlServerReadOnly(false)
                .withSqlIdentifierQuoteChar("`").withSqlDdlCatalog(true).withSqlDdlSchema(false).withSqlDdlTable(false)
                .withSqlIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)
                .withSqlQuotedIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE);
    }

    private static ByteBuffer serializeMetadata(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);

            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize arrow flight sql schema", e);
        }
    }

    private void getStreamStatementResult(String handle, ServerStreamListener listener) {
        String[] handleParts = handle.split(":");
        String executedPeerIdentity = handleParts[0];
        String queryId = handleParts[1];
        // The DoGet may come under another bearer token than the GetFlightInfo did: a client that opens
        // a second connection for the endpoint may authenticate again on it. The ticket names the
        // session that ran the statement, which must still be open.
        ConnectContext connectContext = flightSessionsManager.getConnectContext(executedPeerIdentity);
        // The result is streamed under the session's command lock: the next statement of the
        // session resets the channel, which would close the VectorSchemaRoot while it is being sent.
        FlightProtocolAdapter.of(connectContext).runCommand(connectContext, () -> {
            try {
                final FlightSqlResultCacheEntry flightSqlResultCacheEntry = Objects.requireNonNull(
                        connectContext.getFlightSqlChannel().getResult(queryId));
                final VectorSchemaRoot vectorSchemaRoot = flightSqlResultCacheEntry.getVectorSchemaRoot();
                listener.start(vectorSchemaRoot);
                listener.putNext();
            } catch (Throwable e) {
                String errMsg = "get stream statement failed, " + e.getMessage() + ", " + Util.getRootCauseMessage(e)
                        + ", error code: " + connectContext.getState().getErrorCode() + ", error msg: "
                        + connectContext.getState().getErrorMessage();
                handleStreamException(e, errMsg, listener);
            } finally {
                listener.completed();
                // The result has been sent or sent failed, delete it.
                connectContext.getFlightSqlChannel().invalidate(queryId);
            }
        });
    }

    @Override
    public void getStreamPreparedStatement(final CommandPreparedStatementQuery command, final CallContext context,
            final ServerStreamListener listener) {
        getStreamStatementResult(command.getPreparedStatementHandle().toStringUtf8(), listener);
    }

    @Override
    public void getStreamStatement(final TicketStatementQuery ticketStatementQuery, final CallContext context,
            final ServerStreamListener listener) {
        getStreamStatementResult(ticketStatementQuery.getStatementHandle().toStringUtf8(), listener);
    }

    @Override
    public void closePreparedStatement(final ActionClosePreparedStatementRequest request, final CallContext context,
            final StreamListener<Result> listener) {
        executorService.submit(() -> {
            try {
                String preparedStatementId = preparedStatementId(context, request.getPreparedStatementHandle());
                ConnectContext connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
                FlightProtocolAdapter.of(connectContext).runCommand(connectContext,
                        () -> connectContext.removePreparedQuery(preparedStatementId));
            } catch (final Throwable e) {
                listener.onError(e);
                return;
            }
            listener.onCompleted();
        });
    }

    // What a request answers GetFlightInfo with, and the executor of its last statement and where that
    // statement left its result -- taken before the processor's close clears them from the session.
    private static final class ExecutedQuery {
        private final FlightInfo info;
        private final StmtExecutor executor;
        private final boolean resultFromLocal;

        private ExecutedQuery(FlightInfo info, StmtExecutor executor, boolean resultFromLocal) {
            this.info = info;
            this.executor = executor;
            this.resultFromLocal = resultFromLocal;
        }
    }

    private ExecutedQuery executeQueryStatement(String peerIdentity, ConnectContext connectContext, String query,
            final FlightDescriptor descriptor) {
        try {
            Preconditions.checkState(null != connectContext);
            Preconditions.checkState(!query.isEmpty());
            // Drops what the previous request left on the session: its deferred coordinator (Arrow
            // Flight keeps it alive across GetFlightInfo -> DoGet so the BE can fetch external-table
            // splits during DoGet, and by now that DoGet is done, #62259), a result no
            // getStreamStatement took away, and its endpoints.
            FlightProtocolAdapter.of(connectContext).beginRequest();
            try (FlightSqlConnectProcessor flightSQLConnectProcessor = new FlightSqlConnectProcessor(connectContext)) {
                flightSQLConnectProcessor.handleQuery(query);
                if (connectContext.getState().getStateType() == MysqlStateType.ERR) {
                    throw new RuntimeException("after executeQueryStatement handleQuery");
                }

                StmtExecutor executor = connectContext.getExecutor();
                if (connectContext.isReturnResultFromLocal()) {
                    // set/use etc. stmt returns an OK result by default.
                    if (connectContext.getFlightSqlChannel().resultNum() == 0) {
                        // a random query id and add empty results
                        String queryId = UUID.randomUUID().toString();
                        connectContext.getFlightSqlChannel().addOKResult(queryId, query);

                        final ByteString handle = ByteString.copyFromUtf8(peerIdentity + ":" + queryId);
                        TicketStatementQuery ticketStatement = TicketStatementQuery.newBuilder()
                                .setStatementHandle(handle).build();
                        return new ExecutedQuery(getFlightInfoForSchema(ticketStatement, descriptor,
                                connectContext.getFlightSqlChannel().getResult(queryId).getVectorSchemaRoot()
                                        .getSchema()), executor, true);
                    } else {
                        // A Flight Sql request can only contain one statement that returns result,
                        // otherwise expected thrown exception during execution.
                        Preconditions.checkState(connectContext.getFlightSqlChannel().resultNum() == 1);

                        // The DoGet may come under another bearer token than this GetFlightInfo (see
                        // getStreamStatementResult), so the ticket names this session by its peer identity;
                        // queryId is used to find query results.
                        final ByteString handle = ByteString.copyFromUtf8(
                                peerIdentity + ":" + DebugUtil.printId(connectContext.queryId()));
                        TicketStatementQuery ticketStatement = TicketStatementQuery.newBuilder()
                                .setStatementHandle(handle).build();
                        return new ExecutedQuery(getFlightInfoForSchema(ticketStatement, descriptor,
                                connectContext.getFlightSqlChannel()
                                        .getResult(DebugUtil.printId(connectContext.queryId()))
                                        .getVectorSchemaRoot().getSchema()), executor, true);
                    }
                } else {
                    // Now only query stmt will pull results from BE.
                    flightSQLConnectProcessor.fetchArrowFlightSchema(5000);
                    if (flightSQLConnectProcessor.getArrowSchema() == null) {
                        throw CallStatus.INTERNAL.withDescription("fetch arrow flight schema is null")
                                .toRuntimeException();
                    }

                    List<FlightEndpoint> endpoints = Lists.newArrayList();
                    for (FlightSqlEndpointsLocation endpointLoc : connectContext.getFlightSqlEndpointsLocations()) {
                        TUniqueId tid = endpointLoc.getFinstId();
                        // Ticket contains the IP and Brpc Port of the Doris BE node where the query result is located.
                        final ByteString handle = ByteString.copyFromUtf8(
                                DebugUtil.printId(tid) + "&" + endpointLoc.getResultInternalServiceAddr().hostname + "&"
                                        + endpointLoc.getResultInternalServiceAddr().port + "&"
                                        + Base64.getEncoder().encodeToString(query.getBytes(StandardCharsets.UTF_8)));
                        TicketStatementQuery ticketStatement = TicketStatementQuery.newBuilder()
                                .setStatementHandle(handle).build();
                        Ticket ticket = new Ticket(Any.pack(ticketStatement).toByteArray());
                        Location location;
                        if (endpointLoc.getResultPublicAccessAddr().isSetHostname()) {
                            // In a production environment, it is often inconvenient to expose Doris BE nodes
                            // to the external network.
                            // However, a reverse proxy (such as nginx) can be added to all Doris BE nodes,
                            // and the external client will be randomly routed to a Doris BE node when connecting
                            // to nginx.
                            // The query results of Arrow Flight SQL will be randomly saved on a Doris BE node.
                            // If it is different from the Doris BE node randomly routed by nginx,
                            // data forwarding needs to be done inside the Doris BE node.
                            if (endpointLoc.getResultPublicAccessAddr().isSetPort()) {
                                location = Location.forGrpcInsecure(endpointLoc.getResultPublicAccessAddr().hostname,
                                        endpointLoc.getResultPublicAccessAddr().port);
                            } else {
                                location = Location.forGrpcInsecure(endpointLoc.getResultPublicAccessAddr().hostname,
                                        endpointLoc.getResultFlightServerAddr().port);
                            }
                        } else {
                            location = Location.forGrpcInsecure(endpointLoc.getResultFlightServerAddr().hostname,
                                    endpointLoc.getResultFlightServerAddr().port);
                        }
                        // By default, the query results of all BE nodes will be aggregated to one BE node.
                        // ADBC Client will only receive one endpoint and pull data from the BE node
                        // corresponding to this endpoint.
                        // `set global enable_parallel_result_sink=true;` to allow each BE to return query results
                        // separately. ADBC Client will receive multiple endpoints and pull data from each endpoint.
                        endpoints.add(new FlightEndpoint(ticket, location));
                    }
                    // TODO Set in BE callback after query end, Client will not callback.
                    return new ExecutedQuery(new FlightInfo(flightSQLConnectProcessor.getArrowSchema(), descriptor,
                            endpoints, -1, -1), executor, false);
                }
            }
        } catch (Throwable e) {
            // GetFlightInfo failed (e.g. the BE Arrow schema fetch above timed out or returned an
            // error) after this query's coordinator may already have been deferred during planning.
            // No FlightInfo is returned, so no DoGet will ever pull this query's results; cancel the
            // query on the backends and finalize the deferred coordinator now (releasing its
            // external-table batch SplitSource, query queue slot and query registration) instead of
            // leaking it until the next query starts or the connection is torn down. The previous
            // query's deferred coordinator was already finalized at the top of this method, so this
            // only closes this failed query. See #62259.
            String errMsg = "get flight info statement failed, " + e.getMessage() + ", " + Util.getRootCauseMessage(e)
                    + ", error code: " + connectContext.getState().getErrorCode() + ", error msg: "
                    + connectContext.getState().getErrorMessage();
            connectContext.cancelFlightSqlDeferredExecutors(new Status(TStatusCode.CANCELLED, errMsg));
            LOG.error(errMsg, e);
            throw queryFailure(connectContext.getState(), errMsg, e);
        } finally {
            connectContext.setCommand(MysqlCommand.COM_SLEEP);
        }
    }

    static FlightRuntimeException queryFailure(QueryState state, String message, Throwable cause) {
        if (IncrWindowNotReadyException.isWindowError(state.getErrorCode())) {
            ErrorFlightMetadata metadata = new ErrorFlightMetadata();
            metadata.insert("doris-error-code", Integer.toString(state.getErrorCode().getCode()));
            metadata.insert("doris-error-name", state.getErrorCode().name());
            return CallStatus.UNAVAILABLE.withDescription(message).withCause(cause)
                    .withMetadata(metadata).toRuntimeException();
        }
        return CallStatus.INTERNAL.withDescription(message).withCause(cause).toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoStatement(final CommandStatementQuery request, final CallContext context,
            final FlightDescriptor descriptor) {
        try {
            ConnectContext connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
            return FlightProtocolAdapter.of(connectContext).callCommand(connectContext,
                    () -> executeQueryStatement(context.peerIdentity(), connectContext, request.getQuery(),
                            descriptor).info);
        } catch (FlightRuntimeException e) {
            // Already carries the status meant for the client - UNAVAILABLE from the session's
            // command lock or from an incremental window that is not ready (queryFailure, with its
            // doris-error-code metadata), UNAUTHENTICATED from a closed session, and whatever the
            // session layer refuses a session with. Wrapping it as INTERNAL would hide that, as it
            // did between #67820 and this fix; the other entry points below let it through too.
            throw e;
        } catch (Throwable e) {
            String errMsg = "get flight info statement failed, " + e.getMessage();
            LOG.error(errMsg, e);
            throw CallStatus.INTERNAL.withDescription(errMsg).withCause(e).toRuntimeException();
        }
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(final CommandPreparedStatementQuery command,
            final CallContext context, final FlightDescriptor descriptor) {
        String preparedStatementId = preparedStatementId(context, command.getPreparedStatementHandle());
        ConnectContext connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
        return FlightProtocolAdapter.of(connectContext).callCommand(connectContext,
                () -> executePreparedStatement(context.peerIdentity(), connectContext, preparedStatementId,
                        descriptor));
    }

    private FlightInfo executePreparedStatement(String peerIdentity, ConnectContext connectContext,
            String preparedStatementId, FlightDescriptor descriptor) {
        PreparedQuery prepared = preparedQuery(connectContext, preparedStatementId);
        if (prepared.getSchema() == null) {
            // Prepare could not tell this statement's result schema and advertised the placeholder, so
            // its executions have no schema to be checked against.
            return executeQueryStatement(peerIdentity, connectContext, prepared.getSql(), descriptor).info;
        }
        Schema schema = checkPreparedSchema(connectContext, preparedStatementId, prepared);
        ExecutedQuery executed = executeQueryStatement(peerIdentity, connectContext, prepared.getSql(), descriptor);
        // Another session's DDL can change the result after reanalysis but before execution
        // acquires table locks. Reject the actual schema before publishing a DoGet ticket.
        boolean matches = executed.resultFromLocal
                // A frontend-side result is a header of nullable strings, exactly as Prepare advertised it.
                ? schema.equals(executed.info.getSchema())
                // BE labels may carry the SQL text of an expression or a TVF prefix, and constant folding
                // can narrow nullability: compare the final planner's labels and the positional types.
                : FlightSqlQuerySchema.matchesExecutionSchema(schema, executed.info.getSchema(),
                        ((LogicalPlanAdapter) executed.executor.getParsedStmt()).getColLabels());
        if (!matches) {
            connectContext.removePreparedQuery(preparedStatementId);
            try {
                executed.executor.cancel(new Status(TStatusCode.CANCELLED, "prepared statement schema changed"));
            } catch (Exception e) {
                LOG.warn("Failed to cancel Flight query after prepared schema changed", e);
            } finally {
                // No DoGet will pull this result: drop what the request left behind as the next request
                // would -- its deferred coordinator, the result cached on the channel and its endpoints.
                FlightProtocolAdapter.of(connectContext).beginRequest();
            }
            throw CallStatus.NOT_FOUND.withDescription("Prepared statement schema changed; prepare again")
                    .toRuntimeException();
        }
        // A successful USE or SWITCH in this statement moves its namespace. Rebind only this handle;
        // the other handles still reject a namespace change made outside them.
        prepared.rebind(connectContext.getDefaultCatalog(), connectContext.getDatabase());
        return executed.info;
    }

    @Override
    public SchemaResult getSchemaStatement(final CommandStatementQuery command, final CallContext context,
            final FlightDescriptor descriptor) {
        ConnectContext connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
        return FlightProtocolAdapter.of(connectContext).callCommand(connectContext,
                () -> new SchemaResult(discoverSchema(connectContext, command.getQuery())));
    }

    @Override
    public SchemaResult getSchemaPreparedStatement(final CommandPreparedStatementQuery command,
            final CallContext context, final FlightDescriptor descriptor) {
        String preparedStatementId = preparedStatementId(context, command.getPreparedStatementHandle());
        ConnectContext connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
        return FlightProtocolAdapter.of(connectContext).callCommand(connectContext, () -> {
            PreparedQuery prepared = preparedQuery(connectContext, preparedStatementId);
            if (prepared.getSchema() == null) {
                throw CallStatus.UNIMPLEMENTED.withDescription(
                        "The result schema of this statement is only known once it runs").toRuntimeException();
            }
            return new SchemaResult(checkPreparedSchema(connectContext, preparedStatementId, prepared));
        });
    }

    // The id of a prepared statement this session created: a handle is "<peer identity>:<id>".
    private static String preparedStatementId(CallContext context, ByteString handleBytes) {
        String prefix = context.peerIdentity() + ":";
        String handle = handleBytes.toStringUtf8();
        if (!handle.startsWith(prefix) || handle.length() == prefix.length()) {
            throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid prepared statement handle").toRuntimeException();
        }
        return handle.substring(prefix.length());
    }

    private static PreparedQuery preparedQuery(ConnectContext connectContext, String preparedStatementId) {
        PreparedQuery prepared = connectContext.getPreparedQuery(preparedStatementId);
        if (prepared == null) {
            throw CallStatus.NOT_FOUND.withDescription("Prepared statement expired; prepare again")
                    .toRuntimeException();
        }
        // A checked handle must not run its unqualified SQL in another namespace than its schema was told in.
        if (prepared.getSchema() != null
                && !prepared.isPreparedIn(connectContext.getDefaultCatalog(), connectContext.getDatabase())) {
            connectContext.removePreparedQuery(preparedStatementId);
            throw CallStatus.NOT_FOUND
                    .withDescription("Prepared statement expired; prepare again in the current namespace")
                    .toRuntimeException();
        }
        return prepared;
    }

    // Execution parses the SQL again in the current session. Never silently replace the schema Prepare
    // advertised when a setting such as sql_mode or time_zone changes the result.
    private static Schema checkPreparedSchema(ConnectContext connectContext, String preparedStatementId,
            PreparedQuery prepared) {
        Schema schema = discoverSchema(connectContext, prepared.getSql());
        if (!schema.equals(prepared.getSchema())) {
            connectContext.removePreparedQuery(preparedStatementId);
            throw CallStatus.NOT_FOUND.withDescription("Prepared statement schema changed; prepare again")
                    .toRuntimeException();
        }
        return schema;
    }

    private static Schema discoverSchema(ConnectContext connectContext, String query) {
        try {
            return FlightSqlQuerySchema.analyze(connectContext, query);
        } catch (UndiscoverableSchemaException e) {
            throw CallStatus.UNIMPLEMENTED.withDescription(e.getMessage()).withCause(e).toRuntimeException();
        } catch (FlightRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw CallStatus.INVALID_ARGUMENT.withDescription("Cannot determine query schema: " + e.getMessage())
                    .withCause(e).toRuntimeException();
        }
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(rootAllocator);
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("listFlights unimplemented").toRuntimeException();
    }

    private ActionCreatePreparedStatementResult buildCreatePreparedStatementResult(ByteString handle,
            Schema parameterSchema, Schema metaData) {
        Preconditions.checkState(!Objects.isNull(metaData));
        final ByteString bytes = Objects.isNull(parameterSchema) ? ByteString.EMPTY
                : ByteString.copyFrom(serializeMetadata(parameterSchema));
        return ActionCreatePreparedStatementResult.newBuilder()
                .setDatasetSchema(ByteString.copyFrom(serializeMetadata(metaData))).setParameterSchema(bytes)
                .setPreparedStatementHandle(handle).build();
    }

    @Override
    public void createPreparedStatement(final ActionCreatePreparedStatementRequest request, final CallContext context,
            final StreamListener<Result> listener) {
        // TODO can only execute complete SQL, not support SQL parameters.
        // For Python: the Python code will try to create a prepared statement (this is to fit DBAPI, IIRC) and
        // if the server raises any error except for NotImplemented it will fail. (If it gets NotImplemented,
        // it will ignore and execute without a prepared statement.) see: https://github.com/apache/arrow/issues/38786
        executorService.submit(() -> {
            try {
                ConnectContext connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
                FlightProtocolAdapter.of(connectContext).runCommand(connectContext, () -> createPreparedStatement(
                        request, context.peerIdentity(), connectContext, listener));
            } catch (final Throwable e) {
                // The session could not be resolved or is busy; without this the failure would be
                // lost in the executor and the client would wait for a result that never comes.
                LOG.error("create prepared statement failed", e);
                listener.onError(e);
            }
        });
    }

    private void createPreparedStatement(final ActionCreatePreparedStatementRequest request, String peerIdentity,
            ConnectContext connectContext, final StreamListener<Result> listener) {
        String preparedStatementId = UUID.randomUUID().toString();
        try {
            final String query = request.getQuery();
            // ADBC ExecuteSchema reads this dataset schema directly without calling GetSchema. Analyze
            // before registering a handle so that a statement that cannot run leaves none behind. A
            // statement whose schema only running it can tell keeps the placeholder: the Arrow Flight
            // SQL JDBC driver prepares every statement and does not fall back when Prepare refuses one.
            Schema schema;
            try {
                schema = FlightSqlQuerySchema.analyze(connectContext, query);
            } catch (UndiscoverableSchemaException e) {
                schema = null;
            } catch (FlightRuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw CallStatus.INVALID_ARGUMENT.withDescription("Cannot determine query schema: " + e.getMessage())
                        .withCause(e).toRuntimeException();
            }
            final ByteString handle = ByteString.copyFromUtf8(peerIdentity + ":" + preparedStatementId);
            Result result = new Result(Any.pack(buildCreatePreparedStatementResult(handle,
                    new Schema(Collections.emptyList()), schema == null ? PLACEHOLDER_DATASET_SCHEMA : schema))
                    .toByteArray());
            connectContext.addPreparedQuery(preparedStatementId, new PreparedQuery(query, schema,
                    connectContext.getDefaultCatalog(), connectContext.getDatabase()));
            listener.onNext(result);
        } catch (final Throwable e) {
            connectContext.removePreparedQuery(preparedStatementId);
            LOG.warn("create prepared statement failed", e);
            listener.onError(e instanceof FlightRuntimeException ? e
                    : CallStatus.INTERNAL.withDescription("Create prepared statement failed: " + e.getMessage())
                            .withCause(e).toRuntimeException());
            return;
        }
        listener.onCompleted();
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
        throw CallStatus.UNIMPLEMENTED.withDescription("doExchange unimplemented").toRuntimeException();
    }

    @Override
    public Runnable acceptPutStatement(CommandStatementUpdate command, CallContext context, FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutStatement unimplemented").toRuntimeException();
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate command, CallContext context,
            FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return () -> {
            try {
                while (flightStream.next()) {
                    final VectorSchemaRoot root = flightStream.getRoot();
                    final int rowCount = root.getRowCount();
                    // TODO support update
                    Preconditions.checkState(rowCount == 0);

                    final int recordCount = -1;
                    final DoPutUpdateResult build = DoPutUpdateResult.newBuilder().setRecordCount(recordCount).build();
                    try (final ArrowBuf buffer = rootAllocator.buffer(build.getSerializedSize())) {
                        buffer.writeBytes(build.toByteArray());
                        ackStream.onNext(PutResult.metadata(buffer));
                    }
                }
                ackStream.onCompleted();
            } catch (Throwable e) {
                String errMsg = "acceptPutPreparedStatementUpdate failed, " + e.getMessage() + ", "
                        + Util.getRootCauseMessage(e);
                LOG.error(errMsg, e);
                throw CallStatus.INTERNAL.withDescription(errMsg).withCause(e).toRuntimeException();
            }
        };
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery command, CallContext context,
            FlightStream flightStream, StreamListener<PutResult> ackStream) {
        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutPreparedStatementQuery unimplemented")
                .toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(final CommandGetSqlInfo request, final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(final CommandGetSqlInfo command, final CallContext context,
            final ServerStreamListener listener) {
        this.sqlInfoBuilder.send(command.getInfoList(), listener);
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(CommandGetXdbcTypeInfo request, CallContext context,
            FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_TYPE_INFO_SCHEMA);
    }

    @Override
    public void getStreamTypeInfo(CommandGetXdbcTypeInfo request, CallContext context, ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamTypeInfo unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(final CommandGetCatalogs request, final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(final CallContext context, final ServerStreamListener listener) {
        streamMetadata(context, listener, connectContext -> {
            FlightSqlSchemaHelper flightSqlSchemaHelper = new FlightSqlSchemaHelper(connectContext);
            final Schema schema = Schemas.GET_CATALOGS_SCHEMA;

            try (final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                listener.start(vectorSchemaRoot);
                vectorSchemaRoot.allocateNew();
                flightSqlSchemaHelper.getCatalogs(vectorSchemaRoot);
                listener.putNext();
                listener.completed();
            }
        });
    }

    @Override
    public FlightInfo getFlightInfoSchemas(final CommandGetDbSchemas request, final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(final CommandGetDbSchemas command, final CallContext context,
            final ServerStreamListener listener) {
        streamMetadata(context, listener, connectContext -> {
            FlightSqlSchemaHelper flightSqlSchemaHelper = new FlightSqlSchemaHelper(connectContext);
            flightSqlSchemaHelper.setParameterForGetDbSchemas(command);
            final Schema schema = Schemas.GET_SCHEMAS_SCHEMA;

            try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                listener.start(vectorSchemaRoot);
                vectorSchemaRoot.allocateNew();
                flightSqlSchemaHelper.getSchemas(vectorSchemaRoot);
                listener.putNext();
                listener.completed();
            }
        });
    }

    @Override
    public FlightInfo getFlightInfoTables(final CommandGetTables request, final CallContext context,
            final FlightDescriptor descriptor) {
        Schema schemaToUse = Schemas.GET_TABLES_SCHEMA;
        if (!request.getIncludeSchema()) {
            schemaToUse = Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return getFlightInfoForSchema(request, descriptor, schemaToUse);
    }

    @Override
    public void getStreamTables(final CommandGetTables command, final CallContext context,
            final ServerStreamListener listener) {
        streamMetadata(context, listener, connectContext -> {
            FlightSqlSchemaHelper flightSqlSchemaHelper = new FlightSqlSchemaHelper(connectContext);
            flightSqlSchemaHelper.setParameterForGetTables(command);
            final Schema schema = command.getIncludeSchema() ? Schemas.GET_TABLES_SCHEMA
                    : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;

            try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                listener.start(vectorSchemaRoot);
                vectorSchemaRoot.allocateNew();
                flightSqlSchemaHelper.getTables(vectorSchemaRoot);
                listener.putNext();
                listener.completed();
            }
        });
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(final CommandGetTableTypes request, final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    @Override
    public void getStreamTableTypes(final CallContext context, final ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamTableTypes unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(final CommandGetPrimaryKeys request, final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_PRIMARY_KEYS_SCHEMA);
    }

    @Override
    public void getStreamPrimaryKeys(final CommandGetPrimaryKeys command, final CallContext context,
            final ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamPrimaryKeys unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(final CommandGetExportedKeys request, final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_EXPORTED_KEYS_SCHEMA);
    }

    @Override
    public void getStreamExportedKeys(final CommandGetExportedKeys command, final CallContext context,
            final ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamExportedKeys unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(final CommandGetImportedKeys request, final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_IMPORTED_KEYS_SCHEMA);
    }

    @Override
    public void getStreamImportedKeys(final CommandGetImportedKeys command, final CallContext context,
            final ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamImportedKeys unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(CommandGetCrossReference request, CallContext context,
            FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CROSS_REFERENCE_SCHEMA);
    }

    @Override
    public void getStreamCrossReference(CommandGetCrossReference command, CallContext context,
            ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamCrossReference unimplemented").toRuntimeException();
    }

    /**
     * The actions this producer implements, and only those. Arrow's default lists every Flight SQL
     * action, the transactions and savepoints among them, which this one answers UNIMPLEMENTED, and
     * leaves out the session actions of Flight itself, which it does answer.
     */
    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        for (ActionType action : SUPPORTED_ACTIONS) {
            listener.onNext(action);
        }
        listener.onCompleted();
    }

    /**
     * Sets session options: the current catalog, the current database and session variables, see
     * {@link FlightSessionOptions}. The ADBC Flight SQL driver sends these for the connection's
     * {@code adbc.connection.catalog} / {@code adbc.connection.db_schema} and for its
     * {@code adbc.flight.sql.session.option.*} options; the Flight SQL JDBC driver for its
     * {@code catalog} property. Each option is set on its own and answered on its own: the result
     * names the ones that could not be set and why, and the action itself only fails when the
     * session cannot be reached.
     */
    @Override
    public void setSessionOptions(final SetSessionOptionsRequest request, final CallContext context,
            final StreamListener<SetSessionOptionsResult> listener) {
        try {
            ConnectContext connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
            Map<String, SetSessionOptionsResult.Error> errors = FlightProtocolAdapter.of(connectContext)
                    .callCommand(connectContext,
                            () -> FlightSessionOptions.set(connectContext, request.getSessionOptions()));
            listener.onNext(new SetSessionOptionsResult(errors));
            listener.onCompleted();
        } catch (FlightRuntimeException e) {
            // Same as in getFlightInfoStatement: keep the status the session's command lock chose.
            LOG.warn("set session options failed", e);
            listener.onError(e);
        } catch (Throwable e) {
            String errMsg = "set session options failed, " + e.getMessage();
            LOG.warn(errMsg, e);
            listener.onError(CallStatus.INTERNAL.withDescription(errMsg).withCause(e).toRuntimeException());
        }
    }

    /** The session's options, see {@link FlightSessionOptions#get}. */
    @Override
    public void getSessionOptions(final GetSessionOptionsRequest request, final CallContext context,
            final StreamListener<GetSessionOptionsResult> listener) {
        try {
            ConnectContext connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
            Map<String, SessionOptionValue> options = FlightProtocolAdapter.of(connectContext)
                    .callCommand(connectContext, () -> FlightSessionOptions.get(connectContext));
            listener.onNext(new GetSessionOptionsResult(options));
            listener.onCompleted();
        } catch (FlightRuntimeException e) {
            LOG.warn("get session options failed", e);
            listener.onError(e);
        } catch (Throwable e) {
            String errMsg = "get session options failed, " + e.getMessage();
            LOG.warn(errMsg, e);
            listener.onError(CallStatus.INTERNAL.withDescription(errMsg).withCause(e).toRuntimeException());
        }
    }

    /**
     * Closes the session the way a MySQL client's COM_QUIT ends a connection: its ConnectContext
     * leaves the pool (releasing the cached results, the deferred executors, the transaction and
     * the temporary tables), a statement still running is cancelled, and every later call made
     * with the session's bearer token is refused as UNAUTHENTICATED, since the token is nothing but
     * the name the session had in the pool. The ADBC Flight SQL driver calls this from
     * Connection.Close() and the Flight SQL JDBC driver from Connection.close().
     */
    @Override
    public void closeSession(CloseSessionRequest request, final CallContext context,
            final StreamListener<CloseSessionResult> listener) {
        try {
            flightSessionsManager.closeConnectContext(context.peerIdentity());
        } catch (final Throwable e) {
            LOG.warn("closeSession failed", e);
            listener.onError(
                    CallStatus.INTERNAL.withDescription("closeSession failed").withCause(e).toRuntimeException());
            return;
        }
        listener.onNext(new CloseSessionResult(CloseSessionResult.Status.CLOSED));
        listener.onCompleted();
    }

    @FunctionalInterface
    private interface MetadataStream {
        void send(ConnectContext connectContext) throws Exception;
    }

    // Answers a metadata request (catalogs, schemas, tables) from the session's context, as one
    // command of that session.
    private void streamMetadata(final CallContext context, final ServerStreamListener listener,
            MetadataStream stream) {
        try {
            ConnectContext connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
            FlightProtocolAdapter.of(connectContext).runCommand(connectContext, () -> stream.send(connectContext));
        } catch (FlightRuntimeException e) {
            // Same as in getFlightInfoStatement: keep the status the session's command lock chose.
            LOG.error("stream metadata failed", e);
            listener.error(e);
            throw e;
        } catch (final Throwable e) {
            handleStreamException(e, "", listener);
        }
    }

    private <T extends Message> FlightInfo getFlightInfoForSchema(final T request, final FlightDescriptor descriptor,
            final Schema schema) {
        final Ticket ticket = new Ticket(Any.pack(request).toByteArray());
        final List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, location));

        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    private static void handleStreamException(Throwable e, String errMsg, ServerStreamListener listener) {
        LOG.error(errMsg, e);
        listener.error(CallStatus.INTERNAL.withDescription(errMsg).withCause(e).toRuntimeException());
        throw CallStatus.INTERNAL.withDescription(errMsg).withCause(e).toRuntimeException();
    }
}

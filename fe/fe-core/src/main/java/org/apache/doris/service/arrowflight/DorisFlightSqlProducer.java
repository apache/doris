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

package org.apache.doris.service.arrowflight;

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.service.arrowflight.results.FlightSqlResultCacheEntry;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;

import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
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
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedCaseSensitivity;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class DorisFlightSqlProducer implements FlightSqlProducer, AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(DorisFlightSqlProducer.class);
    private final Location location;
    private final BufferAllocator rootAllocator = new RootAllocator();
    private final SqlInfoBuilder sqlInfoBuilder;
    private final FlightSessionsManager flightSessionsManager;

    public DorisFlightSqlProducer(final Location location, FlightSessionsManager flightSessionsManager) {
        this.location = location;
        this.flightSessionsManager = flightSessionsManager;
        sqlInfoBuilder = new SqlInfoBuilder();
        sqlInfoBuilder.withFlightSqlServerName("DorisFE")
                .withFlightSqlServerVersion("1.0")
                .withFlightSqlServerArrowVersion("13.0")
                .withFlightSqlServerReadOnly(false)
                .withSqlIdentifierQuoteChar("`")
                .withSqlDdlCatalog(true)
                .withSqlDdlSchema(false)
                .withSqlDdlTable(false)
                .withSqlIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)
                .withSqlQuotedIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE);
    }

    @Override
    public void getStreamPreparedStatement(final CommandPreparedStatementQuery command, final CallContext context,
            final ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamPreparedStatement unimplemented").toRuntimeException();
    }

    @Override
    public void closePreparedStatement(final ActionClosePreparedStatementRequest request, final CallContext context,
            final StreamListener<Result> listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("closePreparedStatement unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoStatement(final CommandStatementQuery request, final CallContext context,
            final FlightDescriptor descriptor) {
        ConnectContext connectContext = null;
        try {
            connectContext = flightSessionsManager.getConnectContext(context.peerIdentity());
            // After the previous query was executed, there was no getStreamStatement to take away the result.
            connectContext.getFlightSqlChannel().reset();
            final String query = request.getQuery();
            final FlightSqlConnectProcessor flightSQLConnectProcessor = new FlightSqlConnectProcessor(connectContext);

            flightSQLConnectProcessor.handleQuery(query);
            if (connectContext.getState().getStateType() == MysqlStateType.ERR) {
                throw new RuntimeException("after handleQuery");
            }

            if (connectContext.isReturnResultFromLocal()) {
                // set/use etc. stmt returns an OK result by default.
                if (connectContext.getFlightSqlChannel().resultNum() == 0) {
                    // a random query id and add empty results
                    String queryId = UUID.randomUUID().toString();
                    connectContext.getFlightSqlChannel().addEmptyResult(queryId, query);

                    final ByteString handle = ByteString.copyFromUtf8(context.peerIdentity() + ":" + queryId);
                    TicketStatementQuery ticketStatement = TicketStatementQuery.newBuilder().setStatementHandle(handle)
                            .build();
                    return getFlightInfoForSchema(ticketStatement, descriptor,
                            connectContext.getFlightSqlChannel().getResult(queryId).getVectorSchemaRoot().getSchema());
                }

                // A Flight Sql request can only contain one statement that returns result,
                // otherwise expected thrown exception during execution.
                Preconditions.checkState(connectContext.getFlightSqlChannel().resultNum() == 1);

                // The tokens used for authentication between getStreamStatement and getFlightInfoStatement
                // are different. So put the peerIdentity into the ticket and then getStreamStatement is used to find
                // the correct ConnectContext.
                // queryId is used to find query results.
                final ByteString handle = ByteString.copyFromUtf8(
                        context.peerIdentity() + ":" + DebugUtil.printId(connectContext.queryId()));
                TicketStatementQuery ticketStatement = TicketStatementQuery.newBuilder().setStatementHandle(handle)
                        .build();
                return getFlightInfoForSchema(ticketStatement, descriptor,
                        connectContext.getFlightSqlChannel().getResult(DebugUtil.printId(connectContext.queryId()))
                                .getVectorSchemaRoot().getSchema());
            } else {
                // Now only query stmt will pull results from BE.
                final ByteString handle = ByteString.copyFromUtf8(
                        DebugUtil.printId(connectContext.getFinstId()) + ":" + query);
                Schema schema = flightSQLConnectProcessor.fetchArrowFlightSchema(5000);
                if (schema == null) {
                    throw CallStatus.INTERNAL.withDescription("fetch arrow flight schema is null").toRuntimeException();
                }
                TicketStatementQuery ticketStatement = TicketStatementQuery.newBuilder().setStatementHandle(handle)
                        .build();
                Ticket ticket = new Ticket(Any.pack(ticketStatement).toByteArray());
                // TODO Support multiple endpoints.
                Location location = Location.forGrpcInsecure(connectContext.getResultFlightServerAddr().hostname,
                        connectContext.getResultFlightServerAddr().port);
                List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, location));
                // TODO Set in BE callback after query end, Client will not callback.
                connectContext.setCommand(MysqlCommand.COM_SLEEP);
                return new FlightInfo(schema, descriptor, endpoints, -1, -1);
            }
        } catch (Exception e) {
            if (null != connectContext) {
                connectContext.setCommand(MysqlCommand.COM_SLEEP);
                String errMsg = "get flight info statement failed, " + e.getMessage() + ", " + Util.getRootCauseMessage(
                        e) + ", error code: " + connectContext.getState().getErrorCode() + ", error msg: "
                        + connectContext.getState().getErrorMessage();
                LOG.warn(errMsg, e);
                throw CallStatus.INTERNAL.withDescription(errMsg).withCause(e).toRuntimeException();
            }
            LOG.warn("get flight info statement failed, " + e.getMessage(), e);
            throw CallStatus.INTERNAL.withDescription(Util.getRootCauseMessage(e)).withCause(e).toRuntimeException();
        }
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(final CommandPreparedStatementQuery command,
            final CallContext context, final FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getFlightInfoPreparedStatement unimplemented")
                .toRuntimeException();
    }

    @Override
    public SchemaResult getSchemaStatement(final CommandStatementQuery command, final CallContext context,
            final FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getSchemaStatement unimplemented").toRuntimeException();
    }

    @Override
    public void getStreamStatement(final TicketStatementQuery ticketStatementQuery, final CallContext context,
            final ServerStreamListener listener) {
        ConnectContext connectContext = null;
        final String handle = ticketStatementQuery.getStatementHandle().toStringUtf8();
        String[] handleParts = handle.split(":");
        String executedPeerIdentity = handleParts[0];
        String queryId = handleParts[1];
        try {
            // The tokens used for authentication between getStreamStatement and getFlightInfoStatement are different.
            connectContext = flightSessionsManager.getConnectContext(executedPeerIdentity);
            final FlightSqlResultCacheEntry flightSqlResultCacheEntry = Objects.requireNonNull(
                    connectContext.getFlightSqlChannel().getResult(queryId));
            final VectorSchemaRoot vectorSchemaRoot = flightSqlResultCacheEntry.getVectorSchemaRoot();
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (Exception e) {
            listener.error(e);
            if (null != connectContext) {
                String errMsg = "get stream statement failed, " + e.getMessage() + ", " + Util.getRootCauseMessage(e)
                        + ", error code: " + connectContext.getState().getErrorCode() + ", error msg: "
                        + connectContext.getState().getErrorMessage();
                LOG.warn(errMsg, e);
                throw CallStatus.INTERNAL.withDescription(errMsg).withCause(e).toRuntimeException();
            }
            LOG.warn("get stream statement failed, " + e.getMessage(), e);
            throw CallStatus.INTERNAL.withDescription(Util.getRootCauseMessage(e)).withCause(e).toRuntimeException();
        } finally {
            listener.completed();
            if (null != connectContext) {
                // The result has been sent, delete it.
                connectContext.getFlightSqlChannel().invalidate(queryId);
            }
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

    @Override
    public void createPreparedStatement(final ActionCreatePreparedStatementRequest request, final CallContext context,
            final StreamListener<Result> listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("createPreparedStatement unimplemented").toRuntimeException();
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
        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutPreparedStatementUpdate unimplemented")
                .toRuntimeException();
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
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamCatalogs unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoSchemas(final CommandGetDbSchemas request, final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(final CommandGetDbSchemas command, final CallContext context,
            final ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamSchemas unimplemented").toRuntimeException();
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
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamTables unimplemented").toRuntimeException();
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

    private <T extends Message> FlightInfo getFlightInfoForSchema(final T request, final FlightDescriptor descriptor,
            final Schema schema) {
        final Ticket ticket = new Ticket(Any.pack(request).toByteArray());
        // TODO Support multiple endpoints.
        final List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, location));

        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }
}

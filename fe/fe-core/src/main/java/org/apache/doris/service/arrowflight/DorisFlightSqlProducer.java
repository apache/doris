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

import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;

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
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;

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
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        return FlightSqlProducer.super.getFlightInfo(context, descriptor);
    }

    @Override
    public void getStreamPreparedStatement(final CommandPreparedStatementQuery command, final CallContext context,
            final ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamPreparedStatement unimplemented").toRuntimeException();
    }

    @Override
    public void closePreparedStatement(final ActionClosePreparedStatementRequest request, final CallContext context,
            final StreamListener<Result> listener) {
        listener.onCompleted();
//        throw CallStatus.UNIMPLEMENTED.withDescription("closePreparedStatement unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoStatement(final CommandStatementQuery request, final CallContext context,
            final FlightDescriptor descriptor) {
        ConnectContext connectContext = null;
        try {
            connectContext = flightSessionsManager.getConnectContext("wuwenchi");
            // Only for ConnectContext check timeout.
            connectContext.setCommand(MysqlCommand.COM_QUERY);
            final String query = request.getQuery();
            final FlightStatementExecutor flightStatementExecutor = new FlightStatementExecutor(query, connectContext);

            flightStatementExecutor.executeQuery();

            TicketStatementQuery ticketStatement = TicketStatementQuery.newBuilder()
                    .setStatementHandle(ByteString.copyFromUtf8(
                            DebugUtil.printId(flightStatementExecutor.getFinstId()) + ":" + query)).build();
            final Ticket ticket = new Ticket(Any.pack(ticketStatement).toByteArray());
            // TODO Support multiple endpoints.
            Location location = Location.forGrpcInsecure(flightStatementExecutor.getResultFlightServerAddr().hostname,
                    flightStatementExecutor.getResultFlightServerAddr().port);
            List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, location));

            Schema schema;
            schema = flightStatementExecutor.fetchArrowFlightSchema(5000);
            if (schema == null) {
                throw CallStatus.INTERNAL.withDescription("fetch arrow flight schema is null").toRuntimeException();
            }
            // TODO Set in BE callback after query end, Client client will not callback by default.
            connectContext.setCommand(MysqlCommand.COM_SLEEP);
            return new FlightInfo(schema, descriptor, endpoints, -1, -1);
        } catch (Exception e) {
            if (null != connectContext) {
                connectContext.setCommand(MysqlCommand.COM_SLEEP);
            }
            LOG.warn("get flight info statement failed, " + e.getMessage(), e);
            throw CallStatus.INTERNAL.withDescription(Util.getRootCauseMessage(e)).withCause(e).toRuntimeException();
        }
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(final CommandPreparedStatementQuery command,
            final CallContext context,
            final FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getFlightInfoPreparedStatement unimplemented")
                .toRuntimeException();
    }

    @Override
    public SchemaResult getSchemaStatement(final CommandStatementQuery command, final CallContext context,
            final FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getSchemaStatement unimplemented").toRuntimeException();
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
//        throw CallStatus.UNIMPLEMENTED.withDescription("createPreparedStatement xxxxxx unimplemented").toRuntimeException();

        // Running on another thread
            try {
                final ByteString preparedStatementHandle = copyFrom(randomUUID().toString().getBytes(UTF_8));
//                // Ownership of the connection will be passed to the context. Do NOT close!
//                final Connection connection = dataSource.getConnection();
//                final PreparedStatement preparedStatement = connection.prepareStatement(request.getQuery(),
//                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
//                final StatementContext<PreparedStatement> preparedStatementContext =
//                    new StatementContext<>(preparedStatement, request.getQuery());
//
////                preparedStatementLoadingCache.put(preparedStatementHandle, preparedStatementContext);
//
//                // TODO gen scheam
////                String query = request.getQuery();
////                Parser<ActionCreatePreparedStatementRequest> parserForType = request.getParserForType();
////                parserForType.parseFrom()
//                final Schema parameterSchema =
//                    jdbcToArrowSchema(preparedStatement.getParameterMetaData(), DEFAULT_CALENDAR);
//                ArrayList<Field> fields = new ArrayList<>();
//                org.apache.arrow.vector.types.pojo.Field id = org.apache.arrow.vector.types.pojo.Field.nullable("id",
//                    Types.MinorType.INT.getType());
//                org.apache.arrow.vector.types.pojo.Field val = org.apache.arrow.vector.types.pojo.Field.nullable("val",
//                    Types.MinorType.INT.getType());
//
//                fields.add(id);
//                fields.add(val);
//                Schema parameterSchema = new Schema(fields);

                final List<Field> parameterFields = new ArrayList<>(2);

                ArrowType.Int anInt = new ArrowType.Int(32, true);
                final FieldType fieldType = new FieldType(true, anInt, /*dictionary=*/null);
                parameterFields.add(new Field("id", fieldType, null));

                ArrowType.Int anInt2 = new ArrowType.Int(32, true);
                final FieldType fieldType2 = new FieldType(true, anInt2, /*dictionary=*/null);
                parameterFields.add(new Field("val", fieldType2, null));

//
//                final ResultSetMetaData metaData = preparedStatement.getMetaData();
                final ByteString bytes =
                    ByteString.copyFrom(
                        serializeSchemaData(new Schema(parameterFields)));
                final FlightSql.ActionCreatePreparedStatementResult result = FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                    .setDatasetSchema(bytes)
                    .setParameterSchema(copyFrom(serializeSchemaData(new Schema(parameterFields))))
                    .setParameterSchema(copyFrom(serializeSchemaData(new Schema(parameterFields))))
                    .setPreparedStatementHandle(preparedStatementHandle)
                    .build();
                listener.onNext(new Result(pack(result).toByteArray()));
            } catch (final Throwable t) {
                listener.onError(CallStatus.INTERNAL.withDescription("Unknown error: " + t).toRuntimeException());
                return;
            }
            listener.onCompleted();
    }

    private static ByteBuffer serializeSchemaData(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize schema", e);
        }
    }


    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
        throw CallStatus.UNIMPLEMENTED.withDescription("doExchange unimplemented").toRuntimeException();
    }

    @Override
    public Runnable acceptPutStatement(CommandStatementUpdate command,
            CallContext context, FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutStatement unimplemented").toRuntimeException();
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate command, CallContext context,
            FlightStream flightStream, StreamListener<PutResult> ackStream) {
//        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutPreparedStatementUpdate unimplemented")
//                .toRuntimeException();

//    final StatementContext<PreparedStatement> statement =
//        preparedStatementLoadingCache.getIfPresent(command.getPreparedStatementHandle());

    return () -> {
//      if (statement == null) {
//        ackStream.onError(CallStatus.NOT_FOUND
//            .withDescription("Prepared statement does not exist")
//            .toRuntimeException());
//        return;
//      }
        //        final PreparedStatement preparedStatement = statement.getStatement();

        while (flightStream.next()) {
          final VectorSchemaRoot root = flightStream.getRoot();

          final int rowCount = root.getRowCount();
          final int recordCount = 10;

            System.out.println(rowCount);
          if (rowCount == 0) {
//            preparedStatement.execute();
//            recordCount = preparedStatement.getUpdateCount();
          } else {
//            final JdbcParameterBinder binder = JdbcParameterBinder.builder(preparedStatement, root).bindAll().build();
//            while (binder.next()) {
//              preparedStatement.addBatch();
//            }
//            int[] recordCounts = preparedStatement.executeBatch();
//            recordCount = Arrays.stream(recordCounts).sum();
              System.out.println(root);
          }

          final FlightSql.DoPutUpdateResult build =
              FlightSql.DoPutUpdateResult.newBuilder().setRecordCount(recordCount).build();

          try (final ArrowBuf buffer = rootAllocator.buffer(build.getSerializedSize())) {
            buffer.writeBytes(build.toByteArray());
            ackStream.onNext(PutResult.metadata(buffer));
          }
        }
        ackStream.onCompleted();
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
    public void getStreamTypeInfo(CommandGetXdbcTypeInfo request, CallContext context,
            ServerStreamListener listener) {
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

    @Override
    public void getStreamStatement(final TicketStatementQuery ticketStatementQuery, final CallContext context,
            final ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamStatement unimplemented").toRuntimeException();
    }

    private <T extends Message> FlightInfo getFlightInfoForSchema(final T request, final FlightDescriptor descriptor,
            final Schema schema) {
        final Ticket ticket = new Ticket(Any.pack(request).toByteArray());
        // TODO Support multiple endpoints.
        final List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, location));

        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }
}

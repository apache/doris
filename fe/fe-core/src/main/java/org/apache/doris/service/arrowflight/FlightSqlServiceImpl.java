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

package org.apache.doris.service.arrowflight;

import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
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
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer;
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
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

public class FlightSqlServiceImpl implements FlightSqlProducer, AutoCloseable {
    private static final String DATABASE_URI =
                "jdbc:mysql://localhost:9083/tpch?useSSL=false&autoReconnect=true";
    private static final Calendar DEFAULT_CALENDAR = JdbcToArrowUtils.getUtcCalendar();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private final Location location;
    private final PoolingDataSource<PoolableConnection> dataSource;
    private final BufferAllocator rootAllocator = new RootAllocator();
    private final Cache<ByteString, FlightStatementContext<PreparedStatement>> preparedStatementLoadingCache;
    private final Cache<ByteString, FlightStatementContext<Statement>> statementLoadingCache;
    private final SqlInfoBuilder sqlInfoBuilder;

    public FlightSqlServiceImpl(final Location location) {
        final ConnectionFactory connectionFactory =
                new DriverManagerConnectionFactory(DATABASE_URI, "root", "");
        final PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);
        final ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);

        poolableConnectionFactory.setPool(connectionPool);
        dataSource = new PoolingDataSource<>(connectionPool);

        preparedStatementLoadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .removalListener(new StatementRemovalListener<PreparedStatement>())
                        .build();

        statementLoadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .removalListener(new StatementRemovalListener<>())
                        .build();

        this.location = location;

        sqlInfoBuilder = new SqlInfoBuilder();
        try (final Connection connection = dataSource.getConnection()) {
            final DatabaseMetaData metaData = connection.getMetaData();

            sqlInfoBuilder.withFlightSqlServerName(metaData.getDatabaseProductName())
                    .withFlightSqlServerVersion(metaData.getDatabaseProductVersion())
                    .withFlightSqlServerArrowVersion(metaData.getDriverVersion())
                    .withFlightSqlServerReadOnly(metaData.isReadOnly())
                    .withSqlIdentifierQuoteChar(metaData.getIdentifierQuoteString())
                    .withSqlDdlCatalog(metaData.supportsCatalogsInDataManipulation())
                    .withSqlDdlSchema(metaData.supportsSchemasInDataManipulation())
                    .withSqlDdlTable(metaData.allTablesAreSelectable())
                    .withSqlIdentifierCase(metaData.storesMixedCaseIdentifiers()
                            ? SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE
                                : metaData.storesUpperCaseIdentifiers()
                                    ? SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE
                                        : metaData.storesLowerCaseIdentifiers()
                                            ? SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE
                                                : SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
                    .withSqlQuotedIdentifierCase(metaData.storesMixedCaseQuotedIdentifiers()
                            ? SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE
                                : metaData.storesUpperCaseQuotedIdentifiers()
                                    ? SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE
                                        : metaData.storesLowerCaseQuotedIdentifiers()
                                            ? SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE
                                                : SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private static ArrowType getArrowTypeFromJdbcType(final int jdbcDataType, final int precision, final int scale) {
        final ArrowType type =
                JdbcToArrowUtils.getArrowTypeFromJdbcType(new JdbcFieldInfo(jdbcDataType, precision, scale),
                                                        DEFAULT_CALENDAR);
        return Objects.isNull(type) ? ArrowType.Utf8.INSTANCE : type;
    }

    private static void saveToVector(final Byte data, final UInt1Vector vector, final int index) {
        vectorConsumer(
                data,
                vector,
                fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void saveToVector(final Byte data, final BitVector vector, final int index) {
        vectorConsumer(
                data,
                vector,
                fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void saveToVector(final String data, final VarCharVector vector, final int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, new Text(theData)));
    }

    private static void saveToVector(final Integer data, final IntVector vector, final int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void saveToVector(final byte[] data, final VarBinaryVector vector, final int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void preconditionCheckSaveToVector(final FieldVector vector, final int index) {
        Objects.requireNonNull(vector, "vector cannot be null.");
        Preconditions.checkState(index >= 0, "Index must be a positive number!");
    }

    private static <T, V extends FieldVector> void vectorConsumer(final T data, final V vector,
                                                                  final Consumer<V> consumerIfNullable,
                                                                  final BiConsumer<T, V> defaultConsumer) {
        if (Objects.isNull(data)) {
            consumerIfNullable.accept(vector);
            return;
        }
        defaultConsumer.accept(data, vector);
    }

    private static VectorSchemaRoot getSchemasRoot(final ResultSet data, final BufferAllocator allocator)
            throws SQLException {
        final VarCharVector catalogs = new VarCharVector("catalog_name", allocator);
        final VarCharVector schemas =
                new VarCharVector("db_schema_name", FieldType.notNullable(MinorType.VARCHAR.getType()), allocator);
        final List<FieldVector> vectors = ImmutableList.of(catalogs, schemas);
        vectors.forEach(FieldVector::allocateNew);
        final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
                catalogs, "TABLE_CATALOG",
                schemas, "TABLE_SCHEM");
        saveToVectors(vectorToColumnName, data);
        final int rows =
                vectors.stream().map(FieldVector::getValueCount).findAny().orElseThrow(IllegalStateException::new);
        vectors.forEach(vector -> vector.setValueCount(rows));
        return new VectorSchemaRoot(vectors);
    }

    private static <T extends FieldVector> int saveToVectors(final Map<T, String> vectorToColumnName,
                                                             final ResultSet data, boolean emptyToNull)
            throws SQLException {
        Predicate<ResultSet> alwaysTrue = (resultSet) -> true;
        return saveToVectors(vectorToColumnName, data, emptyToNull, alwaysTrue);
    }

    private static <T extends FieldVector> int saveToVectors(final Map<T, String> vectorToColumnName,
                                                             final ResultSet data, boolean emptyToNull,
                                                             Predicate<ResultSet> resultSetPredicate)
            throws SQLException {
        Objects.requireNonNull(vectorToColumnName, "vectorToColumnName cannot be null.");
        Objects.requireNonNull(data, "data cannot be null.");
        final Set<Entry<T, String>> entrySet = vectorToColumnName.entrySet();
        int rows = 0;

        while (data.next()) {
            if (!resultSetPredicate.test(data)) {
                continue;
            }
            for (final Entry<T, String> vectorToColumn : entrySet) {
                final T vector = vectorToColumn.getKey();
                final String columnName = vectorToColumn.getValue();
                if (vector instanceof VarCharVector) {
                    String thisData = data.getString(columnName);
                    saveToVector(emptyToNull ? Strings.emptyToNull(thisData) : thisData, (VarCharVector) vector, rows);
                } else if (vector instanceof IntVector) {
                    final int intValue = data.getInt(columnName);
                    saveToVector(data.wasNull() ? null : intValue, (IntVector) vector, rows);
                } else if (vector instanceof UInt1Vector) {
                    final byte byteValue = data.getByte(columnName);
                    saveToVector(data.wasNull() ? null : byteValue, (UInt1Vector) vector, rows);
                } else if (vector instanceof BitVector) {
                    final byte byteValue = data.getByte(columnName);
                    saveToVector(data.wasNull() ? null : byteValue, (BitVector) vector, rows);
                } else if (vector instanceof ListVector) {
                    String createParamsValues = data.getString(columnName);

                    UnionListWriter writer = ((ListVector) vector).getWriter();

                    BufferAllocator allocator = vector.getAllocator();
                    final ArrowBuf buf = allocator.buffer(1024);

                    writer.setPosition(rows);
                    writer.startList();

                    if (createParamsValues != null) {
                        String[] split = createParamsValues.split(",");

                        IntStream.range(0, split.length)
                                .forEach(i -> {
                                    byte[] bytes = split[i].getBytes(StandardCharsets.UTF_8);
                                    Preconditions.checkState(bytes.length < 1024,
                                            "The amount of bytes is greater than what the ArrowBuf supports");
                                    buf.setBytes(0, bytes);
                                    writer.varChar().writeVarChar(0, bytes.length, buf);
                                });
                    }
                    buf.close();
                    writer.endList();
                } else {
                    throw CallStatus
                            .INVALID_ARGUMENT.withDescription("Provided vector not supported").toRuntimeException();
                }
            }
            rows++;
        }
        for (final Entry<T, String> vectorToColumn : entrySet) {
            vectorToColumn.getKey().setValueCount(rows);
        }

        return rows;
    }

    private static <T extends FieldVector> void saveToVectors(final Map<T, String> vectorToColumnName,
                                                              final ResultSet data)
            throws SQLException {
        saveToVectors(vectorToColumnName, data, false);
    }

    private static VectorSchemaRoot getTableTypesRoot(final ResultSet data, final BufferAllocator allocator)
            throws SQLException {
        return getRoot(data, allocator, "table_type", "TABLE_TYPE");
    }

    private static VectorSchemaRoot getCatalogsRoot(final ResultSet data, final BufferAllocator allocator)
            throws SQLException {
        return getRoot(data, allocator, "catalog_name", "TABLE_CATALOG");
    }

    private static VectorSchemaRoot getRoot(final ResultSet data, final BufferAllocator allocator,
                                            final String fieldVectorName, final String columnName)
            throws SQLException {
        final VarCharVector dataVector =
                new VarCharVector(fieldVectorName, FieldType.notNullable(MinorType.VARCHAR.getType()), allocator);
        saveToVectors(ImmutableMap.of(dataVector, columnName), data);
        final int rows = dataVector.getValueCount();
        dataVector.setValueCount(rows);
        return new VectorSchemaRoot(Collections.singletonList(dataVector));
    }

    private static VectorSchemaRoot getTypeInfoRoot(CommandGetXdbcTypeInfo request, ResultSet typeInfo,
                                                    final BufferAllocator allocator)
            throws SQLException {
        Preconditions.checkNotNull(allocator, "BufferAllocator cannot be null.");

        VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TYPE_INFO_SCHEMA, allocator);

        Map<FieldVector, String> mapper = new HashMap<>();
        mapper.put(root.getVector("type_name"), "TYPE_NAME");
        mapper.put(root.getVector("data_type"), "DATA_TYPE");
        mapper.put(root.getVector("column_size"), "PRECISION");
        mapper.put(root.getVector("literal_prefix"), "LITERAL_PREFIX");
        mapper.put(root.getVector("literal_suffix"), "LITERAL_SUFFIX");
        mapper.put(root.getVector("create_params"), "CREATE_PARAMS");
        mapper.put(root.getVector("nullable"), "NULLABLE");
        mapper.put(root.getVector("case_sensitive"), "CASE_SENSITIVE");
        mapper.put(root.getVector("searchable"), "SEARCHABLE");
        mapper.put(root.getVector("unsigned_attribute"), "UNSIGNED_ATTRIBUTE");
        mapper.put(root.getVector("fixed_prec_scale"), "FIXED_PREC_SCALE");
        mapper.put(root.getVector("auto_increment"), "AUTO_INCREMENT");
        mapper.put(root.getVector("local_type_name"), "LOCAL_TYPE_NAME");
        mapper.put(root.getVector("minimum_scale"), "MINIMUM_SCALE");
        mapper.put(root.getVector("maximum_scale"), "MAXIMUM_SCALE");
        mapper.put(root.getVector("sql_data_type"), "SQL_DATA_TYPE");
        mapper.put(root.getVector("datetime_subcode"), "SQL_DATETIME_SUB");
        mapper.put(root.getVector("num_prec_radix"), "NUM_PREC_RADIX");

        Predicate<ResultSet> predicate;
        if (request.hasDataType()) {
            predicate = (resultSet) -> {
                try {
                    return resultSet.getInt("DATA_TYPE") == request.getDataType();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            };
        } else {
            predicate = (resultSet -> true);
        }

        int rows = saveToVectors(mapper, typeInfo, true, predicate);

        root.setRowCount(rows);
        return root;
    }

    private static VectorSchemaRoot getTablesRoot(final DatabaseMetaData databaseMetaData,
                                                  final BufferAllocator allocator,
                                                  final boolean includeSchema,
                                                  final String catalog,
                                                  final String schemaFilterPattern,
                                                  final String tableFilterPattern,
                                                  final String... tableTypes)
            throws SQLException, IOException {
        Objects.requireNonNull(allocator, "BufferAllocator cannot be null.");
        final VarCharVector catalogNameVector = new VarCharVector("catalog_name", allocator);
        final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", allocator);
        final VarCharVector tableNameVector =
                new VarCharVector("table_name", FieldType.notNullable(MinorType.VARCHAR.getType()), allocator);
        final VarCharVector tableTypeVector =
                new VarCharVector("table_type", FieldType.notNullable(MinorType.VARCHAR.getType()), allocator);

        final List<FieldVector> vectors = new ArrayList<>(4);
        vectors.add(catalogNameVector);
        vectors.add(schemaNameVector);
        vectors.add(tableNameVector);
        vectors.add(tableTypeVector);

        vectors.forEach(FieldVector::allocateNew);

        final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
                catalogNameVector, "TABLE_CAT",
                schemaNameVector, "TABLE_SCHEM",
                tableNameVector, "TABLE_NAME",
                tableTypeVector, "TABLE_TYPE");

        try (final ResultSet data =
                     Objects.requireNonNull(
                                     databaseMetaData,
                                     String.format("%s cannot be null.", databaseMetaData.getClass().getName()))
                             .getTables(catalog, schemaFilterPattern, tableFilterPattern, tableTypes)) {

            saveToVectors(vectorToColumnName, data, true);
            final int rows =
                    vectors.stream().map(FieldVector::getValueCount).findAny().orElseThrow(IllegalStateException::new);
            vectors.forEach(vector -> vector.setValueCount(rows));

            if (includeSchema) {
                final VarBinaryVector tableSchemaVector = new VarBinaryVector("table_schema",
                        FieldType.notNullable(MinorType.VARBINARY.getType()), allocator);
                tableSchemaVector.allocateNew(rows);

                try (final ResultSet columnsData =
                             databaseMetaData.getColumns(catalog, schemaFilterPattern, tableFilterPattern, null)) {
                    final Map<String, List<Field>> tableToFields = new HashMap<>();

                    while (columnsData.next()) {
                        final String catalogName = columnsData.getString("TABLE_CAT");
                        final String schemaName = columnsData.getString("TABLE_SCHEM");
                        final String tableName = columnsData.getString("TABLE_NAME");
                        final String typeName = columnsData.getString("TYPE_NAME");
                        final String fieldName = columnsData.getString("COLUMN_NAME");
                        final int dataType = columnsData.getInt("DATA_TYPE");
                        final boolean isNullable = columnsData.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;
                        final int precision = columnsData.getInt("COLUMN_SIZE");
                        final int scale = columnsData.getInt("DECIMAL_DIGITS");
                        boolean isAutoIncrement =
                                Objects.equals(columnsData.getString("IS_AUTOINCREMENT"), "YES");

                        final List<Field> fields =
                                tableToFields.computeIfAbsent(tableName, tableNameTmp -> new ArrayList<>());

                        final FlightSqlColumnMetadata columnMetadata = new FlightSqlColumnMetadata.Builder()
                                .catalogName(catalogName)
                                .schemaName(schemaName)
                                .tableName(tableName)
                                .typeName(typeName)
                                .precision(precision)
                                .scale(scale)
                                .isAutoIncrement(isAutoIncrement)
                                .build();

                        final Field field =
                                new Field(
                                        fieldName,
                                        new FieldType(
                                                isNullable,
                                                getArrowTypeFromJdbcType(dataType, precision, scale),
                                                null,
                                                columnMetadata.getMetadataMap()),
                                        null);
                        fields.add(field);
                    }

                    for (int index = 0; index < rows; index++) {
                        final String tableName = tableNameVector.getObject(index).toString();
                        final Schema schema = new Schema(tableToFields.get(tableName));
                        saveToVector(
                                ByteString.copyFrom(serializeMetadata(schema)).toByteArray(),
                                tableSchemaVector, index);
                    }
                }

                tableSchemaVector.setValueCount(rows);
                vectors.add(tableSchemaVector);
            }
        }

        return new VectorSchemaRoot(vectors);
    }

    private static ByteBuffer serializeMetadata(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize schema", e);
        }
    }

    @Override
    public void getStreamPreparedStatement(final CommandPreparedStatementQuery command, final CallContext context,
                                           final ServerStreamListener listener) {
        final ByteString handle = command.getPreparedStatementHandle();
        FlightStatementContext<PreparedStatement> flightStatementContext =
                preparedStatementLoadingCache.getIfPresent(handle);
        Objects.requireNonNull(flightStatementContext);
        final PreparedStatement statement = flightStatementContext.getStatement();
        try (final ResultSet resultSet = statement.executeQuery()) {
            final Schema schema = JdbcToArrowUtils.jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR);
            try (final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                listener.start(vectorSchemaRoot);

                final ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(resultSet, rootAllocator);
                while (iterator.hasNext()) {
                    final VectorSchemaRoot batch = iterator.next();
                    if (batch.getRowCount() == 0) {
                        break;
                    }
                    final VectorUnloader unloader = new VectorUnloader(batch);
                    loader.load(unloader.getRecordBatch());
                    listener.putNext();
                    vectorSchemaRoot.clear();
                }

                listener.putNext();
            }
        } catch (final SQLException | IOException e) {
            listener.error(
                    CallStatus.INTERNAL.withDescription("Failed to prepare statement: " + e).toRuntimeException());
        } finally {
            listener.completed();
        }
    }

    @Override
    public void closePreparedStatement(final ActionClosePreparedStatementRequest request, final CallContext context,
                                       final StreamListener<Result> listener) {
        // Running on another thread
        executorService.submit(() -> {
            try {
                preparedStatementLoadingCache.invalidate(request.getPreparedStatementHandle());
            } catch (final Exception e) {
                listener.onError(e);
                return;
            }
            listener.onCompleted();
        });
    }

    @Override
    public FlightInfo getFlightInfoStatement(final CommandStatementQuery request, final CallContext context,
                                             final FlightDescriptor descriptor) { // 1
        ByteString handle = ByteString.copyFrom(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

        try {
            // Ownership of the connection will be passed to the context. Do NOT close!
            final Connection connection = dataSource.getConnection();
            final Statement statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            final String query = request.getQuery();
            final FlightStatementContext<Statement> flightStatementContext =
                    new FlightStatementContext<>(statement, query);

            statementLoadingCache.put(handle, flightStatementContext);
            // final ResultSet resultSet = statement.executeQuery(query);

            // TicketStatementQuery ticket = TicketStatementQuery.newBuilder()
            //         .setStatementHandle(handle)
            //         .build();
            // return getFlightInfoForSchema(ticket, descriptor,
            //         JdbcToArrowUtils.jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR));
            // return new FlightInfo(JdbcToArrowUtils.jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR),
            //         descriptor, endpoints, -1, -1);

            // #########
            FlightSqlExecutor.executeQuery(flightStatementContext);

            TicketStatementQuery ticketStatement = TicketStatementQuery.newBuilder()
                    .setStatementHandle(ByteString.copyFromUtf8(
                            DebugUtil.printId(flightStatementContext.getFinstId()) + ":" + query)).build();
            final Ticket ticket = new Ticket(Any.pack(ticketStatement).toByteArray());
            // TODO Support multiple endpoints.
            Location location = Location.forGrpcInsecure(flightStatementContext.getResultFlightServerAddr().hostname,
                    flightStatementContext.getResultFlightServerAddr().port);
            List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, location));

            Status status = new Status();
            Schema schema;
            try {
                schema = FlightSqlExecutor.fetchArrowFlightSchema(flightStatementContext,
                        5000, status);
            } catch (Exception e) {
                throw CallStatus.INTERNAL.withDescription("failed to fetch Arrow Flight SQL schema. "
                        + Util.getRootCauseMessage(e)).toRuntimeException();
            }
            if (!status.ok()) {
                throw CallStatus.INTERNAL.withDescription(status.toString()).toRuntimeException();
            }
            if (schema == null) {
                throw CallStatus.INTERNAL.withDescription("schema is null, status: " + status).toRuntimeException();
            }
            return new FlightInfo(schema, descriptor, endpoints, -1, -1);
        } catch (final SQLException e) {
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        }
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(final CommandPreparedStatementQuery command,
                                                     final CallContext context,
                                                     final FlightDescriptor descriptor) {
        final ByteString preparedStatementHandle = command.getPreparedStatementHandle();
        FlightStatementContext<PreparedStatement> flightStatementContext =
                preparedStatementLoadingCache.getIfPresent(preparedStatementHandle);
        try {
            assert flightStatementContext != null;
            PreparedStatement statement = flightStatementContext.getStatement();

            ResultSetMetaData metaData = statement.getMetaData();
            return getFlightInfoForSchema(command, descriptor,
                    JdbcToArrowUtils.jdbcToArrowSchema(metaData, DEFAULT_CALENDAR));
        } catch (final SQLException e) {
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        }
    }

    @Override
    public SchemaResult getSchemaStatement(final CommandStatementQuery command, final CallContext context,
                                           final FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void close() throws Exception {
        try {
            preparedStatementLoadingCache.cleanUp();
        } catch (Throwable t) {
            CallStatus.INTERNAL.withDescription("Unknown error: " + t);
        }

        AutoCloseables.close(dataSource, rootAllocator);
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        // TODO - build example implementation
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void createPreparedStatement(final ActionCreatePreparedStatementRequest request, final CallContext context,
                                        final StreamListener<Result> listener) {
        // Running on another thread
        executorService.submit(() -> {
            try {
                final ByteString preparedStatementHandle =
                        ByteString.copyFrom(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                // Ownership of the connection will be passed to the context. Do NOT close!
                final Connection connection = dataSource.getConnection();
                final PreparedStatement preparedStatement = connection.prepareStatement(request.getQuery(),
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                final FlightStatementContext<PreparedStatement> preparedStatementContext =
                        new FlightStatementContext<>(preparedStatement, request.getQuery());

                preparedStatementLoadingCache.put(preparedStatementHandle, preparedStatementContext);

                final Schema parameterSchema =
                        JdbcToArrowUtils.jdbcToArrowSchema(preparedStatement.getParameterMetaData(), DEFAULT_CALENDAR);

                final ResultSetMetaData metaData = preparedStatement.getMetaData();
                final ByteString bytes = Objects.isNull(metaData)
                        ? ByteString.EMPTY
                        : ByteString.copyFrom(
                                serializeMetadata(JdbcToArrowUtils.jdbcToArrowSchema(metaData, DEFAULT_CALENDAR)));
                final ActionCreatePreparedStatementResult result = ActionCreatePreparedStatementResult.newBuilder()
                        .setDatasetSchema(bytes)
                        .setParameterSchema(ByteString.copyFrom(serializeMetadata(parameterSchema)))
                        .setPreparedStatementHandle(preparedStatementHandle)
                        .build();
                listener.onNext(new Result(Any.pack(result).toByteArray()));
            } catch (final SQLException e) {
                listener.onError(CallStatus.INTERNAL
                        .withDescription("Failed to create prepared statement: " + e)
                        .toRuntimeException());
                return;
            } catch (final Throwable t) {
                listener.onError(CallStatus.INTERNAL.withDescription("Unknown error: " + t).toRuntimeException());
                return;
            }
            listener.onCompleted();
        });
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
        // TODO - build example implementation
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public Runnable acceptPutStatement(CommandStatementUpdate command,
                                       CallContext context, FlightStream flightStream,
                                       StreamListener<PutResult> ackStream) {
        final String query = command.getQuery();

        return () -> {
            try (final Connection connection = dataSource.getConnection();
                    final Statement statement = connection.createStatement()) {
                final int result = statement.executeUpdate(query);

                final DoPutUpdateResult build =
                        DoPutUpdateResult.newBuilder().setRecordCount(result).build();

                try (final ArrowBuf buffer = rootAllocator.buffer(build.getSerializedSize())) {
                    buffer.writeBytes(build.toByteArray());
                    ackStream.onNext(PutResult.metadata(buffer));
                    ackStream.onCompleted();
                }
            } catch (SQLSyntaxErrorException e) {
                ackStream.onError(CallStatus.INVALID_ARGUMENT
                        .withDescription("Failed to execute statement (invalid syntax): " + e)
                        .toRuntimeException());
            } catch (SQLException e) {
                ackStream.onError(CallStatus.INTERNAL
                        .withDescription("Failed to execute statement: " + e)
                        .toRuntimeException());
            }
        };
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate command, CallContext context,
                                                     FlightStream flightStream, StreamListener<PutResult> ackStream) {
        final FlightStatementContext<PreparedStatement> statement =
                preparedStatementLoadingCache.getIfPresent(command.getPreparedStatementHandle());

        return () -> {
            if (statement == null) {
                ackStream.onError(CallStatus.NOT_FOUND
                        .withDescription("Prepared statement does not exist")
                        .toRuntimeException());
                return;
            }
            try {
                final PreparedStatement preparedStatement = statement.getStatement();

                while (flightStream.next()) {
                    final VectorSchemaRoot root = flightStream.getRoot();

                    final int rowCount = root.getRowCount();
                    final int recordCount;

                    if (rowCount == 0) {
                        preparedStatement.execute();
                        recordCount = preparedStatement.getUpdateCount();
                    } else {
                        int[] recordCounts = preparedStatement.executeBatch();
                        recordCount = Arrays.stream(recordCounts).sum();
                    }

                    final DoPutUpdateResult build =
                            DoPutUpdateResult.newBuilder().setRecordCount(recordCount).build();

                    try (final ArrowBuf buffer = rootAllocator.buffer(build.getSerializedSize())) {
                        buffer.writeBytes(build.toByteArray());
                        ackStream.onNext(PutResult.metadata(buffer));
                    }
                }
            } catch (SQLException e) {
                ackStream.onError(
                        CallStatus.INTERNAL.withDescription("Failed to execute update: " + e).toRuntimeException());
                return;
            }
            ackStream.onCompleted();
        };
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery command, CallContext context,
                                                    FlightStream flightStream, StreamListener<PutResult> ackStream) {
        final FlightStatementContext<PreparedStatement> flightStatementContext =
                preparedStatementLoadingCache.getIfPresent(command.getPreparedStatementHandle());

        return () -> {
            assert flightStatementContext != null;
            ackStream.onCompleted();
        };
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(final CommandGetSqlInfo request, final CallContext context,
                                           final FlightDescriptor descriptor) { // 1
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(final CommandGetSqlInfo command, final CallContext context,
                                 final ServerStreamListener listener) { // 1
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
        try (final Connection connection = dataSource.getConnection();
                final ResultSet typeInfo = connection.getMetaData().getTypeInfo();
                final VectorSchemaRoot vectorSchemaRoot = getTypeInfoRoot(request, typeInfo, rootAllocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(final CommandGetCatalogs request, final CallContext context,
                                            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(final CallContext context, final ServerStreamListener listener) {
        try (final Connection connection = dataSource.getConnection();
                final ResultSet catalogs = connection.getMetaData().getCatalogs();
                final VectorSchemaRoot vectorSchemaRoot = getCatalogsRoot(catalogs, rootAllocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoSchemas(final CommandGetDbSchemas request, final CallContext context,
                                           final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(final CommandGetDbSchemas command, final CallContext context,
                                 final ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern =
                        command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        try (final Connection connection = dataSource.getConnection();
                final ResultSet schemas = connection.getMetaData().getSchemas(catalog, schemaFilterPattern);
                final VectorSchemaRoot vectorSchemaRoot = getSchemasRoot(schemas, rootAllocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
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
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern =
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        final String tableFilterPattern =
                command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null;

        final ProtocolStringList protocolStringList = command.getTableTypesList();
        final int protocolSize = protocolStringList.size();
        final String[] tableTypes =
                protocolSize == 0 ? null : protocolStringList.toArray(new String[protocolSize]);

        try (final Connection connection = DriverManager.getConnection(DATABASE_URI);
                final VectorSchemaRoot vectorSchemaRoot = getTablesRoot(
                        connection.getMetaData(),
                        rootAllocator,
                        command.getIncludeSchema(),
                        catalog, schemaFilterPattern, tableFilterPattern, tableTypes)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException | IOException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(final CommandGetTableTypes request, final CallContext context,
                                              final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    @Override
    public void getStreamTableTypes(final CallContext context, final ServerStreamListener listener) {
        try (final Connection connection = dataSource.getConnection();
                final ResultSet tableTypes = connection.getMetaData().getTableTypes();
                final VectorSchemaRoot vectorSchemaRoot = getTableTypesRoot(tableTypes, rootAllocator)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(final CommandGetPrimaryKeys request, final CallContext context,
                                               final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_PRIMARY_KEYS_SCHEMA);
    }

    @Override
    public void getStreamPrimaryKeys(final CommandGetPrimaryKeys command, final CallContext context,
                                     final ServerStreamListener listener) {

        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schema = command.hasDbSchema() ? command.getDbSchema() : null;
        final String table = command.getTable();

        try (Connection connection = DriverManager.getConnection(DATABASE_URI)) {
            final ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(catalog, schema, table);

            final VarCharVector catalogNameVector = new VarCharVector("catalog_name", rootAllocator);
            final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", rootAllocator);
            final VarCharVector tableNameVector = new VarCharVector("table_name", rootAllocator);
            final VarCharVector columnNameVector = new VarCharVector("column_name", rootAllocator);
            final IntVector keySequenceVector = new IntVector("key_sequence", rootAllocator);
            final VarCharVector keyNameVector = new VarCharVector("key_name", rootAllocator);

            final List<FieldVector> vectors =
                    new ArrayList<>(
                            ImmutableList.of(
                                    catalogNameVector, schemaNameVector, tableNameVector, columnNameVector,
                                    keySequenceVector,
                                    keyNameVector));
            vectors.forEach(FieldVector::allocateNew);

            int rows = 0;
            for (; primaryKeys.next(); rows++) {
                saveToVector(primaryKeys.getString("TABLE_CAT"), catalogNameVector, rows);
                saveToVector(primaryKeys.getString("TABLE_SCHEM"), schemaNameVector, rows);
                saveToVector(primaryKeys.getString("TABLE_NAME"), tableNameVector, rows);
                saveToVector(primaryKeys.getString("COLUMN_NAME"), columnNameVector, rows);
                final int key_seq = primaryKeys.getInt("KEY_SEQ");
                saveToVector(primaryKeys.wasNull() ? null : key_seq, keySequenceVector, rows);
                saveToVector(primaryKeys.getString("PK_NAME"), keyNameVector, rows);
            }

            try (final VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors)) {
                vectorSchemaRoot.setRowCount(rows);

                listener.start(vectorSchemaRoot);
                listener.putNext();
            }
        } catch (SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(final CommandGetExportedKeys request, final CallContext context,
                                                final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_EXPORTED_KEYS_SCHEMA);
    }

    @Override
    public void getStreamExportedKeys(final CommandGetExportedKeys command, final CallContext context,
                                      final ServerStreamListener listener) {
        String catalog = command.hasCatalog() ? command.getCatalog() : null;
        String schema = command.hasDbSchema() ? command.getDbSchema() : null;
        String table = command.getTable();

        try (Connection connection = DriverManager.getConnection(DATABASE_URI);
                ResultSet keys = connection.getMetaData().getExportedKeys(catalog, schema, table);
                VectorSchemaRoot vectorSchemaRoot = createVectors(keys)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(final CommandGetImportedKeys request, final CallContext context,
                                                final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_IMPORTED_KEYS_SCHEMA);
    }

    @Override
    public void getStreamImportedKeys(final CommandGetImportedKeys command, final CallContext context,
                                      final ServerStreamListener listener) {
        String catalog = command.hasCatalog() ? command.getCatalog() : null;
        String schema = command.hasDbSchema() ? command.getDbSchema() : null;
        String table = command.getTable();

        try (Connection connection = DriverManager.getConnection(DATABASE_URI);
                ResultSet keys = connection.getMetaData().getImportedKeys(catalog, schema, table);
                VectorSchemaRoot vectorSchemaRoot = createVectors(keys)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (final SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(CommandGetCrossReference request, CallContext context,
                                                  FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CROSS_REFERENCE_SCHEMA);
    }

    @Override
    public void getStreamCrossReference(CommandGetCrossReference command, CallContext context,
                                        ServerStreamListener listener) {
        final String pkCatalog = command.hasPkCatalog() ? command.getPkCatalog() : null;
        final String pkSchema = command.hasPkDbSchema() ? command.getPkDbSchema() : null;
        final String fkCatalog = command.hasFkCatalog() ? command.getFkCatalog() : null;
        final String fkSchema = command.hasFkDbSchema() ? command.getFkDbSchema() : null;
        final String pkTable = command.getPkTable();
        final String fkTable = command.getFkTable();

        try (Connection connection = DriverManager.getConnection(DATABASE_URI);
                ResultSet keys = connection.getMetaData()
                        .getCrossReference(pkCatalog, pkSchema, pkTable, fkCatalog, fkSchema, fkTable);
                VectorSchemaRoot vectorSchemaRoot = createVectors(keys)) {
            listener.start(vectorSchemaRoot);
            listener.putNext();
        } catch (final SQLException e) {
            listener.error(e);
        } finally {
            listener.completed();
        }
    }

    private VectorSchemaRoot createVectors(ResultSet keys) throws SQLException {
        final VarCharVector pkCatalogNameVector = new VarCharVector("pk_catalog_name", rootAllocator);
        final VarCharVector pkSchemaNameVector = new VarCharVector("pk_db_schema_name", rootAllocator);
        final VarCharVector pkTableNameVector = new VarCharVector("pk_table_name", rootAllocator);
        final VarCharVector pkColumnNameVector = new VarCharVector("pk_column_name", rootAllocator);
        final VarCharVector fkCatalogNameVector = new VarCharVector("fk_catalog_name", rootAllocator);
        final VarCharVector fkSchemaNameVector = new VarCharVector("fk_db_schema_name", rootAllocator);
        final VarCharVector fkTableNameVector = new VarCharVector("fk_table_name", rootAllocator);
        final VarCharVector fkColumnNameVector = new VarCharVector("fk_column_name", rootAllocator);
        final IntVector keySequenceVector = new IntVector("key_sequence", rootAllocator);
        final VarCharVector fkKeyNameVector = new VarCharVector("fk_key_name", rootAllocator);
        final VarCharVector pkKeyNameVector = new VarCharVector("pk_key_name", rootAllocator);
        final UInt1Vector updateRuleVector = new UInt1Vector("update_rule", rootAllocator);
        final UInt1Vector deleteRuleVector = new UInt1Vector("delete_rule", rootAllocator);

        Map<FieldVector, String> vectorToColumnName = new HashMap<>();
        vectorToColumnName.put(pkCatalogNameVector, "PKTABLE_CAT");
        vectorToColumnName.put(pkSchemaNameVector, "PKTABLE_SCHEM");
        vectorToColumnName.put(pkTableNameVector, "PKTABLE_NAME");
        vectorToColumnName.put(pkColumnNameVector, "PKCOLUMN_NAME");
        vectorToColumnName.put(fkCatalogNameVector, "FKTABLE_CAT");
        vectorToColumnName.put(fkSchemaNameVector, "FKTABLE_SCHEM");
        vectorToColumnName.put(fkTableNameVector, "FKTABLE_NAME");
        vectorToColumnName.put(fkColumnNameVector, "FKCOLUMN_NAME");
        vectorToColumnName.put(keySequenceVector, "KEY_SEQ");
        vectorToColumnName.put(updateRuleVector, "UPDATE_RULE");
        vectorToColumnName.put(deleteRuleVector, "DELETE_RULE");
        vectorToColumnName.put(fkKeyNameVector, "FK_NAME");
        vectorToColumnName.put(pkKeyNameVector, "PK_NAME");

        final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(
                pkCatalogNameVector, pkSchemaNameVector, pkTableNameVector, pkColumnNameVector, fkCatalogNameVector,
                fkSchemaNameVector, fkTableNameVector, fkColumnNameVector, keySequenceVector, fkKeyNameVector,
                pkKeyNameVector, updateRuleVector, deleteRuleVector);

        vectorSchemaRoot.allocateNew();
        final int rowCount = saveToVectors(vectorToColumnName, keys, true);

        vectorSchemaRoot.setRowCount(rowCount);

        return vectorSchemaRoot;
    }

    @Override
    public void getStreamStatement(final TicketStatementQuery ticketStatementQuery, final CallContext context,
                                   final ServerStreamListener listener) {
        final ByteString handle = ticketStatementQuery.getStatementHandle();
        final FlightStatementContext<Statement> flightStatementContext =
                Objects.requireNonNull(statementLoadingCache.getIfPresent(handle));
        try (final ResultSet resultSet = flightStatementContext.getStatement().getResultSet()) {
            final Schema schema = JdbcToArrowUtils.jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR);
            try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                listener.start(vectorSchemaRoot);

                final ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(resultSet, rootAllocator);
                while (iterator.hasNext()) {
                    final VectorUnloader unloader = new VectorUnloader(iterator.next());
                    loader.load(unloader.getRecordBatch());
                    listener.putNext();
                    vectorSchemaRoot.clear();
                }

                listener.putNext();
            }
        } catch (SQLException | IOException e) {
            listener.error(e);
        } finally {
            listener.completed();
            statementLoadingCache.invalidate(handle);
        }
    }

    private <T extends Message> FlightInfo getFlightInfoForSchema(final T request, final FlightDescriptor descriptor,
                                                                  final Schema schema) { // 1
        final Ticket ticket = new Ticket(Any.pack(request).toByteArray());
        // TODO Support multiple endpoints.
        final List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, location));

        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    private static class StatementRemovalListener<T extends Statement>
            implements RemovalListener<ByteString, FlightStatementContext<T>> { // 1
        @Override
        public void onRemoval(final RemovalNotification<ByteString, FlightStatementContext<T>> notification) {
            try {
                AutoCloseables.close(notification.getValue());
            } catch (final Exception e) {
                // swallow
            }
        }
    }
}

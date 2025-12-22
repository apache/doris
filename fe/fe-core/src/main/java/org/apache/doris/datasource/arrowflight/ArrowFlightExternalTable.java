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

package org.apache.doris.datasource.arrowflight;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.TRemoteDorisTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ArrowFlightExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightExternalTable.class);

    public ArrowFlightExternalTable(long id, String name, String remoteName,
                                    ArrowFlightExternalCatalog catalog, ExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.ARROW_FLIGHT_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TRemoteDorisTable tRemoteDorisTable = new TRemoteDorisTable();
        tRemoteDorisTable.setDbName(dbName);
        tRemoteDorisTable.setTableName(name);
        tRemoteDorisTable.setProperties(getCatalog().getProperties());

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(),
                TTableType.REMOTE_DORIS_TABLE, schema.size(), 0, getName(), dbName);

        tTableDescriptor.setRemoteDorisTable(tRemoteDorisTable);
        return tTableDescriptor;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        for (int retryCount = 0;
                 retryCount < ((ArrowFlightExternalCatalog) getCatalog()).getQueryRetryCount(); retryCount++) {
            try {
                FlightSqlClientLoadBalancer flightSqlClientLoadBalancer =
                        ((ArrowFlightExternalCatalog) catalog).getFlightSqlClientLoadBalancer();
                FlightSqlClientLoadBalancer.FlightSqlClientWithOptions clientWithOptions =
                        flightSqlClientLoadBalancer.randomClient();
                FlightSqlClient flightSqlClient = clientWithOptions.getFlightSqlClient();
                CallOption[] callOptions = clientWithOptions.getCallOptions();

                FlightInfo info = flightSqlClient.getTables(
                        ((ArrowFlightExternalCatalog) catalog).getFlightSqlCatalogName(),
                        dbName,
                        name,
                        null,
                        true,
                        callOptions);

                for (FlightEndpoint endpoint : info.getEndpoints()) {
                    FlightStream stream = flightSqlClient.getStream(endpoint.getTicket(), callOptions);
                    while (stream.next()) {
                        try (VectorSchemaRoot root = stream.getRoot();
                                 VarBinaryVector tableSchemaVector =
                                         (VarBinaryVector) root.getVector(ArrowFlightExternalCatalog.TABLE_SCHEMA)) {

                            List<Column> columns = new ArrayList<>();
                            if (!tableSchemaVector.isNull(0)) {
                                byte[] schemaBytes = tableSchemaVector.get(0);

                                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(schemaBytes);
                                         ReadChannel channel = new ReadChannel(Channels.newChannel(inputStream))) {
                                    Schema schema = MessageSerializer.deserializeSchema(channel);

                                    for (Field field : schema.getFields()) {
                                        columns.add(parseArrowFieldToColumn(field));
                                    }

                                }
                            }
                            if (!columns.isEmpty()) {
                                return Optional.of(new SchemaCacheValue(columns));
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to init schema, catalog name: " + getName(), e);
                        }
                    }
                }
            } catch (FlightRuntimeException e) {
                LOG.warn("initSchema failed, catalog name: {}, retry count: {}", getName(), retryCount, e);
            }
        }

        return Optional.empty();
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        // TODO Should SELECT COUNT (*) be used to query RowCount?
        return -1;
    }

    private Column parseArrowFieldToColumn(Field field) {
        Column column = new Column();
        column.setName(field.getName());
        column.setIsKey(false);
        column.setIsAllowNull(field.isNullable());
        column.setUniqueId(-1);
        column.setType(parseArrowTypeToDorisType(field));
        return column;
    }

    private Type parseArrowTypeToDorisType(Field field) {
        switch (field.getType().getTypeID()) {
            case Bool:
                return Type.BOOLEAN;
            case Int:
                ArrowType.Int intType = (ArrowType.Int) field.getType();
                switch (intType.getBitWidth()) {
                    case 8:
                        return Type.TINYINT;
                    case 16:
                        return Type.SMALLINT;
                    case 32:
                        return Type.INT;
                    case 64:
                        return Type.BIGINT;
                    default:
                        throw new IllegalArgumentException("Invalid integer bit width: "
                            + intType.getBitWidth()
                            + " for ArrowFlight table: "
                            + getName());
                }
            case FloatingPoint:
                ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) field.getType();
                switch (floatType.getPrecision()) {
                    case SINGLE:
                        return Type.FLOAT;
                    case DOUBLE:
                        return Type.DOUBLE;
                    default:
                        throw new IllegalArgumentException("Invalid floating point precision: "
                            + floatType.getPrecision()
                            + " for ArrowFlight table: "
                            + getName());
                }
            case Decimal:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
                return ScalarType.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
            case Utf8:
            case LargeUtf8:
                return Type.STRING;
            case Date:
                return ScalarType.createDateV2Type();
            case Timestamp:
                ArrowType.Timestamp timeStampType = (ArrowType.Timestamp) field.getType();
                int scale;
                switch (timeStampType.getUnit()) {
                    case SECOND:
                        scale = 0;
                        break;
                    case MILLISECOND:
                        scale = 3;
                        break;
                    case MICROSECOND:
                        scale = 6;
                        break;
                    case NANOSECOND:
                        scale = 9;
                        break;
                    default:
                        scale = 0;
                        break;
                }
                return ScalarType.createDatetimeV2Type(scale);
            case Null:
                return Type.NULL;
            case Time:
            case Duration:
            case Interval:
                return Type.BIGINT;
            case List:
            case LargeList:
            case FixedSizeList:
                List<Field> listChildren = field.getChildren();
                Preconditions.checkArgument(
                        listChildren.size() == 1,
                        "Lists have one child Field. Found: %s",
                        listChildren.isEmpty() ? "none" : listChildren);
                return ArrayType.create(
                    parseArrowTypeToDorisType(listChildren.get(0)), listChildren.get(0).isNullable());
            case Map:
                List<Field> mapChildren = field.getChildren();
                Preconditions.checkArgument(
                        mapChildren.size() == 2,
                        "Map have Two child Field. Found: %s",
                        mapChildren.isEmpty() ? "none" : mapChildren);
                return new MapType(
                    parseArrowTypeToDorisType(mapChildren.get(0)), parseArrowTypeToDorisType(mapChildren.get(1)));
            case Struct:
                List<Field> structChildren = field.getChildren();
                Preconditions.checkArgument(!structChildren.isEmpty(),
                        "Struct child Field is none");
                return new StructType(structChildren.stream()
                    .map(f -> new StructField(f.getName(), parseArrowTypeToDorisType(f)))
                    .collect(Collectors.toCollection(ArrayList::new)));
            default:
                return Type.UNSUPPORTED;
        }
    }
}

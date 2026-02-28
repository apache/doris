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

package org.apache.doris.cdcclient.source.deserialize;

import org.apache.doris.cdcclient.utils.ConfigUtil;
import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.debezium.utils.TemporalConversions;
import org.apache.flink.table.data.TimestampData;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.doris.cdcclient.common.Constants.DORIS_DELETE_SIGN;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Bits;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SourceRecord ==> [{},{}] */
public class DebeziumJsonDeserializer
        implements SourceRecordDeserializer<SourceRecord, List<String>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumJsonDeserializer.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    @Setter private ZoneId serverTimeZone = ZoneId.systemDefault();

    public DebeziumJsonDeserializer() {}

    @Override
    public void init(Map<String, String> props) {
        this.serverTimeZone =
                ConfigUtil.getServerTimeZoneFromJdbcUrl(props.get(DataSourceConfigKeys.JDBC_URL));
    }

    @Override
    public List<String> deserialize(Map<String, String> context, SourceRecord record)
            throws IOException {
        if (RecordUtils.isDataChangeRecord(record)) {
            LOG.trace("Process data change record: {}", record);
            return deserializeDataChangeRecord(record);
        } else if (RecordUtils.isSchemaChangeEvent(record)) {
            return Collections.emptyList();
        } else {
            return Collections.emptyList();
        }
    }

    private List<String> deserializeDataChangeRecord(SourceRecord record) throws IOException {
        List<String> rows = new ArrayList<>();
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        if (Envelope.Operation.DELETE.equals(op)) {
            String deleteRow = extractBeforeRow(value, valueSchema);
            if (StringUtils.isNotEmpty(deleteRow)) {
                rows.add(deleteRow);
            }
        } else if (Envelope.Operation.READ.equals(op)
                || Envelope.Operation.CREATE.equals(op)
                || Envelope.Operation.UPDATE.equals(op)) {
            String insertRow = extractAfterRow(value, valueSchema);
            if (StringUtils.isNotEmpty(insertRow)) {
                rows.add(insertRow);
            }
        }
        return rows;
    }

    private String extractAfterRow(Struct value, Schema valueSchema)
            throws JsonProcessingException {
        Map<String, Object> record = new HashMap<>();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        if (after == null) {
            return null;
        }
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        afterSchema
                .fields()
                .forEach(
                        field -> {
                            Object valueConverted =
                                    convert(field.schema(), after.getWithoutDefault(field.name()));
                            record.put(field.name(), valueConverted);
                        });
        record.put(DORIS_DELETE_SIGN, 0);
        return objectMapper.writeValueAsString(record);
    }

    private String extractBeforeRow(Struct value, Schema valueSchema)
            throws JsonProcessingException {
        Map<String, Object> record = new HashMap<>();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        if (before == null) {
            return null;
        }
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        beforeSchema
                .fields()
                .forEach(
                        field -> {
                            Object valueConverted =
                                    convert(field.schema(), before.getWithoutDefault(field.name()));
                            record.put(field.name(), valueConverted);
                        });
        record.put(DORIS_DELETE_SIGN, 1);
        return objectMapper.writeValueAsString(record);
    }

    private Object convert(Schema fieldSchema, Object dbzObj) {
        if (dbzObj == null) {
            return null;
        }
        String name = fieldSchema.name();
        Schema.Type type = fieldSchema.type();
        if (StringUtils.isEmpty(name)) {
            switch (type) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                    return Long.parseLong(dbzObj.toString());
                case FLOAT32:
                    return Float.parseFloat(dbzObj.toString());
                case FLOAT64:
                    return Double.parseDouble(dbzObj.toString());
                case BOOLEAN:
                    return Boolean.parseBoolean(dbzObj.toString());
                case STRING:
                case ARRAY:
                case MAP:
                case STRUCT:
                    return dbzObj.toString();
                case BYTES:
                    return convertToBinary(dbzObj, fieldSchema);
                default:
                    LOG.debug("Unsupported type: {}, transform value to string", type);
                    return dbzObj.toString();
            }
        } else {
            switch (name) {
                case Time.SCHEMA_NAME:
                case MicroTime.SCHEMA_NAME:
                case NanoTime.SCHEMA_NAME:
                    return convertToTime(dbzObj, fieldSchema);
                case io.debezium.time.Date.SCHEMA_NAME:
                    return TemporalConversions.toLocalDate(dbzObj).toString();
                case Timestamp.SCHEMA_NAME:
                case MicroTimestamp.SCHEMA_NAME:
                case NanoTimestamp.SCHEMA_NAME:
                    return convertTimestamp(name, dbzObj);
                case ZonedTimestamp.SCHEMA_NAME:
                    return convertZoneTimestamp(dbzObj);
                case Decimal.LOGICAL_NAME:
                    return convertDecimal(dbzObj, fieldSchema);
                case Bits.LOGICAL_NAME:
                    return dbzObj;
                case Point.LOGICAL_NAME:
                case Geometry.LOGICAL_NAME:
                case Geography.LOGICAL_NAME:
                    return convertPoint(dbzObj);
                default:
                    LOG.debug(
                            "Unsupported type: {} with name {}, transform value to string",
                            type,
                            name);
                    return dbzObj.toString();
            }
        }
    }

    private Object convertPoint(Object dbzObj) {
        // the Geometry datatype in PostgreSQL will be converted to
        // a String with Json format
        try {
            Struct geometryStruct = (Struct) dbzObj;
            byte[] wkb = geometryStruct.getBytes("wkb");

            String geoJson = OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
            JsonNode originGeoNode = objectMapper.readTree(geoJson);

            Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
            Map<String, Object> geometryInfo = new HashMap<>();
            String geometryType = originGeoNode.get("type").asText();

            geometryInfo.put("type", geometryType);
            if ("GeometryCollection".equals(geometryType)) {
                geometryInfo.put("geometries", originGeoNode.get("geometries"));
            } else {
                geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
            }

            geometryInfo.put("srid", srid.orElse(0));
            return objectMapper.writeValueAsString(geometryInfo);
        } catch (Exception e) {
            LOG.debug(
                    "Failed to parse Geometry datatype, converting the value to string {}",
                    dbzObj.toString());
            return dbzObj.toString();
        }
    }

    private Object convertZoneTimestamp(Object dbzObj) {
        if (dbzObj instanceof String) {
            String str = (String) dbzObj;
            // TIMESTAMP_LTZ type is encoded in string type
            Instant instant = Instant.parse(str);
            return TimestampData.fromLocalDateTime(LocalDateTime.ofInstant(instant, serverTimeZone))
                    .toTimestamp()
                    .toString();
        }
        LOG.warn("Unable to convert to zone timestamp, default {}", dbzObj);
        return dbzObj.toString();
    }

    private Object convertTimestamp(String typeName, Object dbzObj) {
        if (dbzObj instanceof Long) {
            switch (typeName) {
                case Timestamp.SCHEMA_NAME:
                    return TimestampData.fromEpochMillis((Long) dbzObj).toTimestamp().toString();
                case MicroTimestamp.SCHEMA_NAME:
                    long micro = (long) dbzObj;
                    return TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000))
                            .toTimestamp()
                            .toString();
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) dbzObj;
                    return TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000))
                            .toTimestamp()
                            .toString();
            }
        }
        LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
        return java.sql.Timestamp.valueOf(localDateTime);
    }

    protected Object convertToBinary(Object dbzObj, Schema schema) {
        if (dbzObj instanceof byte[]) {
            return dbzObj;
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            LOG.warn("Unable to convert to binary, default {}", dbzObj);
            return dbzObj.toString();
        }
    }

    private Object convertDecimal(Object dbzObj, Schema schema) {
        BigDecimal bigDecimal;
        if (dbzObj instanceof byte[]) {
            // decimal.handling.mode=precise
            bigDecimal = Decimal.toLogical(schema, (byte[]) dbzObj);
        } else if (dbzObj instanceof String) {
            // decimal.handling.mode=string
            bigDecimal = new BigDecimal((String) dbzObj);
        } else if (dbzObj instanceof Double) {
            // decimal.handling.mode=double
            bigDecimal = BigDecimal.valueOf((Double) dbzObj);
        } else {
            if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
                SpecialValueDecimal decimal = VariableScaleDecimal.toLogical((Struct) dbzObj);
                bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
            } else {
                // fallback to string
                bigDecimal = new BigDecimal(dbzObj.toString());
            }
        }
        return bigDecimal;
    }

    protected Object convertToTime(Object dbzObj, Schema schema) {
        try {
            if (dbzObj instanceof Long) {
                switch (schema.name()) {
                    case MicroTime.SCHEMA_NAME:
                        // micro to nano
                        return LocalTime.ofNanoOfDay((Long) dbzObj * 1000L).toString();
                    case NanoTime.SCHEMA_NAME:
                        return LocalTime.ofNanoOfDay((Long) dbzObj).toString();
                }
            } else if (dbzObj instanceof Integer) {
                // millis to nano
                return LocalTime.ofNanoOfDay((Integer) dbzObj * 1_000_000L).toString();
            } else if (dbzObj instanceof java.util.Date) {
                long millisOfDay = ((Date) dbzObj).getTime() % (24 * 60 * 60 * 1000);
                // mills to nano
                return LocalTime.ofNanoOfDay(millisOfDay * 1_000_000L).toString();
            }
            // get number of milliseconds of the day
            return TemporalConversions.toLocalTime(dbzObj).toString();
        } catch (DateTimeException ex) {
            LOG.warn("Unable to convert to time, default {}", dbzObj);
            return dbzObj.toString();
        }
    }
}

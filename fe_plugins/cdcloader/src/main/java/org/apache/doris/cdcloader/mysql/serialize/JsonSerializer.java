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

package org.apache.doris.cdcloader.mysql.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.Date;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cdc.debezium.utils.TemporalConversions;
import org.apache.flink.table.data.TimestampData;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonSerializer implements DorisRecordSerializer<SourceRecord, List<String>>{
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonSerializer.class);
    private static final String DELETE_SIGN_KEY = "__DORIS_DELETE_SIGN__";
    private static ObjectMapper objectMapper = new ObjectMapper();

    public JsonSerializer() {
    }

    @Override
    public List<String> serialize(SourceRecord record) throws IOException {
        List<String> rows = new ArrayList<>();
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        String table = value.getStruct(Envelope.FieldName.SOURCE).getString("table");
        Schema valueSchema = record.valueSchema();
        if(Envelope.Operation.DELETE.equals(op)){
            String deleteRow = extractBeforeRow(value, valueSchema);
            if(StringUtils.isNotEmpty(deleteRow)){
                rows.add(table + "|" + deleteRow);
            }
        }else if(Envelope.Operation.READ.equals(op)
            || Envelope.Operation.CREATE.equals(op)
            || Envelope.Operation.UPDATE.equals(op)){
            String insertRow = extractAfterRow(value, valueSchema);
            if(StringUtils.isNotEmpty(insertRow)){
                rows.add(table + "|" + insertRow);
            }
        }
        return rows;
    }

    private String extractAfterRow(Struct value, Schema valueSchema) throws JsonProcessingException {
        Map<String, Object> record = new HashMap<>();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        if(after == null){
            return null;
        }
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        afterSchema.fields().forEach(field -> {
            Object valueConverted = convert(field.schema(), after.getWithoutDefault(field.name()));
            record.put(field.name(), valueConverted);
        });
        record.put(DELETE_SIGN_KEY, 0);
        System.out.println(record);
        return objectMapper.writeValueAsString(record);
    }

    private String extractBeforeRow(Struct value, Schema valueSchema) throws JsonProcessingException {
        Map<String, Object> record = new HashMap<>();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        if(before == null){
            return null;
        }
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        beforeSchema.fields().forEach(field -> {
            Object valueConverted = convert(field.schema(), before.getWithoutDefault(field.name()));
            record.put(field.name(), valueConverted);
        });
        record.put(DELETE_SIGN_KEY, 1);
        return objectMapper.writeValueAsString(record);
    }

    private Object convert(Schema fieldSchema, Object dbzObj){
        if(dbzObj == null){
            return null;
        }
        String name = fieldSchema.name();
        Schema.Type type = fieldSchema.type();
        if(StringUtils.isEmpty(name)){
            switch (type){
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
                case BYTES:
                    return dbzObj.toString();
                default:
                    LOG.warn("Unsupported type: {}, transform value to string", type);
                    return dbzObj.toString();
            }
        }else {
            switch (name){
                case Date.SCHEMA_NAME:
                    return TemporalConversions.toLocalDate(dbzObj).toString();
                case Timestamp.SCHEMA_NAME:
                case MicroTimestamp.SCHEMA_NAME:
                case NanoTimestamp.SCHEMA_NAME:
                    return convertTimestamp(name, dbzObj);
                case ZonedTimestamp.SCHEMA_NAME:
                    return convertZoneTimestamp(dbzObj);
                case Decimal.LOGICAL_NAME:
                    return convertDecimal(dbzObj, fieldSchema);
                default:
                    LOG.warn("Unsupported type: {} with name {}, transform value to string", type, name);
                    return dbzObj.toString();
            }
        }
    }

    private Object convertZoneTimestamp(Object dbzObj) {
        if (dbzObj instanceof String) {
            String str = (String) dbzObj;
            // TIMESTAMP_LTZ type is encoded in string type
            Instant instant = Instant.parse(str);
            return TimestampData.fromLocalDateTime(
                LocalDateTime.ofInstant(instant, ZoneId.systemDefault())).toTimestamp().toString();
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
                    return TimestampData.fromEpochMillis(
                        micro / 1000, (int) (micro % 1000 * 1000)).toTimestamp().toString();
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) dbzObj;
                    return TimestampData.fromEpochMillis(
                        nano / 1000_000, (int) (nano % 1000_000)).toTimestamp().toString();
            }
        }
        LocalDateTime localDateTime =
            TemporalConversions.toLocalDateTime(dbzObj, ZoneId.systemDefault());
        return java.sql.Timestamp.valueOf(localDateTime);
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
                SpecialValueDecimal decimal =
                    VariableScaleDecimal.toLogical((Struct) dbzObj);
                bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
            } else {
                // fallback to string
                bigDecimal = new BigDecimal(dbzObj.toString());
            }
        }
        return bigDecimal;
    }

}

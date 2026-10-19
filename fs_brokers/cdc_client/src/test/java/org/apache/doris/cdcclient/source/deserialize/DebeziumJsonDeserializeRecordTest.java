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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.common.Constants;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;

/**
 * Unit tests for the full {@link DebeziumJsonDeserializer#deserialize} path: operation handling
 * (create/update/delete), the {@code __DORIS_DELETE_SIGN} marker, column exclusion, and the
 * non-data-change short-circuit. Records are built from an in-memory Debezium envelope, so no
 * database is required.
 */
class DebeziumJsonDeserializeRecordTest {

    private static final String DELETE_SIGN = Constants.DORIS_DELETE_SIGN;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Schema rowSchema =
            SchemaBuilder.struct()
                    .name("t1.Value")
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("amount", Decimal.builder(2).optional().build())
                    .build();

    private final Schema sourceSchema =
            SchemaBuilder.struct().name("source").field("table", Schema.STRING_SCHEMA).build();

    private final Envelope envelope =
            Envelope.defineSchema()
                    .withName("t1.Envelope")
                    .withRecord(rowSchema)
                    .withSource(sourceSchema)
                    .build();

    @Test
    void create_emitsAfterRowWithDeleteSignZero() throws Exception {
        DebeziumJsonDeserializer deserializer = newDeserializer(new HashMap<>());
        SourceRecord record =
                record(envelopeValue(Envelope.Operation.CREATE, null, row(1, "alice", "12.34")));

        DeserializeResult result = deserializer.deserialize(new HashMap<>(), record);

        assertEquals(DeserializeResult.Type.DML, result.getType());
        assertEquals(1, result.getRecords().size());
        JsonNode row = MAPPER.readTree(result.getRecords().get(0));
        assertEquals(1, row.get("id").asInt());
        assertEquals("alice", row.get("name").asText());
        assertEquals(0, new BigDecimal("12.34").compareTo(row.get("amount").decimalValue()));
        assertEquals(0, row.get(DELETE_SIGN).asInt());
    }

    @Test
    void update_emitsAfterRowWithDeleteSignZero() throws Exception {
        DebeziumJsonDeserializer deserializer = newDeserializer(new HashMap<>());
        SourceRecord record =
                record(
                        envelopeValue(
                                Envelope.Operation.UPDATE,
                                row(1, "old", "1.00"),
                                row(1, "new", "2.00")));

        DeserializeResult result = deserializer.deserialize(new HashMap<>(), record);

        JsonNode row = MAPPER.readTree(result.getRecords().get(0));
        assertEquals("new", row.get("name").asText());
        assertEquals(0, row.get(DELETE_SIGN).asInt());
    }

    @Test
    void delete_emitsBeforeRowWithDeleteSignOne() throws Exception {
        DebeziumJsonDeserializer deserializer = newDeserializer(new HashMap<>());
        SourceRecord record =
                record(envelopeValue(Envelope.Operation.DELETE, row(7, "bob", "9.99"), null));

        DeserializeResult result = deserializer.deserialize(new HashMap<>(), record);

        assertEquals(1, result.getRecords().size());
        JsonNode row = MAPPER.readTree(result.getRecords().get(0));
        assertEquals(7, row.get("id").asInt());
        assertEquals(1, row.get(DELETE_SIGN).asInt());
    }

    @Test
    void excludeColumns_dropsConfiguredColumn() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("table.t1.exclude_columns", "name");
        DebeziumJsonDeserializer deserializer = newDeserializer(config);

        SourceRecord record =
                record(envelopeValue(Envelope.Operation.CREATE, null, row(1, "alice", "12.34")));
        DeserializeResult result = deserializer.deserialize(new HashMap<>(), record);

        JsonNode row = MAPPER.readTree(result.getRecords().get(0));
        assertFalse(row.has("name"));
        assertTrue(row.has("id"));
    }

    @Test
    void nonDataChangeRecord_returnsEmpty() throws Exception {
        DebeziumJsonDeserializer deserializer = newDeserializer(new HashMap<>());
        // A record whose value schema has no "op" field is not a data-change record.
        SourceRecord record = new SourceRecord(null, null, "t1", sourceSchema, source("t1"));

        DeserializeResult result = deserializer.deserialize(new HashMap<>(), record);

        assertEquals(DeserializeResult.Type.EMPTY, result.getType());
    }

    // ─── helpers ───────────────────────────────────────────────────────────────

    private DebeziumJsonDeserializer newDeserializer(Map<String, String> config) {
        DebeziumJsonDeserializer deserializer = new DebeziumJsonDeserializer();
        deserializer.init(config);
        return deserializer;
    }

    private SourceRecord record(Struct value) {
        return new SourceRecord(null, null, "t1", envelope.schema(), value);
    }

    private Struct envelopeValue(Envelope.Operation op, Struct before, Struct after) {
        Struct value = new Struct(envelope.schema());
        value.put(Envelope.FieldName.OPERATION, op.code());
        if (before != null) {
            value.put(Envelope.FieldName.BEFORE, before);
        }
        if (after != null) {
            value.put(Envelope.FieldName.AFTER, after);
        }
        value.put(Envelope.FieldName.SOURCE, source("t1"));
        return value;
    }

    private Struct source(String table) {
        return new Struct(sourceSchema).put("table", table);
    }

    private Struct row(int id, String name, String amount) {
        return new Struct(rowSchema)
                .put("id", id)
                .put("name", name)
                .put("amount", new BigDecimal(amount));
    }
}

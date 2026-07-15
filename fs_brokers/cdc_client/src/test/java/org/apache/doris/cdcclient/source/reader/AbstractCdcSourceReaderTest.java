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

package org.apache.doris.cdcclient.source.reader;

import org.apache.doris.cdcclient.source.deserialize.DebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.reader.mysql.MySqlSourceReader;
import org.apache.doris.cdcclient.source.reader.postgres.PostgresSourceReader;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

class AbstractCdcSourceReaderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void serializeTableSchemasPropagatesSerializationFailure() {
        PostgresSourceReader reader = new PostgresSourceReader();
        Map<TableId, TableChanges.TableChange> invalidSchemas = new HashMap<>();
        invalidSchemas.put(new TableId(null, "public", "events"), null);
        reader.setTableSchemas(invalidSchemas);

        assertThrows(RuntimeException.class, reader::serializeTableSchemas);
    }

    @Test
    void setTableSchemasKeepsReaderAndSerializerBaselineInSync() {
        PostgresSourceReader reader = new PostgresSourceReader();
        Map<TableId, TableChanges.TableChange> schemas = new HashMap<>();

        reader.setTableSchemas(schemas);

        DebeziumJsonDeserializer serializer =
                (DebeziumJsonDeserializer) reader.getSerializer();
        assertSame(schemas, reader.getTableSchemas());
        assertSame(schemas, serializer.getTableSchemas());
    }

    @Test
    void releaseStaysBaseImplSoReplicationSlotIsKept() throws Exception {
        // release must stay the base impl (close drops the slot, release must not) so a reschedule keeps the slot.
        Method pgRelease = PostgresSourceReader.class.getMethod("release", JobBaseConfig.class);
        assertEquals(AbstractCdcSourceReader.class, pgRelease.getDeclaringClass());
        Method mysqlRelease = MySqlSourceReader.class.getMethod("release", JobBaseConfig.class);
        assertEquals(AbstractCdcSourceReader.class, mysqlRelease.getDeclaringClass());
    }

    @Test
    void convertBoundsRestoresDateFromString() {
        Object[] raw = new Object[] {"2025-01-06"};
        Object[] out = AbstractCdcSourceReader.convertBounds(raw, Date.class, MAPPER);
        assertTrue(out[0] instanceof Date);
        assertEquals(Date.valueOf("2025-01-06"), out[0]);
    }

    @Test
    void convertBoundsRestoresTimestampFromString() {
        Object[] raw = new Object[] {"2025-01-06 12:34:56"};
        Object[] out = AbstractCdcSourceReader.convertBounds(raw, Timestamp.class, MAPPER);
        assertTrue(out[0] instanceof Timestamp);
        assertEquals(Timestamp.valueOf("2025-01-06 12:34:56"), out[0]);
    }

    @Test
    void convertBoundsRestoresLongFromJsonInteger() {
        // Jackson typically deserializes JSON integer to Integer; restore as Long for int8 cols.
        Object[] raw = new Object[] {123456};
        Object[] out = AbstractCdcSourceReader.convertBounds(raw, Long.class, MAPPER);
        assertTrue(out[0] instanceof Long);
        assertEquals(123456L, out[0]);
    }

    @Test
    void convertBoundsRestoresBigDecimalFromString() {
        Object[] raw = new Object[] {"12.34"};
        Object[] out = AbstractCdcSourceReader.convertBounds(raw, BigDecimal.class, MAPPER);
        assertTrue(out[0] instanceof BigDecimal);
        assertEquals(new BigDecimal("12.34"), out[0]);
    }

    @Test
    void convertBoundsPreservesStringForVarcharColumns() {
        Object[] raw = new Object[] {"event_id_42"};
        Object[] out = AbstractCdcSourceReader.convertBounds(raw, String.class, MAPPER);
        assertEquals("event_id_42", out[0]);
    }

    @Test
    void convertBoundsReturnsNullForNullInput() {
        assertNull(AbstractCdcSourceReader.convertBounds(null, Date.class, MAPPER));
    }

    @Test
    void convertBoundsKeepsNullElement() {
        Object[] raw = new Object[] {null};
        Object[] out = AbstractCdcSourceReader.convertBounds(raw, Date.class, MAPPER);
        assertEquals(1, out.length);
        assertNull(out[0]);
    }

    @Test
    void convertBoundsHandlesMultiElementArray() {
        Object[] raw = new Object[] {"2025-01-06", null};
        Object[] out = AbstractCdcSourceReader.convertBounds(raw, Date.class, MAPPER);
        assertEquals(2, out.length);
        assertEquals(Date.valueOf("2025-01-06"), out[0]);
        assertNull(out[1]);
    }

    @Test
    void restoresDateChunkKeyAfterFeRoundTrip() {
        Map<String, Object> feOffset = new HashMap<>();
        feOffset.put("splitId", "public.events:0");
        feOffset.put("tableId", "public.events");
        feOffset.put("splitKey", Arrays.asList("event_date"));
        feOffset.put("splitStart", null);
        feOffset.put("splitEnd", Arrays.asList("2025-01-06"));

        SnapshotSplit deserialized = MAPPER.convertValue(feOffset, SnapshotSplit.class);
        assertEquals(String.class, deserialized.getSplitEnd()[0].getClass());

        Object[] restored =
                AbstractCdcSourceReader.convertBounds(
                        deserialized.getSplitEnd(), Date.class, MAPPER);
        assertEquals(Date.class, restored[0].getClass());
        assertEquals(Date.valueOf("2025-01-06"), restored[0]);
    }

    @Test
    void restoresTimestampChunkKeyAfterFeRoundTrip() {
        Map<String, Object> feOffset = new HashMap<>();
        feOffset.put("splitId", "public.orders:7");
        feOffset.put("tableId", "public.orders");
        feOffset.put("splitKey", Arrays.asList("created_at"));
        feOffset.put("splitStart", Arrays.asList("2025-01-06 00:00:00"));
        feOffset.put("splitEnd", Arrays.asList("2025-01-07 00:00:00"));

        SnapshotSplit deserialized = MAPPER.convertValue(feOffset, SnapshotSplit.class);

        Object[] restoredStart =
                AbstractCdcSourceReader.convertBounds(
                        deserialized.getSplitStart(), Timestamp.class, MAPPER);
        Object[] restoredEnd =
                AbstractCdcSourceReader.convertBounds(
                        deserialized.getSplitEnd(), Timestamp.class, MAPPER);
        assertEquals(Timestamp.class, restoredStart[0].getClass());
        assertEquals(Timestamp.valueOf("2025-01-06 00:00:00"), restoredStart[0]);
        assertEquals(Timestamp.valueOf("2025-01-07 00:00:00"), restoredEnd[0]);
    }

    @Test
    void restoresBigintChunkKeyAfterFeRoundTrip() {
        Map<String, Object> feOffset = new HashMap<>();
        feOffset.put("splitId", "public.orders:0");
        feOffset.put("tableId", "public.orders");
        feOffset.put("splitKey", Arrays.asList("id"));
        feOffset.put("splitStart", Arrays.asList(100));
        feOffset.put("splitEnd", Arrays.asList(200));

        SnapshotSplit deserialized = MAPPER.convertValue(feOffset, SnapshotSplit.class);

        Object[] restoredStart =
                AbstractCdcSourceReader.convertBounds(
                        deserialized.getSplitStart(), Long.class, MAPPER);
        Object[] restoredEnd =
                AbstractCdcSourceReader.convertBounds(
                        deserialized.getSplitEnd(), Long.class, MAPPER);
        assertEquals(Long.class, restoredStart[0].getClass());
        assertEquals(100L, restoredStart[0]);
        assertEquals(200L, restoredEnd[0]);
    }
}

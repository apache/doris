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

package org.apache.doris.connector.fluss;

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The handle is the connector's contract with the engine, and the SPI declares it {@link
 * java.io.Serializable}. These pin the two ways that contract can break silently.
 */
public class FlussTableHandleTest {

    private static FlussTableHandle handle(String tableName, long tableId, int schemaId) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("table.datalake.enabled", "true");
        properties.put("table.datalake.paimon.warehouse", "/tmp/lake");
        Map<String, DataType> keyTypes = new LinkedHashMap<>();
        keyTypes.put("dt", DataTypes.STRING());
        keyTypes.put("id", DataTypes.INT());
        return new FlussTableHandle("db", tableName, tableId, schemaId, true,
                Arrays.asList("dt", "id"), Collections.singletonList("id"), 4,
                Collections.singletonList("dt"), true, "paimon", properties, keyTypes);
    }

    @Test
    public void survivesJavaSerializationWithEveryFieldIntact() {
        // A field that fails to serialize does not fail loudly here — it comes back null and the plan
        // built from it is silently wrong (no lake configuration, zero buckets), so every field the
        // scan side reads is checked on the far side of the round trip.
        FlussTableHandle restored = roundTrip(handle("pk_table", 7L, 3));

        Assertions.assertEquals("db", restored.getDatabaseName());
        Assertions.assertEquals("pk_table", restored.getTableName());
        Assertions.assertEquals(7L, restored.getTableId());
        Assertions.assertEquals(3, restored.getSchemaId());
        Assertions.assertTrue(restored.hasPrimaryKey());
        Assertions.assertEquals(Arrays.asList("dt", "id"), restored.getPrimaryKeys());
        Assertions.assertEquals(Collections.singletonList("id"), restored.getBucketKeys());
        Assertions.assertEquals(4, restored.getBucketCount());
        Assertions.assertEquals(Collections.singletonList("dt"), restored.getPartitionKeys());
        Assertions.assertTrue(restored.isPartitioned());
        Assertions.assertTrue(restored.isDataLakeEnabled());
        Assertions.assertEquals("paimon", restored.getDataLakeFormat());
        Assertions.assertEquals("/tmp/lake", restored.getProperties().get("table.datalake.paimon.warehouse"));
        // The key column types decide whether the table can be read as its lake plus its log at all, and
        // they are the one part of the schema the handle carries, so they have to survive the trip too.
        Assertions.assertEquals(DataTypes.INT(), restored.getKeyColumnTypes().get("id"));
        Assertions.assertEquals(DataTypes.STRING(), restored.getKeyColumnTypes().get("dt"));
        Assertions.assertEquals(handle("pk_table", 7L, 3), restored);
    }

    /**
     * A bucket's rows are keyed by the primary key MINUS the partition columns: a bucket lives inside one
     * partition, so the partition columns are the same for every row in it and carrying them would make
     * the key wider than it is. Order follows the primary key's own.
     */
    @Test
    public void physicalPrimaryKeyDropsThePartitionColumns() {
        Assertions.assertEquals(Collections.singletonList("id"),
                handle("t", 1L, 1).getPhysicalPrimaryKeys());
    }

    @Test
    public void identityIsTheTableAtItsSchemaVersion() {
        Assertions.assertEquals(handle("t", 1L, 1), handle("t", 1L, 1));
        Assertions.assertEquals(handle("t", 1L, 1).hashCode(), handle("t", 1L, 1).hashCode());

        // A different schema version describes a different set of columns; treating the two as the same
        // handle would let a cached value from before an ALTER answer for the table after it.
        Assertions.assertNotEquals(handle("t", 1L, 1), handle("t", 1L, 2));
        // A table dropped and recreated under the same name is a different table.
        Assertions.assertNotEquals(handle("t", 1L, 1), handle("t", 2L, 1));
        Assertions.assertNotEquals(handle("t", 1L, 1), handle("other", 1L, 1));
    }

    /**
     * The segment is part of identity because the two segments hold different rows. If it were not, any
     * cache keyed by handle would let {@code tbl}'s entry answer for {@code tbl$log} — returning the whole
     * table under a name that promised only the part outside the lake.
     */
    @Test
    public void identityAlsoCoversWhichSegmentTheHandleReads() {
        Assertions.assertNotEquals(handle("t", 1L, 1), handle("t", 1L, 1).asLogOnly());
        Assertions.assertNotEquals(handle("t", 1L, 1).hashCode(),
                handle("t", 1L, 1).asLogOnly().hashCode());
        Assertions.assertEquals(handle("t", 1L, 1).asLogOnly(), handle("t", 1L, 1).asLogOnly());
    }

    @Test
    public void readingTheLogTailKeepsEveryFactAboutTheTable() {
        FlussTableHandle base = handle("t", 1L, 1);
        FlussTableHandle logOnly = base.asLogOnly();

        Assertions.assertFalse(base.isLogOnly());
        Assertions.assertTrue(logOnly.isLogOnly());
        Assertions.assertEquals(FlussTableHandle.ReadMode.LOG_ONLY, logOnly.getReadMode());
        // Same table, read differently: everything planning reads about the table itself has to be the
        // same object-for-object, or the two halves would be planned against two different tables.
        Assertions.assertEquals(base.getDatabaseName(), logOnly.getDatabaseName());
        Assertions.assertEquals(base.getTableName(), logOnly.getTableName());
        Assertions.assertEquals(base.getTableId(), logOnly.getTableId());
        Assertions.assertEquals(base.getSchemaId(), logOnly.getSchemaId());
        Assertions.assertEquals(base.hasPrimaryKey(), logOnly.hasPrimaryKey());
        Assertions.assertEquals(base.getPrimaryKeys(), logOnly.getPrimaryKeys());
        Assertions.assertEquals(base.getBucketKeys(), logOnly.getBucketKeys());
        Assertions.assertEquals(base.getBucketCount(), logOnly.getBucketCount());
        Assertions.assertEquals(base.getPartitionKeys(), logOnly.getPartitionKeys());
        Assertions.assertEquals(base.isDataLakeEnabled(), logOnly.isDataLakeEnabled());
        Assertions.assertEquals(base.getDataLakeFormat(), logOnly.getDataLakeFormat());
        Assertions.assertEquals(base.getProperties(), logOnly.getProperties());
        Assertions.assertEquals(base.getKeyColumnTypes(), logOnly.getKeyColumnTypes());
        // Idempotent: the engine may resolve the same sub-table more than once per statement.
        Assertions.assertSame(logOnly, logOnly.asLogOnly());
    }

    /**
     * The segment travels to the BE-facing plan inside the handle, so it has to survive the same round
     * trip as everything else. Coming back as DEFAULT would turn a log-tail scan into a full-table one.
     */
    @Test
    public void theSegmentSurvivesSerialization() {
        Assertions.assertTrue(roundTrip(handle("t", 1L, 1).asLogOnly()).isLogOnly());
        Assertions.assertFalse(roundTrip(handle("t", 1L, 1)).isLogOnly());
    }

    private static FlussTableHandle roundTrip(FlussTableHandle handle) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(handle);
            }
            try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                return (FlussTableHandle) in.readObject();
            }
        } catch (Exception e) {
            throw new AssertionError("the handle must be serializable", e);
        }
    }
}

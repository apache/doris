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

package org.apache.doris.tools.ssb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

class SsbConnectorTest {
    private final Path dbgen = Path.of(System.getProperty("ssb.test.dbgen"));

    @TempDir
    Path temporary;

    @Test
    void metadataAndPluginDiscovery() throws Exception {
        Plugin plugin = ServiceLoader.load(Plugin.class).stream()
                .map(ServiceLoader.Provider::get).filter(p -> p instanceof SsbPlugin).findFirst().orElseThrow();
        var factory = plugin.getConnectorFactories().iterator().next();
        Assertions.assertEquals("ssb", factory.getName());
        Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create("ssb", Map.of("command", "anything"), null));

        var connector = new SsbConnector(dbgen);
        ConnectorMetadata metadata = connector.getMetadata(null, SsbConnector.Transaction.INSTANCE);
        Assertions.assertEquals(List.of("sf1", "sf100", "sf1000"), metadata.listSchemaNames(null));
        Assertions.assertEquals(15, metadata.listTables(null, Optional.empty()).size());
        Assertions.assertTrue(metadata.listTables(null, Optional.of("sf2")).isEmpty());
        Assertions.assertNull(metadata.getTableHandle(null, new SchemaTableName("sf2", "customer")));
        Assertions.assertNull(metadata.getTableHandle(null, new SchemaTableName("sf1", "unknown")));
        var handle = metadata.getTableHandle(null, new SchemaTableName("sf1", "lineorder"));
        Assertions.assertNotNull(handle);
        Assertions.assertEquals(17, metadata.getColumnHandles(null, handle).size());
        try (var splits = connector.getSplitManager().getSplits(SsbConnector.Transaction.INSTANCE, null, handle,
                DynamicFilter.EMPTY, Constraint.alwaysTrue())) {
            Assertions.assertEquals(10, splits.getNextBatch(100).get().getSplits().size());
            Assertions.assertTrue(splits.isFinished());
        }
    }

    @Test
    void handlesRoundTripBetweenFrontendAndBackend() throws Exception {
        // Trino disables Jackson's automatic field/getter/constructor discovery.
        ObjectMapper mapper = new ObjectMapperProvider().get();
        for (Object handle : List.of(new SsbConnector.Table("sf1000", "lineorder"),
                new SsbConnector.Column(16), new SsbConnector.Split(10, 10), SsbConnector.Transaction.INSTANCE)) {
            Assertions.assertEquals(handle, mapper.readValue(mapper.writeValueAsBytes(handle), handle.getClass()));
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> new SsbConnector.Table("sf1; anything", "lineorder"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new SsbConnector.Split(0, 10));
    }

    @Test
    void projectionPreservesColumnOrderAndTypes() {
        RecordSet records = new SsbRecordSet(dbgen, new SsbConnector.Table("sf1", "customer"),
                new SsbConnector.Split(1, 1), List.of(7, 0, 1));
        try (RecordCursor cursor = records.cursor()) {
            Assertions.assertTrue(cursor.advanceNextPosition());
            Assertions.assertEquals("BUILDING", cursor.getSlice(0).toStringUtf8());
            Assertions.assertEquals(1, cursor.getLong(1));
            Assertions.assertEquals("Customer#000000001", cursor.getSlice(2).toStringUtf8());
            Assertions.assertFalse(cursor.isNull(0));
            Assertions.assertTrue(cursor.getCompletedBytes() > 0);
            Assertions.assertTrue(cursor.getReadTimeNanos() > 0);
        }
    }

    @Test
    void dimensionsMatchTheExistingGenerator() throws Exception {
        // SHA-256 of the existing tool's individual-table SF1 output (TZ=UTC), including
        // its trailing pipe and newline. Check all rows, not just a generated sample.
        verifyTable(SsbTable.CUSTOMER, 30000, "e84ecfcde0f73bbc48f710ffe1c6d340321e875171013b272cbccee45f1d3cbc");
        verifyTable(SsbTable.PART, 200000, "9fb468e8a3e6ca9f5b6f841cf5a81276315fd241fbfdb35c8839328729bb816b");
        verifyTable(SsbTable.SUPPLIER, 2000, "90b636621dc449bedce76f347af5802701db6f6334d209f002d73f4044cf9404");
        verifyTable(SsbTable.DATES, 2556, "7057acc5ebc0b92225a4937488028767f46e9f8719126a8e29d4bf164e5461d1");
    }

    private void verifyTable(SsbTable table, long expectedRows, String expectedDigest) throws Exception {
        verifyTable(table, 1, expectedRows, expectedDigest);
    }

    @Test
    void lineorderMatchesTheExistingTenPartitionLoad() throws Exception {
        verifyTable(SsbTable.LINEORDER, 10, 6001215,
                "67243ab009a55fe0a848a15404233789b0b48ff11ae913f85e22957a7751c7e8");
    }

    private void verifyTable(SsbTable table, int partitions, long expectedRows, String expectedDigest) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long rows = 0;
        for (int part = 1; part <= partitions; part++) {
            RecordSet records = new SsbRecordSet(dbgen, new SsbConnector.Table("sf1", table.tableName()),
                    new SsbConnector.Split(part, partitions), IntStream.range(0, table.columns.size()).boxed().toList());
            try (RecordCursor cursor = records.cursor()) {
                while (cursor.advanceNextPosition()) {
                    StringBuilder row = new StringBuilder();
                    for (int i = 0; i < table.columns.size(); i++) {
                        row.append(cursor.getType(i).equals(BigintType.BIGINT)
                                ? Long.toString(cursor.getLong(i)) : cursor.getSlice(i).toStringUtf8()).append('|');
                    }
                    digest.update(row.append('\n').toString().getBytes(StandardCharsets.US_ASCII));
                    rows++;
                }
            }
        }
        Assertions.assertEquals(expectedRows, rows, table.tableName());
        Assertions.assertEquals(expectedDigest, HexFormat.of().formatHex(digest.digest()), table.tableName());
    }

    @Test
    void lineorderPartitionsAndLargeKeys() {
        // The last partition starts after 90% of the orders. Exercise SF1000 without
        // materializing billions of rows, including order keys beyond signed INT.
        try (RecordCursor cursor = new SsbRecordSet(dbgen, new SsbConnector.Table("sf1000", "lineorder"),
                new SsbConnector.Split(10, 10), List.of(0, 1, 2, 3, 4)).cursor()) {
            Assertions.assertTrue(cursor.advanceNextPosition());
            Assertions.assertEquals(5400000001L, cursor.getLong(0));
            Assertions.assertEquals(1, cursor.getLong(1));
            Assertions.assertTrue(cursor.getLong(2) <= 30000000);
            Assertions.assertTrue(cursor.getLong(4) <= 2000000);
        }
    }

    @Test
    void nonzeroExitIsNotTreatedAsEndOfData() throws Exception {
        Path failingGenerator = temporary.resolve("dbgen");
        Files.writeString(failingGenerator, "#!/bin/sh\nexit 23\n");
        Assertions.assertTrue(failingGenerator.toFile().setExecutable(true));
        try (RecordCursor cursor = new SsbRecordSet(failingGenerator, new SsbConnector.Table("sf1", "customer"),
                new SsbConnector.Split(1, 1), List.of(0)).cursor()) {
            Assertions.assertTrue(Assertions.assertThrows(IllegalStateException.class, cursor::advanceNextPosition)
                    .getMessage().contains("23"));
        }
    }

    @Test
    void closingACursorStopsItsProducer() throws Exception {
        var childPidsBefore = ProcessHandle.current().children().map(ProcessHandle::pid).toList();
        RecordCursor cursor = new SsbRecordSet(dbgen, new SsbConnector.Table("sf1000", "lineorder"),
                new SsbConnector.Split(1, 10), List.of(0)).cursor();
        Assertions.assertTrue(cursor.advanceNextPosition());
        var producer = ProcessHandle.current().children()
                .filter(child -> !childPidsBefore.contains(child.pid())).findFirst().orElseThrow();
        cursor.close();
        producer.onExit().get(5, TimeUnit.SECONDS);
        Assertions.assertFalse(producer.isAlive());
        cursor.close();
        Assertions.assertFalse(cursor.advanceNextPosition());
    }
}

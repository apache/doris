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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TDeltaLakeDeletionVectorDesc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.deletionvectors.Base85Codec;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

class DeltaKernelCompatTest {
    @TempDir
    Path warehouse;

    @Test
    void resolvesAllDvStorageTypesOffsetsPaddingAndZeroCardinality() throws Exception {
        createTable();
        UUID uuid = UUID.fromString("00000000-0000-0000-0000-000000000123");
        append("relative", "u", "sub/dir" + Base85Codec.encodeUUID(uuid), 7, 4, 3);
        append("absolute", "p", "s3a://bucket/prefix/dv.bin", 9, 4, 3);
        append("offsetless", "p", "s3a://bucket/prefix/dv.bin", null, 4, 3);
        byte[] payload = {1, 2, 3};
        append("inline", "i", Base85Codec.encodeBytes(payload), null, 3, 3);
        append("empty", "i", "", null, 0, 0);
        try (DeltaLakeFileIO io = new DeltaLakeFileIO(new Configuration(), new RecordingConnectorContext())) {
            Engine engine = DefaultEngine.create(io);
            Snapshot snapshot = TableManager.loadSnapshot(warehouse.resolve("db/table").toString()).build(engine);
            Map<String, Optional<TDeltaLakeDeletionVectorDesc>> descriptors = new HashMap<>();
            DeltaLakeScanFiles.forEach(snapshot, engine, true, row -> {
                String path = DeltaKernelCompat.fileStatus(row).getPath();
                descriptors.put(path.substring(path.lastIndexOf('/') + 1),
                        DeltaKernelCompat.deletionVector(row, snapshot.getPath()));
                Assertions.assertEquals(10, DeltaKernelCompat.numRecords(row).orElseThrow());
            });
            TDeltaLakeDeletionVectorDesc relative = descriptors.get("relative").orElseThrow();
            Assertions.assertTrue(relative.getPath().endsWith("/db/table/sub/dir/deletion_vector_" + uuid + ".bin"));
            Assertions.assertEquals(7, relative.getOffset());
            Assertions.assertEquals(4, relative.getSizeInBytes());
            Assertions.assertEquals(3, relative.getCardinality());
            Assertions.assertEquals("s3a://bucket/prefix/dv.bin", descriptors.get("absolute").orElseThrow().getPath());
            Assertions.assertEquals(9, descriptors.get("absolute").orElseThrow().getOffset());
            Assertions.assertEquals(0, descriptors.get("offsetless").orElseThrow().getOffset());
            Assertions.assertArrayEquals(payload, descriptors.get("inline").orElseThrow().getInlineData());
            Assertions.assertFalse(descriptors.get("inline").orElseThrow().isSetPath());
            Assertions.assertTrue(descriptors.get("empty").isEmpty());
        }
        TestingConnectorSession session = new TestingConnectorSession("dv-count");
        try (Connector connector = new DeltaLakeConnectorProvider().create(Map.of("warehouse", warehouse.toString()),
                new RecordingConnectorContext()); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "table").orElseThrow();
            Assertions.assertEquals(38, connector.getScanPlanProvider().planScan(session,
                    ConnectorScanRequest.builder(handle, Collections.emptyList()).countPushdown(true).build())
                    .get(0).getPushDownRowCount());
        }
    }

    @Test
    void rejectsDeletionCardinalityLargerThanTheDataFileRowCount() throws Exception {
        createTable();
        append("invalid", "p", "s3a://bucket/dv.bin", 0, 4, 11);
        TestingConnectorSession session = new TestingConnectorSession("invalid-count");
        try (Connector connector = new DeltaLakeConnectorProvider().create(Map.of("warehouse", warehouse.toString()),
                new RecordingConnectorContext()); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "table").orElseThrow();
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> connector.getScanPlanProvider().canServeMetadataOnlyCount(session, handle, Optional.empty()));
        }
    }

    private void createTable() throws Exception {
        Path table = warehouse.resolve("db/table");
        DeltaLakeTestTables.commit(table, 0, "dv-table", new StructType().add("id", LongType.LONG));
        Path log = table.resolve("_delta_log/00000000000000000000.json");
        String contents = Files.readString(log);
        String protocol = new ObjectMapper().writeValueAsString(Map.of("protocol", Map.of("minReaderVersion", 3,
                "minWriterVersion", 7, "readerFeatures", new String[] {"deletionVectors"},
                "writerFeatures", new String[] {"deletionVectors"})));
        Files.writeString(log, protocol + contents.substring(contents.indexOf('\n')));
    }

    private void append(String name, String storage, String path, Integer offset, int size, long cardinality)
            throws Exception {
        Map<String, Object> dv = new HashMap<>(Map.of("storageType", storage, "pathOrInlineDv", path,
                "sizeInBytes", size, "cardinality", cardinality));
        if (offset != null) {
            dv.put("offset", offset);
        }
        String add = new ObjectMapper().writeValueAsString(Map.of("add", Map.of("path", name, "size", 10,
                "partitionValues", Collections.emptyMap(), "modificationTime", 1L, "dataChange", true,
                "stats", "{\"numRecords\":10}", "deletionVector", dv)));
        Files.writeString(warehouse.resolve("db/table/_delta_log/00000000000000000000.json"),
                add + "\n", StandardOpenOption.APPEND);
    }
}

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

import org.apache.doris.thrift.TDeltaLakeDeletionVectorDesc;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

class DeltaKernelSnapshotTest {
    @Test
    void replaysClassicCheckpointWithDeletionVectors() throws Exception {
        assertSnapshot("dv-partitioned-with-checkpoint", 15, 10, 15, 37, 2);
    }

    @Test
    void readsBothV2CheckpointRepresentationsIncludingSidecars() throws Exception {
        assertSnapshot("v2-checkpoint-json", 2, 2, 4, 10, 0);
        assertSnapshot("v2-checkpoint-parquet", 2, 2, 4, 10, 0);
    }

    @Test
    void confinesKernelInternalImportsToCompatibilityClass() throws Exception {
        Path source = Path.of("src/main/java/org/apache/doris/connector/deltalake");
        try (Stream<Path> files = Files.walk(source)) {
            for (Path file : (Iterable<Path>) files.filter(path -> path.toString().endsWith(".java"))::iterator) {
                if (!file.getFileName().toString().equals("DeltaKernelCompat.java")) {
                    Assertions.assertFalse(Files.readString(file).contains("io.delta.kernel.internal"),
                            "Kernel internal dependency outside DeltaKernelCompat: " + file);
                }
            }
        }
    }

    private void assertSnapshot(String table, long version, long checkpoint, long files,
            long records, long deleted) throws Exception {
        try (DeltaLakeFileIO io = new DeltaLakeFileIO(new Configuration(), new RecordingConnectorContext())) {
            assertSnapshot(DefaultEngine.create(io), table, version, checkpoint, files, records, deleted);
        }
    }

    private void assertSnapshot(Engine engine, String table, long version, long checkpoint, long files,
            long records, long deleted) throws Exception {
        Path path = Path.of(getClass().getResource("/deltalake/" + table).toURI());
        Snapshot snapshot = TableManager.loadSnapshot(path.toString()).build(engine);
        DeltaLakeFeatureCheck.check(snapshot);
        Assertions.assertEquals(version, snapshot.getVersion());
        Assertions.assertEquals(Optional.of(checkpoint), snapshot.getLogSegment().getCheckpointVersionOpt());
        long fileCount = 0;
        long rowCount = 0;
        long dvCount = 0;
        try (CloseableIterator<FilteredColumnarBatch> batches =
                snapshot.getScanBuilder().build().getScanFiles(engine, true)) {
            while (batches.hasNext()) {
                try (CloseableIterator<Row> rows = batches.next().getRows()) {
                    while (rows.hasNext()) {
                        Row file = rows.next();
                        Assertions.assertTrue(DeltaKernelCompat.fileStatus(file).getSize() > 0);
                        rowCount += DeltaKernelCompat.numRecords(file).orElseThrow();
                        Optional<TDeltaLakeDeletionVectorDesc> dv =
                                DeltaKernelCompat.deletionVector(file, path.toString());
                        dvCount += dv.map(TDeltaLakeDeletionVectorDesc::getCardinality).orElse(0L);
                        fileCount++;
                    }
                }
            }
        }
        Assertions.assertEquals(files, fileCount);
        Assertions.assertEquals(records, rowCount);
        Assertions.assertEquals(deleted, dvCount);
    }
}

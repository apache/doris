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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Run with Paimon 2.1.0 core/format and dependencies on the classpath. */
public class GenerateVectorFixture {
    public static void main(String[] args) throws Exception {
        java.nio.file.Path output = java.nio.file.Paths.get(args[0]);
        generate(output, false);
        generate(output.resolve("nullable"), true);
    }

    private static void generate(java.nio.file.Path output, boolean nullable) throws Exception {
        java.nio.file.Path temporary = Files.createTempDirectory("paimon-vector-fixture-");
        try (IOManager ioManager = IOManager.create(temporary.resolve("spill").toString())) {
            Path tablePath = new Path(temporary.toUri());
            LocalFileIO io = LocalFileIO.create();
            new FileSystemSchemaManager(io, tablePath).createTable(Schema.newBuilder()
                    .column("id", DataTypes.INT().notNull())
                    .column("embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()).copy(nullable))
                    .primaryKey("id").option("bucket", "1").option("merge-engine", "first-row")
                    .option("write-only", "false").option("commit.force-compact", "true")
                    .option("target-file-size", "1 b").option("compaction.force-rewrite-all-files", "true")
                    .option("file.format", "parquet")
                    .option("deletion-vectors.enabled", "false")
                    .option("scan.manifest.parallelism", "1").build());
            FileStoreTable table = FileStoreTableFactory.create(io, tablePath);
            for (int start : new int[] {0, 1024}) {
                BatchWriteBuilder builder = table.newBatchWriteBuilder();
                try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
                    write.withIOManager(ioManager);
                    for (int id = start; id < start + 1024; id++) {
                        write.write(GenericRow.of(id, nullable && id == 5 ? null
                                : BinaryVector.fromPrimitiveArray(new float[] {id, 0})));
                    }
                    commit.commit(write.prepareCommit());
                }
            }
            List<DataFileMeta> files = new ArrayList<>();
            List<Split> planned = FileStoreTableFactory.create(io, tablePath)
                    .newReadBuilder().newScan().plan().splits();
            for (Split split : planned) {
                files.addAll(((DataSplit) split).dataFiles());
            }
            if (files.size() != 4 || files.stream().anyMatch(file -> file.level() == 0)) {
                throw new IllegalStateException("Expected four compacted files, got " + files);
            }
            Files.createDirectories(output.resolve("bucket-0"));
            for (DataFileMeta file : files) {
                Files.copy(temporary.resolve("bucket-0").resolve(file.fileName()),
                        output.resolve("bucket-0").resolve(file.fileName()));
            }
            // The native search resolves the pinned snapshot even with preplanned bucket work.
            // Keep the SDK-generated metadata rather than synthesizing a snapshot by hand.
            for (String directory : new String[] {"snapshot", "manifest", "schema"}) {
                java.nio.file.Path root = temporary.resolve(directory);
                if (!Files.exists(root)) {
                    continue;
                }
                try (java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(root)) {
                    for (java.nio.file.Path source : (Iterable<java.nio.file.Path>) paths::iterator) {
                        java.nio.file.Path destination = output.resolve(temporary.relativize(source));
                        if (Files.isDirectory(source)) {
                            Files.createDirectories(destination);
                        } else {
                            Files.copy(source, destination);
                        }
                    }
                }
            }
            Map<String, String> options = new HashMap<>(table.schema().options());
            options.remove("path");
            options.put("pk-vector.index.columns", "embedding");
            options.put("fields.embedding.pk-vector.index.type", "ivf-rq");
            options.put("fields.embedding.pk-vector.distance.metric", "l2");
            options.put("vector-index.search-mode", "full");
            TableSchema schema = table.schema().copy(options);
            Files.writeString(output.resolve("schema.json"), schema.toString());
            options.put("fields.embedding.pk-vector.distance.metric", "inner_product");
            Files.writeString(output.resolve("schema-dot.json"), table.schema().copy(options).toString());
            DataSplit data = DataSplit.builder().withSnapshot(((DataSplit) planned.get(0)).snapshotId())
                    .withPartition(BinaryRow.EMPTY_ROW).withBucket(0).withBucketPath("__BUCKET__")
                    .withTotalBuckets(1).withDataFiles(files)
                    .rawConvertible(((DataSplit) planned.get(0)).rawConvertible()).build();
            for (String name : new String[] {"full", "ranges", "empty"}) {
                Map<String, List<Range>> ranges = new HashMap<>();
                for (DataFileMeta file : files) {
                    List<Range> allowed;
                    if (name.equals("empty")) {
                        allowed = Collections.emptyList();
                    } else if (name.equals("ranges")) {
                        boolean firstFile = file.minKey().getInt(0) == 0;
                        allowed = Collections.singletonList(new Range(firstFile ? 4 : 1, firstFile ? 6 : 2));
                    } else {
                        allowed = Collections.singletonList(new Range(0, file.rowCount() - 1));
                    }
                    ranges.put(file.fileName(), allowed);
                }
                BucketVectorSearchSplit split = new BucketVectorSearchSplit(data, Collections.emptyList(), ranges);
                DataOutputSerializer bytes = new DataOutputSerializer(256);
                split.serialize(bytes);
                Files.write(output.resolve(name + ".bin"), bytes.getCopyOfBuffer());
            }
        } finally {
            try (java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(temporary)) {
                for (java.nio.file.Path path : (Iterable<java.nio.file.Path>) paths
                        .sorted(java.util.Comparator.reverseOrder())::iterator) {
                    Files.delete(path);
                }
            }
        }
    }
}

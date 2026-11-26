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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

/** Adds duplicate position-delete files for Doris external Iceberg regression tests. */
public final class CreateIcebergDuplicatePositionDeleteFixture {
    private static final String CATALOG_NAME = "demo";

    private CreateIcebergDuplicatePositionDeleteFixture() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "Usage: CreateIcebergDuplicatePositionDeleteFixture "
                            + "<namespace> <table> <comma-separated-positions>");
        }

        Catalog catalog = loadCatalog();
        Table table = catalog.loadTable(TableIdentifier.of(args[0], args[1]));
        if (!table.spec().isUnpartitioned()) {
            throw new IllegalStateException("Duplicate position-delete fixture needs an unpartitioned table");
        }

        DataFile dataFile = onlyDataFile(table);
        if (dataFile.recordCount() != 4L) {
            throw new IllegalStateException("Expected one 4-row data file, got "
                    + dataFile.recordCount() + " rows in " + dataFile.path());
        }
        if (dataFile.splitOffsets() != null && dataFile.splitOffsets().size() > 1) {
            throw new IllegalStateException("Expected one Parquet row group, got split offsets "
                    + dataFile.splitOffsets());
        }

        List<Long> positions = parsePositions(args[2]);
        for (long position : positions) {
            if (position < 0 || position >= dataFile.recordCount()) {
                throw new IllegalArgumentException("Position " + position
                        + " is outside data file row count " + dataFile.recordCount());
            }
            addPositionDelete(table, dataFile, position);
            table.refresh();
        }

        System.out.println("Created duplicate position-delete fixture for " + args[0] + "." + args[1]
                + " dataFile=" + dataFile.path() + " positions=" + positions);
    }

    private static Catalog loadCatalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "rest");
        properties.put("uri", "http://rest:8181");
        properties.put("warehouse", "s3://warehouse/wh/");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", "http://minio:9000");
        properties.put("s3.path-style-access", "true");
        properties.put("s3.region", "us-east-1");
        return CatalogUtil.buildIcebergCatalog(CATALOG_NAME, properties, null);
    }

    private static DataFile onlyDataFile(Table table) throws IOException {
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> plannedTasks = table.newScan().planFiles()) {
            for (FileScanTask task : plannedTasks) {
                tasks.add(task);
            }
        }

        if (tasks.size() != 1) {
            throw new IllegalStateException("Expected exactly one data file, got " + tasks.size());
        }
        if (!tasks.get(0).deletes().isEmpty()) {
            throw new IllegalStateException("Fixture table must not have existing delete files");
        }
        return tasks.get(0).file();
    }

    private static List<Long> parsePositions(String rawPositions) {
        List<Long> positions = new ArrayList<>();
        for (String rawPosition : rawPositions.split(",")) {
            String trimmed = rawPosition.trim();
            if (!trimmed.isEmpty()) {
                positions.add(Long.parseLong(trimmed));
            }
        }
        if (positions.isEmpty()) {
            throw new IllegalArgumentException("At least one position must be provided");
        }
        return positions;
    }

    private static void addPositionDelete(Table table, DataFile dataFile, long position) throws IOException {
        String dataPath = dataFile.path().toString();
        String deletePath = table.location() + "/data/duplicate-position-delete-"
                + position + "-" + System.nanoTime() + ".parquet";
        OutputFile output = table.io().newOutputFile(deletePath);
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(output)
                .forTable(table)
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(table.spec())
                .overwrite()
                .buildPositionWriter();
        try {
            writer.write(PositionDelete.<Record>create().set(dataPath, position));
        } finally {
            writer.close();
        }

        DeleteFile deleteFile = writer.toDeleteFile();
        if (deleteFile.recordCount() != 1L) {
            throw new IllegalStateException("Expected one-row delete file, got "
                    + deleteFile.recordCount() + " rows in " + deleteFile.path());
        }

        table.newRowDelta()
                .validateFromSnapshot(table.currentSnapshot().snapshotId())
                .validateDataFilesExist(Collections.singletonList(dataPath))
                .addDeletes(deleteFile)
                .commit();
    }
}

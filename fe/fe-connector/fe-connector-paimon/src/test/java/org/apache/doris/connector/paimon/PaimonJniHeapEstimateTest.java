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

package org.apache.doris.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PaimonJniHeapEstimateTest {

    private static final long MB = 1024L * 1024;

    // 128 MB row groups, 64 MB stripes, four columns a file (4 MB of dictionaries).
    private final PaimonJniHeapEstimate estimate = new PaimonJniHeapEstimate(128 * MB, 64 * MB, 4);

    /**
     * The split that ran a 2 GB heap out sixteen at a time: an uncompacted primary-key bucket whose 130 MB
     * file overlaps a 12 MB one. Merged, both are open at once.
     */
    @Test
    public void mergedSplitHoldsARowGroupOfEveryFile() {
        DataSplit split = split(false, file("data-0.parquet", 130 * MB), file("data-1.parquet", 12 * MB));
        Assertions.assertEquals((130 + 4) * MB + (12 + 4) * MB, estimate.bytesOf(split));
    }

    @Test
    public void splitThatNeedsNoMergingHoldsItsLargestFile() {
        DataSplit split = split(true, file("data-0.parquet", 130 * MB), file("data-1.parquet", 12 * MB));
        Assertions.assertEquals((130 + 4) * MB, estimate.bytesOf(split));
    }

    /** Moving on to the next row group, a file has the old one and the new one in the heap together. */
    @Test
    public void fileOfMoreThanOneRowGroupHoldsTwoAtMost() {
        Assertions.assertEquals((200 + 4) * MB, estimate.bytesOf(split(false, file("data-0.parquet", 200 * MB))));
        Assertions.assertEquals((256 + 4) * MB, estimate.bytesOf(split(false, file("data-0.parquet", 900 * MB))));
    }

    @Test
    public void anOrcFileHoldsItsStripe() {
        Assertions.assertEquals((128 + 4) * MB, estimate.bytesOf(split(false, file("data-0.ORC", 300 * MB))));
        Assertions.assertEquals((40 + 4) * MB, estimate.bytesOf(split(false, file("data-0.orc", 40 * MB))));
    }

    /**
     * The row group comes from the table's options with the precedence paimon's writers apply, and every
     * file has a dictionary for each column, the keys a second time beside the sequence and row kind.
     */
    @Test
    public void theTableOptionsSetTheRowGroupAndTheColumns(@TempDir Path warehouse) throws Exception {
        // (id INT, v STRING, w BIGINT) keyed by id: 3 + 1 + 2 = 6 columns, 6 MB of dictionaries a file.
        DataSplit oneBigFile = split(false, file("data-0.parquet", 100 * MB));
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Assertions.assertEquals((100 + 6) * MB, PaimonJniHeapEstimate.of(
                    table(catalog, "defaults", Collections.emptyMap())).bytesOf(oneBigFile));
            Assertions.assertEquals((64 + 6) * MB, PaimonJniHeapEstimate.of(
                    table(catalog, "parquet_option", Collections.singletonMap("parquet.block.size",
                            String.valueOf(32 * MB)))).bytesOf(oneBigFile));
            // file.block-size wins over the format's own option.
            Map<String, String> both = new HashMap<>();
            both.put("file.block-size", "16 mb");
            both.put("parquet.block.size", String.valueOf(32 * MB));
            Assertions.assertEquals((32 + 6) * MB, PaimonJniHeapEstimate.of(
                    table(catalog, "block_size", both)).bytesOf(oneBigFile));
        }
    }

    private static Table table(Catalog catalog, String name, Map<String, String> options) throws Exception {
        Identifier id = Identifier.create("db", name);
        catalog.createTable(id, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("v", DataTypes.STRING())
                .column("w", DataTypes.BIGINT())
                .primaryKey("id")
                .option("bucket", "1")
                .options(options)
                .build(), false);
        return catalog.getTable(id);
    }

    private static DataFileMeta file(String name, long size) {
        return DataFileMeta.forAppend(name, size, 1000L, SimpleStats.EMPTY_STATS, 0L, 0L, 0L,
                Collections.emptyList(), null, null, null, null, null, null);
    }

    private static DataSplit split(boolean rawConvertible, DataFileMeta... files) {
        return DataSplit.builder()
                .withSnapshot(1L)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withDataFiles(Arrays.asList(files))
                .rawConvertible(rawConvertible)
                .build();
    }
}

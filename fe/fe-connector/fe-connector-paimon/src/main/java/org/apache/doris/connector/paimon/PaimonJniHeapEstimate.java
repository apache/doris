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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongFunction;

/**
 * The JVM heap BE's JNI reader of a paimon {@link DataSplit} holds, declared to BE's JNI heap gate
 * ({@code TFileRangeDesc.jni_heap_bytes}) for a statement that sets {@code enable_jni_heap_admission}.
 *
 * <p>A JNI read of a split whose files overlap - a primary-key table written to since it was last
 * compacted - merges them: paimon opens one file of every sorted run of a section at once, and an open
 * parquet file keeps the compressed column chunks of its current row group in the heap until it moves
 * on to the next, when the old and the new are there together for a moment. ORC keeps a stripe the
 * same way. Sixteen such splits read at once are what runs a 2 GB heap out. So a split that has to be
 * merged holds, steadily, the sum over its files of one row group - two for a file that has more than one
 * - plus a dictionary page for every column of every file. That takes all the files as one section,
 * which is exact for the uncompacted tables that need the gate and over the mark for files that do not
 * overlap, which are read one section after another. A split that needs no merging reads its files one
 * after another and holds one file's share.
 *
 * <p>A file's row groups and columns are those of the schema version it was written under
 * ({@link DataFileMeta#schemaId()}): paimon's writers take the row group size from the table's options,
 * and an ALTER TABLE that changes them, or the columns, leaves the files written before it as they are.
 * A write job can also pass the row group size as a dynamic option, which no schema version records; a
 * file written so is taken by its schema version's options, and declared below what it holds when the
 * job's row groups were larger.
 *
 * <p>That steady hold is what is declared, not everything a merge ever holds: while it decodes, its column
 * batches add brief peaks on top, measured at about three times the declaration for a split of a 126 MB
 * file read on its own. The peaks are left to the heap outside the budget - the other half of it with
 * jni_scanner_heap_budget_ratio at its default - which has room for some of the admitted readers to peak
 * at once but not for all of them; a read that needs more fails as it does with enable_jni_heap_admission
 * off.
 *
 * <p>Every column is taken as read. A narrow projection reads less, but a merge also reads every key
 * column, the sequence number and the row kind whatever is projected, and declaring less than a merge
 * holds steadily would let the gate admit more readers than its budget fits.
 */
final class PaimonJniHeapEstimate {

    // What paimon's writers fall back to when neither file.block-size nor the format's own option is
    // set (CoreOptions.FILE_BLOCK_SIZE).
    static final long DEFAULT_PARQUET_ROW_GROUP_BYTES = 128L * 1024 * 1024;
    static final long DEFAULT_ORC_STRIPE_BYTES = 64L * 1024 * 1024;
    // The largest dictionary page paimon's parquet writer keeps for a column.
    static final long DICTIONARY_BYTES_PER_COLUMN = 1024L * 1024;

    // How the files written under a schema version are laid out, by its id, each worked out once.
    private final LongFunction<FileLayout> layoutOfSchema;
    private final Map<Long, FileLayout> layouts = new HashMap<>();

    PaimonJniHeapEstimate(LongFunction<FileLayout> layoutOfSchema) {
        this.layoutOfSchema = layoutOfSchema;
    }

    /**
     * For the splits of {@code table}, each file laid out by the schema version it was written under:
     * the table's own is at hand, and {@code schemaAt} reads the others by id.
     */
    static PaimonJniHeapEstimate of(FileStoreTable table, LongFunction<TableSchema> schemaAt) {
        TableSchema own = table.schema();
        return new PaimonJniHeapEstimate(
                schemaId -> FileLayout.of(schemaId == own.id() ? own : schemaAt.apply(schemaId)));
    }

    /** For a table whose schema versions are not at hand: every file laid out by the options it shows. */
    static PaimonJniHeapEstimate of(Table table) {
        FileLayout layout = FileLayout.of(table.options(), table.rowType().getFieldCount(),
                table.primaryKeys().size());
        return new PaimonJniHeapEstimate(schemaId -> layout);
    }

    long bytesOf(DataSplit split) {
        long total = 0;
        long largest = 0;
        for (DataFileMeta file : split.dataFiles()) {
            long share = layouts.computeIfAbsent(file.schemaId(), layoutOfSchema::apply).bytesHeld(file);
            total += share;
            largest = Math.max(largest, share);
        }
        return split.rawConvertible() ? largest : total;
    }

    /** The row group size and the columns of the files written under one schema version. */
    static final class FileLayout {
        private final long parquetRowGroupBytes;
        private final long orcStripeBytes;
        private final long dictionaryBytes;

        FileLayout(long parquetRowGroupBytes, long orcStripeBytes, int columns) {
            this.parquetRowGroupBytes = parquetRowGroupBytes;
            this.orcStripeBytes = orcStripeBytes;
            this.dictionaryBytes = columns * DICTIONARY_BYTES_PER_COLUMN;
        }

        static FileLayout of(TableSchema schema) {
            return of(schema.options(), schema.fields().size(), schema.primaryKeys().size());
        }

        static FileLayout of(Map<String, String> options, int fields, int primaryKeys) {
            // The precedence paimon's writers apply: file.block-size for every format, else the format's
            // own option, else its default.
            String blockSize = options.get(CoreOptions.FILE_BLOCK_SIZE.key());
            long parquet = bytes(blockSize != null ? blockSize : options.get("parquet.block.size"),
                    DEFAULT_PARQUET_ROW_GROUP_BYTES);
            long orc = bytes(blockSize != null ? blockSize : options.get("orc.stripe.size"),
                    DEFAULT_ORC_STRIPE_BYTES);
            // A primary-key table's data file stores the keys a second time, as _KEY_ columns, beside the
            // sequence number and the row kind.
            return new FileLayout(parquet, orc, fields + (primaryKeys > 0 ? primaryKeys + 2 : 0));
        }

        // A row group - two for a file that has more than one - and a dictionary page for every column.
        long bytesHeld(DataFileMeta file) {
            long rowGroup = file.fileName().toLowerCase(Locale.ROOT).endsWith(".orc")
                    ? orcStripeBytes
                    : parquetRowGroupBytes;
            long rowGroups = file.fileSize() <= rowGroup
                    ? file.fileSize()
                    : Math.min(file.fileSize(), 2 * rowGroup);
            return rowGroups + dictionaryBytes;
        }

        private static long bytes(String value, long defaultBytes) {
            return value == null ? defaultBytes : MemorySize.parse(value).getBytes();
        }
    }
}

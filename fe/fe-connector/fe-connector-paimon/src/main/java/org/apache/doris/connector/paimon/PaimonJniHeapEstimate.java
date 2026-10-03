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
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;

import java.util.Locale;
import java.util.Map;

/**
 * The JVM heap BE's JNI reader of a paimon {@link DataSplit} holds, declared to BE's JNI heap gate
 * ({@code TFileRangeDesc.jni_heap_bytes}) for a statement that sets {@code enable_jni_heap_admission}.
 *
 * <p>A JNI read of a split whose files overlap - a primary-key table written to since it was last
 * compacted - merges them: paimon opens one file of every sorted run of a section at once, and an open
 * parquet file keeps the compressed column chunks of its current row group in the heap until it moves
 * on to the next, when the old and the new are there together for a moment. ORC keeps a stripe the
 * same way. Sixteen such splits read at once are what runs a 2 GB heap out. So a split that has to be
 * merged holds at most the sum over its files of one row group - two for a file that has more than one
 * - plus a dictionary page for every column of every file. That takes all the files as one section,
 * which is exact for the uncompacted tables that need the gate and over the mark for files that do not
 * overlap, which are read one section after another. A split that needs no merging reads its files one
 * after another and holds one file's share.
 *
 * <p>Every column is taken as read. A narrow projection reads less, but a merge also reads every key
 * column, the sequence number and the row kind whatever is projected, and an estimate below what the
 * reader holds is the one mistake the gate cannot absorb.
 */
final class PaimonJniHeapEstimate {

    // What paimon's writers fall back to when neither file.block-size nor the format's own option is
    // set (CoreOptions.FILE_BLOCK_SIZE).
    static final long DEFAULT_PARQUET_ROW_GROUP_BYTES = 128L * 1024 * 1024;
    static final long DEFAULT_ORC_STRIPE_BYTES = 64L * 1024 * 1024;
    // The largest dictionary page paimon's parquet writer keeps for a column.
    static final long DICTIONARY_BYTES_PER_COLUMN = 1024L * 1024;

    private final long parquetRowGroupBytes;
    private final long orcStripeBytes;
    private final long dictionaryBytesPerFile;

    PaimonJniHeapEstimate(long parquetRowGroupBytes, long orcStripeBytes, int columnsPerFile) {
        this.parquetRowGroupBytes = parquetRowGroupBytes;
        this.orcStripeBytes = orcStripeBytes;
        this.dictionaryBytesPerFile = columnsPerFile * DICTIONARY_BYTES_PER_COLUMN;
    }

    static PaimonJniHeapEstimate of(Table table) {
        Map<String, String> options = table.options();
        // The precedence paimon's writers apply: file.block-size for every format, else the format's
        // own option, else its default.
        String blockSize = options.get(CoreOptions.FILE_BLOCK_SIZE.key());
        long parquet = bytes(blockSize != null ? blockSize : options.get("parquet.block.size"),
                DEFAULT_PARQUET_ROW_GROUP_BYTES);
        long orc = bytes(blockSize != null ? blockSize : options.get("orc.stripe.size"),
                DEFAULT_ORC_STRIPE_BYTES);
        // A primary-key table's data file stores the keys a second time, as _KEY_ columns, beside the
        // sequence number and the row kind.
        int keys = table.primaryKeys().size();
        int columns = table.rowType().getFieldCount() + (keys > 0 ? keys + 2 : 0);
        return new PaimonJniHeapEstimate(parquet, orc, columns);
    }

    long bytesOf(DataSplit split) {
        long total = 0;
        long largest = 0;
        for (DataFileMeta file : split.dataFiles()) {
            long share = rowGroupsHeld(file) + dictionaryBytesPerFile;
            total += share;
            largest = Math.max(largest, share);
        }
        return split.rawConvertible() ? largest : total;
    }

    private long rowGroupsHeld(DataFileMeta file) {
        long rowGroup = file.fileName().toLowerCase(Locale.ROOT).endsWith(".orc")
                ? orcStripeBytes
                : parquetRowGroupBytes;
        return file.fileSize() <= rowGroup ? file.fileSize() : Math.min(file.fileSize(), 2 * rowGroup);
    }

    private static long bytes(String value, long defaultBytes) {
        return value == null ? defaultBytes : MemorySize.parse(value).getBytes();
    }
}

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
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongFunction;

/**
 * The JVM heap BE's JNI reader of a paimon split holds, declared to BE's JNI heap gate
 * ({@code TFileRangeDesc.jni_heap_bytes}) for a statement that sets {@code enable_jni_heap_admission}.
 *
 * <p>A JNI read of a {@link DataSplit} whose files overlap - a primary-key table written to since it was last
 * compacted - merges them: paimon opens one file of every sorted run of a section at once, and an open
 * parquet file keeps the compressed column chunks of its current row group in the heap until it moves
 * on to the next, when the old and the new are there together for a moment. ORC keeps a stripe the
 * same way. Sixteen such splits read at once are what runs a 2 GB heap out. So a split that has to be
 * merged holds, steadily, the sum over its files of one row group - two for a file that has more than one
 * - plus a dictionary page for every column of every file, a file storing a column for every primitive
 * value a field nests. That takes all the files as one section, which is exact for the uncompacted tables
 * that need the gate and over the mark for files that do not overlap, which are read one section after
 * another. A split that needs no merging reads its files one after another and holds one file's share -
 * unless a file of a primary-key table in it was written before paimon counted deleted rows: paimon plans
 * such files as needing no merging, but merges them when it reads them, as they may hold deletes.
 *
 * <p>A diff read between two snapshots ({@code @incr} with {@code incrementalBetweenScanMode} diff) plans an
 * {@link IncrementalSplit} instead, of the files only the earlier snapshot has and those only the later one
 * has. Of a primary-key table it merges each of the two and compares the merges, which are open together,
 * so it holds what one merge of all those files holds; of an append table it reads the later snapshot's
 * files one after another. The other splits that are not DataSplits, those of the metadata system tables
 * and of format tables, hold little and declare nothing.
 *
 * <p>A file's row groups, dictionary pages and columns are those of the schema version it was written under
 * ({@link DataFileMeta#schemaId()}): paimon's writers take the row group size and the largest parquet
 * dictionary page ({@code parquet.dictionary.page.size}) from the table's options, and an ALTER TABLE that
 * changes them, or the columns, leaves the files written before it as they are.
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
    // The largest dictionary page paimon's parquet writer keeps for a column unless the table's
    // parquet.dictionary.page.size says otherwise (parquet's DEFAULT_DICTIONARY_PAGE_SIZE); an ORC file is
    // allowed as much for each of its columns.
    static final long DEFAULT_DICTIONARY_PAGE_BYTES = 1024L * 1024;

    // A primary-key table's diff read merges its files, as does its read of files that do not count their
    // deleted rows; an append table's reads them one after another.
    private final boolean primaryKeyTable;
    // How the files written under a schema version are laid out, by its id, each worked out once.
    private final LongFunction<FileLayout> layoutOfSchema;
    private final Map<Long, FileLayout> layouts = new HashMap<>();

    PaimonJniHeapEstimate(boolean primaryKeyTable, LongFunction<FileLayout> layoutOfSchema) {
        this.primaryKeyTable = primaryKeyTable;
        this.layoutOfSchema = layoutOfSchema;
    }

    /**
     * For the splits of {@code table}, each file laid out by the schema version it was written under:
     * the table's own is at hand, and {@code schemaAt} reads the others by id.
     */
    static PaimonJniHeapEstimate of(FileStoreTable table, LongFunction<TableSchema> schemaAt) {
        TableSchema own = table.schema();
        return new PaimonJniHeapEstimate(!own.primaryKeys().isEmpty(),
                schemaId -> FileLayout.of(schemaId == own.id() ? own : schemaAt.apply(schemaId)));
    }

    /** For a table whose schema versions are not at hand: every file laid out by the options it shows. */
    static PaimonJniHeapEstimate of(Table table) {
        FileLayout layout = FileLayout.of(table.options(), FileLayout.columns(table.rowType()),
                table.primaryKeys().size());
        return new PaimonJniHeapEstimate(!table.primaryKeys().isEmpty(), schemaId -> layout);
    }

    long bytesOf(Split split) {
        if (split instanceof DataSplit) {
            DataSplit dataSplit = (DataSplit) split;
            return readsFileByFile(dataSplit) ? largest(dataSplit.dataFiles()) : total(dataSplit.dataFiles());
        }
        // A fallback-branch table wraps the splits that are not DataSplits; its FallbackDataSplit is one.
        Split read = split instanceof FallbackReadFileStoreTable.FallbackSplit
                ? ((FallbackReadFileStoreTable.FallbackSplit) split).wrapped()
                : split;
        if (!(read instanceof IncrementalSplit)) {
            return 0;
        }
        IncrementalSplit diff = (IncrementalSplit) read;
        return primaryKeyTable ? total(diff.beforeFiles()) + total(diff.afterFiles()) : largest(diff.afterFiles());
    }

    // Whether paimon reads the split's files one after another, as PrimaryKeyTableRawFileSplitReadProvider
    // decides: it was planned as needing no merging and, of a primary-key table, every file counts its deleted
    // rows. A file written before paimon counted them may hold deletes, so a split with such files is merged.
    private boolean readsFileByFile(DataSplit split) {
        return split.rawConvertible() && (!primaryKeyTable
                || split.dataFiles().stream().allMatch(file -> file.deleteRowCount().isPresent()));
    }

    private long total(List<DataFileMeta> files) {
        return files.stream().mapToLong(this::bytesHeld).sum();
    }

    private long largest(List<DataFileMeta> files) {
        return files.stream().mapToLong(this::bytesHeld).max().orElse(0);
    }

    private long bytesHeld(DataFileMeta file) {
        return layouts.computeIfAbsent(file.schemaId(), layoutOfSchema::apply).bytesHeld(file);
    }

    /** How the files written under one schema version are laid out: row groups, dictionaries, columns. */
    static final class FileLayout {
        private final long parquetRowGroupBytes;
        private final long orcStripeBytes;
        private final long parquetDictionaryBytes;
        private final long orcDictionaryBytes;

        FileLayout(long parquetRowGroupBytes, long orcStripeBytes, long parquetDictionaryPageBytes, int columns) {
            this.parquetRowGroupBytes = parquetRowGroupBytes;
            this.orcStripeBytes = orcStripeBytes;
            this.parquetDictionaryBytes = columns * parquetDictionaryPageBytes;
            this.orcDictionaryBytes = columns * DEFAULT_DICTIONARY_PAGE_BYTES;
        }

        static FileLayout of(TableSchema schema) {
            return of(schema.options(), columns(schema.logicalRowType()), schema.primaryKeys().size());
        }

        static FileLayout of(Map<String, String> options, int fieldColumns, int primaryKeys) {
            // The precedence paimon's writers apply: file.block-size for every format, else the format's
            // own option, else its default.
            String blockSize = options.get(CoreOptions.FILE_BLOCK_SIZE.key());
            long parquet = bytes(blockSize != null ? blockSize : options.get("parquet.block.size"),
                    DEFAULT_PARQUET_ROW_GROUP_BYTES);
            long orc = bytes(blockSize != null ? blockSize : options.get("orc.stripe.size"),
                    DEFAULT_ORC_STRIPE_BYTES);
            // paimon hands the table's parquet.* options to its parquet writer, this one as the largest
            // dictionary page it keeps for a column.
            long dictionaryPage = bytes(options.get("parquet.dictionary.page.size"), DEFAULT_DICTIONARY_PAGE_BYTES);
            // A primary-key table's data file stores the keys a second time, as _KEY_ columns, beside the
            // sequence number and the row kind.
            return new FileLayout(parquet, orc, dictionaryPage,
                    fieldColumns + (primaryKeys > 0 ? primaryKeys + 2 : 0));
        }

        // The columns a file stores for a field of this type, each with a dictionary of its own: one for every
        // primitive value it nests - every field of a row, an array's element, a map's key and value, a
        // multiset's element beside its count, a variant's value beside its metadata. A primary key is never
        // nested, so each _KEY_ column is one.
        static int columns(DataType type) {
            if (type instanceof RowType) {
                return ((RowType) type).getFields().stream().mapToInt(field -> columns(field.type())).sum();
            }
            if (type instanceof ArrayType) {
                return columns(((ArrayType) type).getElementType());
            }
            if (type instanceof MapType) {
                MapType map = (MapType) type;
                return columns(map.getKeyType()) + columns(map.getValueType());
            }
            if (type instanceof MultisetType) {
                return columns(((MultisetType) type).getElementType()) + 1;
            }
            return type instanceof VariantType ? 2 : 1;
        }

        // A row group - two for a file that has more than one - and a dictionary page for every column.
        long bytesHeld(DataFileMeta file) {
            boolean orc = file.fileName().toLowerCase(Locale.ROOT).endsWith(".orc");
            long rowGroup = orc ? orcStripeBytes : parquetRowGroupBytes;
            long rowGroups = file.fileSize() <= rowGroup
                    ? file.fileSize()
                    : Math.min(file.fileSize(), 2 * rowGroup);
            return rowGroups + (orc ? orcDictionaryBytes : parquetDictionaryBytes);
        }

        private static long bytes(String value, long defaultBytes) {
            return value == null ? defaultBytes : MemorySize.parse(value).getBytes();
        }
    }
}

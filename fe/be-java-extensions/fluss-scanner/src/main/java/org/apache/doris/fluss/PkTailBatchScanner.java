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

package org.apache.doris.fluss;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The log tail of one bucket of a primary-key table, returned as the state that tail ended in.
 *
 * <p>This is the fluss half of a union read: the lake holds the bucket up to the offset it was tiered
 * at, and this contributes what has happened to that bucket since. The change log cannot be returned
 * as it stands — it records every intermediate state, so a key written three times appears three
 * times — so the range is replayed by key first, exactly the way fluss's own bounded primary-key
 * reader reduces the log half of a snapshot read.
 *
 * <p><b>A delete leaves a tombstone rather than removing the key.</b> The two look interchangeable
 * from here, and are not: the lake rows this tail supersedes are hidden by a key set that BE's C++
 * side builds from the same offsets, and that set covers every key the tail touched — deleted ones
 * included. So a key deleted in the tail is already gone from the lake half, and what this reader owes
 * is only the keys that survived. Keeping the tombstone keeps that fact countable
 * ({@link #getTombstoneKeys()}) instead of indistinguishable from a key that was never written.
 *
 * <p><b>The key is the physical primary key</b> — the primary key minus the partition columns. Rows
 * are only ever compared within one bucket of one partition, where the partition columns are equal by
 * construction, and both halves of the union agree on that: fluss's own kv storage keys the same way.
 *
 * <p>The whole range is collected before anything is returned, which is what a merge of any shape
 * costs; {@code fluss.union.max_tail_rows} is what stops a tail that grew unbounded (tiering stopped,
 * say) from being read into a shared BE process without a limit.
 *
 * <p>Rows are held across polls, as fluss's own bounded reader does with the same records.
 */
class PkTailBatchScanner implements BatchScanner {

    private final BoundedLogRecords records;
    private final TableBucket tableBucket;
    private final long maxTailRows;

    /** Reads the key columns out of a scan row; reused, because each key is encoded on the spot. */
    private final ProjectedRow keyRow;
    private final KeyEncoder keyEncoder;

    /**
     * The replayed state: the last record of each key, in the order the key was first seen. A
     * {@code null} value is a tombstone — the key ended the range deleted.
     */
    private final Map<TailKey, InternalRow> state = new LinkedHashMap<>();

    private long recordsRead;
    private long tombstoneKeys;
    private boolean emitted;

    /**
     * @param projection      table field indexes the query asked for, in the order it wants them back;
     *                        may be empty, which a count-shaped query produces
     * @param logStartOffset  where the lake snapshot this tail follows left off
     * @param maxTailRows     how many change log records this range may hold before it is refused
     */
    PkTailBatchScanner(Table table, TableBucket tableBucket, int[] projection,
            long logStartOffset, long logStopOffset, long maxTailRows) {
        TableInfo tableInfo = table.getTableInfo();
        if (!tableInfo.hasPrimaryKey()) {
            // Replaying by key needs a key. A log table's records are rows in their own right and are
            // read as a LOG range; reaching here with one means the range was planned as the wrong kind.
            throw new IllegalArgumentException("a primary-key log tail was planned for "
                    + tableInfo.getTablePath() + ", which has no primary key");
        }
        this.tableBucket = tableBucket;
        this.maxTailRows = maxTailRows;

        List<String> primaryKeys = tableInfo.getPhysicalPrimaryKeys();
        RowType rowType = tableInfo.getRowType();
        // What to ask fluss for: the requested columns, plus any key column not among them. The key is
        // needed to replay whether or not the query selects it, and appending keeps the requested
        // columns at the front — so a returned row's first fields are the requested ones, in order, and
        // the row loop reads them positionally without knowing this reader added anything.
        List<Integer> scanProjection = new ArrayList<>(projection.length + primaryKeys.size());
        for (int index : projection) {
            scanProjection.add(index);
        }
        int[] keyIndexesInTable = new int[primaryKeys.size()];
        int[] keyIndexesInScanRow = new int[primaryKeys.size()];
        for (int i = 0; i < primaryKeys.size(); i++) {
            int indexInTable = rowType.getFieldIndex(primaryKeys.get(i));
            keyIndexesInTable[i] = indexInTable;
            int indexInScanRow = scanProjection.indexOf(indexInTable);
            if (indexInScanRow < 0) {
                scanProjection.add(indexInTable);
                indexInScanRow = scanProjection.size() - 1;
            }
            keyIndexesInScanRow[i] = indexInScanRow;
        }
        this.keyRow = ProjectedRow.from(keyIndexesInScanRow);
        // The table's own key encoder, chosen the way fluss chooses it for this table. Nothing outside
        // this JVM sees these bytes — they only have to tell two different keys apart, which is the one
        // thing every encoder this factory returns guarantees.
        this.keyEncoder = KeyEncoder.ofPrimaryKeyEncoder(
                Schema.getKeyRowType(tableInfo.getSchema(), keyIndexesInTable),
                primaryKeys, tableInfo.getTableConfig(), tableInfo.isDefaultBucketKey());

        int[] fields = new int[scanProjection.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = scanProjection.get(i);
        }
        this.records = new BoundedLogRecords(table, tableBucket, fields,
                logStartOffset, logStopOffset);
    }

    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (emitted) {
            return null;
        }
        if (!records.isFinished()) {
            for (ScanRecord record : records.poll(timeout)) {
                replay(record);
            }
            // An empty batch, not the end: the caller polls again. Only when the range has been read to
            // its end is there an answer to give.
            return CloseableIterator.emptyIterator();
        }
        emitted = true;
        List<InternalRow> surviving = new ArrayList<>();
        for (InternalRow row : state.values()) {
            if (row != null) {
                surviving.add(row);
            }
        }
        tombstoneKeys = state.size() - surviving.size();
        // The surviving rows are held by the list now; the keys that indexed them are the tail's whole
        // memory cost and nothing reads them again.
        state.clear();
        return CloseableIterator.wrap(surviving.iterator());
    }

    private void replay(ScanRecord record) {
        recordsRead++;
        if (recordsRead > maxTailRows) {
            throw new IllegalStateException("the log tail of bucket " + tableBucket.getBucket()
                    + " holds more than " + maxTailRows + " change log records, the limit set by '"
                    + "fluss.union_read.max_tail_rows'. Read the table as pure fluss with"
                    + " 'fluss.union_read.mode=disabled', wait for tiering to move the tail into the"
                    + " lake, or raise the limit");
        }
        InternalRow row = record.getRow();
        TailKey key = new TailKey(keyEncoder.encodeKey(keyRow.replaceRow(row)));
        switch (record.getChangeType()) {
            case INSERT:
            case UPDATE_AFTER:
                state.put(key, row);
                break;
            case DELETE:
            case UPDATE_BEFORE:
                state.put(key, null);
                break;
            default:
                // A primary-key table's change log holds no other kind. One appearing means fluss now
                // describes a change this reader has never replayed, and guessing at it would return a
                // row nobody can check.
                throw new IllegalStateException("the change log of a primary-key table produced a '"
                        + record.getChangeType() + "' record, which this reader does not know how to"
                        + " replay");
        }
    }

    /** How many change log records the tail held. */
    long getRecordsRead() {
        return recordsRead;
    }

    /**
     * How many keys the tail ended deleted, known once the range has been replayed. Those are the keys
     * whose lake rows disappear without anything being returned in their place.
     */
    long getTombstoneKeys() {
        return tombstoneKeys;
    }

    @Override
    public void close() throws IOException {
        records.close();
    }

    /** An encoded primary key, by value: {@code byte[]} is compared by identity on its own. */
    private static final class TailKey {

        private final byte[] encoded;
        private final int hash;

        TailKey(byte[] encoded) {
            this.encoded = encoded;
            this.hash = Arrays.hashCode(encoded);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TailKey && Arrays.equals(encoded, ((TailKey) other).encoded);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}

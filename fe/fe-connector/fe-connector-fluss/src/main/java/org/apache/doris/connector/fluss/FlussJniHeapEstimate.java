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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.scan.ConnectorScanRange;

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The JVM heap BE's JNI reader of a fluss primary-key range holds, declared to BE's JNI heap gate
 * ({@code TFileRangeDesc.jni_heap_bytes}) for a statement that sets {@code enable_jni_heap_admission}.
 *
 * <p>Two kinds of range keep rows in the heap until they close, and both get there before their first
 * batch. A PK_FULL range replays the change log after its kv snapshot into a map ordered by key, which it
 * then merges with the snapshot (SafeKvSnapshotAndLogBatchScanner); a key the log changed more than once
 * keeps its first row besides its last, because the map keeps the first key object and that points at the
 * first row. A PK_TAIL range keeps the last row of every key in its slice of the log (PkTailBatchScanner).
 * Every row is the deep copy fluss makes of a fetched record: a GenericRow of boxed fields. So a PK_FULL
 * range holds at most N x (R + 112) bytes and a PK_TAIL range N x (R + 96), N being the records between
 * its offsets - more than its keys - and R a row. A LOG range streams; a lake range is paimon's to declare.
 *
 * <p>R follows from the column types, but for strings and bytes, whose length nothing in fluss's metadata
 * gives: they are taken at {@link #DEFAULT_VARLEN_BYTES}.
 */
final class FlussJniHeapEstimate {

    // A key's entry: in PK_FULL's TreeMap with the ProjectedRow standing for the key, in PK_TAIL's
    // LinkedHashMap with the key encoded into a byte[].
    static final long PK_FULL_ENTRY_BYTES = 112;
    static final long PK_TAIL_ENTRY_BYTES = 96;
    static final long DEFAULT_VARLEN_BYTES = 64;

    private FlussJniHeapEstimate() {
    }

    /** A row of the fields at {@code fieldIndexes}: a GenericRow and its Object[], then every field. */
    static long rowBytes(RowType rowType, Collection<Integer> fieldIndexes) {
        long bytes = 16 + align8(16 + 4L * fieldIndexes.size());
        for (int index : fieldIndexes) {
            bytes += fieldBytes(rowType.getTypeAt(index));
        }
        return bytes;
    }

    static long fieldBytes(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                // Boolean and Byte hand out cached instances.
                return 0;
            case SMALLINT:
            case INTEGER:
            case FLOAT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return 16;
            case BIGINT:
            case DOUBLE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 24;
            case DECIMAL:
                // fluss's Decimal keeps the BigDecimal it was made from, and that its BigInteger.
                return 136;
            case BINARY:
            case BYTES:
                return align8(16 + DEFAULT_VARLEN_BYTES);
            default:
                // A string is a BinaryString over a MemorySegment over a byte[]; the nested types are
                // taken at the same size, being no better known.
                return 88 + align8(16 + DEFAULT_VARLEN_BYTES);
        }
    }

    /** {@code ranges} with every PK_FULL and PK_TAIL range declaring its heap; the rest as they were. */
    static List<ConnectorScanRange> declare(List<ConnectorScanRange> ranges, long rowBytes) {
        List<ConnectorScanRange> declared = new ArrayList<>(ranges.size());
        for (ConnectorScanRange range : ranges) {
            declared.add(range instanceof FlussScanRange ? declare((FlussScanRange) range, rowBytes) : range);
        }
        return declared;
    }

    private static FlussScanRange declare(FlussScanRange range, long rowBytes) {
        long entryBytes;
        switch (range.getRangeType()) {
            case PK_FULL:
                entryBytes = PK_FULL_ENTRY_BYTES;
                break;
            case PK_TAIL:
                entryBytes = PK_TAIL_ENTRY_BYTES;
                break;
            default:
                return range;
        }
        long start = Long.parseLong(range.getProperties().get(FlussScanRange.PROP_LOG_START_OFFSET));
        long stop = Long.parseLong(range.getProperties().get(FlussScanRange.PROP_LOG_STOP_OFFSET));
        // A bucket never snapshotted starts at the earliest-offset sentinel, which is below 0, and is
        // replayed from the first record its log still has.
        long records = stop - Math.max(start, 0);
        return records > 0 ? range.withJniHeapBytes(records * (rowBytes + entryBytes)) : range;
    }

    private static long align8(long bytes) {
        return (bytes + 7) & ~7L;
    }
}

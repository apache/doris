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
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * One bucket's log over {@code [logStartOffset, logStopOffset)}, polled in batches.
 *
 * <p>A fluss log scanner is a streaming reader with no end, so the bound has to be imposed here, and
 * reaching it has to be detected three ways — all of them taken from fluss's own bounded reader,
 * {@code KvSnapshotAndLogBatchScanner#pollLogRecords}:
 * <ul>
 *   <li>a record at or past the stopping offset is not ours — drop it and stop;</li>
 *   <li>after the record at {@code stopping - 1}, stop immediately. The record AT the stopping offset
 *       may never exist (it is where the log had got to, not a row), so polling on would block
 *       forever;</li>
 *   <li>if the fetch consumed up to the stopping offset without yielding a record there, stop too.
 *       That is the case the first two miss: the tail of the range can be control records, which
 *       occupy offsets but are never handed to a scanner.</li>
 * </ul>
 *
 * <p><b>This is the only place those three live.</b> Both readers of a bounded log range consume it —
 * {@link BoundedLogBatchScanner}, which wants the rows and throws the change type away, and
 * {@link PkTailBatchScanner}, which replays the change types by key. A second copy of the stopping
 * rules would be a second chance to get one of them wrong, and the one they exist for (the third)
 * only shows itself as a query that never returns.
 *
 * <p>Records are handed out as fluss returned them, and the caller may hold them past the next poll:
 * that is what fluss's own bounded primary-key reader does while it collects a whole range before
 * merging it.
 */
class BoundedLogRecords implements Closeable {

    private final LogScanner logScanner;
    private final TableBucket tableBucket;
    private final long logStopOffset;

    private boolean finished;

    /**
     * @param projection     table field indexes to read, in the order the caller wants them back; must
     *                       not be empty, which fluss rejects outright
     * @param logStartOffset a real offset, or fluss's {@code LogScanner.EARLIEST_OFFSET} sentinel
     */
    BoundedLogRecords(Table table, TableBucket tableBucket, int[] projection,
            long logStartOffset, long logStopOffset) {
        this.tableBucket = tableBucket;
        this.logStopOffset = logStopOffset;
        LogScanner scanner = table.newScan().project(projection).createLogScanner();
        try {
            Long partitionId = tableBucket.getPartitionId();
            if (partitionId == null) {
                scanner.subscribe(tableBucket.getBucket(), logStartOffset);
            } else {
                scanner.subscribe(partitionId, tableBucket.getBucket(), logStartOffset);
            }
        } catch (RuntimeException | Error e) {
            // The scanner is already running its fetcher threads; leaving it unreferenced would keep
            // them alive for the life of the BE process.
            try {
                scanner.close();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        this.logScanner = scanner;
    }

    /** Whether the range has been read to its end; polling after that returns nothing more. */
    boolean isFinished() {
        return finished;
    }

    /**
     * The next records of the range, in log order. May be empty while the fetch is still on its way,
     * which says nothing about whether the range is done — {@link #isFinished()} does.
     */
    List<ScanRecord> poll(Duration timeout) {
        if (finished) {
            return new ArrayList<>();
        }
        ScanRecords scanRecords = logScanner.poll(timeout);
        List<ScanRecord> records = new ArrayList<>();
        for (ScanRecord record : scanRecords.records(tableBucket)) {
            long offset = record.logOffset();
            if (offset >= logStopOffset) {
                // Past the end of this range: another query's rows, not ours.
                finished = true;
                break;
            }
            records.add(record);
            if (offset >= logStopOffset - 1) {
                // The last record of the range. Do not poll again: the record AT the stopping offset
                // may not exist, and waiting for it never returns.
                finished = true;
                break;
            }
        }
        Long consumedUpToOffset = scanRecords.consumedUpToOffset(tableBucket);
        if (consumedUpToOffset != null && consumedUpToOffset >= logStopOffset) {
            // The fetch reached the end of the range without necessarily yielding a record there — the
            // tail can be control records, which take offsets but are never scanned. Without this the
            // loop would poll for a row that is never coming.
            finished = true;
        }
        return records;
    }

    @Override
    public void close() throws IOException {
        try {
            logScanner.close();
        } catch (Exception e) {
            throw new IOException("Failed to close the fluss log scanner", e);
        }
    }
}

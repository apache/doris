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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * One bucket of a fluss log table over {@code [logStartOffset, logStopOffset)}, as a bounded scanner.
 *
 * <p>Every record of a log table is a row of it, so this is {@link BoundedLogRecords} with the change
 * type dropped — an append-only log has nothing to replay.
 *
 * <p>Shaped as a {@link BatchScanner} so that the log read and the primary-key reads
 * ({@code KvSnapshotAndLogBatchScanner} and {@link PkTailBatchScanner}) present the same interface to
 * {@link FlussJniScanner} — what separates them belongs here, not in the row loop.
 */
class BoundedLogBatchScanner implements BatchScanner {

    private final BoundedLogRecords records;

    /**
     * @param projection     table field indexes to read, in the order the caller wants them back; must
     *                       not be empty, which fluss rejects outright
     * @param logStartOffset a real offset, or fluss's {@code LogScanner.EARLIEST_OFFSET} sentinel
     */
    BoundedLogBatchScanner(Table table, TableBucket tableBucket, int[] projection,
            long logStartOffset, long logStopOffset) {
        this.records = new BoundedLogRecords(table, tableBucket, projection,
                logStartOffset, logStopOffset);
    }

    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) {
        if (records.isFinished()) {
            return null;
        }
        List<ScanRecord> batch = records.poll(timeout);
        List<InternalRow> rows = new ArrayList<>(batch.size());
        for (ScanRecord record : batch) {
            rows.add(record.getRow());
        }
        return CloseableIterator.wrap(rows.iterator());
    }

    @Override
    public void close() throws IOException {
        records.close();
    }
}

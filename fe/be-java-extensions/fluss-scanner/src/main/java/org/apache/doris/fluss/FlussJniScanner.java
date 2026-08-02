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

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.KvSnapshotAndLogBatchScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Reads one fluss scan range — one bucket of one partition, bounded by log offsets.
 *
 * <p>The parameters are the two maps FE built, merged by BE: the scan-level one (connection, table)
 * and the per-range one (which bucket, which offsets). Nothing between FE and here type-checks them,
 * so every one this class needs is read through {@link #required}, which names the missing key rather
 * than letting a null reach fluss.
 *
 * <p><b>Two ways to read, one loop.</b> A log table's range is its bucket's records over
 * {@code [start, stop)}, in log order. A primary-key table's range cannot be read that way — its log
 * is a change log, so replaying it verbatim returns superseded and deleted rows — and is read instead
 * as a kv snapshot merged with the change log that followed it, by fluss's own
 * {@code KvSnapshotAndLogBatchScanner}. Both are fluss {@code BatchScanner}s, so the row loop below
 * does not know which one it is draining.
 *
 * <p>Partition columns are not read here. FE declares them to the engine, which leaves them out of
 * {@code required_fields} and fills them from the range itself, so the projection this builds covers
 * only the data columns — the same division the paimon scanner works under, and the one a union read
 * needs both halves to agree on.
 */
public class FlussJniScanner extends JniScanner {

    private static final Logger LOG = LoggerFactory.getLogger(FlussJniScanner.class);

    /** Prefix for the fluss client configuration; the rest of the key is fluss's own option name. */
    private static final String CLIENT_PREFIX = "fluss.client.";
    private static final String DB_NAME = "fluss.db_name";
    private static final String TABLE_NAME = "fluss.table_name";
    private static final String RANGE_TYPE = "fluss.range_type";
    private static final String PARTITION_ID = "fluss.partition_id";
    private static final String BUCKET_ID = "fluss.bucket_id";
    private static final String LOG_START_OFFSET = "fluss.log_start_offset";
    private static final String LOG_STOP_OFFSET = "fluss.log_stop_offset";
    private static final String KV_SNAPSHOT_ID = "fluss.kv_snapshot_id";

    private static final String RANGE_TYPE_LOG = "LOG";
    private static final String RANGE_TYPE_PK_FULL = "PK_FULL";

    /**
     * How long one poll waits for data. Only affects how often the loop spins, never correctness: the
     * loop keeps polling until the scanner reports it has reached the end of the range.
     */
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final Map<String, String> params;
    private final ClassLoader classLoader;
    private final FlussColumnValue columnValue;

    private final boolean primaryKeyRange;
    private final long logStopOffset;
    private final long logStartOffset;
    /** Kv snapshot to start a primary-key read from; {@code -1} when the bucket has never had one. */
    private final long kvSnapshotId;
    private final int bucketId;
    /** {@code null} on an unpartitioned table, which fluss subscribes to by bucket alone. */
    private final Long partitionId;

    private Connection connection;
    private Table table;
    private BatchScanner scanner;

    /** Fluss types of the projected columns, positionally aligned with {@link #fields}. */
    private List<DataType> projectedTypes;

    private CloseableIterator<InternalRow> currentBatch;
    private boolean finished;
    private long rowsRead;

    public FlussJniScanner(int batchSize, Map<String, String> params) {
        this.params = params;
        this.classLoader = this.getClass().getClassLoader();

        String rangeType = required(RANGE_TYPE);
        this.primaryKeyRange = RANGE_TYPE_PK_FULL.equals(rangeType);
        if (!primaryKeyRange && !RANGE_TYPE_LOG.equals(rangeType)) {
            // Union ranges are planned but not read yet; failing here beats returning the fluss half
            // of a union read as if it were the whole table.
            throw new IllegalArgumentException(
                    "fluss scan range type '" + rangeType + "' is not supported yet; expected "
                            + RANGE_TYPE_LOG + " or " + RANGE_TYPE_PK_FULL);
        }
        // Every range field is parsed here, before open() creates a connection: a range that cannot be
        // read should say so without having contacted the cluster first.
        this.logStopOffset = Long.parseLong(required(LOG_STOP_OFFSET));
        this.logStartOffset = Long.parseLong(required(LOG_START_OFFSET));
        this.kvSnapshotId = primaryKeyRange ? Long.parseLong(required(KV_SNAPSHOT_ID)) : -1L;
        this.bucketId = Integer.parseInt(required(BUCKET_ID));
        String partition = params.get(PARTITION_ID);
        this.partitionId = partition == null ? null : Long.parseLong(partition);

        String[] requiredFields = splitOn(params.get("required_fields"), ",");
        String[] requiredTypes = splitOn(params.get("columns_types"), "#");
        if (requiredFields.length != requiredTypes.length) {
            throw new IllegalArgumentException("required_fields size " + requiredFields.length
                    + " does not match columns_types size " + requiredTypes.length);
        }
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        initTableInfo(columnTypes, requiredFields, batchSize);
        this.columnValue = new FlussColumnValue(
                params.getOrDefault("time_zone", TimeZone.getDefault().getID()));
    }

    @Override
    public void open() throws IOException {
        ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();
        // The fluss client spawns its own threads (netty IO, metadata updater) while connecting, and a
        // thread inherits the context classloader of whoever created it. Started under BE's loader they
        // would not see fluss at all.
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            connection = ConnectionFactory.createConnection(clientConfig());
            table = connection.getTable(TablePath.of(required(DB_NAME), required(TABLE_NAME)));

            RowType rowType = table.getTableInfo().getRowType();
            int[] projection = projectionOf(rowType);
            projectedTypes = new ArrayList<>(projection.length);
            for (int index : projection) {
                projectedTypes.add(rowType.getTypeAt(index));
            }

            long tableId = table.getTableInfo().getTableId();
            TableBucket tableBucket = partitionId == null
                    ? new TableBucket(tableId, bucketId)
                    : new TableBucket(tableId, partitionId, bucketId);
            scanner = primaryKeyRange
                    ? primaryKeyScanner(tableBucket, projection)
                    : new BoundedLogBatchScanner(table, tableBucket, scanProjection(projection),
                            logStartOffset, logStopOffset);
        } catch (Throwable e) {
            try {
                close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw new IOException("Failed to open the fluss scanner for "
                    + params.get(DB_NAME) + "." + params.get(TABLE_NAME)
                    + " bucket " + params.get(BUCKET_ID), e);
        } finally {
            Thread.currentThread().setContextClassLoader(callerLoader);
        }
    }

    /**
     * The kv snapshot of this bucket merged with the change log that followed it, by fluss's own
     * bounded primary-key reader. That class is {@code @Internal} — this connector is pinned to the
     * fluss version it ships with, and an upgrade has to re-run these tests.
     *
     * <p>The projection goes in as-is, including when it is empty, unlike the log path: this reader
     * appends the primary-key columns to what it asks fluss for (it needs them to merge on) and
     * projects the merged row back down to exactly what was requested, so it never asks fluss for the
     * empty projection fluss rejects.
     *
     * <p>It buffers the whole bounded change-log range in memory before merging, which is fluss's own
     * design for a batch read; the log tail after a snapshot is what bounds that, so a bucket whose
     * snapshot is far behind costs the most.
     */
    private BatchScanner primaryKeyScanner(TableBucket tableBucket, int[] projection) {
        return new KvSnapshotAndLogBatchScanner(
                table, tableBucket, kvSnapshotId, logStartOffset, logStopOffset, projection);
    }

    /**
     * Where each required column sits in the table's row type. Resolved by NAME at open time rather
     * than shipped as indexes from FE: the two are separated by planning, and a column added in
     * between would shift every index after it.
     */
    private int[] projectionOf(RowType rowType) {
        List<String> fieldNames = rowType.getFieldNames();
        int[] projection = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            int index = fieldNames.indexOf(fields[i]);
            if (index < 0) {
                throw new IllegalStateException("Column '" + fields[i]
                        + "' is not in the fluss table, which has " + fieldNames
                        + ". The table's schema changed after the query was planned.");
            }
            projection[i] = index;
        }
        return projection;
    }

    /**
     * What to actually ask fluss for. Normally the projected columns, but a query can need none of
     * them — selecting only partition columns leaves this scanner with an empty projection, because
     * BE materializes those from the range instead. Fluss rejects an empty projection outright
     * ({@code Projection.of}), so the narrowest legal request stands in for it. Nothing reads the
     * column that comes back: {@link #getNext} iterates over {@link #fields}, which is empty, and
     * reports the row count alone. A fluss table always has a first column.
     */
    private static int[] scanProjection(int[] projection) {
        return projection.length == 0 ? new int[] {0} : projection;
    }

    private Configuration clientConfig() {
        Configuration config = new Configuration();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(CLIENT_PREFIX)) {
                config.setString(entry.getKey().substring(CLIENT_PREFIX.length()), entry.getValue());
            }
        }
        return config;
    }

    @Override
    protected int getNext() throws IOException {
        int rows = 0;
        while (rows < getBatchSize()) {
            if (currentBatch == null || !currentBatch.hasNext()) {
                if (finished) {
                    break;
                }
                // A batch scanner returns null only at the end of the range; an empty batch means it
                // has more to do first (the primary-key reader drains the change log that way).
                currentBatch = scanner.pollBatch(POLL_TIMEOUT);
                finished = currentBatch == null;
                continue;
            }
            columnValue.setRow(currentBatch.next());
            for (int i = 0; i < fields.length; i++) {
                columnValue.setIdx(i, types[i], projectedTypes.get(i));
                appendData(i, columnValue);
            }
            rows++;
        }
        if (fields.length == 0 && rows > 0) {
            // A count-shaped read projects nothing; the vector table still needs the row count.
            vectorTable.appendVirtualData(rows);
        }
        rowsRead += rows;
        return rows;
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        // Close everything even if an earlier close throws: a leaked fluss connection keeps its netty
        // and metadata-updater threads alive for the life of the BE process.
        failure = closeQuietly(scanner, "scanner", failure);
        scanner = null;
        failure = closeQuietly(table, "table", failure);
        table = null;
        failure = closeQuietly(connection, "connection", failure);
        connection = null;
        currentBatch = null;
        if (failure != null) {
            throw failure;
        }
    }

    private IOException closeQuietly(AutoCloseable closeable, String what, IOException previous) {
        if (closeable == null) {
            return previous;
        }
        try {
            closeable.close();
            return previous;
        } catch (Exception e) {
            LOG.warn("Failed to close the fluss {}", what, e);
            IOException failure = new IOException("Failed to close the fluss " + what, e);
            if (previous == null) {
                return failure;
            }
            previous.addSuppressed(failure);
            return previous;
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> statistics = new HashMap<>();
        statistics.put("counter:FlussJniRowsRead", String.valueOf(rowsRead));
        statistics.put("gauge:FlussJniRequiredFieldCount", String.valueOf(fields.length));
        return statistics;
    }

    private String required(String key) {
        String value = params.get(key);
        if (value == null) {
            throw new IllegalArgumentException(
                    "fluss scanner parameter '" + key + "' is missing; got keys " + params.keySet());
        }
        return value;
    }

    private static String[] splitOn(String value, String separator) {
        if (value == null || value.isEmpty()) {
            return new String[0];
        }
        return value.split(separator);
    }
}

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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Reads one fluss scan range — one bucket of one partition, over a bounded log offset range.
 *
 * <p>The parameters are the two maps FE built, merged by BE: the scan-level one (connection, table)
 * and the per-range one (which bucket, which offsets). Nothing between FE and here type-checks them,
 * so every one this class needs is read through {@link #required}, which names the missing key rather
 * than letting a null reach fluss.
 *
 * <p><b>Where the read stops.</b> A fluss log scanner is a streaming reader with no end; the range's
 * stopping offset is what bounds it, and reaching it has to be detected three ways, all of which come
 * from fluss's own bounded reader ({@code KvSnapshotAndLogBatchScanner#pollLogRecords}):
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

    private static final String RANGE_TYPE_LOG = "LOG";

    /**
     * How long one poll waits for data. Only affects how often the loop spins, never correctness: the
     * loop keeps polling until one of the three stop conditions above fires.
     */
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final Map<String, String> params;
    private final ClassLoader classLoader;
    private final FlussColumnValue columnValue;

    private final long logStopOffset;
    private final long logStartOffset;
    private final int bucketId;
    /** {@code null} on an unpartitioned table, which fluss subscribes to by bucket alone. */
    private final Long partitionId;

    private Connection connection;
    private Table table;
    private LogScanner logScanner;
    private TableBucket tableBucket;

    /** Fluss types of the projected columns, positionally aligned with {@link #fields}. */
    private List<DataType> projectedTypes;

    private Iterator<ScanRecord> pending = Collections.emptyIterator();
    private boolean finished;
    private long rowsRead;

    public FlussJniScanner(int batchSize, Map<String, String> params) {
        this.params = params;
        this.classLoader = this.getClass().getClassLoader();

        String rangeType = required(RANGE_TYPE);
        if (!RANGE_TYPE_LOG.equals(rangeType)) {
            // Primary-key and union ranges are planned but not read yet; failing here beats reading
            // a primary-key table as a raw changelog and returning superseded rows.
            throw new IllegalArgumentException(
                    "fluss scan range type '" + rangeType + "' is not supported yet; expected "
                            + RANGE_TYPE_LOG);
        }
        // Every range field is parsed here, before open() creates a connection: a range that cannot be
        // read should say so without having contacted the cluster first.
        this.logStopOffset = Long.parseLong(required(LOG_STOP_OFFSET));
        this.logStartOffset = Long.parseLong(required(LOG_START_OFFSET));
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

            logScanner = table.newScan().project(projection).createLogScanner();
            subscribe();
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

    private void subscribe() {
        long tableId = table.getTableInfo().getTableId();
        if (partitionId == null) {
            tableBucket = new TableBucket(tableId, bucketId);
            logScanner.subscribe(bucketId, logStartOffset);
        } else {
            tableBucket = new TableBucket(tableId, partitionId, bucketId);
            logScanner.subscribe(partitionId, bucketId, logStartOffset);
        }
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
            if (!pending.hasNext()) {
                if (finished) {
                    break;
                }
                pending = poll();
                continue;
            }
            ScanRecord record = pending.next();
            if (record.logOffset() >= logStopOffset) {
                // Past the end of this range: another query's rows, not ours.
                finished = true;
                pending = Collections.emptyIterator();
                break;
            }
            columnValue.setRow(record.getRow());
            for (int i = 0; i < fields.length; i++) {
                columnValue.setIdx(i, types[i], projectedTypes.get(i));
                appendData(i, columnValue);
            }
            rows++;
            if (record.logOffset() >= logStopOffset - 1) {
                // The last record of the range. Do not poll again: the record AT the stopping offset
                // may not exist, and waiting for it never returns.
                finished = true;
                pending = Collections.emptyIterator();
                break;
            }
        }
        if (fields.length == 0 && rows > 0) {
            // A count-shaped read projects nothing; the vector table still needs the row count.
            vectorTable.appendVirtualData(rows);
        }
        rowsRead += rows;
        return rows;
    }

    private Iterator<ScanRecord> poll() {
        ScanRecords scanRecords = logScanner.poll(POLL_TIMEOUT);
        Long consumedUpToOffset = scanRecords.consumedUpToOffset(tableBucket);
        if (consumedUpToOffset != null && consumedUpToOffset >= logStopOffset) {
            // The fetch reached the end of the range without necessarily yielding a record there — the
            // tail can be control records, which take offsets but are never scanned. Without this the
            // loop would poll for a row that is never coming.
            finished = true;
        }
        return scanRecords.records(tableBucket).iterator();
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        // Close everything even if an earlier close throws: a leaked fluss connection keeps its netty
        // and metadata-updater threads alive for the life of the BE process.
        failure = closeQuietly(logScanner, "log scanner", failure);
        logScanner = null;
        failure = closeQuietly(table, "table", failure);
        table = null;
        failure = closeQuietly(connection, "connection", failure);
        connection = null;
        pending = Collections.emptyIterator();
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

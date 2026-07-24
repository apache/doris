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

package org.apache.doris.maxcompute;

import org.apache.doris.common.jni.JniWriter;
import org.apache.doris.common.jni.vec.VectorColumn;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.common.maxcompute.MCProperties;
import org.apache.doris.common.maxcompute.MCUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.type.TypeInfo;
import com.google.common.base.Strings;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * MaxComputeJniWriter writes C++ Block data to MaxCompute tables via Storage API (Arrow).
 * Loaded by C++ as: org/apache/doris/maxcompute/MaxComputeJniWriter
 */
public class MaxComputeJniWriter extends JniWriter {
    private static final Logger LOG = Logger.getLogger(MaxComputeJniWriter.class);

    private static final String ACCESS_KEY = "access_key";
    private static final String SECRET_KEY = "secret_key";
    private static final String ENDPOINT = "endpoint";
    private static final String QUOTA = "quota";
    private static final String PROJECT = "project";
    private static final String TABLE = "table";
    private static final String TXN_ID = "txn_id";
    private static final String WRITE_SESSION_ID = "write_session_id";
    private static final String BLOCK_ID = "block_id";
    private static final String PARTITION_SPEC = "partition_spec";
    private static final String CONNECT_TIMEOUT = "connect_timeout";
    private static final String READ_TIMEOUT = "read_timeout";
    private static final String RETRY_COUNT = "retry_count";

    private final Map<String, String> params;
    private final String endpoint;
    private final String project;
    private final String tableName;
    private final String quota;
    private final long txnId;
    private final String writeSessionId;
    private final Long preallocatedBlockId;
    private final String partitionSpec;
    private final int connectTimeout;
    private final int readTimeout;
    private final int retryCount;
    private final long maxBlockBytes;
    private final MaxComputeFeClient feClient;

    // Storage API objects
    private TableBatchWriteSession writeSession;
    private BatchWriter<VectorSchemaRoot> batchWriter;
    private BufferAllocator allocator;
    private WriterOptions writerOptions;
    private List<TypeInfo> columnTypeInfos;
    private List<String> columnNames;
    private long currentBlockId = -1L;
    private long currentBlockWrittenBytes = 0L;
    // Per-row Arrow payload size observed from previously written ranges. Used to bound
    // how many rows are materialized into a single Arrow root, so a large incoming JNI
    // block is never copied whole before its size is known. Refined as ranges are written.
    private long observedBytesPerRow = 0L;
    private final List<WriterCommitMessage> commitMessages = new ArrayList<>();

    // Statistics
    private long writtenRows = 0;
    private long writtenBytes = 0;

    public MaxComputeJniWriter(int batchSize, Map<String, String> params) {
        super(batchSize, params);
        this.params = params;
        this.endpoint = Objects.requireNonNull(params.get(ENDPOINT), "required property '" + ENDPOINT + "'.");
        this.project = Objects.requireNonNull(params.get(PROJECT), "required property '" + PROJECT + "'.");
        this.tableName = Objects.requireNonNull(params.get(TABLE), "required property '" + TABLE + "'.");
        this.quota = params.getOrDefault(QUOTA, "");
        String txnIdStr = params.get(TXN_ID);
        if (Strings.isNullOrEmpty(txnIdStr)) {
            throw new IllegalArgumentException(
                    "MaxCompute writer requires txn_id from FE; mixed-version FE/BE is not supported");
        }
        this.txnId = Long.parseLong(txnIdStr);
        this.writeSessionId = Objects.requireNonNull(params.get(WRITE_SESSION_ID),
                "required property '" + WRITE_SESSION_ID + "'.");
        this.preallocatedBlockId = params.containsKey(BLOCK_ID)
                ? Long.parseLong(params.get(BLOCK_ID)) : null;
        this.partitionSpec = params.getOrDefault(PARTITION_SPEC, "");
        this.connectTimeout = Integer.parseInt(params.getOrDefault(CONNECT_TIMEOUT, "10"));
        this.readTimeout = Integer.parseInt(params.getOrDefault(READ_TIMEOUT, "120"));
        this.retryCount = Integer.parseInt(params.getOrDefault(RETRY_COUNT, "4"));
        this.maxBlockBytes = Long.parseLong(
                params.getOrDefault(MCProperties.WRITE_MAX_BLOCK_BYTES,
                        MCProperties.DEFAULT_WRITE_MAX_BLOCK_BYTES));
        this.feClient = MaxComputeFeClient.create(params);
    }

    @Override
    public void open() throws IOException {
        try {
            Odps odps = MCUtils.createMcClient(params);
            odps.setDefaultProject(project);
            odps.setEndpoint(endpoint);

            Credentials credentials = Credentials.newBuilder().withAccount(odps.getAccount())
                    .withAppAccount(odps.getAppAccount()).build();

            RestOptions restOptions = RestOptions.newBuilder()
                    .withConnectTimeout(connectTimeout)
                    .withReadTimeout(readTimeout)
                    .withRetryTimes(retryCount).build();

            EnvironmentSettings settings = EnvironmentSettings.newBuilder()
                    .withCredentials(credentials)
                    .withServiceEndpoint(odps.getEndpoint())
                    .withQuotaName(Strings.isNullOrEmpty(quota) ? null : quota)
                    .withRestOptions(restOptions)
                    .build();

            // Restore the write session created by FE
            writeSession = new TableWriteSessionBuilder()
                    .identifier(com.aliyun.odps.table.TableIdentifier.of(project, tableName))
                    .withSessionId(writeSessionId)
                    .withSettings(settings)
                    .buildBatchWriteSession();

            // SDK skips ArrowOptions when restoring session via withSessionId,
            // set it via reflection to avoid NPE in ArrowWriterImpl
            ArrowOptions arrowOptions = ArrowOptions.newBuilder()
                    .withDatetimeUnit(TimestampUnit.MILLI)
                    .withTimestampUnit(TimestampUnit.MILLI)
                    .build();
            java.lang.reflect.Field arrowField = writeSession.getClass()
                    .getSuperclass().getDeclaredField("arrowOptions");
            arrowField.setAccessible(true);
            arrowField.set(writeSession, arrowOptions);

            // Get schema info for type mapping
            com.aliyun.odps.table.DataSchema dataSchema = writeSession.requiredSchema();
            columnTypeInfos = new java.util.ArrayList<>();
            columnNames = new java.util.ArrayList<>();
            for (com.aliyun.odps.Column col : dataSchema.getColumns()) {
                columnTypeInfos.add(col.getTypeInfo());
                columnNames.add(col.getName());
            }

            allocator = new RootAllocator(Long.MAX_VALUE);

            writerOptions = WriterOptions.newBuilder()
                    .withSettings(settings)
                    .withCompressionCodec(CompressionCodec.ZSTD)
                    .build();
            openBatchWriter(resolveInitialBlockId());

            LOG.info("MaxComputeJniWriter opened: project=" + project + ", table=" + tableName
                    + ", writeSessionId=" + writeSessionId + ", partitionSpec=" + partitionSpec
                    + ", blockId=" + currentBlockId);
        } catch (Exception e) {
            String errorMsg = "Failed to open MaxCompute write session for table " + project + "." + tableName;
            LOG.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        }
    }

    @Override
    protected void writeInternal(VectorTable inputTable) throws IOException {
        int numRows = inputTable.getNumRows();
        int numCols = inputTable.getNumColumns();
        if (numRows == 0) {
            return;
        }

        try {
            writeBatch(inputTable, numRows, numCols);
        } catch (Exception e) {
            String errorMsg = "Failed to write data to MaxCompute table " + project + "." + tableName;
            LOG.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        }
    }

    private long resolveInitialBlockId() throws IOException {
        return preallocatedBlockId != null ? preallocatedBlockId : requestBlockId();
    }

    private long requestBlockId() throws IOException {
        return feClient.requestBlockId(txnId, writeSessionId);
    }

    private void openBatchWriter(long blockId) throws IOException {
        currentBlockId = blockId;
        currentBlockWrittenBytes = 0L;
        batchWriter = writeSession.createArrowWriter(blockId, WriterAttemptId.of(0), writerOptions);
    }

    private void closeCurrentBatchWriterAndCollectCommit() throws IOException {
        if (batchWriter == null) {
            return;
        }
        WriterCommitMessage commitMessage = batchWriter.commit();
        if (commitMessage != null) {
            commitMessages.add(commitMessage);
        }
        batchWriter = null;
    }

    private void rotateCurrentBatchWriter() throws IOException {
        closeCurrentBatchWriterAndCollectCommit();
        openBatchWriter(requestBlockId());
    }

    private void writeBatch(VectorTable inputTable, int numRows, int numCols) throws IOException {
        int rowStart = 0;
        while (rowStart < numRows) {
            // Bound the rows copied into one Arrow root using the per-row size observed so
            // far, so an oversized incoming block is never materialized whole before we know
            // whether it fits the current block.
            int probeEnd = rowStart + boundedProbeRowCount(observedBytesPerRow, maxBlockBytes, numRows - rowStart);
            try (VectorSchemaRoot root = buildRowRangeRoot(inputTable, numCols, rowStart, probeEnd)) {
                int probeRows = probeEnd - rowStart;
                long probeBytes = estimateBatchPayloadBytes(root);
                observedBytesPerRow = probeBytes / probeRows;
                if (currentBlockWrittenBytes + probeBytes <= maxBlockBytes) {
                    writeRoot(root, probeRows, probeBytes);
                    rowStart = probeEnd;
                    continue;
                }

                // The probe overflows the current block. Split it WITHOUT rebuilding: the binary
                // search measures leading-row sizes from this already-built root via
                // getBufferSizeFor, then we slice off the prefix that fits. The remaining rows are
                // rebuilt on the next iteration (after rotating), so no Arrow buffer outlives the
                // current block writer.
                RowRange rowRange = findPartialRowRange(rowStart, probeEnd, currentBlockWrittenBytes,
                        maxBlockBytes, (rangeStart, rangeEnd) -> prefixBufferBytes(root, rangeEnd - rangeStart));
                if (rowRange.rotateBeforeWrite) {
                    rotateCurrentBatchWriter();
                    continue;
                }

                int headRows = rowRange.rowEnd - rowStart;
                try (VectorSchemaRoot head = root.slice(0, headRows)) {
                    writeRoot(head, headRows, rowRange.bytes);
                }
                rowStart = rowRange.rowEnd;
            }
            if (rowStart < numRows && currentBlockWrittenBytes >= maxBlockBytes) {
                rotateCurrentBatchWriter();
            }
        }
    }

    // Off-heap payload bytes of the leading rowCount rows of an already-built Arrow root,
    // read from the existing column buffers (getBufferSizeFor) without rebuilding any vector.
    static long prefixBufferBytes(VectorSchemaRoot root, int rowCount) {
        long total = 0L;
        for (FieldVector vector : root.getFieldVectors()) {
            total += vector.getBufferSizeFor(rowCount);
        }
        return total;
    }

    /**
     * Choose how many rows to materialize into the next Arrow root, bounded so a large
     * incoming JNI block is never copied whole before its size is known. The bound targets
     * roughly one MaxCompute block worth of payload using {@code observedBytesPerRow}; before
     * any range has been measured it probes a single row, then sizes from that row's measured
     * Arrow payload. The result is at least one row and never exceeds {@code remainingRows}.
     */
    static int boundedProbeRowCount(long observedBytesPerRow, long maxBlockBytes, int remainingRows) {
        long cap;
        if (observedBytesPerRow <= 0L) {
            cap = 1L;
        } else {
            cap = Math.max(1L, maxBlockBytes / observedBytesPerRow);
        }
        if (cap >= remainingRows) {
            return remainingRows;
        }
        return (int) cap;
    }

    private void writeRoot(VectorSchemaRoot root, int numRows, long batchBytes) throws IOException {
        batchWriter.write(root);
        batchWriter.flush();

        writtenRows += numRows;
        currentBlockWrittenBytes += batchBytes;
        writtenBytes += batchBytes;
    }

    static RowRange findPartialRowRange(int rowStart, int numRows, long currentBlockWrittenBytes,
            long maxBlockBytes, RowRangeByteEstimator estimator) throws IOException {
        int low = rowStart + 1;
        int high = numRows - 1;
        int bestEnd = rowStart;
        long bestBytes = 0L;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            long rangeBytes = estimator.estimate(rowStart, mid);
            if (currentBlockWrittenBytes + rangeBytes <= maxBlockBytes) {
                bestEnd = mid;
                bestBytes = rangeBytes;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (bestEnd > rowStart) {
            return RowRange.write(bestEnd, bestBytes);
        }
        if (currentBlockWrittenBytes > 0) {
            return RowRange.rotateBeforeWrite();
        }
        return RowRange.write(rowStart + 1, estimator.estimate(rowStart, rowStart + 1));
    }

    interface RowRangeByteEstimator {
        long estimate(int rowStart, int rowEnd) throws IOException;
    }

    static class RowRange {
        final int rowEnd;
        final long bytes;
        final boolean rotateBeforeWrite;

        private RowRange(int rowEnd, long bytes, boolean rotateBeforeWrite) {
            this.rowEnd = rowEnd;
            this.bytes = bytes;
            this.rotateBeforeWrite = rotateBeforeWrite;
        }

        static RowRange write(int rowEnd, long bytes) {
            return new RowRange(rowEnd, bytes, false);
        }

        static RowRange rotateBeforeWrite() {
            return new RowRange(-1, 0L, true);
        }
    }

    // Estimate an Arrow batch's payload size from its column buffer sizes (O(columns)).
    static long estimateBatchPayloadBytes(VectorSchemaRoot root) {
        long total = 0L;
        for (FieldVector vector : root.getFieldVectors()) {
            total += vector.getBufferSize();
        }
        return total;
    }

    private VectorSchemaRoot buildRowRangeRoot(VectorTable inputTable, int numCols, int rowStart, int rowEnd) {
        int rowCount = rowEnd - rowStart;
        VectorSchemaRoot root = batchWriter.newElement();
        root.setRowCount(rowCount);
        for (int col = 0; col < numCols && col < columnTypeInfos.size(); col++) {
            fillArrowVectorStreaming(root, col, columnTypeInfos.get(col).getOdpsType(),
                    inputTable.getColumn(col), rowStart, rowCount);
        }
        return root;
    }

    /**
     * Fill an Arrow vector by reading data directly from a VectorColumn,
     * one row at a time. For STRING columns, this reads bytes directly
     * (getBytesWithOffset) instead of creating String objects, eliminating
     * the String -> byte[] double-copy that caused heap exhaustion.
     */
    private void fillArrowVectorStreaming(VectorSchemaRoot root, int colIdx, OdpsType odpsType,
                                          VectorColumn vc, int rowOffset, int numRows) {
        switch (odpsType) {
            case BOOLEAN: {
                BitVector vec = (BitVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, vc.getBoolean(rowOffset + i) ? 1 : 0);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case TINYINT: {
                TinyIntVector vec = (TinyIntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, vc.getByte(rowOffset + i));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case SMALLINT: {
                SmallIntVector vec = (SmallIntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, vc.getShort(rowOffset + i));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case INT: {
                IntVector vec = (IntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, vc.getInt(rowOffset + i));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case BIGINT: {
                BigIntVector vec = (BigIntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, vc.getLong(rowOffset + i));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case FLOAT: {
                Float4Vector vec = (Float4Vector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, vc.getFloat(rowOffset + i));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DOUBLE: {
                Float8Vector vec = (Float8Vector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, vc.getDouble(rowOffset + i));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DECIMAL: {
                DecimalVector vec = (DecimalVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, vc.getDecimal(rowOffset + i));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case STRING:
            case VARCHAR:
            case CHAR: {
                VarCharVector vec = (VarCharVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.setSafe(i, vc.getBytesWithOffset(rowOffset + i));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DATE: {
                DateDayVector vec = (DateDayVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, (int) vc.getDate(rowOffset + i).toEpochDay());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DATETIME:
            case TIMESTAMP: {
                TimeStampMilliVector vec = (TimeStampMilliVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        long millis = vc.getDateTime(rowOffset + i)
                                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        vec.set(i, millis);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case BINARY: {
                VarBinaryVector vec = (VarBinaryVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (vc.isNullAt(rowOffset + i)) {
                        vec.setNull(i);
                    } else {
                        vec.setSafe(i, vc.getBytesWithOffset(rowOffset + i));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            default: {
                // Complex types still fall back to object materialization; the
                // optimization target here is large variable-width primitive columns.
                Object[] colData = vc.getObjectColumn(rowOffset, rowOffset + numRows);
                fillArrowVector(root, colIdx, odpsType, colData, 0, numRows);
                break;
            }
        }
    }

    private void fillArrowVector(VectorSchemaRoot root, int colIdx, OdpsType odpsType,
                                 Object[] colData, int startRow, int numRows) {
        switch (odpsType) {
            case BOOLEAN: {
                BitVector vec = (BitVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, (Boolean) value ? 1 : 0);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case TINYINT: {
                TinyIntVector vec = (TinyIntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) value).byteValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case SMALLINT: {
                SmallIntVector vec = (SmallIntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) value).shortValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case INT: {
                IntVector vec = (IntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) value).intValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case BIGINT: {
                BigIntVector vec = (BigIntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) value).longValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case FLOAT: {
                Float4Vector vec = (Float4Vector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) value).floatValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DOUBLE: {
                Float8Vector vec = (Float8Vector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) value).doubleValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DECIMAL: {
                DecimalVector vec = (DecimalVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        BigDecimal bd = (value instanceof BigDecimal)
                                ? (BigDecimal) value
                                : new BigDecimal(value.toString());
                        vec.set(i, bd);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case STRING:
            case VARCHAR:
            case CHAR: {
                VarCharVector vec = (VarCharVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        byte[] bytes;
                        if (value instanceof byte[]) {
                            bytes = (byte[]) value;
                        } else {
                            bytes = value.toString().getBytes(StandardCharsets.UTF_8);
                        }
                        vec.setSafe(i, bytes);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DATE: {
                DateDayVector vec = (DateDayVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else if (value instanceof LocalDate) {
                        vec.set(i, (int) ((LocalDate) value).toEpochDay());
                    } else {
                        vec.set(i, (int) LocalDate.parse(value.toString()).toEpochDay());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DATETIME:
            case TIMESTAMP: {
                TimeStampMilliVector vec = (TimeStampMilliVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else if (value instanceof LocalDateTime) {
                        long millis = ((LocalDateTime) value)
                                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        vec.set(i, millis);
                    } else if (value instanceof java.sql.Timestamp) {
                        vec.set(i, ((java.sql.Timestamp) value).getTime());
                    } else {
                        long millis = LocalDateTime.parse(value.toString())
                                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        vec.set(i, millis);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case BINARY: {
                VarBinaryVector vec = (VarBinaryVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else if (value instanceof byte[]) {
                        vec.setSafe(i, (byte[]) value);
                    } else {
                        vec.setSafe(i, value.toString().getBytes(StandardCharsets.UTF_8));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case ARRAY: {
                ListVector listVec = (ListVector) root.getVector(colIdx);
                listVec.allocateNew();
                FieldVector dataVec = listVec.getDataVector();
                int elemIdx = 0;
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        listVec.setNull(i);
                    } else {
                        List<?> list = (List<?>) value;
                        listVec.startNewValue(i);
                        for (Object elem : list) {
                            writeListElement(dataVec, elemIdx++, elem);
                        }
                        listVec.endValue(i, list.size());
                    }
                }
                listVec.setValueCount(numRows);
                dataVec.setValueCount(elemIdx);
                break;
            }
            case MAP: {
                MapVector mapVec = (MapVector) root.getVector(colIdx);
                mapVec.allocateNew();
                StructVector structVec = (StructVector) mapVec.getDataVector();
                FieldVector keyVec = structVec.getChildrenFromFields().get(0);
                FieldVector valVec = structVec.getChildrenFromFields().get(1);
                int elemIdx = 0;
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        mapVec.setNull(i);
                    } else {
                        Map<?, ?> map = (Map<?, ?>) value;
                        mapVec.startNewValue(i);
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            structVec.setIndexDefined(elemIdx);
                            writeListElement(keyVec, elemIdx, entry.getKey());
                            writeListElement(valVec, elemIdx, entry.getValue());
                            elemIdx++;
                        }
                        mapVec.endValue(i, map.size());
                    }
                }
                mapVec.setValueCount(numRows);
                structVec.setValueCount(elemIdx);
                keyVec.setValueCount(elemIdx);
                valVec.setValueCount(elemIdx);
                break;
            }
            case STRUCT: {
                StructVector structVec = (StructVector) root.getVector(colIdx);
                structVec.allocateNew();
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        structVec.setNull(i);
                    } else {
                        structVec.setIndexDefined(i);
                        Map<?, ?> struct = (Map<?, ?>) value;
                        for (FieldVector childVec : structVec.getChildrenFromFields()) {
                            Object val = struct.get(childVec.getName());
                            writeListElement(childVec, i, val);
                        }
                    }
                }
                structVec.setValueCount(numRows);
                for (FieldVector childVec : structVec.getChildrenFromFields()) {
                    childVec.setValueCount(numRows);
                }
                break;
            }
            default: {
                // Fallback: write as VarChar
                VarCharVector vec = (VarCharVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    Object value = colData[startRow + i];
                    if (value == null) {
                        vec.setNull(i);
                    } else {
                        vec.setSafe(i, value.toString().getBytes(StandardCharsets.UTF_8));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
        }
    }

    private void writeListElement(FieldVector vec, int idx, Object elem) {
        if (elem == null) {
            if (vec instanceof BaseFixedWidthVector) {
                ((BaseFixedWidthVector) vec).setNull(idx);
            } else if (vec instanceof BaseVariableWidthVector) {
                ((BaseVariableWidthVector) vec).setNull(idx);
            } else if (vec instanceof StructVector) {
                ((StructVector) vec).setNull(idx);
            } else if (vec instanceof MapVector) {
                ((MapVector) vec).setNull(idx);
            } else if (vec instanceof ListVector) {
                ((ListVector) vec).setNull(idx);
            }
            return;
        }
        if (vec instanceof VarCharVector) {
            byte[] bytes = elem instanceof byte[] ? (byte[]) elem
                    : elem.toString().getBytes(StandardCharsets.UTF_8);
            ((VarCharVector) vec).setSafe(idx, bytes);
        } else if (vec instanceof IntVector) {
            ((IntVector) vec).setSafe(idx, ((Number) elem).intValue());
        } else if (vec instanceof BigIntVector) {
            ((BigIntVector) vec).setSafe(idx, ((Number) elem).longValue());
        } else if (vec instanceof Float8Vector) {
            ((Float8Vector) vec).setSafe(idx, ((Number) elem).doubleValue());
        } else if (vec instanceof Float4Vector) {
            ((Float4Vector) vec).setSafe(idx, ((Number) elem).floatValue());
        } else if (vec instanceof SmallIntVector) {
            ((SmallIntVector) vec).setSafe(idx, ((Number) elem).shortValue());
        } else if (vec instanceof TinyIntVector) {
            ((TinyIntVector) vec).setSafe(idx, ((Number) elem).byteValue());
        } else if (vec instanceof BitVector) {
            ((BitVector) vec).setSafe(idx, (Boolean) elem ? 1 : 0);
        } else if (vec instanceof DecimalVector) {
            BigDecimal bd = elem instanceof BigDecimal ? (BigDecimal) elem
                    : new BigDecimal(elem.toString());
            ((DecimalVector) vec).setSafe(idx, bd);
        } else if (vec instanceof StructVector) {
            StructVector structVec = (StructVector) vec;
            structVec.setIndexDefined(idx);
            Map<?, ?> struct = (Map<?, ?>) elem;
            for (FieldVector childVec : structVec.getChildrenFromFields()) {
                writeListElement(childVec, idx, struct.get(childVec.getName()));
            }
        } else if (vec instanceof MapVector) {
            MapVector mapVec = (MapVector) vec;
            StructVector entryVec = (StructVector) mapVec.getDataVector();
            FieldVector keyVec = entryVec.getChildrenFromFields().get(0);
            FieldVector valVec = entryVec.getChildrenFromFields().get(1);
            Map<?, ?> map = (Map<?, ?>) elem;
            int offset = mapVec.startNewValue(idx);
            int j = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                entryVec.setIndexDefined(offset + j);
                writeListElement(keyVec, offset + j, entry.getKey());
                writeListElement(valVec, offset + j, entry.getValue());
                j++;
            }
            mapVec.endValue(idx, map.size());
            entryVec.setValueCount(offset + j);
            keyVec.setValueCount(offset + j);
            valVec.setValueCount(offset + j);
        } else if (vec instanceof ListVector) {
            ListVector listVec = (ListVector) vec;
            FieldVector dataVec = listVec.getDataVector();
            List<?> list = (List<?>) elem;
            int offset = listVec.startNewValue(idx);
            for (int j = 0; j < list.size(); j++) {
                writeListElement(dataVec, offset + j, list.get(j));
            }
            listVec.endValue(idx, list.size());
            dataVec.setValueCount(offset + list.size());
        } else {
            byte[] bytes = elem.toString().getBytes(StandardCharsets.UTF_8);
            ((VarCharVector) vec).setSafe(idx, bytes);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            closeCurrentBatchWriterAndCollectCommit();
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
            LOG.info("MaxComputeJniWriter closed: writeSessionId=" + writeSessionId
                    + ", partitionSpec=" + partitionSpec
                    + ", writtenRows=" + writtenRows
                    + ", lastBlockId=" + currentBlockId
                    + ", commitMessageCount=" + commitMessages.size());
        } catch (Exception e) {
            String errorMsg = "Failed to close MaxCompute arrow writer";
            LOG.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        } finally {
            feClient.close();
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> stats = new HashMap<>();
        stats.put("mc_partition_spec", partitionSpec != null ? partitionSpec : "");

        // Serialize commit messages to Base64.
        if (!commitMessages.isEmpty()) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(commitMessages);
                oos.close();
                stats.put("mc_commit_message", Base64.getEncoder().encodeToString(baos.toByteArray()));
            } catch (IOException e) {
                LOG.error("Failed to serialize WriterCommitMessages", e);
            }
        }

        stats.put("counter:WrittenRows", String.valueOf(writtenRows));
        stats.put("bytes:WrittenBytes", String.valueOf(writtenBytes));
        stats.put("timer:WriteTime", String.valueOf(writeTime));
        stats.put("timer:ReadTableTime", String.valueOf(readTableTime));
        return stats;
    }
}

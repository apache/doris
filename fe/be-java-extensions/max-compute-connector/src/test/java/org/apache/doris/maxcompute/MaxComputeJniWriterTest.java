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

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.arrow.ArrowReaderFactory;
import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.table.arrow.ArrowWriterFactory;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;

public class MaxComputeJniWriterTest {
    @Test
    public void testZstdArrowRoundTrip() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newBuilder()
                .withCredentials(Credentials.newBuilder()
                        .withAccount(new AliyunAccount("test-access-key", "test-secret-key")).build())
                .build();
        WriterOptions writerOptions = WriterOptions.newBuilder().withSettings(settings)
                .withCompressionCodec(CompressionCodec.ZSTD).build();
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.of(new IntVector("c", allocator))) {
            IntVector vector = (IntVector) root.getVector(0);
            vector.allocateNew(1024);
            for (int i = 0; i < 1024; i++) {
                vector.set(i, i % 7);
            }
            vector.setNull(5);
            root.setRowCount(1024);

            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ArrowWriter writer = ArrowWriterFactory.getRecordBatchWriter(bytes, writerOptions)) {
                writer.writeBatch(root);
            }

            ReaderOptions readerOptions = ReaderOptions.newBuilder().withSettings(settings)
                    .withBufferAllocator(allocator).withCompressionCodec(CompressionCodec.ZSTD)
                    .withReuseBatch(true).build();
            try (ArrowReader reader = ArrowReaderFactory.getRecordBatchReader(
                    new ByteArrayInputStream(bytes.toByteArray()), readerOptions)) {
                Assert.assertTrue(reader.nextBatch());
                VectorSchemaRoot decoded = reader.getCurrentValue();
                Assert.assertEquals(root.getRowCount(), decoded.getRowCount());
                IntVector decodedVector = (IntVector) decoded.getVector(0);
                for (int i = 0; i < root.getRowCount(); i++) {
                    Assert.assertEquals(vector.getObject(i), decodedVector.getObject(i));
                }
                Assert.assertFalse(reader.nextBatch());
            }
        }
    }

    @Test
    public void testTimestampVectorsAcrossTimezonesAndDstOverlap() {
        java.util.TimeZone original = java.util.TimeZone.getDefault();
        try (BufferAllocator allocator = new RootAllocator()) {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/New_York"}) {
                java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(zone));
                try (org.apache.arrow.vector.TimeStampMicroTZVector vector =
                        new org.apache.arrow.vector.TimeStampMicroTZVector("ts", allocator, zone)) {
                    vector.allocateNew(1);
                    vector.setValueCount(1);
                    MaxComputeColumnValue value = new MaxComputeColumnValue(vector, 0, java.time.ZoneId.of(zone));
                    for (long micros : new long[] {-1L, 0L, 1636263000123456L, 1636266600123456L}) {
                        vector.setSafe(0, micros);
                        java.time.Instant expected = java.time.Instant.ofEpochSecond(
                                Math.floorDiv(micros, 1_000_000), Math.floorMod(micros, 1_000_000) * 1000);
                        Assert.assertEquals(java.time.LocalDateTime.ofInstant(expected, java.time.ZoneOffset.UTC),
                                value.getTimeStampTz());
                    }
                    vector.setNull(0);
                    Assert.assertTrue(value.isNull());
                }
            }
        } finally {
            java.util.TimeZone.setDefault(original);
        }
    }

    @Test
    public void testTimestampWriteKeepsUtcMicros() throws Exception {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("endpoint", "http://localhost");
        params.put("project", "test_project");
        params.put("table", "events");
        params.put("txn_id", "1");
        params.put("write_session_id", "test_session");
        params.put("fe_host", "localhost");
        params.put("fe_port", "9020");
        MaxComputeJniWriter writer = new MaxComputeJniWriter(1, params);
        org.apache.doris.common.jni.utils.OffHeap.setTesting();
        org.apache.doris.common.jni.vec.VectorColumn column =
                org.apache.doris.common.jni.vec.VectorColumn.createWritableColumn(
                        org.apache.doris.common.jni.vec.ColumnType.parseType("ts", "timestamptz(6)"), 1);
        java.time.LocalDateTime utc = java.time.LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000);
        column.appendTimeStampTz(utc);
        try (BufferAllocator allocator = new RootAllocator();
                org.apache.arrow.vector.TimeStampMicroVector vector =
                        new org.apache.arrow.vector.TimeStampMicroVector("ts", allocator);
                VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vector))) {
            java.lang.reflect.Method streaming = MaxComputeJniWriter.class.getDeclaredMethod(
                    "fillArrowVectorStreaming", VectorSchemaRoot.class, int.class, com.aliyun.odps.OdpsType.class,
                    org.apache.doris.common.jni.vec.VectorColumn.class, int.class, int.class);
            streaming.setAccessible(true);
            streaming.invoke(writer, root, 0, com.aliyun.odps.OdpsType.TIMESTAMP, column, 0, 1);
            Assert.assertEquals(-1L, vector.get(0));
            java.lang.reflect.Method buffered = MaxComputeJniWriter.class.getDeclaredMethod(
                    "fillArrowVector", VectorSchemaRoot.class, int.class, com.aliyun.odps.OdpsType.class,
                    Object[].class, int.class, int.class);
            buffered.setAccessible(true);
            buffered.invoke(writer, root, 0, com.aliyun.odps.OdpsType.TIMESTAMP, new Object[] {utc}, 0, 1);
            Assert.assertEquals(-1L, vector.get(0));
        } finally {
            column.close();
            writer.close();
        }
    }

    @Test
    public void testTimestampReadPreservesInstant() {
        try (BufferAllocator allocator = new RootAllocator();
                org.apache.arrow.vector.TimeStampMicroTZVector vector =
                        new org.apache.arrow.vector.TimeStampMicroTZVector("ts", allocator, "UTC")) {
            vector.allocateNew(1);
            vector.setSafe(0, -1L);
            vector.setValueCount(1);
            MaxComputeColumnValue value = new MaxComputeColumnValue(vector, 0, java.time.ZoneId.of("Asia/Shanghai"));
            Assert.assertEquals(java.time.LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000),
                    value.getTimeStampTz());
        }
    }

    @Test
    public void testPrefixBufferBytesMeasuresLeadingRowsWithoutRebuild() {
        try (BufferAllocator allocator = new RootAllocator();
                IntVector vec = new IntVector("c", allocator)) {
            vec.allocateNew(8);
            for (int i = 0; i < 8; i++) {
                vec.set(i, i);
            }
            vec.setValueCount(8);
            try (VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec))) {
                // The whole-root measurement must match estimateBatchPayloadBytes...
                Assert.assertEquals(MaxComputeJniWriter.estimateBatchPayloadBytes(root),
                        MaxComputeJniWriter.prefixBufferBytes(root, root.getRowCount()));
                // ...and a leading prefix must be strictly smaller, computed from the
                // already-built buffers (no rebuild).
                Assert.assertTrue(MaxComputeJniWriter.prefixBufferBytes(root, 4)
                        < MaxComputeJniWriter.prefixBufferBytes(root, 8));
            }
        }
    }

    @Test
    public void testFindPartialRowRangeFillsRemainingBlock() throws Exception {
        MaxComputeJniWriter.RowRange range = MaxComputeJniWriter.findPartialRowRange(
                0, 4, 60L, 100L, prefixEstimator(10L, 20L, 30L, 40L));

        Assert.assertFalse(range.rotateBeforeWrite);
        Assert.assertEquals(2, range.rowEnd);
        Assert.assertEquals(30L, range.bytes);
    }

    @Test
    public void testFindPartialRowRangeRotatesWhenNoRowFitsNonEmptyBlock() throws Exception {
        MaxComputeJniWriter.RowRange range = MaxComputeJniWriter.findPartialRowRange(
                0, 3, 95L, 100L, prefixEstimator(10L, 20L, 30L));

        Assert.assertTrue(range.rotateBeforeWrite);
    }

    @Test
    public void testFindPartialRowRangeKeepsSingleOversizeFallbackOnEmptyBlock() throws Exception {
        MaxComputeJniWriter.RowRange range = MaxComputeJniWriter.findPartialRowRange(
                0, 3, 0L, 5L, prefixEstimator(10L, 20L, 30L));

        Assert.assertFalse(range.rotateBeforeWrite);
        Assert.assertEquals(1, range.rowEnd);
        Assert.assertEquals(10L, range.bytes);
    }

    @Test
    public void testFindPartialRowRangeUsesRowStartOffset() throws Exception {
        MaxComputeJniWriter.RowRange range = MaxComputeJniWriter.findPartialRowRange(
                1, 4, 50L, 100L, prefixEstimator(999L, 30L, 30L, 50L));

        Assert.assertFalse(range.rotateBeforeWrite);
        Assert.assertEquals(2, range.rowEnd);
        Assert.assertEquals(30L, range.bytes);
    }

    @Test
    public void testBoundedProbeRowCountBootstrapsWithSingleRow() {
        // No per-row estimate yet: bootstrap by measuring a single row's real Arrow payload,
        // so an oversized input is never copied whole and we never guess a row count.
        int probeRows = MaxComputeJniWriter.boundedProbeRowCount(0L, 64L * 1024 * 1024, 1_000_000);

        Assert.assertEquals(1, probeRows);
    }

    @Test
    public void testBoundedProbeRowCountTargetsOneBlockAfterMeasurement() {
        // 1 KiB/row against a 64 MiB block => ~65536 rows fill one block.
        int probeRows = MaxComputeJniWriter.boundedProbeRowCount(1024L, 64L * 1024 * 1024, 1_000_000);

        Assert.assertEquals(65536, probeRows);
        Assert.assertTrue(probeRows < 1_000_000);
    }

    @Test
    public void testBoundedProbeRowCountReturnsRemainingWhenItFitsCap() {
        // A small input that comfortably fits one block is probed in one shot.
        int probeRows = MaxComputeJniWriter.boundedProbeRowCount(1024L, 64L * 1024 * 1024, 4096);

        Assert.assertEquals(4096, probeRows);
    }

    @Test
    public void testBoundedProbeRowCountProbesSingleRowWhenRowExceedsBlock() {
        // A single row larger than a whole block must still make progress (never 0 rows).
        int probeRows = MaxComputeJniWriter.boundedProbeRowCount(
                128L * 1024 * 1024, 64L * 1024 * 1024, 1_000_000);

        Assert.assertEquals(1, probeRows);
    }

    private static MaxComputeJniWriter.RowRangeByteEstimator prefixEstimator(long... rowBytes) {
        return (rowStart, rowEnd) -> {
            long bytes = 0L;
            for (int i = rowStart; i < rowEnd; i++) {
                bytes += rowBytes[i];
            }
            return bytes;
        };
    }
}

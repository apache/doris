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

package org.apache.doris.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PaimonJniScannerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testConstructorAcceptsEmptyProjection() {
        new PaimonJniScanner(128, createBaseParams());
    }

    @Test
    public void testExplicitTableReadBatchSizeOverridesDorisDefault() {
        PaimonJniScanner scanner = new PaimonJniScanner(128, createBaseParams());

        Map<String, String> defaultOptions = scanner.buildTableOptions(Collections.emptyMap());
        Assert.assertEquals("128", defaultOptions.get(CoreOptions.READ_BATCH_SIZE.key()));

        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.READ_BATCH_SIZE.key(), "4096");
        Map<String, String> options = scanner.buildTableOptions(tableOptions);
        Assert.assertEquals("4096", options.get(CoreOptions.READ_BATCH_SIZE.key()));
    }

    @Test
    public void testDorisAsyncReaderSafetyOptionOverridesTableOption() {
        Map<String, String> params = createBaseParams();
        params.put(PaimonJniScanner.DORIS_ENABLE_FILE_READER_ASYNC, "false");
        PaimonJniScanner scanner = new PaimonJniScanner(128, params);
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.FILE_READER_ASYNC_THRESHOLD.key(), "1 MiB");

        Map<String, String> options = scanner.buildTableOptions(tableOptions);

        Assert.assertEquals(PaimonJniScanner.MAX_ASYNC_READ_THRESHOLD,
                options.get(CoreOptions.FILE_READER_ASYNC_THRESHOLD.key()));
    }

    @Test
    public void testIOManagerOptionHelpers() throws Exception {
        Map<String, String> params = createBaseParams();
        Assert.assertFalse(PaimonJniScanner.isIOManagerEnabled(params));

        params.put(PaimonJniScanner.ENABLE_JNI_IO_MANAGER, "true");
        File tempDir = new File(temporaryFolder.getRoot(), "paimon-io-manager");
        params.put(PaimonJniScanner.JNI_IO_MANAGER_TMP_DIR, tempDir.getAbsolutePath());

        Assert.assertTrue(PaimonJniScanner.isIOManagerEnabled(params));
        Assert.assertEquals(tempDir.getAbsolutePath(), PaimonJniScanner.getIOManagerTempDirs(params));
        Assert.assertNull(PaimonJniScanner.getIOManagerImplClass(params));
        PaimonJniScanner.createIOManager(tempDir.getAbsolutePath()).close();
        Assert.assertTrue(tempDir.exists());
    }

    @Test
    public void testCreateDefaultAndCustomIOManager() throws Exception {
        File tempDir = new File(temporaryFolder.getRoot(), "paimon-io-manager-impl");
        IOManager defaultIOManager = PaimonJniScanner.createIOManager(tempDir.getAbsolutePath());
        Assert.assertTrue(defaultIOManager instanceof IOManagerImpl);
        defaultIOManager.close();

        Map<String, String> params = createBaseParams();
        params.put(PaimonJniScanner.JNI_IO_MANAGER_IMPL_CLASS, TestIOManager.class.getName());
        Assert.assertEquals(TestIOManager.class.getName(), PaimonJniScanner.getIOManagerImplClass(params));
        IOManager customIOManager = PaimonJniScanner.createIOManager(
                tempDir.getAbsolutePath(), PaimonJniScanner.getIOManagerImplClass(params));
        Assert.assertTrue(customIOManager instanceof TestIOManager);
        Assert.assertArrayEquals(new String[] {tempDir.getAbsolutePath()}, customIOManager.tempDirs());
    }

    @Test
    public void testCloseCleansIOManagerTempDirectory() throws Exception {
        File tempDir = temporaryFolder.newFolder("paimon-io-manager-clean");
        IOManager ioManager = PaimonJniScanner.createIOManager(tempDir.getAbsolutePath());
        FileIOChannel.ID channel = ioManager.createChannel();
        File spillFile = channel.getPathFile();
        Assert.assertTrue(spillFile.createNewFile());
        File spillDir = spillFile.getParentFile();
        Assert.assertTrue(spillDir.exists());

        PaimonJniScanner scanner = new PaimonJniScanner(128, createBaseParams());
        Field ioManagerField = PaimonJniScanner.class.getDeclaredField("ioManager");
        ioManagerField.setAccessible(true);
        ioManagerField.set(scanner, ioManager);
        Assert.assertEquals("1", scanner.getStatistics().get("gauge:PaimonJniIOManagerEnabled"));

        scanner.close();
        Assert.assertFalse(spillDir.exists());
    }

    @Test
    public void testStatisticsIncludePaimonDiagnostics() throws Exception {
        Map<String, String> params = createBaseParams();
        params.put("paimon_split", "encoded-split");
        params.put("paimon_predicate", "encoded-predicate");
        PaimonJniScanner scanner = new PaimonJniScanner(128, params);
        setTableOptions(scanner, Collections.singletonMap("file-reader-async-threshold", "10 MiB"));

        Map<String, String> statistics = scanner.getStatistics();

        Assert.assertEquals("0", statistics.get("gauge:PaimonJniIOManagerEnabled"));
        Assert.assertEquals("0", statistics.get("gauge:PaimonJniRequiredFieldCount"));
        Assert.assertEquals("13", statistics.get("counter:PaimonJniSplitEncodedLength"));
        Assert.assertEquals("17", statistics.get("counter:PaimonJniPredicateEncodedLength"));
        Assert.assertEquals("1", statistics.get("gauge:PaimonJniAsyncThresholdConfigured"));
        Assert.assertEquals(String.valueOf(10L * 1024L * 1024L),
                statistics.get("bytes_gauge:PaimonJniAsyncThresholdBytes"));
        Assert.assertTrue(statistics.containsKey("gauge:PaimonJniAsyncReaderThreadCount"));
        Assert.assertTrue(statistics.containsKey("gauge:PaimonJniActiveScannerCount"));
        Assert.assertFalse(statistics.containsKey("peak:PaimonJniActiveScannerPeakCount"));
        Assert.assertFalse(statistics.containsKey("peak:PaimonJniAsyncReaderThreadPeakCount"));
        Assert.assertTrue(statistics.containsKey("counter:PaimonJniReadBatchCalls"));
        Assert.assertTrue(statistics.containsKey("timer:PaimonJniScannerOpenTime"));
        Assert.assertTrue(statistics.containsKey("timer:PaimonJniReadBatchTime"));
        Assert.assertTrue(Long.parseLong(statistics.get("bytes_gauge:PaimonJniJvmHeapUsed")) > 0);
        Assert.assertTrue(Long.parseLong(statistics.get("bytes_gauge:PaimonJniJvmHeapCommitted")) > 0);
    }

    @Test
    public void testCountThreadsByNamePrefix() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            started.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "paimon-reader-async-thread-test");

        thread.start();
        try {
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(PaimonJniScanner.countThreadsByNamePrefix("paimon-reader-async-thread") >= 1);
        } finally {
            release.countDown();
            thread.join(5000);
        }
    }

    @Test
    public void testParseDataSizeBytes() {
        Assert.assertEquals(Long.valueOf(1024L), PaimonJniScanner.parseDataSizeBytes("1 KiB").get());
        Assert.assertEquals(Long.valueOf(10L * 1024L * 1024L),
                PaimonJniScanner.parseDataSizeBytes("10 MiB").get());
        Assert.assertEquals(Long.valueOf(2L * 1024L * 1024L * 1024L),
                PaimonJniScanner.parseDataSizeBytes("2GB").get());
        Assert.assertFalse(PaimonJniScanner.parseDataSizeBytes("unknown").isPresent());
    }

    @Test
    public void testCloseReleasesActiveRecordIterator() throws Exception {
        PaimonJniScanner scanner = new PaimonJniScanner(128, createBaseParams());
        AtomicBoolean released = new AtomicBoolean(false);
        RecordReader.RecordIterator<InternalRow> recordIterator =
                new RecordReader.RecordIterator<InternalRow>() {
                    @Override
                    public InternalRow next() {
                        return null;
                    }

                    @Override
                    public void releaseBatch() {
                        released.set(true);
                    }
                };

        Field recordIteratorField = PaimonJniScanner.class.getDeclaredField("recordIterator");
        recordIteratorField.setAccessible(true);
        recordIteratorField.set(scanner, recordIterator);

        scanner.close();

        Assert.assertTrue(released.get());
        Assert.assertNull(recordIteratorField.get(scanner));
    }

    @Test
    public void testFailedCloseRetainsResourcesForRetry() throws Exception {
        PaimonJniScanner scanner = new PaimonJniScanner(128, createBaseParams());
        AtomicInteger iteratorCloseCalls = new AtomicInteger();
        RecordReader.RecordIterator<InternalRow> recordIterator =
                new RecordReader.RecordIterator<InternalRow>() {
                    @Override
                    public InternalRow next() {
                        return null;
                    }

                    @Override
                    public void releaseBatch() {
                        if (iteratorCloseCalls.incrementAndGet() == 1) {
                            throw new RuntimeException("injected iterator close failure");
                        }
                    }
                };
        AtomicInteger readerCloseCalls = new AtomicInteger();
        RecordReader<InternalRow> reader = new RecordReader<InternalRow>() {
            @Override
            public RecordIterator<InternalRow> readBatch() {
                return null;
            }

            @Override
            public void close() throws IOException {
                if (readerCloseCalls.incrementAndGet() == 1) {
                    throw new IOException("injected reader close failure");
                }
            }
        };
        RetryableIOManager ioManager = new RetryableIOManager();

        Field recordIteratorField = PaimonJniScanner.class.getDeclaredField("recordIterator");
        recordIteratorField.setAccessible(true);
        recordIteratorField.set(scanner, recordIterator);
        Field readerField = PaimonJniScanner.class.getDeclaredField("reader");
        readerField.setAccessible(true);
        readerField.set(scanner, reader);
        Field ioManagerField = PaimonJniScanner.class.getDeclaredField("ioManager");
        ioManagerField.setAccessible(true);
        ioManagerField.set(scanner, ioManager);

        try {
            scanner.close();
            Assert.fail("expected the first close to fail");
        } catch (IOException expected) {
            Assert.assertEquals("Failed to release Paimon record iterator", expected.getMessage());
        }
        Assert.assertSame(recordIterator, recordIteratorField.get(scanner));
        Assert.assertSame(reader, readerField.get(scanner));
        Assert.assertSame(ioManager, ioManagerField.get(scanner));

        scanner.close();
        Assert.assertNull(recordIteratorField.get(scanner));
        Assert.assertNull(readerField.get(scanner));
        Assert.assertNull(ioManagerField.get(scanner));
        Assert.assertEquals(2, iteratorCloseCalls.get());
        Assert.assertEquals(2, readerCloseCalls.get());
        Assert.assertEquals(2, ioManager.closeCalls.get());
    }

    private Map<String, String> createBaseParams() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "");
        params.put("columns_types", "");
        params.put("paimon_split", "");
        params.put("paimon_predicate", "");
        return params;
    }

    private void setTableOptions(PaimonJniScanner scanner, Map<String, String> options) throws Exception {
        Table table = (Table) Proxy.newProxyInstance(
                Table.class.getClassLoader(), new Class[] {Table.class}, (proxy, method, args) -> {
                    if ("options".equals(method.getName())) {
                        return options;
                    }
                    if ("toString".equals(method.getName())) {
                        return "TestPaimonTable";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        Field tableField = PaimonJniScanner.class.getDeclaredField("table");
        tableField.setAccessible(true);
        tableField.set(scanner, table);
    }

    public static class TestIOManager implements IOManager {
        private final String[] tempDirs;

        public TestIOManager(String[] tempDirs) {
            this.tempDirs = tempDirs;
        }

        @Override
        public FileIOChannel.ID createChannel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIOChannel.ID createChannel(String channelName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[] tempDirs() {
            return tempDirs;
        }

        @Override
        public FileIOChannel.Enumerator createChannelEnumerator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channel) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferFileReader createBufferFileReader(FileIOChannel.ID channel) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    public static class RetryableIOManager extends TestIOManager {
        private final AtomicInteger closeCalls = new AtomicInteger();

        public RetryableIOManager() {
            super(new String[0]);
        }

        @Override
        public void close() {
            if (closeCalls.incrementAndGet() == 1) {
                throw new RuntimeException("injected IO manager close failure");
            }
        }
    }

    @Test
    public void testGetFieldIndexMatchesMixedCaseColumns() {
        Assert.assertEquals(1, PaimonJniScanner.getFieldIndex(Arrays.asList("data", "mIxEd_COL", "PART"),
                "mixed_col"));
        Assert.assertEquals(2, PaimonJniScanner.getFieldIndex(Arrays.asList("data", "mIxEd_COL", "PART"),
                "part"));
        Assert.assertEquals(-1, PaimonJniScanner.getFieldIndex(Arrays.asList("data", "mIxEd_COL", "PART"),
                "missing_col"));
    }
}

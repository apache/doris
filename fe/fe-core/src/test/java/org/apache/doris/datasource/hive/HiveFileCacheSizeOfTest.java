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

package org.apache.doris.datasource.hive;

import org.apache.doris.common.util.LocationPath;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.BlockLocation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance and concurrency tests for SizeOf calculations in HiveMetaStoreCache.
 *
 * Test scenarios:
 * 1. Single call latency - measure overhead of one SizeOf calculation
 * 2. Batch calculation - simulate real cache sizes (1K, 10K, 100K entries)
 * 3. Concurrent access - multiple threads calculating sizes simultaneously
 * 4. Real scenario simulation - size calculation while cache is being modified
 */
public class HiveFileCacheSizeOfTest {

    private static final int WARMUP_ITERATIONS = 1000;

    @BeforeAll
    public static void warmup() {
        // JVM warmup to ensure JIT compilation
        HiveMetaStoreCache.FileCacheKey key = createFileCacheKey(0, 10);
        HiveMetaStoreCache.FileCacheValue value = createFileCacheValue(10, 3);
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            key.getRetainedSizeInBytes();
            value.getRetainedSizeInBytes();
        }
    }

    // ==================== Helper Methods ====================

    private static HiveMetaStoreCache.FileCacheKey createFileCacheKey(int id, int partitionCount) {
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitionValues.add("partition_value_" + i + "_" + id);
        }
        return new HiveMetaStoreCache.FileCacheKey(
                id,
                "hdfs://namenode:8020/user/hive/warehouse/db.db/table/partition=" + id + "/data",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                partitionValues
        );
    }

    private static HiveMetaStoreCache.FileCacheValue createFileCacheValue(int fileCount, int blockCount) {
        HiveMetaStoreCache.FileCacheValue value = new HiveMetaStoreCache.FileCacheValue();
        value.setPartitionValues(Lists.newArrayList("part1", "part2"));

        for (int i = 0; i < fileCount; i++) {
            HiveMetaStoreCache.HiveFileStatus status = new HiveMetaStoreCache.HiveFileStatus();
            status.setLength(128 * 1024 * 1024L); // 128MB
            status.setBlockSize(128 * 1024 * 1024L);
            status.setModificationTime(System.currentTimeMillis());

            // Create block locations
            BlockLocation[] blocks = new BlockLocation[blockCount];
            for (int j = 0; j < blockCount; j++) {
                blocks[j] = new BlockLocation(
                        new String[]{"host1:50010", "host2:50010", "host3:50010"},
                        new String[]{"host1", "host2", "host3"},
                        j * 64 * 1024 * 1024L,
                        64 * 1024 * 1024L
                );
            }
            status.setBlockLocations(blocks);

            // Set path
            String location = "hdfs://namenode:8020/user/hive/warehouse/db.db/table/part=1/file_" + i + ".parquet";
            status.setPath(LocationPath.of(location));

            // Use addFileStatus to properly update cached size
            value.addFileStatus(status);
        }
        return value;
    }

    // ==================== Test 1: Single Call Latency ====================

    @Test
    public void testSingleCallLatency() {
        HiveMetaStoreCache.FileCacheKey key = createFileCacheKey(1, 5);
        HiveMetaStoreCache.FileCacheValue value = createFileCacheValue(10, 3);

        int iterations = 10000;

        // Measure FileCacheKey.getRetainedSizeInBytes()
        long startKey = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            key.getRetainedSizeInBytes();
        }
        long keyTimeNs = (System.nanoTime() - startKey) / iterations;

        // Measure FileCacheValue.getRetainedSizeInBytes()
        long startValue = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            value.getRetainedSizeInBytes();
        }
        long valueTimeNs = (System.nanoTime() - startValue) / iterations;

        System.out.println("=== Single Call Latency ===");
        System.out.println("FileCacheKey.getRetainedSizeInBytes(): " + keyTimeNs + " ns/call");
        System.out.println("FileCacheValue.getRetainedSizeInBytes() (10 files, 3 blocks each): " + valueTimeNs + " ns/call");

        // Sanity check: should be sub-microsecond for key, and reasonable for value
        Assertions.assertTrue(keyTimeNs < 10_000, "Key size calculation should be < 10us, got: " + keyTimeNs + "ns");
        Assertions.assertTrue(valueTimeNs < 100_000, "Value size calculation should be < 100us, got: " + valueTimeNs + "ns");
    }

    // ==================== Test 2: Batch Calculation Performance ====================

    @Test
    public void testBatchCalculationPerformance() {
        int[] cacheSizes = {1_000, 10_000, 100_000};

        System.out.println("\n=== Batch Calculation Performance ===");
        System.out.printf("%-15s %-15s %-20s %-15s%n", "Cache Size", "Total Time(ms)", "Avg Per Entry(ns)", "Memory(MB)");

        for (int cacheSize : cacheSizes) {
            // Create cache entries
            List<HiveMetaStoreCache.FileCacheKey> keys = new ArrayList<>(cacheSize);
            List<HiveMetaStoreCache.FileCacheValue> values = new ArrayList<>(cacheSize);

            for (int i = 0; i < cacheSize; i++) {
                keys.add(createFileCacheKey(i, 3));
                values.add(createFileCacheValue(5, 2)); // 5 files, 2 blocks each
            }

            // Measure total calculation time
            long startTime = System.nanoTime();
            long totalSize = 0;
            for (int i = 0; i < cacheSize; i++) {
                totalSize += keys.get(i).getRetainedSizeInBytes();
                totalSize += values.get(i).getRetainedSizeInBytes();
            }
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            long avgPerEntryNs = TimeUnit.NANOSECONDS.toNanos(System.nanoTime() - startTime) / cacheSize;

            System.out.printf("%-15d %-15d %-20d %-15.2f%n",
                    cacheSize, elapsedMs, avgPerEntryNs, totalSize / (1024.0 * 1024.0));

            // Performance assertions
            if (cacheSize == 10_000) {
                // 10K entries should complete in < 500ms
                Assertions.assertTrue(elapsedMs < 500,
                        "10K entries should calculate in < 500ms, got: " + elapsedMs + "ms");
            }
        }
    }

    // ==================== Test 3: Concurrent Access Safety ====================

    @Test
    public void testConcurrentSizeCalculation() throws Exception {
        int threadCount = 8;
        int iterations = 1000;

        // Shared cache entries
        HiveMetaStoreCache.FileCacheKey key = createFileCacheKey(1, 5);
        HiveMetaStoreCache.FileCacheValue value = createFileCacheValue(10, 3);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicLong totalKeySize = new AtomicLong(0);
        AtomicLong totalValueSize = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);

        // Expected values (calculated once for comparison)
        long expectedKeySize = key.getRetainedSizeInBytes();
        long expectedValueSize = value.getRetainedSizeInBytes();

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    for (int i = 0; i < iterations; i++) {
                        long keySize = key.getRetainedSizeInBytes();
                        long valueSize = value.getRetainedSizeInBytes();

                        totalKeySize.addAndGet(keySize);
                        totalValueSize.addAndGet(valueSize);

                        // Verify consistency
                        if (keySize != expectedKeySize || valueSize != expectedValueSize) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long startTime = System.nanoTime();
        startLatch.countDown(); // Start all threads
        doneLatch.await(30, TimeUnit.SECONDS);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        executor.shutdown();

        int totalCalls = threadCount * iterations;
        System.out.println("\n=== Concurrent Access Test ===");
        System.out.println("Threads: " + threadCount);
        System.out.println("Iterations per thread: " + iterations);
        System.out.println("Total calls: " + totalCalls);
        System.out.println("Total time: " + elapsedMs + " ms");
        System.out.println("Throughput: " + (totalCalls * 1000L / elapsedMs) + " calls/sec");
        System.out.println("Errors: " + errorCount.get());

        // Assertions
        Assertions.assertEquals(0, errorCount.get(), "Should have no calculation errors");
        Assertions.assertEquals(expectedKeySize * totalCalls, totalKeySize.get(), "Key sizes should be consistent");
        Assertions.assertEquals(expectedValueSize * totalCalls, totalValueSize.get(), "Value sizes should be consistent");
    }

    // ==================== Test 4: Real Scenario - Concurrent Read/Write ====================

    @Test
    public void testConcurrentReadWriteScenario() throws Exception {
        int readerThreads = 4;
        int writerThreads = 2;
        int durationMs = 3000;

        // Simulate a cache using ConcurrentHashMap
        ConcurrentHashMap<HiveMetaStoreCache.FileCacheKey, HiveMetaStoreCache.FileCacheValue> cache =
                new ConcurrentHashMap<>();

        // Pre-populate with some entries
        for (int i = 0; i < 1000; i++) {
            cache.put(createFileCacheKey(i, 3), createFileCacheValue(5, 2));
        }

        AtomicLong readerOps = new AtomicLong(0);
        AtomicLong writerOps = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);
        AtomicBoolean running = new AtomicBoolean(true);

        ExecutorService executor = Executors.newFixedThreadPool(readerThreads + writerThreads);

        // Reader threads - simulate getTotalFileCacheMemorySizeBytes()
        for (int t = 0; t < readerThreads; t++) {
            executor.submit(() -> {
                while (running.get()) {
                    try {
                        long totalSize = 0;
                        for (Map.Entry<HiveMetaStoreCache.FileCacheKey, HiveMetaStoreCache.FileCacheValue> entry
                                : cache.entrySet()) {
                            totalSize += entry.getKey().getRetainedSizeInBytes();
                            totalSize += entry.getValue().getRetainedSizeInBytes();
                        }
                        readerOps.incrementAndGet();
                        // Sanity check: size should be positive
                        if (totalSize <= 0) {
                            errors.incrementAndGet();
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }
            });
        }

        // Writer threads - simulate cache updates
        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                int counter = 0;
                while (running.get()) {
                    try {
                        int id = 10000 + threadId * 100000 + counter++;
                        HiveMetaStoreCache.FileCacheKey key = createFileCacheKey(id, 3);
                        HiveMetaStoreCache.FileCacheValue value = createFileCacheValue(5, 2);
                        cache.put(key, value);

                        // Also remove old entries to keep cache size bounded
                        if (counter % 10 == 0 && cache.size() > 2000) {
                            Iterator<HiveMetaStoreCache.FileCacheKey> iterator = cache.keySet().iterator();
                            if (iterator.hasNext()) {
                                cache.remove(iterator.next());
                            }
                        }
                        writerOps.incrementAndGet();
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }
            });
        }

        // Run for specified duration
        Thread.sleep(durationMs);
        running.set(false);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n=== Concurrent Read/Write Scenario ===");
        System.out.println("Duration: " + durationMs + " ms");
        System.out.println("Reader threads: " + readerThreads);
        System.out.println("Writer threads: " + writerThreads);
        System.out.println("Reader ops (size calculations): " + readerOps.get());
        System.out.println("Writer ops (cache updates): " + writerOps.get());
        System.out.println("Reader throughput: " + (readerOps.get() * 1000L / durationMs) + " ops/sec");
        System.out.println("Final cache size: " + cache.size());
        System.out.println("Errors: " + errors.get());

        // Assertions
        Assertions.assertEquals(0, errors.get(), "Should have no errors during concurrent access");
        Assertions.assertTrue(readerOps.get() > 0, "Should have completed some reader operations");
        Assertions.assertTrue(writerOps.get() > 0, "Should have completed some writer operations");
    }

    // ==================== Test 5: Memory Size Accuracy ====================

    @Test
    public void testMemorySizeAccuracy() {
        // Test with known structures to verify size calculation is reasonable
        HiveMetaStoreCache.FileCacheKey key = createFileCacheKey(1, 0);
        long keySize = key.getRetainedSizeInBytes();

        HiveMetaStoreCache.FileCacheValue emptyValue = new HiveMetaStoreCache.FileCacheValue();
        long emptyValueSize = emptyValue.getRetainedSizeInBytes();

        HiveMetaStoreCache.FileCacheValue valueWith10Files = createFileCacheValue(10, 3);
        long valueWith10FilesSize = valueWith10Files.getRetainedSizeInBytes();

        HiveMetaStoreCache.FileCacheValue valueWith100Files = createFileCacheValue(100, 3);
        long valueWith100FilesSize = valueWith100Files.getRetainedSizeInBytes();

        HiveMetaStoreCache.FileCacheValue valueWith100KFiles = createFileCacheValue(100_000, 3);
        long valueWith100KFilesSize = valueWith100KFiles.getRetainedSizeInBytes();

        System.out.println("\n=== Memory Size Accuracy ===");
        System.out.println("FileCacheKey (no partitions): " + keySize + " bytes");
        System.out.println("FileCacheValue (empty): " + emptyValueSize + " bytes");
        System.out.println("FileCacheValue (10 files, 3 blocks): " + valueWith10FilesSize + " bytes");
        System.out.println("FileCacheValue (100 files, 3 blocks): " + valueWith100FilesSize + " bytes");
        System.out.println("FileCacheValue (100K files, 3 blocks): " + valueWith100KFilesSize + " bytes"
                + " (" + String.format("%.2f", valueWith100KFilesSize / (1024.0 * 1024.0)) + " MB)");

        // Sanity checks
        Assertions.assertTrue(keySize > 0, "Key size should be positive");
        Assertions.assertTrue(emptyValueSize > 0, "Empty value size should be positive");
        Assertions.assertTrue(valueWith10FilesSize > emptyValueSize, "Value with files should be larger");
        // 100 files should be roughly 10x the size of 10 files (for the files portion)
        double ratio = (double) valueWith100FilesSize / valueWith10FilesSize;
        System.out.println("Size ratio (100 files / 10 files): " + String.format("%.2f", ratio));
        Assertions.assertTrue(ratio > 5 && ratio < 15, "Size should scale roughly linearly with file count");

        // 100K files should be roughly 1000x the size of 100 files
        double ratio100K = (double) valueWith100KFilesSize / valueWith100FilesSize;
        System.out.println("Size ratio (100K files / 100 files): " + String.format("%.2f", ratio100K));
        Assertions.assertTrue(ratio100K > 500 && ratio100K < 1500,
                "Size should scale roughly linearly with file count");
    }

    // ==================== Test 6: Large File Count Scalability ====================

    @Test
    public void testLargeFileCountScalability() {
        int[] fileCounts = {10, 100, 1_000, 10_000, 100_000};
        int blockCount = 3;

        System.out.println("\n=== Large File Count Scalability ===");
        System.out.printf("%-15s %-20s %-20s %-15s%n",
                "File Count", "Create Time(ms)", "GetSize Time(ns)", "Memory(MB)");

        for (int fileCount : fileCounts) {
            // Measure creation time (includes incremental size calculation)
            long createStart = System.nanoTime();
            HiveMetaStoreCache.FileCacheValue value = createFileCacheValue(fileCount, blockCount);
            long createTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - createStart);

            // Get size (should be O(1) now)
            long size = value.getRetainedSizeInBytes();

            // Call multiple times to get average
            int iterations = 1000;
            long totalGetSizeTime = 0;
            for (int i = 0; i < iterations; i++) {
                long start = System.nanoTime();
                value.getRetainedSizeInBytes();
                totalGetSizeTime += System.nanoTime() - start;
            }
            long avgGetSizeTimeNs = totalGetSizeTime / iterations;

            double memoryMb = size / (1024.0 * 1024.0);

            System.out.printf("%-15d %-20d %-20d %-15.2f%n",
                    fileCount, createTimeMs, avgGetSizeTimeNs, memoryMb);

            // Assertions
            // getRetainedSizeInBytes should be O(1), so even with 100K files it should be < 1000ns
            if (fileCount >= 10_000) {
                Assertions.assertTrue(avgGetSizeTimeNs < 1000,
                        "getRetainedSizeInBytes() should be O(1), but took " + avgGetSizeTimeNs + "ns for " + fileCount + " files");
            }
        }
    }
}

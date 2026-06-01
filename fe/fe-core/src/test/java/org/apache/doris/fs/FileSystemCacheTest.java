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

package org.apache.doris.fs;

import org.apache.doris.datasource.property.storage.StorageProperties;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSystemCacheTest {

    @Test
    public void testEvictedFileSystemClosesAfterLastLeaseIsReleased() {
        CountingFileSystem first = new CountingFileSystem();
        CountingFileSystem second = new CountingFileSystem();
        AtomicInteger loadCount = new AtomicInteger();
        FileSystemCache cache = new FileSystemCache(1L, OptionalLong.empty(),
                key -> loadCount.getAndIncrement() == 0 ? first : second);

        FileSystemCache.FileSystemCacheKey firstKey = key("hdfs://ns1");
        FileSystemCache.FileSystemLease firstLease = cache.getFileSystem(firstKey);
        Assert.assertSame(first, firstLease.fileSystem());

        FileSystemCache.FileSystemLease secondLease = cache.getFileSystem(key("hdfs://ns2"));
        cache.cleanUp();

        Assert.assertEquals(0, first.getCloseCount());
        Assert.assertEquals(0, second.getCloseCount());

        firstLease.close();
        Assert.assertEquals(1, first.getCloseCount());
        Assert.assertEquals(0, second.getCloseCount());

        secondLease.close();
        Assert.assertEquals(0, second.getCloseCount());
    }

    @Test
    public void testUncachedFileSystemClosesWhenLeaseIsReleased() {
        CountingFileSystem fileSystem = new CountingFileSystem();
        FileSystemCache cache = new FileSystemCache(0L, OptionalLong.empty(), key -> fileSystem);

        FileSystemCache.FileSystemLease lease = cache.getFileSystem(key("hdfs://ns1"));
        Assert.assertSame(fileSystem, lease.fileSystem());
        Assert.assertEquals(0, fileSystem.getCloseCount());

        lease.close();
        Assert.assertEquals(1, fileSystem.getCloseCount());
    }

    @Test
    public void testLeaseCloseIsConcurrentIdempotent() throws InterruptedException {
        CountingFileSystem fileSystem = new CountingFileSystem();
        FileSystemCache cache = new FileSystemCache(1L, OptionalLong.empty(), key -> fileSystem);

        FileSystemCache.FileSystemLease lease = cache.getFileSystem(key("hdfs://ns1"));
        FileSystemCache.FileSystemLease evictingLease = cache.getFileSystem(key("hdfs://ns2"));
        cache.cleanUp();

        closeConcurrently(lease);
        Assert.assertEquals(1, fileSystem.getCloseCount());

        evictingLease.close();
    }

    @Test
    public void testUncachedLeaseCloseIsConcurrentIdempotent() throws InterruptedException {
        CountingFileSystem fileSystem = new CountingFileSystem();
        FileSystemCache cache = new FileSystemCache(0L, OptionalLong.empty(), key -> fileSystem);

        closeConcurrently(cache.getFileSystem(key("hdfs://ns1")));

        Assert.assertEquals(1, fileSystem.getCloseCount());
    }

    private static void closeConcurrently(FileSystemCache.FileSystemLease lease) throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        Thread first = closeThread(lease, ready, start);
        Thread second = closeThread(lease, ready, start);
        first.start();
        second.start();
        ready.await();
        start.countDown();
        first.join();
        second.join();
    }

    private static Thread closeThread(FileSystemCache.FileSystemLease lease,
            CountDownLatch ready, CountDownLatch start) {
        return new Thread(() -> {
            ready.countDown();
            try {
                start.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            lease.close();
        });
    }

    private static FileSystemCache.FileSystemCacheKey key(String fsIdent) {
        return new FileSystemCache.FileSystemCacheKey(fsIdent, StorageProperties.createPrimary(
                Collections.singletonMap(StorageProperties.FS_HDFS_SUPPORT, "true")));
    }

    private static class CountingFileSystem extends MemoryFileSystem {
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        int getCloseCount() {
            return closeCount.get();
        }
    }
}

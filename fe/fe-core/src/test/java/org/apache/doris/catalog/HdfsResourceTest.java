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

package org.apache.doris.catalog;

import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class HdfsResourceTest {
    private static final long TIMEOUT_SECONDS = 5;

    @Test
    public void testModifyWaitsForProcNodeRead() throws Exception {
        BlockingReadHdfsResource resource = createResource();
        assertModifyWaitsForRead(resource, () -> {
            resource.getProcNodeData(new BaseProcResult());
            return null;
        });
    }

    @Test
    public void testModifyWaitsForCopiedPropertiesRead() throws Exception {
        BlockingReadHdfsResource resource = createResource();
        assertModifyWaitsForRead(resource, resource::getCopiedProperties);
    }

    private BlockingReadHdfsResource createResource() throws Exception {
        BlockingReadHdfsResource resource = new BlockingReadHdfsResource("hdfs_resource");
        resource.setProperties(ImmutableMap.of(
                HdfsResource.HADOOP_FS_NAME, "hdfs://namenode:8020",
                "hadoop.username", "doris"));
        return resource;
    }

    private void assertModifyWaitsForRead(BlockingReadHdfsResource resource, Callable<?> readTask)
            throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch writeCompleted = new CountDownLatch(1);
        try {
            Future<?> readFuture = executor.submit(readTask);
            Assert.assertTrue(resource.readLockEntered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            Future<?> writeFuture = executor.submit(() -> {
                try {
                    resource.modifyProperties(ImmutableMap.of("dfs.replication", "3"));
                } finally {
                    writeCompleted.countDown();
                }
                return null;
            });

            Assert.assertTrue(resource.writeLockAttempted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertFalse(writeCompleted.await(100, TimeUnit.MILLISECONDS));

            resource.allowReadToFinish.countDown();
            readFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            writeFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assert.assertEquals("3", resource.getCopiedProperties().get("dfs.replication"));
        } finally {
            resource.allowReadToFinish.countDown();
            executor.shutdownNow();
        }
    }

    private static class BlockingReadHdfsResource extends HdfsResource {
        private final CountDownLatch readLockEntered = new CountDownLatch(1);
        private final CountDownLatch allowReadToFinish = new CountDownLatch(1);
        private final CountDownLatch writeLockAttempted = new CountDownLatch(1);

        private BlockingReadHdfsResource(String name) {
            super(name);
        }

        @Override
        public void readLock() {
            super.readLock();
            readLockEntered.countDown();
            try {
                allowReadToFinish.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        @Override
        public void writeLock() {
            writeLockAttempted.countDown();
            super.writeLock();
        }
    }
}

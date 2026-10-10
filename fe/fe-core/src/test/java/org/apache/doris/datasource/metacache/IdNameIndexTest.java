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

package org.apache.doris.datasource.metacache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class IdNameIndexTest {

    @Test
    public void testPutAndIdempotentPut() {
        IdNameIndex index = new IdNameIndex("test database");

        index.put(1L, "db1");
        index.checkCanPut(1L, "db1");
        index.put(1L, "db1");

        Assertions.assertEquals("db1", index.getName(1L));
        Assertions.assertTrue(index.containsMappingForTest(1L, "db1"));
        Assertions.assertEquals(1, index.sizeForTest());
    }

    @Test
    public void testCheckCanPutRejectsConflictWithoutPublishing() {
        IdNameIndex index = new IdNameIndex("test database");
        index.put(1L, "db1");

        Assertions.assertThrows(IllegalStateException.class, () -> index.checkCanPut(2L, "db1"));
        Assertions.assertThrows(IllegalStateException.class, () -> index.checkCanPut(1L, "db2"));

        Assertions.assertTrue(index.containsMappingForTest(1L, "db1"));
        Assertions.assertNull(index.getName(2L));
        Assertions.assertEquals(1, index.sizeForTest());
    }

    @Test
    public void testCompatibleCheckDoesNotWaitForMutationMonitor() throws Exception {
        IdNameIndex index = new IdNameIndex("test database");
        index.put(1L, "db1");

        assertCheckDoesNotWaitForMutationMonitor(index, () -> index.checkCanPut(1L, "db1"));
    }

    @Test
    public void testAbsentCheckDoesNotWaitForMutationMonitor() throws Exception {
        IdNameIndex index = new IdNameIndex("test database");

        assertCheckDoesNotWaitForMutationMonitor(index, () -> index.checkCanPut(1L, "db1"));
    }

    @Test
    public void testRejectsSameNameWithDifferentIdWithoutPartialUpdate() {
        IdNameIndex index = new IdNameIndex("test database");
        index.put(1L, "db1");

        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class, () -> index.put(2L, "db1"));

        Assertions.assertTrue(exception.getMessage().contains("test database"));
        Assertions.assertTrue(exception.getMessage().contains("id 1"));
        Assertions.assertTrue(exception.getMessage().contains("id 2"));
        Assertions.assertTrue(index.containsMappingForTest(1L, "db1"));
        Assertions.assertNull(index.getName(2L));
        Assertions.assertEquals(1, index.sizeForTest());
    }

    @Test
    public void testRejectsSameIdWithDifferentNameWithoutPartialUpdate() {
        IdNameIndex index = new IdNameIndex("test table");
        index.put(1L, "tbl1");

        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class, () -> index.put(1L, "tbl2"));

        Assertions.assertTrue(exception.getMessage().contains("test table"));
        Assertions.assertTrue(exception.getMessage().contains("tbl1"));
        Assertions.assertTrue(exception.getMessage().contains("tbl2"));
        Assertions.assertTrue(index.containsMappingForTest(1L, "tbl1"));
        Assertions.assertEquals(1, index.sizeForTest());
    }

    @Test
    public void testRemoveNameAndMissingRemovalAreIdempotent() {
        IdNameIndex index = new IdNameIndex("test table");
        index.put(1L, "tbl1");
        index.put(2L, "tbl2");

        index.removeName("tbl1");
        index.removeName("tbl1");

        Assertions.assertNull(index.getName(1L));
        Assertions.assertEquals("tbl2", index.getName(2L));
        Assertions.assertEquals(1, index.sizeForTest());
    }

    @Test
    public void testClearRemovesBothDirections() {
        IdNameIndex index = new IdNameIndex("test table");
        index.put(1L, "tbl1");
        index.put(2L, "tbl2");

        index.clear();

        Assertions.assertNull(index.getName(1L));
        Assertions.assertNull(index.getName(2L));
        Assertions.assertEquals(0, index.sizeForTest());
        index.put(3L, "tbl1");
        Assertions.assertTrue(index.containsMappingForTest(3L, "tbl1"));
    }

    @Test
    public void testCaseInsensitiveFallbackDoesNotChangeExactIdentity() {
        IdNameIndex index = new IdNameIndex("test table");
        index.put(1L, "MixedTable");

        Assertions.assertEquals("MixedTable", index.findNameIgnoreCase("mixedtable"));
        Assertions.assertNull(index.findNameIgnoreCase("missing"));
        Assertions.assertTrue(index.containsMappingForTest(1L, "MixedTable"));
    }

    @Test
    public void testConcurrentConflictingPublicationKeepsOneCompleteMapping() throws Exception {
        IdNameIndex index = new IdNameIndex("test database");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<Boolean> first = executor.submit(() -> putAfterStart(index, start, 1L, "db"));
            Future<Boolean> second = executor.submit(() -> putAfterStart(index, start, 2L, "db"));
            start.countDown();

            Assertions.assertNotEquals(first.get(), second.get());
            Assertions.assertEquals(1, index.sizeForTest());
            String firstName = index.getName(1L);
            String secondName = index.getName(2L);
            Assertions.assertTrue(("db".equals(firstName) && secondName == null)
                    || (firstName == null && "db".equals(secondName)));
        } finally {
            executor.shutdownNow();
        }
    }

    private static boolean putAfterStart(
            IdNameIndex index, CountDownLatch start, long id, String name) throws InterruptedException {
        start.await();
        try {
            index.put(id, name);
            return true;
        } catch (IllegalStateException e) {
            return false;
        }
    }

    private static void assertCheckDoesNotWaitForMutationMonitor(
            IdNameIndex index, Runnable check) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        try {
            synchronized (index) {
                Future<?> result = executor.submit(() -> {
                    started.countDown();
                    check.run();
                });
                Assertions.assertTrue(started.await(3L, TimeUnit.SECONDS));
                result.get(3L, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }
}

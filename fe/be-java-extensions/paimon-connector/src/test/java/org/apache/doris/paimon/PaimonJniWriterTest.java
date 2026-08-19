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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PaimonJniWriterTest {

    @Test
    public void testManagedMemoryPoolRequiresAtLeastOnePage() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new DorisMemorySegmentPool(32 * 1024, 64 * 1024, 1L));
        Assertions.assertTrue(exception.getMessage().contains("at least one page"));
    }

    @Test
    public void testOpenFailureRestoresContextClassLoader() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        URLClassLoader testClassLoader = new URLClassLoader(new URL[0], originalClassLoader);
        PaimonJniWriter writer = new PaimonJniWriter();
        thread.setContextClassLoader(testClassLoader);
        try {
            Assertions.assertThrows(Exception.class, () -> writer.open(
                    "not-a-serialized-table", Collections.emptyMap(), new String[0],
                    1L, "test-user", false, false, "UTC", 64L * 1024 * 1024, 1L, 1L));
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
        } finally {
            try {
                writer.close();
                Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
            } finally {
                thread.setContextClassLoader(originalClassLoader);
                testClassLoader.close();
            }
        }
    }

    @Test
    public void testAbortRestoresContextClassLoader() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        URLClassLoader testClassLoader = new URLClassLoader(new URL[0], originalClassLoader);
        PaimonJniWriter writer = new PaimonJniWriter();
        thread.setContextClassLoader(testClassLoader);
        try {
            writer.abort();
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
        } finally {
            try {
                writer.close();
            } finally {
                thread.setContextClassLoader(originalClassLoader);
                testClassLoader.close();
            }
        }
    }

    @Test
    public void testDataEntryPointFailuresRestoreContextClassLoader() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        URLClassLoader testClassLoader = new URLClassLoader(new URL[0], originalClassLoader);
        PaimonJniWriter writer = new PaimonJniWriter();
        thread.setContextClassLoader(testClassLoader);
        try {
            Assertions.assertThrows(Exception.class,
                    () -> writer.write(ByteBuffer.allocateDirect(0)));
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());

            Assertions.assertThrows(Exception.class, writer::prepareCommit);
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
        } finally {
            thread.setContextClassLoader(originalClassLoader);
            testClassLoader.close();
        }
    }

    @Test
    public void testCloseWaitsForCompactionExecutor() throws Exception {
        PaimonJniWriter writer = new PaimonJniWriter();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        try {
            executor.execute(() -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    stopped.countDown();
                }
            });
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));

            Field executorField = PaimonJniWriter.class.getDeclaredField("compactionExecutor");
            executorField.setAccessible(true);
            executorField.set(writer, executor);

            writer.close();
            Assertions.assertTrue(stopped.await(10, TimeUnit.SECONDS));
            Assertions.assertTrue(executor.isTerminated());
        } finally {
            executor.shutdownNow();
        }
    }
}

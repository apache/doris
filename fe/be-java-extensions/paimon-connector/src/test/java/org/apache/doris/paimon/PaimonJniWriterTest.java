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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;

public class PaimonJniWriterTest {

    @Test
    public void testManagedMemoryPoolRequiresAtLeastOnePage() {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new DorisMemorySegmentPool(32 * 1024, 64 * 1024, 1L));
        Assertions.assertTrue(exception.getMessage().contains("at least one page"));
        Assertions.assertDoesNotThrow(
                () -> new DorisMemorySegmentPool(64 * 1024, 64 * 1024, 1L));
    }

    @Test
    public void testMergeTreeMemoryPoolRequiresAtLeastThreePages() {
        int pageSize = 64 * 1024;
        long writeBufferSize = 4L * pageSize;
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonJniWriter.validateAndGetMemoryPoolLimit(
                        writeBufferSize, 2L * pageSize, pageSize, true));
        Assertions.assertTrue(exception.getMessage().contains("requires at least 3 memory pages"));
        Assertions.assertTrue(exception.getMessage().contains("effectivePoolLimit=131072"));

        Assertions.assertEquals(3L * pageSize,
                PaimonJniWriter.validateAndGetMemoryPoolLimit(
                        writeBufferSize, 3L * pageSize, pageSize, true));
        Assertions.assertEquals(pageSize,
                PaimonJniWriter.validateAndGetMemoryPoolLimit(
                        pageSize, pageSize, pageSize, false));
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
                    1L, "test-user", false, false, "UTC", System.getProperty("java.io.tmpdir"),
                    64L * 1024 * 1024, 1L));
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
                    () -> writer.writeArrow(0L, 0L));
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());

            Assertions.assertThrows(Exception.class, writer::prepareCommit);
            Assertions.assertSame(testClassLoader, thread.getContextClassLoader());
        } finally {
            thread.setContextClassLoader(originalClassLoader);
            testClassLoader.close();
        }
    }
}

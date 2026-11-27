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

package org.apache.doris.writer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

class LocalFileResourceCleanupTest {
    @Test
    void flushFailureStillAttemptsClose() throws Exception {
        LocalFileJniWriter writer = new LocalFileJniWriter(1, Collections.emptyMap());
        IOException failure = new IOException("injected flush failure");
        int[] closes = {0};
        BufferedWriter fileWriter = new BufferedWriter(new StringWriter()) {
            @Override
            public void flush() throws IOException {
                throw failure;
            }

            @Override
            public void close() throws IOException {
                closes[0]++;
                super.close();
            }
        };
        Field field = LocalFileJniWriter.class.getDeclaredField("fileWriter");
        field.setAccessible(true);
        field.set(writer, fileWriter);
        try {
            IOException error = Assertions.assertThrows(IOException.class, writer::close);
            System.out.println("CLEANUP_PROBE local-file flush failure close=" + closes[0]);
            Assertions.assertAll(
                    () -> Assertions.assertSame(failure, error),
                    () -> Assertions.assertEquals(1, closes[0], "flush must not skip close"));
        } finally {
            fileWriter.close();
        }
    }

    @Test
    void flushAndCloseFailuresRetainOriginalFailureAndDoNotRetry() throws Exception {
        IOException flushFailure = new IOException("flush failed");
        IOException closeFailure = new IOException("close failed");
        int[] closes = {0};
        LocalFileJniWriter writer = writerWith(new BufferedWriter(new StringWriter()) {
            @Override
            public void flush() throws IOException {
                throw flushFailure;
            }

            @Override
            public void close() throws IOException {
                closes[0]++;
                throw closeFailure;
            }
        });
        Assertions.assertSame(flushFailure, Assertions.assertThrows(IOException.class, writer::close));
        Assertions.assertArrayEquals(new Throwable[] {closeFailure}, flushFailure.getSuppressed());
        Assertions.assertSame(flushFailure, Assertions.assertThrows(IOException.class, writer::close));
        Assertions.assertEquals(1, closes[0]);
    }

    @Test
    void runtimeCloseFailureIsNotLostOnReentry() throws Exception {
        RuntimeException failure = new IllegalStateException("close failed");
        LocalFileJniWriter writer = writerWith(new BufferedWriter(new StringWriter()) {
            @Override
            public void close() {
                throw failure;
            }
        });
        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, writer::close));
        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, writer::close));
    }

    @Test
    void sameFlushAndCloseExceptionDoesNotSelfSuppress() throws Exception {
        IOException failure = new IOException("flush and close failed");
        LocalFileJniWriter writer = writerWith(new BufferedWriter(new StringWriter()) {
            @Override
            public void flush() throws IOException {
                throw failure;
            }

            @Override
            public void close() throws IOException {
                throw failure;
            }
        });
        Assertions.assertSame(failure, Assertions.assertThrows(IOException.class, writer::close));
        Assertions.assertEquals(0, failure.getSuppressed().length);
    }

    @Test
    void failedFlushStillClosesRealFile(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("cleanup.csv");
        BufferedWriter underlying = Files.newBufferedWriter(file);
        underlying.write("1,hello\n");
        LocalFileJniWriter writer = writerWith(new BufferedWriter(underlying) {
            @Override
            public void flush() throws IOException {
                throw new IOException("injected flush failure");
            }
        });
        Assertions.assertThrows(IOException.class, writer::close);
        Assertions.assertThrows(IOException.class, () -> underlying.write("closed"));
        Assertions.assertEquals("1,hello\n", Files.readString(file));
    }

    @Test
    void successfulRealFileCloseCanBeRepeated(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("success.csv");
        BufferedWriter underlying = Files.newBufferedWriter(file);
        underlying.write("2,ok\n");
        LocalFileJniWriter writer = writerWith(underlying);
        writer.close();
        writer.close();
        Assertions.assertEquals("2,ok\n", Files.readString(file));
        Assertions.assertThrows(IOException.class, () -> underlying.write("closed"));
    }

    private static LocalFileJniWriter writerWith(BufferedWriter fileWriter) throws Exception {
        LocalFileJniWriter writer = new LocalFileJniWriter(1, Collections.emptyMap());
        Field field = LocalFileJniWriter.class.getDeclaredField("fileWriter");
        field.setAccessible(true);
        field.set(writer, fileWriter);
        return writer;
    }

}

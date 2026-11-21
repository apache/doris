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


package org.apache.doris.connector.deltalake;

import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class DeltaLakeFileIOTest {
    @TempDir
    Path directory;

    @Test
    void listsSortedSuffixAndReadsSeekableInputs() throws Exception {
        Files.writeString(directory.resolve("a.json"), "abcdef");
        Files.writeString(directory.resolve("c.json"), "c");
        Files.writeString(directory.resolve("b.json"), "b");
        Files.createDirectory(directory.resolve("b_directory"));
        try (DeltaLakeFileIO io = fileIO(new RecordingConnectorContext())) {
            List<String> names = new ArrayList<>();
            try (CloseableIterator<FileStatus> files = io.listFrom(directory.resolve("b.json").toString())) {
                while (files.hasNext()) {
                    names.add(new org.apache.hadoop.fs.Path(files.next().getPath()).getName());
                }
            }
            Assertions.assertEquals(Arrays.asList("b.json", "c.json"), names);
            String path = io.resolvePath(directory.resolve("a.json").toString());
            Assertions.assertEquals(6, io.getFileStatus(path).getSize());
            try (SeekableInputStream stream = io.newInputFile(path, 6).newStream()) {
                stream.seek(2);
                byte[] bytes = new byte[4];
                stream.readFully(bytes, 0, bytes.length);
                Assertions.assertEquals("cdef", new String(bytes, StandardCharsets.UTF_8));
            }
            Assertions.assertThrows(UnsupportedOperationException.class, () -> io.mkdirs(path));
            Assertions.assertThrows(UnsupportedOperationException.class, () -> io.delete(path));
            Assertions.assertThrows(UnsupportedOperationException.class, () -> io.newOutputFile(path));
            Assertions.assertThrows(UnsupportedOperationException.class, () -> io.copyFileAtomically(path, path, true));
        }
    }

    @Test
    void sharesOnlyWithinCatalogAndClosesIdempotently() throws Exception {
        RecordingConnectorContext context = new RecordingConnectorContext();
        DeltaLakeFileIO first = fileIO(context);
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(directory.toUri());
        try (DeltaLakeFileIO second = fileIO(new RecordingConnectorContext())) {
            FileSystem expected = first.fileSystem(path);
            ExecutorService executor = Executors.newFixedThreadPool(4);
            try {
                List<Future<FileSystem>> reads = new ArrayList<>();
                for (int i = 0; i < 16; i++) {
                    reads.add(executor.submit(() -> first.fileSystem(path)));
                }
                for (Future<FileSystem> read : reads) {
                    Assertions.assertSame(expected, read.get());
                }
            } finally {
                executor.shutdownNow();
            }
            Assertions.assertEquals(1, context.authenticationCalls.get());
            Assertions.assertEquals(DeltaLakeFileIO.class.getClassLoader(), context.observedClassLoader);
            FileSystem independent = second.fileSystem(path);
            Assertions.assertNotSame(expected, independent);
            first.close();
            first.close();
            Assertions.assertThrows(IOException.class, () -> first.fileSystem(path));
            Assertions.assertTrue(independent.exists(path));
        } finally {
            first.close();
        }
    }

    @Test
    void restoresContextClassLoaderAfterFailure() {
        RecordingConnectorContext delegate = new RecordingConnectorContext();
        TcclPinningConnectorContext context = new TcclPinningConnectorContext(delegate, () -> null);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        IOException failure = new IOException("read failed");
        Assertions.assertSame(failure, Assertions.assertThrows(IOException.class,
                () -> context.executeAuthenticated(() -> {
                    Assertions.assertEquals(TcclPinningConnectorContext.class.getClassLoader(),
                            Thread.currentThread().getContextClassLoader());
                    throw failure;
                })));
        Assertions.assertSame(original, Thread.currentThread().getContextClassLoader());
    }

    private DeltaLakeFileIO fileIO(RecordingConnectorContext context) {
        Configuration conf = new Configuration();
        conf.setBoolean("fs.file.impl.disable.cache", true);
        return new DeltaLakeFileIO(conf, new TcclPinningConnectorContext(context, () -> null));
    }
}

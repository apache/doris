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

package org.apache.doris.filesystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

class FileSystemUtilTest {

    // --- extractParentDirectory ---

    @Test
    void extractParentDirectoryWithSlash() {
        assertEquals("hdfs://nn/a/b/", FileSystemUtil.extractParentDirectory("hdfs://nn/a/b/file.csv"));
    }

    @Test
    void extractParentDirectoryRootFile() {
        assertEquals("hdfs://nn/", FileSystemUtil.extractParentDirectory("hdfs://nn/file.csv"));
    }

    @Test
    void extractParentDirectoryNoSlash() {
        assertEquals("file.csv", FileSystemUtil.extractParentDirectory("file.csv"));
    }

    @Test
    void extractParentDirectoryMultiLevel() {
        assertEquals("s3://bucket/a/b/c/", FileSystemUtil.extractParentDirectory("s3://bucket/a/b/c/d.txt"));
    }

    @Test
    void extractParentDirectoryBucketRoot() {
        assertEquals("s3://bucket/", FileSystemUtil.extractParentDirectory("s3://bucket/file"));
    }

    // --- asyncRenameFiles ---

    @Test
    void asyncRenameFilesCallsRenameForEachFile() throws Exception {
        FileSystem mockFs = mock(FileSystem.class);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);
        List<String> fileNames = List.of("a.txt", "b.txt", "c.txt");

        FileSystemUtil.asyncRenameFiles(mockFs, Runnable::run, futures, cancelled,
                "s3://bucket/src/", "s3://bucket/dst/", fileNames);

        assertEquals(3, futures.size());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        ArgumentCaptor<Location> srcCaptor = ArgumentCaptor.forClass(Location.class);
        ArgumentCaptor<Location> dstCaptor = ArgumentCaptor.forClass(Location.class);
        verify(mockFs, times(3)).rename(srcCaptor.capture(), dstCaptor.capture());

        List<Location> sources = srcCaptor.getAllValues();
        List<Location> dests = dstCaptor.getAllValues();
        assertEquals(Location.of("s3://bucket/src/a.txt"), sources.get(0));
        assertEquals(Location.of("s3://bucket/dst/a.txt"), dests.get(0));
        assertEquals(Location.of("s3://bucket/src/b.txt"), sources.get(1));
        assertEquals(Location.of("s3://bucket/dst/b.txt"), dests.get(1));
    }

    @Test
    void asyncRenameFilesSkipsWhenCancelled() throws Exception {
        FileSystem mockFs = mock(FileSystem.class);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        AtomicBoolean cancelled = new AtomicBoolean(true);

        FileSystemUtil.asyncRenameFiles(mockFs, Runnable::run, futures, cancelled,
                "s3://bucket/src", "s3://bucket/dst", List.of("a.txt"));

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        verify(mockFs, never()).rename(any(), any());
    }

    // --- asyncRenameDir ---

    @Test
    void asyncRenameDirCallsRenameDirectory() throws Exception {
        FileSystem mockFs = mock(FileSystem.class);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        AtomicBoolean cancelled = new AtomicBoolean(false);
        Runnable callback = mock(Runnable.class);

        FileSystemUtil.asyncRenameDir(mockFs, Runnable::run, futures, cancelled,
                "s3://bucket/src", "s3://bucket/dst", callback);

        assertEquals(1, futures.size());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        verify(mockFs).renameDirectory(
                Location.of("s3://bucket/src"),
                Location.of("s3://bucket/dst"),
                callback);
    }

    @Test
    void asyncRenameDirSkipsWhenCancelled() throws Exception {
        FileSystem mockFs = mock(FileSystem.class);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        AtomicBoolean cancelled = new AtomicBoolean(true);

        FileSystemUtil.asyncRenameDir(mockFs, Runnable::run, futures, cancelled,
                "s3://bucket/src", "s3://bucket/dst", () -> {});

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        verify(mockFs, never()).renameDirectory(any(), any(), any());
    }
}

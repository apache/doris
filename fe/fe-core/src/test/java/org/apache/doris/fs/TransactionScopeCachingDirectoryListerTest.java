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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/plugin/trino-hive/src/test/java/io/trino/plugin/hive/fs/TestTransactionScopeCachingDirectoryLister.java
// and modified by Doris

package org.apache.doris.fs;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystemIOException;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.RemoteIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// some tests may invalidate the whole cache affecting therefore other concurrent tests
@Execution(ExecutionMode.SAME_THREAD)
public class TransactionScopeCachingDirectoryListerTest {
    private static FileEntry fileEntry(String uri) {
        return new FileEntry(Location.of(uri), 1, false, 0L, null);
    }

    @Test
    public void testConcurrentDirectoryListing()
            throws FileSystemIOException {
        TableIf table = Mockito.mock(TableIf.class);
        FileEntry firstFile = fileEntry("file:/x/x");
        FileEntry secondFile = fileEntry("file:/x/y");
        FileEntry thirdFile = fileEntry("file:/y/z");

        String path1 = "file:/x";
        String path2 = "file:/y";

        CountingDirectoryLister countingLister = new CountingDirectoryLister(
                ImmutableMap.of(
                        path1, ImmutableList.of(firstFile, secondFile),
                        path2, ImmutableList.of(thirdFile)));

        TransactionScopeCachingDirectoryLister cachingLister = (TransactionScopeCachingDirectoryLister)
                new TransactionScopeCachingDirectoryListerFactory(2).get(countingLister);

        assertFiles(cachingLister.listFiles(null, true, table, path2), ImmutableList.of(thirdFile));

        Assert.assertEquals(1, countingLister.getListCount());

        // listing path2 again shouldn't increase listing count
        Assert.assertTrue(cachingLister.isCached(path2));
        assertFiles(cachingLister.listFiles(null, true, table, path2), ImmutableList.of(thirdFile));
        Assert.assertEquals(1, countingLister.getListCount());


        // start listing path1 concurrently
        RemoteIterator<FileEntry> path1FilesA = cachingLister.listFiles(null, true, table, path1);
        RemoteIterator<FileEntry> path1FilesB = cachingLister.listFiles(null, true, table, path1);
        Assert.assertEquals(2, countingLister.getListCount());

        // list path1 files using both iterators concurrently
        Assert.assertSame(firstFile, path1FilesA.next());
        Assert.assertSame(firstFile, path1FilesB.next());
        Assert.assertSame(secondFile, path1FilesB.next());
        Assert.assertSame(secondFile, path1FilesA.next());
        Assert.assertFalse(path1FilesA.hasNext());
        Assert.assertFalse(path1FilesB.hasNext());
        Assert.assertEquals(2, countingLister.getListCount());

        Assert.assertFalse(cachingLister.isCached(path2));
        assertFiles(cachingLister.listFiles(null, true, table, path2), ImmutableList.of(thirdFile));
        Assert.assertEquals(3, countingLister.getListCount());
    }

    @Test
    public void testConcurrentDirectoryListingException()
            throws FileSystemIOException {
        TableIf table = Mockito.mock(TableIf.class);
        FileEntry file = fileEntry("file:/x/x");

        String path = "file:/x";

        CountingDirectoryLister countingLister = new CountingDirectoryLister(ImmutableMap.of(path, ImmutableList.of(file)));
        DirectoryLister cachingLister = new TransactionScopeCachingDirectoryListerFactory(1).get(countingLister);

        // start listing path concurrently
        countingLister.setThrowException(true);
        RemoteIterator<FileEntry> filesA = cachingLister.listFiles(null, true, table, path);
        RemoteIterator<FileEntry> filesB = cachingLister.listFiles(null, true, table, path);
        Assert.assertEquals(1, countingLister.getListCount());

        // listing should throw an exception
        Assert.assertThrows(FileSystemIOException.class, () -> filesA.hasNext());


        // listing again should succeed
        countingLister.setThrowException(false);
        assertFiles(cachingLister.listFiles(null, true, table, path), ImmutableList.of(file));
        Assert.assertEquals(2, countingLister.getListCount());

        // listing using second concurrently initialized DirectoryLister should fail
        Assert.assertThrows(FileSystemIOException.class, () -> filesB.hasNext());

    }

    private void assertFiles(RemoteIterator<FileEntry> iterator, List<FileEntry> expectedFiles)
            throws FileSystemIOException {
        ImmutableList.Builder<FileEntry> actualFiles = ImmutableList.builder();
        while (iterator.hasNext()) {
            actualFiles.add(iterator.next());
        }
        Assert.assertEquals(expectedFiles, actualFiles.build());
    }

    private static class CountingDirectoryLister
            implements DirectoryLister {
        private final Map<String, List<FileEntry>> fileStatuses;
        private int listCount;
        private boolean throwException;

        public CountingDirectoryLister(Map<String, List<FileEntry>> fileStatuses) {
            this.fileStatuses = Objects.requireNonNull(fileStatuses, "fileStatuses is null");
        }

        @Override
        public RemoteIterator<FileEntry> listFiles(org.apache.doris.filesystem.FileSystem fs, boolean recursive,
                TableIf table, String location)
                throws FileSystemIOException {
            // No specific recursive files-only listing implementation
            listCount++;
            return throwingRemoteIterator(Objects.requireNonNull(fileStatuses.get(location)), throwException);
        }

        public void setThrowException(boolean throwException) {
            this.throwException = throwException;
        }

        public int getListCount() {
            return listCount;
        }
    }

    static RemoteIterator<FileEntry> throwingRemoteIterator(List<FileEntry> files, boolean throwException) {
        return new RemoteIterator<FileEntry>() {
            private final Iterator<FileEntry> iterator = ImmutableList.copyOf(files).iterator();

            @Override
            public boolean hasNext()
                    throws FileSystemIOException {
                if (throwException) {
                    throw new FileSystemIOException("File system io exception.");
                }
                return iterator.hasNext();
            }

            @Override
            public FileEntry next() {
                return iterator.next();
            }
        };
    }
}

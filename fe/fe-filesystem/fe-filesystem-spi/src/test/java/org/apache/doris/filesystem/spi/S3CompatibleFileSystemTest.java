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

package org.apache.doris.filesystem.spi;

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.UploadPartResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link S3CompatibleFileSystem}'s paginating list iterator.
 *
 * <p>Object stores return directory-marker placeholders (keys ending in "/") interleaved with
 * real objects, and a truncated page may contain nothing but markers. The iterator must keep
 * paging past such fully-filtered pages instead of reporting end-of-listing.
 */
class S3CompatibleFileSystemTest {

    /**
     * A truncated page whose keys are all directory markers filters down to an empty buffer;
     * the iterator must continue to the next page rather than stop with files left behind.
     */
    @Test
    void testIteratorSkipsMarkerOnlyTruncatedPage() throws IOException {
        PagingObjStorage storage = new PagingObjStorage(List.of(
                new RemoteObjects(List.of(
                        remoteObject("data/a/"),
                        remoteObject("data/b/")), true, "t1"),
                new RemoteObjects(List.of(
                        remoteObject("data/c/"),
                        remoteObject("data/file1.csv")), true, "t2"),
                new RemoteObjects(List.of(
                        remoteObject("data/file2.csv")), false, null)));
        TestFileSystem fs = new TestFileSystem(storage);

        List<String> uris = new ArrayList<>();
        try (FileIterator it = fs.list(Location.of("s3://bucket/data"))) {
            while (it.hasNext()) {
                FileEntry entry = it.next();
                uris.add(entry.location().uri());
            }
        }

        Assertions.assertEquals(
                List.of("s3://bucket/data/file1.csv", "s3://bucket/data/file2.csv"), uris);
        Assertions.assertEquals(3, storage.pagesServed);
    }

    /**
     * A marker-only final page must simply end the iteration, not loop or throw.
     */
    @Test
    void testIteratorEndsOnMarkerOnlyFinalPage() throws IOException {
        PagingObjStorage storage = new PagingObjStorage(List.of(
                new RemoteObjects(List.of(
                        remoteObject("data/file1.csv")), true, "t1"),
                new RemoteObjects(List.of(
                        remoteObject("data/z/")), false, null)));
        TestFileSystem fs = new TestFileSystem(storage);

        List<String> uris = new ArrayList<>();
        try (FileIterator it = fs.list(Location.of("s3://bucket/data"))) {
            while (it.hasNext()) {
                uris.add(it.next().location().uri());
            }
        }

        Assertions.assertEquals(List.of("s3://bucket/data/file1.csv"), uris);
        Assertions.assertEquals(2, storage.pagesServed);
    }

    private static RemoteObject remoteObject(String key) {
        return new RemoteObject(key, key, "etag", key.endsWith("/") ? 0 : 10, 1234567890L);
    }

    /** Minimal concrete subclass; only the inherited list iterator is exercised. */
    private static class TestFileSystem extends S3CompatibleFileSystem {
        TestFileSystem(ObjStorage<?> storage) {
            super(storage, false, Set.of("s3"));
        }
    }

    /**
     * {@link ObjStorage} stub serving a fixed sequence of listing pages, verifying that the
     * continuation token of page N is echoed back when fetching page N + 1.
     */
    private static class PagingObjStorage implements ObjStorage<Object> {
        private final List<RemoteObjects> pages;
        private int pagesServed = 0;

        PagingObjStorage(List<RemoteObjects> pages) {
            this.pages = pages;
        }

        @Override
        public RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException {
            String expectedToken = pagesServed == 0 ? null : pages.get(pagesServed - 1).getContinuationToken();
            Assertions.assertEquals(expectedToken, continuationToken,
                    "listObjects must resume from the previous page's continuation token");
            Assertions.assertTrue(pagesServed < pages.size(), "listed past the final page");
            return pages.get(pagesServed++);
        }

        @Override
        public Object getClient() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteObject headObject(String remotePath) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putObject(String remotePath, RequestBody requestBody) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteObject(String remotePath) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyObject(String srcPath, String dstPath) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String initiateMultipartUpload(String remotePath) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public UploadPartResult uploadPart(String remotePath, String uploadId,
                int partNum, RequestBody body) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void completeMultipartUpload(String remotePath, String uploadId,
                List<UploadPartResult> parts) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }
}

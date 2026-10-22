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
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.UploadPartResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Tests for {@link S3CompatibleFileSystem}'s paginating list iterator.
 *
 * <p>Object stores return directory-marker placeholders (keys ending in "/") interleaved with
 * real objects, and a truncated page may contain nothing but markers. The iterator must keep
 * paging past such fully-filtered pages instead of reporting end-of-listing.
 */
class S3CompatibleFileSystemTest {

    @Test
    void numericRangesRejectExcessiveExpansion() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> S3CompatibleFileSystem.compileGlobPattern("data_{1..10001}.csv"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> S3CompatibleFileSystem.compileGlobPattern("{1..101}/data_{1..101}.csv"));
    }

    @Test
    void nestedNumericAlternativesAddRatherThanMultiply() {
        String glob = "data_{a{1..101},b{1..101}}.csv";
        Pattern compiled = S3CompatibleFileSystem.compileGlobPattern(glob);
        for (String prefix : List.of("a", "b")) {
            for (int i = 1; i <= 101; i++) {
                Assertions.assertTrue(compiled.matcher("data_" + prefix + i + ".csv").matches());
            }
        }
        Assertions.assertFalse(compiled.matcher("data_a102.csv").matches());
        Assertions.assertFalse(compiled.matcher("data_ab1.csv").matches());
        Assertions.assertDoesNotThrow(() -> GlobPattern.expandNumericRanges("{a{1..5000},b{1..5000}}"));
        Assertions.assertDoesNotThrow(() -> GlobPattern.expandNumericRanges("{{1..50},x}{1..196}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> GlobPattern.expandNumericRanges("{{1..50},x}{1..197}"));
    }

    @Test
    void nestedRangesRetainAllocationAndCartesianLimits() {
        Assertions.assertDoesNotThrow(() -> GlobPattern.expandNumericRanges("{a{1..50}{1..100},b{1..5000}}"));
        for (String glob : List.of("{a{1..50}{1..100},b{1..5001}}",
                "{a{1..101},b{1..101}}/{1..50}", "{a{1..6000},b{1..6000}}",
                "{1..100}{a,b}{1..100}")) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> GlobPattern.expandNumericRanges(glob), glob);
        }
        Assertions.assertDoesNotThrow(() -> GlobPattern.expandNumericRanges("{1..100}{1..100}"));
        Assertions.assertEquals("{1,2}{1,2}", GlobPattern.expandNumericRanges("{1..2,1,2}{1..2}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> GlobPattern.expandNumericRanges("{1..2}".repeat(64)));
        Assertions.assertDoesNotThrow(() -> GlobPattern.expandNumericRanges("{a,b}".repeat(64)));
        Assertions.assertDoesNotThrow(() -> GlobPattern.expandNumericRanges(
                "{".repeat(64) + "1..2" + "}".repeat(64)));
    }

    @Test
    void nestedAlternativesKeepOuterCommasStructural() {
        Pattern compiled = S3CompatibleFileSystem.compileGlobPattern(
                "data_{a{1..2},b{3..4},plain}.csv");
        for (String name : List.of("a1", "a2", "b3", "b4", "plain")) {
            Assertions.assertTrue(compiled.matcher("data_" + name + ".csv").matches(), name);
        }
        for (String name : List.of("a3", "b2", "a1,b3,plain", "a1,b3", "1")) {
            Assertions.assertFalse(compiled.matcher("data_" + name + ".csv").matches(), name);
        }
        Assertions.assertTrue(S3CompatibleFileSystem.compileGlobPattern("{a{1..2},b},tail")
                .matcher("a1,tail").matches());
        Assertions.assertFalse(S3CompatibleFileSystem.compileGlobPattern("{a{1..2},b},tail")
                .matcher("tail").matches());
    }

    @Test
    void nestedNumericAlternativesListExactObjectKeys() throws IOException {
        List<String> keys = List.of("data_a1.csv", "data_a101.csv", "data_a102.csv", "data_b1.csv",
                "data_b101.csv", "data_c1.csv");
        PagingObjStorage storage = new PagingObjStorage(List.of()) {
            @Override
            public RemoteObjects listObjects(String remotePath, String continuationToken) {
                String prefix = ObjectStorageUri.parse(remotePath, false, Set.of("s3")).key();
                List<RemoteObject> matches = new ArrayList<>();
                for (String key : keys) {
                    if (key.startsWith(prefix)) {
                        matches.add(remoteObject(key));
                    }
                }
                return new RemoteObjects(matches, false, null);
            }
        };
        try (TestFileSystem fs = new TestFileSystem(storage)) {
            GlobListing listing = fs.globListWithLimit(
                    Location.of("s3://bucket/data_{a{1..101},b{1..101}}.csv"), null, 0, 0);
            List<String> actual = new ArrayList<>();
            for (FileEntry file : listing.getFiles()) {
                actual.add(file.location().uri());
            }
            Assertions.assertEquals(List.of("s3://bucket/data_a1.csv", "s3://bucket/data_a101.csv",
                    "s3://bucket/data_b1.csv", "s3://bucket/data_b101.csv"), actual);
        }
    }

    @Test
    void braceArmsRemainDistinctUntilTheSuffixIsAppended() {
        Assertions.assertEquals(List.of("data_1.csv", "data_101.csv"),
                S3CompatibleFileSystem.expandedGlobListPrefixes("data_{1,101}.csv"));
        Assertions.assertEquals(List.of("data_1/", "data_101/"),
                S3CompatibleFileSystem.expandedGlobListPrefixes("data_{1,101}/*.csv"));
        Assertions.assertEquals(List.of("a101suffix", "a1suffix", "bsuffix"),
                S3CompatibleFileSystem.expandedGlobListPrefixes("{a{1,101},b}suffix"));
        Assertions.assertEquals(List.of("asuffix", "suffix"),
                S3CompatibleFileSystem.expandedGlobListPrefixes("{,a}suffix"));
        Assertions.assertEquals(List.of("data_1"),
                S3CompatibleFileSystem.expandedGlobListPrefixes("data_{1,101}*"));
    }

    @Test
    void numericRangesPreserveMixedReverseAndLiteralKeySyntax() {
        for (String name : List.of("data_3.csv", "data_2.csv", "data_1.csv", "data_other.csv")) {
            Assertions.assertTrue(S3CompatibleFileSystem.compileGlobPattern("data_{3..1,other,2}.csv")
                    .matcher(name).matches());
        }
        Assertions.assertTrue(S3CompatibleFileSystem.compileGlobPattern("part=a+b/$x%2F#_{1..2}.csv")
                .matcher("part=a+b/$x%2F#_2.csv").matches());
    }

    @Test
    void numericRangesPreserveEscapedBraceAlternatives() {
        String glob = "data_{1..2,\\{special\\}}.csv";
        Assertions.assertEquals("data_{1,2,\\{special\\}}.csv", GlobPattern.expandNumericRanges(glob));
        for (String name : List.of("data_1.csv", "data_2.csv", "data_{special}.csv")) {
            Assertions.assertTrue(S3CompatibleFileSystem.compileGlobPattern(glob).matcher(name).matches(), name);
        }
        Assertions.assertFalse(S3CompatibleFileSystem.compileGlobPattern(glob)
                .matcher("data_1..2.csv").matches());
        Assertions.assertEquals("{1,2,a\\,b}", GlobPattern.expandNumericRanges("{1..2,a\\,b}"));
        Assertions.assertEquals("{1,2,[{},]}", GlobPattern.expandNumericRanges("{1..2,[{},]}"));
        Assertions.assertEquals("{{1,2},\\{literal\\}}",
                GlobPattern.expandNumericRanges("{{1..2},\\{literal\\}}"));
    }

    @Test
    void numericRangesHandleIntegerEndpointsWithoutOverflow() {
        Assertions.assertEquals("{2147483646,2147483647}",
                GlobPattern.expandNumericRanges("{2147483646..2147483647}"));
        Assertions.assertEquals("{-2147483647,-2147483648,x}",
                GlobPattern.expandNumericRanges("{-2147483647..-2147483648,x}"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> GlobPattern.expandNumericRanges("{-2147483648..2147483647,x}"));
    }

    @Test
    void numericRangesLeaveLiteralBracesAndOtherGlobSyntaxUnchanged() {
        for (String pattern : List.of("data_\\{1..3}.csv", "data_[{1..3}].csv", "data_{one,two}.csv",
                "data_{-1..3}.csv", "data_*.csv")) {
            Assertions.assertEquals(pattern, GlobPattern.expandNumericRanges(pattern));
        }
        Assertions.assertEquals("[{1..3}]/{1,2}", GlobPattern.expandNumericRanges("[{1..3}]/{1..2}"));
    }

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

    @Test
    void cancellationInsideMarkerPaginationPreventsTheNextRequest() throws IOException {
        PagingObjStorage storage = new PagingObjStorage(List.of(
                new RemoteObjects(List.of(remoteObject("data/a/")), true, "t1"),
                new RemoteObjects(List.of(remoteObject("data/file.csv")), false, null)));
        try (FileIterator iterator = new TestFileSystem(storage).list(Location.of("s3://bucket/data"))
                .withCancellationCheck(() -> {
                    if (storage.pagesServed == 1) {
                        throw new java.util.concurrent.CancellationException("scan stopped");
                    }
                })) {
            Assertions.assertThrows(java.util.concurrent.CancellationException.class, iterator::hasNext);
        }
        Assertions.assertEquals(1, storage.pagesServed);
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

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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link S3FileSystem} using a mock {@link S3ObjStorage}.
 * No real AWS credentials or S3 connectivity required.
 */
class S3FileSystemTest {

    private S3ObjStorage mockStorage;
    private S3FileSystem fs;

    @BeforeEach
    void setUp() {
        mockStorage = Mockito.mock(S3ObjStorage.class);
        fs = new S3FileSystem(mockStorage);
    }

    // ------------------------------------------------------------------
    // exists() (inherited from ObjFileSystem)
    // ------------------------------------------------------------------

    @Test
    void exists_returnsTrueWhenHeadObjectSucceeds() throws IOException {
        when(mockStorage.headObject("s3://bucket/key"))
                .thenReturn(new RemoteObject("key", "key", null, 100L, 0L));

        assertTrue(fs.exists(Location.of("s3://bucket/key")));
    }

    @Test
    void exists_returnsFalseForFileNotFoundException() throws IOException {
        when(mockStorage.headObject("s3://bucket/missing"))
                .thenThrow(new FileNotFoundException("not found"));

        assertFalse(fs.exists(Location.of("s3://bucket/missing")));
    }

    @Test
    void exists_returnsFalseFor404InMessage() throws IOException {
        when(mockStorage.headObject("s3://bucket/gone"))
                .thenThrow(new IOException("HTTP 404 Not Found"));

        assertFalse(fs.exists(Location.of("s3://bucket/gone")));
    }

    // ------------------------------------------------------------------
    // mkdirs()
    // ------------------------------------------------------------------

    @Test
    void mkdirs_putsZeroByteMarkerWithTrailingSlash() throws IOException {
        fs.mkdirs(Location.of("s3://bucket/dir/subdir"));

        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockStorage).putObject(pathCaptor.capture(), bodyCaptor.capture());

        assertEquals("s3://bucket/dir/subdir/", pathCaptor.getValue(),
                "mkdirs must append trailing slash for directory marker");
        assertEquals(0, bodyCaptor.getValue().contentLength(),
                "Directory marker must be zero bytes");
    }

    @Test
    void mkdirs_doesNotDoubleSlashIfAlreadyPresent() throws IOException {
        fs.mkdirs(Location.of("s3://bucket/dir/"));

        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockStorage).putObject(pathCaptor.capture(), any(RequestBody.class));

        assertEquals("s3://bucket/dir/", pathCaptor.getValue());
    }

    // ------------------------------------------------------------------
    // delete()
    // ------------------------------------------------------------------

    @Test
    void delete_nonRecursiveDeletesExactObject() throws IOException {
        fs.delete(Location.of("s3://bucket/file.txt"), false);

        verify(mockStorage).deleteObject("s3://bucket/file.txt");
        verify(mockStorage, never()).listObjects(anyString(), any());
    }

    @Test
    void delete_nonRecursiveSwallowsNotFoundError() throws IOException {
        doThrow(new FileNotFoundException("not found"))
                .when(mockStorage).deleteObject("s3://bucket/gone");

        // Should not throw
        fs.delete(Location.of("s3://bucket/gone"), false);
    }

    @Test
    void delete_recursiveDeletesAllObjectsUnderPrefix() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("dir/a.txt", "a.txt", null, 10L, 0L),
                        new RemoteObject("dir/b.txt", "b.txt", null, 20L, 0L)),
                false, null);
        when(mockStorage.listObjects(eq("s3://bucket/dir/"), any())).thenReturn(page);

        fs.delete(Location.of("s3://bucket/dir"), true);

        // reconstructUri uses scheme://bucket/ + key, where key is the full object key
        verify(mockStorage).deleteObject("s3://bucket/dir/a.txt");
        verify(mockStorage).deleteObject("s3://bucket/dir/b.txt");
        verify(mockStorage).deleteObject("s3://bucket/dir");
    }

    // ------------------------------------------------------------------
    // rename()
    // ------------------------------------------------------------------

    @Test
    void rename_copyThenDelete() throws IOException {
        fs.rename(Location.of("s3://bucket/old"), Location.of("s3://bucket/new"));

        verify(mockStorage).copyObject("s3://bucket/old", "s3://bucket/new");
        verify(mockStorage).deleteObject("s3://bucket/old");
    }

    // ------------------------------------------------------------------
    // longestNonGlobPrefix() - package-visible static
    // ------------------------------------------------------------------

    @Test
    void longestNonGlobPrefix_noGlobReturnsFullPattern() {
        assertEquals("data/2024/file.csv", S3FileSystem.longestNonGlobPrefix("data/2024/file.csv"));
    }

    @Test
    void longestNonGlobPrefix_starTruncatesAtStar() {
        assertEquals("data/2024/", S3FileSystem.longestNonGlobPrefix("data/2024/*.csv"));
    }

    @Test
    void longestNonGlobPrefix_questionMarkTruncates() {
        assertEquals("data/file", S3FileSystem.longestNonGlobPrefix("data/file?.csv"));
    }

    @Test
    void longestNonGlobPrefix_bracketTruncates() {
        assertEquals("data/", S3FileSystem.longestNonGlobPrefix("data/[abc].csv"));
    }

    @Test
    void longestNonGlobPrefix_braceTruncates() {
        assertEquals("data/", S3FileSystem.longestNonGlobPrefix("data/{1..3}.csv"));
    }

    @Test
    void longestNonGlobPrefix_backslashTruncates() {
        assertEquals("data/", S3FileSystem.longestNonGlobPrefix("data/\\*.csv"));
    }

    @Test
    void longestNonGlobPrefix_emptyForLeadingStar() {
        assertEquals("", S3FileSystem.longestNonGlobPrefix("*.csv"));
    }

    // ------------------------------------------------------------------
    // close()
    // ------------------------------------------------------------------

    @Test
    void close_delegatesToObjStorage() throws IOException {
        fs.close();
        verify(mockStorage).close();
    }
}

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

package org.apache.doris.job.offset.s3;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class S3SourceOffsetProviderTest {
    private static final Map<String, String> TVF_PROPS = Map.of(
            "uri", "s3://bucket/data/*.csv", "s3.endpoint", "s3.us-east-1.amazonaws.com",
            "s3.region", "us-east-1", "s3.access_key", "ak", "s3.secret_key", "sk");
    private static final StreamingJobProperties ONCE_PROPS = new StreamingJobProperties(
            Map.of("s3.ingestion_mode", "ONCE", "s3.max_batch_files", "1"));

    @Test
    public void testOnceMarksLastBatchAndRecovers() throws Exception {
        FileSystem fs = Mockito.mock(FileSystem.class);
        try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            factory.when(() -> FileSystemFactory.getFileSystem(Mockito.any(StorageAdapter.class))).thenReturn(fs);
            Mockito.when(fs.globListWithLimit(Mockito.any(Location.class), Mockito.isNull(),
                    Mockito.eq(1L), Mockito.eq(1L))).thenReturn(page("data/a.csv", "data/b.csv"));
            Mockito.when(fs.globListWithLimit(Mockito.any(Location.class), Mockito.isNull(),
                    Mockito.eq(ONCE_PROPS.getS3BatchBytes()), Mockito.eq(1L)))
                    .thenReturn(page("data/a.csv", "data/b.csv"));
            Mockito.when(fs.globListWithLimit(Mockito.any(Location.class), Mockito.eq("data/a.csv"),
                    Mockito.eq(1L), Mockito.eq(1L)))
                    .thenReturn(page("data/b.csv", "data/b.csv"));
            Mockito.when(fs.globListWithLimit(Mockito.any(Location.class), Mockito.eq("data/a.csv"),
                    Mockito.eq(ONCE_PROPS.getS3BatchBytes()), Mockito.eq(1L)))
                    .thenReturn(page("data/b.csv", "data/b.csv"));
            Mockito.when(fs.globListWithLimit(Mockito.any(Location.class), Mockito.eq("data/b.csv"),
                    Mockito.eq(1L), Mockito.eq(1L)))
                    .thenReturn(new GlobListing(Collections.emptyList(), "bucket", "data/", ""));

            S3SourceOffsetProvider provider = new S3SourceOffsetProvider(ONCE_PROPS);
            Assertions.assertTrue(provider.hasMoreDataToConsume());
            provider.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertTrue(provider.hasMoreDataToConsume());
            S3Offset first = provider.getNextOffset(ONCE_PROPS, TVF_PROPS);
            Assertions.assertEquals("data/a.csv", first.getEndFile());
            Assertions.assertFalse(provider.hasReachedEnd(first));
            Assertions.assertEquals("data/a.csv", provider.getNextOffset(ONCE_PROPS, TVF_PROPS).getEndFile());
            provider.updateOffset(provider.deserializeOffset(first.toSerializedJson()));
            Assertions.assertTrue(provider.hasMoreDataToConsume());
            S3Offset second = provider.getNextOffset(ONCE_PROPS, TVF_PROPS);
            Assertions.assertEquals("data/b.csv", second.getEndFile());
            Assertions.assertTrue(provider.hasReachedEnd(second));
            Assertions.assertFalse(provider.hasReachedEnd());
            // A concurrent metadata probe cannot change this task's completion decision.
            provider.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertTrue(provider.hasReachedEnd(second));
            Assertions.assertEquals("data/b.csv", provider.getNextOffset(ONCE_PROPS, TVF_PROPS).getEndFile());
            Assertions.assertFalse(second.toSerializedJson().contains("lastBatch"));
            provider.updateOffset(provider.deserializeOffset(second.toSerializedJson()));
            Assertions.assertFalse(provider.hasMoreDataToConsume());
            provider.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertTrue(provider.hasReachedEnd());

            S3SourceOffsetProvider recovered = new S3SourceOffsetProvider(ONCE_PROPS);
            recovered.restoreFromPersistInfo(provider.getPersistInfo());
            Assertions.assertFalse(recovered.hasReachedEnd());
            Assertions.assertFalse(recovered.hasMoreDataToConsume());
            recovered.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertTrue(recovered.hasReachedEnd());

            Mockito.when(fs.globListWithLimit(Mockito.any(Location.class), Mockito.eq("data/b.csv"),
                    Mockito.eq(1L), Mockito.eq(1L))).thenReturn(page("data/c.csv", "data/c.csv"));
            S3SourceOffsetProvider recoveredWithNewFile = new S3SourceOffsetProvider(ONCE_PROPS);
            recoveredWithNewFile.restoreFromPersistInfo(provider.getPersistInfo());
            recoveredWithNewFile.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertFalse(recoveredWithNewFile.hasReachedEnd());
            Assertions.assertTrue(recoveredWithNewFile.hasMoreDataToConsume());
        }
    }

    @Test
    public void testEmptyAndListingErrorRemainDistinct() throws Exception {
        FileSystem fs = Mockito.mock(FileSystem.class);
        try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            factory.when(() -> FileSystemFactory.getFileSystem(Mockito.any(StorageAdapter.class))).thenReturn(fs);
            Mockito.when(fs.globListWithLimit(Mockito.any(Location.class), Mockito.isNull(),
                    Mockito.anyLong(), Mockito.anyLong()))
                    .thenThrow(new IOException("listing failed"))
                    .thenReturn(new GlobListing(Collections.emptyList(), "bucket", "data/", ""));

            S3SourceOffsetProvider once = new S3SourceOffsetProvider(ONCE_PROPS);
            Assertions.assertThrows(IOException.class, () -> once.fetchRemoteMeta(TVF_PROPS));
            Assertions.assertFalse(once.hasReachedEnd());
            once.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertFalse(once.hasReachedEnd());
            Assertions.assertTrue(once.hasMoreDataToConsume());
            RuntimeException emptyError = Assertions.assertThrows(RuntimeException.class,
                    () -> once.getNextOffset(ONCE_PROPS, TVF_PROPS));
            Assertions.assertTrue(emptyError.getMessage().contains("No new files found in path:"));

            S3SourceOffsetProvider lexical = new S3SourceOffsetProvider();
            Assertions.assertThrows(RuntimeException.class, () -> lexical.getNextOffset(ONCE_PROPS, TVF_PROPS));
        }
    }

    private static GlobListing page(String key, String maxFile) {
        return new GlobListing(List.of(new FileEntry(Location.of("s3://bucket/" + key),
                10, false, 0, null)), "bucket", "data/", maxFile);
    }
}

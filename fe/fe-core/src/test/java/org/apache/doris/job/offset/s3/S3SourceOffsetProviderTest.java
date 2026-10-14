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

import org.apache.doris.backup.Status;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.GlobListResult;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class S3SourceOffsetProviderTest {
    private static final Map<String, String> TVF_PROPS = ImmutableMap.of(
            "uri", "s3://bucket/data/*.csv", "s3.endpoint", "s3.us-east-1.amazonaws.com",
            "s3.region", "us-east-1", "s3.access_key", "ak", "s3.secret_key", "sk");
    private static final StreamingJobProperties ONCE_PROPS = new StreamingJobProperties(
            ImmutableMap.of("s3.ingestion_mode", "ONE_TIME", "s3.max_batch_files", "1"));

    @Test
    public void testOnceMarksLastBatchAndRecovers() throws Exception {
        RemoteFileSystem fs = Mockito.mock(RemoteFileSystem.class);
        try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            factory.when(() -> FileSystemFactory.get(Mockito.any(StorageProperties.class))).thenReturn(fs);
            Mockito.when(fs.globListWithLimit(Mockito.anyString(), Mockito.anyList(), Mockito.isNull(),
                    Mockito.eq(1L), Mockito.eq(1L))).thenAnswer(page("data/a.csv", "data/b.csv"));
            Mockito.when(fs.globListWithLimit(Mockito.anyString(), Mockito.anyList(), Mockito.isNull(),
                    Mockito.eq(ONCE_PROPS.getS3BatchBytes()), Mockito.eq(1L)))
                    .thenAnswer(page("data/a.csv", "data/b.csv"));
            Mockito.when(fs.globListWithLimit(Mockito.anyString(), Mockito.anyList(), Mockito.eq("data/a.csv"),
                    Mockito.eq(1L), Mockito.eq(1L)))
                    .thenAnswer(page("data/b.csv", "data/b.csv"));
            Mockito.when(fs.globListWithLimit(Mockito.anyString(), Mockito.anyList(), Mockito.eq("data/a.csv"),
                    Mockito.eq(ONCE_PROPS.getS3BatchBytes()), Mockito.eq(1L)))
                    .thenAnswer(page("data/b.csv", "data/b.csv"));
            Mockito.when(fs.globListWithLimit(Mockito.anyString(), Mockito.anyList(), Mockito.eq("data/b.csv"),
                    Mockito.eq(1L), Mockito.eq(1L)))
                    .thenReturn(new GlobListResult(Status.OK, "", "bucket", "data/"));

            S3SourceOffsetProvider provider = new S3SourceOffsetProvider(ONCE_PROPS);
            Assertions.assertTrue(provider.hasMoreDataToConsume());
            provider.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertTrue(provider.hasMoreDataToConsume());
            S3Offset first = provider.getNextOffset(ONCE_PROPS, TVF_PROPS);
            Assertions.assertEquals("data/a.csv", first.getEndFile());
            Assertions.assertEquals("s3://bucket/data/a.csv", first.getFileStatuses().get(0).getPath());
            Assertions.assertFalse(first.toSerializedJson().contains("fileStatuses"));
            Assertions.assertFalse(first.isEmpty());
            Assertions.assertTrue(provider.deserializeOffset(first.toSerializedJson()).isEmpty());
            Assertions.assertFalse(first.isLastBatch());
            Assertions.assertEquals("data/a.csv", provider.getNextOffset(ONCE_PROPS, TVF_PROPS).getEndFile());
            provider.updateOffset(provider.deserializeOffset(first.toSerializedJson()));
            Assertions.assertTrue(provider.hasMoreDataToConsume());
            S3Offset second = provider.getNextOffset(ONCE_PROPS, TVF_PROPS);
            Assertions.assertEquals("data/b.csv", second.getEndFile());
            Assertions.assertTrue(second.isLastBatch());
            Assertions.assertFalse(provider.hasReachedEnd());
            // A concurrent metadata probe cannot change this task's completion decision.
            provider.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertTrue(second.isLastBatch());
            Assertions.assertFalse(provider.hasReachedEnd());
            Assertions.assertEquals("data/b.csv", provider.getNextOffset(ONCE_PROPS, TVF_PROPS).getEndFile());
            provider.updateOffset(provider.deserializeOffset(second.toSerializedJson()));
            Assertions.assertTrue(provider.hasReachedEnd());
            Assertions.assertFalse(provider.hasMoreDataToConsume());

            S3SourceOffsetProvider recovered = new S3SourceOffsetProvider(ONCE_PROPS);
            recovered.restoreFromPersistInfo(provider.getPersistInfo());
            Assertions.assertTrue(recovered.hasReachedEnd());
            Assertions.assertFalse(recovered.hasMoreDataToConsume());

            recovered.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertTrue(recovered.hasReachedEnd());

            Mockito.when(fs.globListWithLimit(Mockito.anyString(), Mockito.anyList(), Mockito.eq("data/b.csv"),
                    Mockito.eq(1L), Mockito.eq(1L))).thenAnswer(page("data/c.csv", "data/c.csv"));
            S3SourceOffsetProvider recoveredWithNewFile = new S3SourceOffsetProvider(ONCE_PROPS);
            recoveredWithNewFile.restoreFromPersistInfo(provider.getPersistInfo());
            recoveredWithNewFile.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertTrue(recoveredWithNewFile.hasReachedEnd());
            Assertions.assertFalse(recoveredWithNewFile.hasMoreDataToConsume());

            S3SourceOffsetProvider legacyWithNewFile = new S3SourceOffsetProvider();
            legacyWithNewFile.restoreFromPersistInfo("{\"endFile\":\"data/b.csv\"}");
            legacyWithNewFile.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertFalse(legacyWithNewFile.hasReachedEnd());
            Assertions.assertTrue(legacyWithNewFile.hasMoreDataToConsume());

            S3SourceOffsetProvider lexical = new S3SourceOffsetProvider();
            lexical.restoreFromPersistInfo(provider.getPersistInfo());
            lexical.fetchRemoteMeta(TVF_PROPS);
            Assertions.assertFalse(lexical.hasReachedEnd());
            Assertions.assertTrue(lexical.hasMoreDataToConsume());
        }
    }

    @Test
    public void testEmptyAndListingErrorRemainDistinct() throws Exception {
        RemoteFileSystem fs = Mockito.mock(RemoteFileSystem.class);
        try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            factory.when(() -> FileSystemFactory.get(Mockito.any(StorageProperties.class))).thenReturn(fs);
            Mockito.when(fs.globListWithLimit(Mockito.anyString(), Mockito.anyList(), Mockito.isNull(),
                    Mockito.anyLong(), Mockito.anyLong()))
                    .thenReturn(new GlobListResult(new Status(Status.ErrCode.COMMON_ERROR, "listing failed")))
                    .thenReturn(new GlobListResult(Status.OK, "", "bucket", "data/"));

            S3SourceOffsetProvider once = new S3SourceOffsetProvider(ONCE_PROPS);
            Exception listingError = Assertions.assertThrows(Exception.class, () -> once.fetchRemoteMeta(TVF_PROPS));
            Assertions.assertTrue(listingError.getMessage().contains("listing failed"));
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

    private static Answer<GlobListResult> page(String key, String maxFile) {
        return invocation -> {
            List<RemoteFile> files = invocation.getArgument(1);
            files.add(new RemoteFile("s3://bucket/" + key, true, 10, 0));
            return new GlobListResult(Status.OK, maxFile, "bucket", "data/");
        };
    }

    @Test
    void testOrdinaryFileListIsUnchanged() {
        Assertions.assertEquals("s3://bucket/prefix/{001,normal.csv}",
                S3SourceOffsetProvider.buildFileListUri(statuses("prefix/", Arrays.asList("001", "normal.csv"))));
    }

    @Test
    void testFileListsAtBucketRootAndAcrossDirectories() {
        Assertions.assertEquals("s3://bucket/{a.csv,b.csv}",
                S3SourceOffsetProvider.buildFileListUri(statuses("", ImmutableList.of("a.csv", "b.csv"))));
        Assertions.assertEquals("s3://bucket/logs/{a/1.csv,b/2.csv}",
                S3SourceOffsetProvider.buildFileListUri(statuses("logs/", ImmutableList.of("a/1.csv", "b/2.csv"))));
        Assertions.assertEquals("s3://bucket/logs/a/{1.csv}",
                S3SourceOffsetProvider.buildFileListUri(statuses("logs/a/", ImmutableList.of("1.csv"))));
    }

    @Test
    void testCommaFileDoesNotSelectOtherObjects() throws Exception {
        assertLiteralFiles("prefix/", Arrays.asList("comma,a.csv", "normal.csv"),
                Arrays.asList("prefix/comma", "prefix/a.csv", "prefix/comma,a.csv.bak"));
        assertLiteralFiles("prefix/", Collections.singletonList("comma,a.csv"),
                Arrays.asList("prefix/comma", "prefix/a.csv"));
    }

    @Test
    void testLiteralGlobCharactersInDirectoriesAndFiles() throws Exception {
        assertLiteralFiles("prefix/[dir],{part}/",
                Arrays.asList("a[b].csv", "a{b,c}.csv", "a*.csv", "a\\b.csv", "$1.csv", "001"),
                Arrays.asList("prefix/[dir],{part}/ab.csv", "prefix/[dir],{part}/other.csv",
                        "prefix/[dir],{part}/1", "prefix/d,part/001"));
    }

    private static List<TBrokerFileStatus> statuses(String prefix, List<String> files) {
        return files.stream().map(file -> new TBrokerFileStatus("s3://bucket/" + prefix + file, false, 10, true))
                .collect(Collectors.toList());
    }

    private void assertLiteralFiles(String prefix, List<String> files, List<String> otherKeys) throws Exception {
        // Check the audit URI with the LIST matcher; execution uses the original file statuses.
        String uri = S3SourceOffsetProvider.buildFileListUri(statuses(prefix, files));
        String keyPattern = S3URI.create(uri).getKey();
        PathMatcher matcher = FileSystems.getDefault()
                .getPathMatcher("glob:" + Paths.get(S3Util.extendGlobs(keyPattern)));
        for (String file : files) {
            Assertions.assertTrue(matcher.matches(Paths.get(prefix + file)), "Missing file: " + file);
        }
        for (String key : otherKeys) {
            Assertions.assertFalse(matcher.matches(Paths.get(key)), "Unexpected file: " + key);
        }
    }
}

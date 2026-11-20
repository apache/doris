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

package org.apache.doris.fs;

import org.apache.doris.backup.Status;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.HdfsCompatibleProperties;
import org.apache.doris.fs.obj.AzureObjStorage;
import org.apache.doris.fs.obj.S3ObjStorage;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

class StreamingDirectoryListerTest {
    @Test
    void s3FetchesNextPageOnlyOnDemandAndCancellationStopsRequests() throws Exception {
        AbstractS3CompatibleProperties props = Mockito.mock(AbstractS3CompatibleProperties.class);
        Mockito.when(props.getUsePathStyle()).thenReturn("false");
        Mockito.when(props.getForceParsingByStandardUrl()).thenReturn("false");
        S3ObjStorage storage = Mockito.spy(new S3ObjStorage(props));
        S3Client client = Mockito.mock(S3Client.class);
        Mockito.doReturn(client).when(storage).getClient();
        Mockito.when(client.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(
                ListObjectsV2Response.builder().contents(object("dir/a")).nextContinuationToken("page2").build(),
                ListObjectsV2Response.builder().contents(object("dir/b")).build());
        AtomicBoolean cancelled = new AtomicBoolean();
        try (RemoteIterator<RemoteFile> files = storage.listFilesIterator("s3://bucket/dir", true, cancelled::get)) {
            Mockito.verifyNoInteractions(client);
            Assertions.assertEquals("s3://bucket/dir/a", files.next().getPath().toString());
            Mockito.verify(client).listObjectsV2(Mockito.any(ListObjectsV2Request.class));
            Assertions.assertEquals("s3://bucket/dir/b", files.next().getPath().toString());
            Assertions.assertFalse(files.hasNext());
        }
        Mockito.verify(client).listObjectsV2(Mockito.argThat((ListObjectsV2Request request) ->
                "page2".equals(request.continuationToken()) && request.delimiter() == null));
        Mockito.clearInvocations(client);
        try (RemoteIterator<RemoteFile> files = storage.listFilesIterator("s3://bucket/dir", true, cancelled::get)) {
            cancelled.set(true);
            Assertions.assertThrows(CancellationException.class, files::hasNext);
            Mockito.verifyNoInteractions(client);
        }
        Mockito.verify(client, Mockito.never()).close();
        Mockito.reset(client);
        Mockito.when(client.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(
                ListObjectsV2Response.builder().contents(object("dir/a")).nextContinuationToken("page2").build())
                .thenThrow(software.amazon.awssdk.services.s3.model.NoSuchKeyException.builder().build());
        try (RemoteIterator<RemoteFile> files = storage.listFilesIterator("s3://bucket/dir", true, () -> false)) {
            files.next();
            Assertions.assertThrows(FileSystemIOException.class, files::hasNext,
                    "A missing later page must not turn a partial directory into a complete cache entry");
        }
    }

    @Test
    void azureDescendsIntoDirectoriesLazilyAndKeepsMetadata() throws Exception {
        AzureProperties props = Mockito.mock(AzureProperties.class);
        AzureObjStorage storage = Mockito.spy(new AzureObjStorage(props));
        BlobServiceClient client = Mockito.mock(BlobServiceClient.class);
        BlobContainerClient container = Mockito.mock(BlobContainerClient.class);
        Mockito.doReturn(client).when(storage).getClient();
        Mockito.when(client.getBlobContainerClient("bucket")).thenReturn(container);
        BlobItem first = new BlobItem().setName("dir/a").setProperties(new BlobItemProperties()
                .setContentLength(7L).setLastModified(OffsetDateTime.parse("2023-01-01T00:00:09Z")));
        BlobItem directory = new BlobItem().setName("dir/sub/").setIsPrefix(true);
        BlobItem child = new BlobItem().setName("dir/sub/b").setProperties(first.getProperties());
        // listBlobsByHierarchy returns PagedIterable, whose iteration remains page-driven in the SDK.
        com.azure.core.http.rest.PagedIterable<BlobItem> root = Mockito.mock(
                com.azure.core.http.rest.PagedIterable.class);
        com.azure.core.http.rest.PagedIterable<BlobItem> nested = Mockito.mock(
                com.azure.core.http.rest.PagedIterable.class);
        Mockito.when(root.iterator()).thenReturn(Arrays.asList(first, directory).iterator());
        Mockito.when(nested.iterator()).thenReturn(Collections.singletonList(child).iterator());
        Mockito.when(container.listBlobsByHierarchy("dir/")).thenReturn(root);
        Mockito.when(container.listBlobsByHierarchy("dir/sub/")).thenReturn(nested);
        try (RemoteIterator<RemoteFile> files = storage.listFilesIterator("s3://bucket/dir", true, () -> false)) {
            Mockito.verifyNoInteractions(container);
            RemoteFile file = files.next();
            Assertions.assertEquals(7L, file.getSize());
            Assertions.assertEquals(9L, file.getModificationTime());
            Mockito.verify(container, Mockito.never()).listBlobsByHierarchy("dir/sub/");
            Assertions.assertEquals("s3://bucket/dir/sub/b", files.next().getPath().toString());
            Assertions.assertFalse(files.hasNext());
        }
    }

    @Test
    void hdfsRetainsNativeIteratorAndDoesNotCloseBorrowedFileSystem() throws Exception {
        HdfsCompatibleProperties props = Mockito.mock(HdfsCompatibleProperties.class);
        Mockito.when(props.getOrigProps()).thenReturn(Collections.emptyMap());
        HadoopAuthenticator auth = Mockito.mock(HadoopAuthenticator.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(auth.getUGI()).thenReturn(UserGroupInformation.createRemoteUser("listing-test"));
        Mockito.when(props.getHadoopAuthenticator()).thenReturn(auth);
        DFSFileSystem fs = Mockito.spy(new DFSFileSystem(props));
        org.apache.hadoop.fs.FileSystem nativeFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.doReturn(nativeFs).when(fs).nativeFileSystem(Mockito.any());
        org.apache.hadoop.fs.RemoteIterator<LocatedFileStatus> source = Mockito.mock(
                org.apache.hadoop.fs.RemoteIterator.class);
        Mockito.when(nativeFs.listFiles(Mockito.any(), Mockito.eq(true))).thenReturn(source);
        LocatedFileStatus status = new LocatedFileStatus(
                new org.apache.hadoop.fs.FileStatus(10, false, 1, 128, 99, new Path("hdfs://test/p/a")),
                new BlockLocation[0]);
        java.util.Iterator<LocatedFileStatus> statuses = Collections.singletonList(status).iterator();
        Mockito.when(source.hasNext()).thenAnswer(call -> statuses.hasNext());
        Mockito.when(source.next()).thenAnswer(call -> statuses.next());
        try (RemoteIterator<RemoteFile> files = fs.listFilesIterator("hdfs://test/p", true, () -> false)) {
            Mockito.verifyNoInteractions(nativeFs);
            Assertions.assertEquals(10, files.next().getSize());
            Assertions.assertFalse(files.hasNext());
        }
        Mockito.verify(source).next();
        Mockito.verify(nativeFs, Mockito.never()).close();
    }

    @Test
    void brokerFallbackKeepsSingleEagerRpcAndPropagatesErrors() throws Exception {
        FileSystem fs = Mockito.mock(FileSystem.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(fs.listFiles(Mockito.eq("broker/path"), Mockito.eq(true), Mockito.anyList()))
                .thenAnswer(call -> {
                    call.<List<RemoteFile>>getArgument(2).add(new RemoteFile("a", true, 10, 0));
                    return Status.OK;
                });
        try (RemoteIterator<RemoteFile> files = fs.listFilesIterator("broker/path", true, () -> false)) {
            Assertions.assertEquals("a", files.next().getName());
            Assertions.assertFalse(files.hasNext());
        }
        Mockito.verify(fs).listFiles(Mockito.eq("broker/path"), Mockito.eq(true), Mockito.anyList());
        Mockito.when(fs.listFiles(Mockito.eq("missing"), Mockito.eq(true), Mockito.anyList()))
                .thenReturn(new Status(Status.ErrCode.NOT_FOUND, "missing"));
        FileSystemIOException error = Assertions.assertThrows(FileSystemIOException.class,
                () -> fs.listFilesIterator("missing", true, () -> false));
        Assertions.assertEquals(Status.ErrCode.NOT_FOUND, error.getErrorCode().get());
    }

    private static S3Object object(String key) {
        return S3Object.builder().key(key).size(10L).lastModified(Instant.EPOCH).build();
    }
}

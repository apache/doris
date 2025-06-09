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

package org.apache.doris.fs.remote;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.backup.Status;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RemoteFileSystemTest {

    private RemoteFileSystem remoteFileSystem;
    private FileSystem mockFileSystem;

    @BeforeEach
    void setUp() {
        remoteFileSystem = Mockito.spy(new RemoteFileSystem("test", StorageBackend.StorageType.HDFS) {
            @Override
            public StorageProperties getStorageProperties() {
                return null;
            }

            @Override
            public Status exists(String remotePath) {
                return null;
            }

            @Override
            public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
                return null;
            }

            @Override
            public Status upload(String localPath, String remotePath) {
                return null;
            }

            @Override
            public Status directUpload(String content, String remoteFile) {
                return null;
            }

            @Override
            public Status rename(String origFilePath, String destFilePath) {
                return null;
            }

            @Override
            public Status delete(String remotePath) {
                return null;
            }

            @Override
            public Status makeDir(String remotePath) {
                return null;
            }

            @Override
            public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
                return null;
            }
        });
        mockFileSystem = Mockito.mock(FileSystem.class);
    }

    @Test
    @DisplayName("listFiles should return OK status and populate result list when files are found")
    void listFilesReturnsOkWhenFilesFound() throws Exception {
        String remotePath = "/test/path";
        List<RemoteFile> result = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> mockIterator = Mockito.mock(RemoteIterator.class);
        LocatedFileStatus mockFileStatus = Mockito.mock(LocatedFileStatus.class);

        Mockito.doReturn(mockFileSystem).when(remoteFileSystem).nativeFileSystem(remotePath);
        Mockito.when(mockFileSystem.listFiles(new Path(remotePath), true)).thenReturn(mockIterator);
        Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
        Mockito.when(mockIterator.next()).thenReturn(mockFileStatus);
        Mockito.when(mockFileStatus.getPath()).thenReturn(new Path("/test/path/file1"));
        Mockito.when(mockFileStatus.isDirectory()).thenReturn(false);
        Mockito.when(mockFileStatus.getLen()).thenReturn(100L);
        Mockito.when(mockFileStatus.getBlockSize()).thenReturn(128L);
        Mockito.when(mockFileStatus.getModificationTime()).thenReturn(123456789L);
        Mockito.when(mockFileStatus.getBlockLocations()).thenReturn(null);

        Status status = remoteFileSystem.listFiles(remotePath, true, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("/test/path/file1", result.get(0).getPath().toString());
    }

    @Test
    @DisplayName("listFiles should return NOT_FOUND status when FileNotFoundException is thrown")
    void listFilesReturnsNotFoundWhenFileNotFoundExceptionThrown() throws Exception {

        mockFileSystem = Mockito.mock(FileSystem.class);
        String remotePath = "/nonexistent/path";
        List<RemoteFile> result = new ArrayList<>();
        Mockito.doReturn(mockFileSystem).when(remoteFileSystem).nativeFileSystem(remotePath);
        Mockito.doThrow(new FileNotFoundException("File not found"))
                .when(mockFileSystem).listFiles(Mockito.any(Path.class), Mockito.anyBoolean());

        Status status = remoteFileSystem.listFiles(remotePath, true, result);

        Assertions.assertEquals(Status.ErrCode.NOT_FOUND, status.getErrCode());
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("listDirectories should return OK status and populate result set with directories")
    void listDirectoriesReturnsOkWhenDirectoriesFound() throws Exception {
        String remotePath = "/test/path";
        Set<String> result = new HashSet<>();
        FileStatus[] mockFileStatuses = {
                Mockito.mock(FileStatus.class),
                Mockito.mock(FileStatus.class)
        };

        Mockito.doReturn(mockFileSystem).when(remoteFileSystem).nativeFileSystem(remotePath);
        Mockito.when(mockFileSystem.listStatus(new Path(remotePath))).thenReturn(mockFileStatuses);
        Mockito.when(mockFileStatuses[0].isDirectory()).thenReturn(true);
        Mockito.when(mockFileStatuses[0].getPath()).thenReturn(new Path("/test/path/dir1"));
        Mockito.when(mockFileStatuses[1].isDirectory()).thenReturn(false);

        Status status = remoteFileSystem.listDirectories(remotePath, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains("/test/path/dir1/"));
    }

    @Test
    @DisplayName("renameDir should return COMMON_ERROR when destination directory exists")
    void renameDirReturnsErrorWhenDestinationExists() throws Exception {
        String origFilePath = "/test/path/orig";
        String destFilePath = "/test/path/dest";

        Mockito.doReturn(new Status(Status.ErrCode.OK, ""))
                .when(remoteFileSystem).exists(destFilePath);

        Status status = remoteFileSystem.renameDir(origFilePath, destFilePath, () -> {
        });

        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
        Assertions.assertEquals("Destination directory already exists: /test/path/dest", status.getErrMsg());
    }
}

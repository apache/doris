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

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RemoteFileSystemTest {

    private RemoteFileSystem remoteFileSystem;
    private FileSystem mockFileSystem;

    private static class DummyRemoteFileSystem extends RemoteFileSystem {
        public DummyRemoteFileSystem() {
            super("dummy", StorageBackend.StorageType.HDFS);
        }

        @Override
        public Status exists(String path) {
            return Status.OK;
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
        public Status makeDir(String path) {
            return Status.OK;
        }

        @Override
        public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
            return null;
        }

        @Override
        public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
            return null;
        }

        @Override
        public Status rename(String src, String dst) {
            return new Status(Status.ErrCode.OK, "Rename operation not implemented in DummyRemoteFileSystem");
        }

        @Override
        public Status delete(String remotePath) {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public StorageProperties getStorageProperties() {
            return null;
        }

        public Status rename(String path) {
            return new Status(Status.ErrCode.OK, "Rename operation not implemented in DummyRemoteFileSystem");
        }
    }

    @Test
    public void testRenameDir_destExists() {
        RemoteFileSystem fs = new DummyRemoteFileSystem() {
            @Override
            public Status exists(String path) {
                if (path.equals("s3://bucket/target")) {
                    return Status.OK;
                }
                return new Status(Status.ErrCode.NOT_FOUND, "not found");
            }

            @Override
            public Status rename(String path) {
                return null;
            }
        };

        Status result = fs.renameDir("s3://bucket/src", "s3://bucket/target", () -> {
        });
        Assertions.assertFalse(result.ok());
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, result.getErrCode());
    }

    @Test
    public void testRenameDir_makeDirSuccess() {
        AtomicBoolean runFlag = new AtomicBoolean(false);

        RemoteFileSystem fs = new DummyRemoteFileSystem() {
            @Override
            public Status exists(String path) {
                if (path.equals("s3://bucket/target")) {
                    return new Status(Status.ErrCode.NOT_FOUND, "not found");
                }
                if (path.equals("s3://bucket")) {
                    return new Status(Status.ErrCode.NOT_FOUND, "not found");
                }
                return Status.OK;
            }

            @Override
            public Status makeDir(String path) {
                return Status.OK;
            }

            @Override
            public Status rename(String src, String dst) {
                return Status.OK;
            }

            @Override
            public Status rename(String path) {
                return null;
            }
        };

        Status result = fs.renameDir("s3://bucket/src", "s3://bucket/target", () -> runFlag.set(true));
        Assertions.assertTrue(result.ok());
        Assertions.assertTrue(runFlag.get());
    }

    @Test
    public void testRenameDir_makeDirFails() {
        RemoteFileSystem fs = new DummyRemoteFileSystem() {
            @Override
            public Status exists(String path) {
                if (path.equals("s3://bucket/target")) {
                    return new Status(Status.ErrCode.NOT_FOUND, "not found");
                }
                if (path.equals("s3://bucket")) {
                    return new Status(Status.ErrCode.NOT_FOUND, "not found");
                }
                return Status.OK;
            }

            @Override
            public Status rename(String src, String dst) {
                return new Status(Status.ErrCode.COMMON_ERROR, "mkdir failed");
            }

            @Override
            public Status rename(String path) {
                return new Status(Status.ErrCode.COMMON_ERROR, "mkdir failed");
            }

        };

        Status result = fs.renameDir("s3://bucket/src", "s3://bucket/target", () -> {
            //no-op
        });
        Assertions.assertFalse(result.ok());
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, result.getErrCode());
        Assertions.assertTrue(result.getErrMsg().contains("mkdir failed"));
    }
}

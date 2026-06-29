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

import org.apache.doris.backup.Status;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SwitchingFileSystemTest {

    @Test
    public void testNormalizeOssBucketEndpointPathBeforeDelegating() {
        RecordingFileSystem delegate = new RecordingFileSystem();
        SwitchingFileSystem fs = new TestSwitchingFileSystem(delegate, createOssStorageProperties());

        String sourcePath = "oss://my-bucket.oss-cn-beijing-internal.aliyuncs.com/path/to/source";
        String destPath = "oss://my-bucket.oss-cn-beijing-internal.aliyuncs.com/path/to/dest";

        fs.deleteDirectory(sourcePath);
        Assertions.assertEquals("s3://my-bucket/path/to/source", delegate.remotePath);
        Assertions.assertEquals("s3://my-bucket/path/to/source", delegate.selectedLocation);

        fs.listFiles(sourcePath, true, Collections.emptyList());
        Assertions.assertEquals("s3://my-bucket/path/to/source", delegate.remotePath);
        Assertions.assertEquals("s3://my-bucket/path/to/source", delegate.selectedLocation);

        fs.rename(sourcePath, destPath);
        Assertions.assertEquals("s3://my-bucket/path/to/source", delegate.remotePath);
        Assertions.assertEquals("s3://my-bucket/path/to/dest", delegate.destPath);
        Assertions.assertEquals("s3://my-bucket/path/to/source", delegate.selectedLocation);

        fs.upload("/tmp/local-file", destPath);
        Assertions.assertEquals("/tmp/local-file", delegate.localPath);
        Assertions.assertEquals("s3://my-bucket/path/to/dest", delegate.remotePath);
        Assertions.assertEquals("s3://my-bucket/path/to/dest", delegate.selectedLocation);
    }

    private static Map<StorageProperties.Type, StorageProperties> createOssStorageProperties() {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "oss-cn-beijing-internal.aliyuncs.com");
        origProps.put("oss.access_key", "ak");
        origProps.put("oss.secret_key", "sk");
        origProps.put(StorageProperties.FS_OSS_SUPPORT, "true");

        Map<StorageProperties.Type, StorageProperties> storageProperties = new HashMap<>();
        storageProperties.put(StorageProperties.Type.OSS, StorageProperties.createPrimary(origProps));
        return storageProperties;
    }

    private static class TestSwitchingFileSystem extends SwitchingFileSystem {
        private final RecordingFileSystem delegate;

        TestSwitchingFileSystem(RecordingFileSystem delegate,
                Map<StorageProperties.Type, StorageProperties> storagePropertiesMap) {
            super(null, storagePropertiesMap);
            this.delegate = delegate;
        }

        @Override
        public FileSystem fileSystem(String location) {
            delegate.selectedLocation = location;
            return delegate;
        }
    }

    private static class RecordingFileSystem implements FileSystem {
        private String selectedLocation;
        private String localPath;
        private String remotePath;
        private String destPath;

        @Override
        public Map<String, String> getProperties() {
            return null;
        }

        @Override
        public Status exists(String remotePath) {
            this.remotePath = remotePath;
            return Status.OK;
        }

        @Override
        public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
            this.remotePath = remoteFilePath;
            this.localPath = localFilePath;
            return Status.OK;
        }

        @Override
        public Status upload(String localPath, String remotePath) {
            this.localPath = localPath;
            this.remotePath = remotePath;
            return Status.OK;
        }

        @Override
        public Status directUpload(String content, String remoteFile) {
            this.remotePath = remoteFile;
            return Status.OK;
        }

        @Override
        public Status rename(String origFilePath, String destFilePath) {
            this.remotePath = origFilePath;
            this.destPath = destFilePath;
            return Status.OK;
        }

        @Override
        public Status delete(String remotePath) {
            this.remotePath = remotePath;
            return Status.OK;
        }

        @Override
        public Status deleteDirectory(String dir) {
            this.remotePath = dir;
            return Status.OK;
        }

        @Override
        public Status makeDir(String remotePath) {
            this.remotePath = remotePath;
            return Status.OK;
        }

        @Override
        public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
            this.remotePath = remotePath;
            return Status.OK;
        }

        @Override
        public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
            this.remotePath = remotePath;
            return Status.OK;
        }

        @Override
        public Status listDirectories(String remotePath, Set<String> result) {
            this.remotePath = remotePath;
            return Status.OK;
        }
    }
}

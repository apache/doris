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

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.Location;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class S3SourceOffsetProviderTest {
    @Test
    void objectKeyUsesThePathForEveryObjectStoreScheme() {
        Assertions.assertEquals("dir/part-000.parquet",
                S3SourceOffsetProvider.objectKey("s3://bucket/dir/part-000.parquet"));
        Assertions.assertEquals("dir/part-000.parquet",
                S3SourceOffsetProvider.objectKey(
                        "abfss://container@account.dfs.core.windows.net/dir/part-000.parquet"));
        Assertions.assertEquals("dir/part-000.parquet",
                S3SourceOffsetProvider.objectKey("wasbs://container@account.blob.core.windows.net/dir/part-000.parquet"));
        Assertions.assertEquals("dir/", S3SourceOffsetProvider.objectKey("dir/"));
    }

    @Test
    void fileListsPreserveTheOriginalObjectStoreUri() {
        List<FileEntry> s3Files = List.of(
                file("s3://bucket/dir/part-000.parquet"),
                file("s3://bucket/dir/part-001.parquet"));
        Assertions.assertEquals("s3://bucket/dir/{part-000.parquet,part-001.parquet}",
                S3SourceOffsetProvider.buildFileLists(s3Files, "dir/"));

        List<FileEntry> azureFiles = List.of(
                file("abfss://container@account.dfs.core.windows.net/dir/part-000.parquet"),
                file("abfss://container@account.dfs.core.windows.net/dir/part-001.parquet"));
        Assertions.assertEquals("abfss://container@account.dfs.core.windows.net/dir/"
                        + "{part-000.parquet,part-001.parquet}",
                S3SourceOffsetProvider.buildFileLists(azureFiles, "dir/"));
    }

    private static FileEntry file(String uri) {
        return new FileEntry(Location.of(uri), 1, false, 0, null);
    }
}

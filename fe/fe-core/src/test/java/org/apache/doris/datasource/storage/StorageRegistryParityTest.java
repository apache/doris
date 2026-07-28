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

package org.apache.doris.datasource.storage;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Frozen golden table for {@link StorageRegistry}'s scheme routing. The expected values were
 * originally verified cell-by-cell against the legacy scheme-type mapper (deleted in Phase D);
 * they are wire/behavior contracts — scheme routing drives LocationPath and the BE TFileType
 * leg — and must not change without a deliberate compatibility decision.
 */
public class StorageRegistryParityTest {

    private static void assertRow(String scheme, StorageTypeId type, FileSystemType fsType, TFileType fileType) {
        Assertions.assertEquals(type, StorageRegistry.fromScheme(scheme), "type for " + scheme);
        Assertions.assertEquals(fsType, StorageRegistry.fromSchemeToFileSystemType(scheme), "fsType for " + scheme);
        Assertions.assertEquals(fileType, StorageRegistry.fromSchemeToFileType(scheme), "fileType for " + scheme);
        // case-insensitivity
        Assertions.assertEquals(type, StorageRegistry.fromScheme(scheme.toUpperCase()), "upper " + scheme);
    }

    @Test
    public void testFrozenSchemeTable() {
        assertRow("s3", StorageTypeId.S3, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("s3a", StorageTypeId.S3, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("s3n", StorageTypeId.S3, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("cosn", StorageTypeId.COS, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("ofs", StorageTypeId.BROKER, FileSystemType.OFS, TFileType.FILE_BROKER);
        assertRow("gfs", StorageTypeId.BROKER, FileSystemType.HDFS, TFileType.FILE_BROKER);
        assertRow("jfs", StorageTypeId.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS);
        assertRow("viewfs", StorageTypeId.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS);
        assertRow("file", StorageTypeId.LOCAL, FileSystemType.FILE, TFileType.FILE_LOCAL);
        assertRow("oss", StorageTypeId.OSS, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("obs", StorageTypeId.OBS, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("cos", StorageTypeId.COS, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("gs", StorageTypeId.GCS, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("abfs", StorageTypeId.AZURE, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("abfss", StorageTypeId.AZURE, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("wasb", StorageTypeId.AZURE, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("wasbs", StorageTypeId.AZURE, FileSystemType.S3, TFileType.FILE_S3);
        assertRow("hdfs", StorageTypeId.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS);
        assertRow("local", StorageTypeId.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS);
        assertRow("http", StorageTypeId.HTTP, FileSystemType.HTTP, TFileType.FILE_HTTP);
        assertRow("https", StorageTypeId.HTTP, FileSystemType.HTTP, TFileType.FILE_HTTP);
    }

    @Test
    public void testDefaultsAndUnknowns() {
        // blank/absent scheme defaults to HDFS (legacy no-scheme paths)
        Assertions.assertEquals(StorageTypeId.HDFS, StorageRegistry.fromScheme(null));
        Assertions.assertEquals(StorageTypeId.HDFS, StorageRegistry.fromScheme(" "));
        Assertions.assertEquals(FileSystemType.HDFS, StorageRegistry.fromSchemeToFileSystemType(null));
        Assertions.assertEquals(TFileType.FILE_HDFS, StorageRegistry.fromSchemeToFileType(null));
        // unknown scheme returns null (callers decide the fallback)
        Assertions.assertNull(StorageRegistry.fromScheme("ftp"));
    }

    @Test
    public void testLoadBearingQuirksFrozen() {
        // "oss" resolves to plain OSS for scheme-only lookups (OSS_HDFS is context-identified).
        Assertions.assertEquals(StorageTypeId.OSS, StorageRegistry.fromScheme("oss"));
        // "local" is HDFS (legacy quirk); "file" is LOCAL.
        Assertions.assertEquals(StorageTypeId.HDFS, StorageRegistry.fromScheme("local"));
        Assertions.assertEquals(StorageTypeId.LOCAL, StorageRegistry.fromScheme("file"));
        // ofs/gfs remain broker-routed.
        Assertions.assertEquals(StorageTypeId.BROKER, StorageRegistry.fromScheme("ofs"));
        Assertions.assertEquals(StorageTypeId.BROKER, StorageRegistry.fromScheme("gfs"));
    }
}

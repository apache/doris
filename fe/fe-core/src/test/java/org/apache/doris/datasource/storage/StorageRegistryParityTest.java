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

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.SchemaTypeMapper;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Phase B1 golden test: StorageRegistry must agree with the legacy SchemaTypeMapper on
 * every scheme it serves (SchemaTypeMapper is the oracle until Phase D deletes it), and
 * StorageTypeId must be a name-compatible successor of StorageProperties.Type.
 */
public class StorageRegistryParityTest {

    private static final String[] ALL_SCHEMES = {
            "s3", "s3a", "s3n", "cosn", "ofs", "gfs", "jfs", "viewfs", "file", "oss", "obs",
            "cos", "gs", "abfs", "abfss", "wasb", "wasbs", "hdfs", "local", "http", "https"};

    @Test
    public void testTypeIdIsNameCompatibleSuccessorOfLegacyType() {
        for (StorageProperties.Type legacy : StorageProperties.Type.values()) {
            Assertions.assertEquals(legacy.name(), StorageTypeId.valueOf(legacy.name()).name());
        }
        // JFS deliberately has no id: fe-core treats jfs:// as HDFS.
        Assertions.assertEquals(StorageProperties.Type.values().length, StorageTypeId.values().length);
    }

    @Test
    public void testSchemeToTypeAgreesWithSchemaTypeMapper() {
        for (String scheme : ALL_SCHEMES) {
            StorageProperties.Type legacy = SchemaTypeMapper.fromSchema(scheme);
            StorageTypeId modern = StorageRegistry.fromScheme(scheme);
            Assertions.assertEquals(legacy.name(), modern.name(), "scheme: " + scheme);
            // case-insensitivity parity
            Assertions.assertEquals(modern, StorageRegistry.fromScheme(scheme.toUpperCase()));
        }
        // blank/absent scheme defaults to HDFS (legacy no-scheme paths)
        Assertions.assertEquals(StorageTypeId.HDFS, StorageRegistry.fromScheme(null));
        Assertions.assertEquals(StorageTypeId.HDFS, StorageRegistry.fromScheme(" "));
        // unknown scheme: null, same as the oracle
        Assertions.assertNull(StorageRegistry.fromScheme("ftp"));
        Assertions.assertNull(SchemaTypeMapper.fromSchema("ftp"));
    }

    @Test
    public void testSchemeToFileSystemTypeAgreesWithSchemaTypeMapper() {
        for (String scheme : ALL_SCHEMES) {
            Assertions.assertEquals(SchemaTypeMapper.fromSchemaToFileSystemType(scheme),
                    StorageRegistry.fromSchemeToFileSystemType(scheme), "scheme: " + scheme);
        }
        Assertions.assertEquals(SchemaTypeMapper.fromSchemaToFileSystemType(null),
                StorageRegistry.fromSchemeToFileSystemType(null));
    }

    @Test
    public void testSchemeToTFileTypeAgreesWithSchemaTypeMapper() {
        for (String scheme : ALL_SCHEMES) {
            Assertions.assertEquals(SchemaTypeMapper.fromSchemaToFileType(scheme),
                    StorageRegistry.fromSchemeToFileType(scheme), "scheme: " + scheme);
        }
        Assertions.assertEquals(TFileType.FILE_HDFS, StorageRegistry.fromSchemeToFileType(null));
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

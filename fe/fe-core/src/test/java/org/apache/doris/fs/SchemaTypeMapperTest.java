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

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SchemaTypeMapperTest {
    @Test
    public void testFromSchema_AllDefinedSchemas() {
        for (SchemaTypeMapper mapper : SchemaTypeMapper.values()) {
            if (mapper.equals(SchemaTypeMapper.OSS_HDFS)) {
                continue;
            }
            String schema = mapper.getSchema();

            StorageProperties.Type expectedType = SchemaTypeMapper.fromSchema(schema);
            Assertions.assertEquals(mapper.getStorageType(), expectedType, "Schema: " + schema);

            FileSystemType expectedFsType = SchemaTypeMapper.fromSchemaToFileSystemType(schema);
            Assertions.assertEquals(mapper.getFileSystemType(), expectedFsType, "Schema: " + schema);

            TFileType expectedFileType = SchemaTypeMapper.fromSchemaToFileType(schema);
            Assertions.assertEquals(mapper.getFileType(), expectedFileType, "Schema: " + schema);
        }
    }

    @Test
    public void testFromSchema_NullAndBlankSchema() {
        Assertions.assertEquals(StorageProperties.Type.HDFS, SchemaTypeMapper.fromSchema(null));
        Assertions.assertEquals(StorageProperties.Type.HDFS, SchemaTypeMapper.fromSchema(""));
        Assertions.assertEquals(StorageProperties.Type.HDFS, SchemaTypeMapper.fromSchema(" "));
    }

    @Test
    public void testFromSchemaToFileSystemType_NullSchema() {
        Assertions.assertEquals(FileSystemType.HDFS, SchemaTypeMapper.fromSchemaToFileSystemType(null));
    }

    @Test
    public void testFromSchemaToFileType_NullSchema() {
        Assertions.assertEquals(TFileType.FILE_HDFS, SchemaTypeMapper.fromSchemaToFileType(null));
    }

    @Test
    public void testFromSchema_UnknownSchema() {
        Assertions.assertNull(SchemaTypeMapper.fromSchema("unknown-schema"));
        Assertions.assertNull(SchemaTypeMapper.fromSchemaToFileSystemType("unknown-schema"));
        Assertions.assertNull(SchemaTypeMapper.fromSchemaToFileType("unknown-schema"));
    }
}


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

package org.apache.doris.datasource.paimon;

import org.apache.doris.thrift.TPaimonCppWriteDescriptor;
import org.apache.doris.thrift.TPaimonWriteMode;

import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonCppWriteSupportTest {
    private static final List<String> COLUMNS = Arrays.asList("id", "name");

    private FileStoreTable table(Map<String, String> overrides, List<String> partitions,
            List<String> keys, String location) {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "-1");
        options.put("file.format", "parquet");
        options.put("write-only", "true");
        options.putAll(overrides);
        TableSchema schema = new TableSchema(7,
                Arrays.asList(new DataField(0, "id", DataTypes.INT().notNull()),
                        new DataField(1, "name", DataTypes.STRING())),
                1, partitions, keys, options, null);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.options()).thenReturn(options);
        Mockito.when(table.location()).thenReturn(new Path(location));
        return table;
    }

    private FileStoreTable table(Map<String, String> overrides) {
        return table(overrides, Collections.emptyList(), Collections.emptyList(), "file:///tmp/paimon");
    }

    @Test
    public void testSupportedAppendDescriptor() {
        FileStoreTable table = table(Collections.emptyMap());
        Assert.assertNull(PaimonCppWriteSupport.unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND));
        TPaimonCppWriteDescriptor desc = PaimonCppWriteSupport.describe(table);
        Assert.assertEquals(1, desc.getVersion());
        Assert.assertEquals(7, desc.getSchemaId());
        Assert.assertEquals("INTEGER", desc.getColumns().get(0).getType());
        Assert.assertFalse(desc.getColumns().get(0).isNullable());
        Assert.assertTrue(desc.getColumns().get(1).isNullable());
        Assert.assertFalse(table.options().containsKey("manifest.format"));
        Assert.assertEquals("avro", desc.getOptions().get("manifest.format"));
    }

    @Test
    public void testUnsupportedOptionsAndModes() {
        for (Map<String, String> options : Arrays.asList(
                Collections.singletonMap("bucket", "4"),
                Collections.singletonMap("write-only", "false"),
                Collections.singletonMap("file.format", "orc"),
                Collections.singletonMap("variant.inferShreddingSchema", "true"),
                Collections.singletonMap("changelog-producer", "input"))) {
            Assert.assertNotNull(PaimonCppWriteSupport.unsupportedReason(
                    table(options), COLUMNS, TPaimonWriteMode.APPEND));
        }
        for (TPaimonWriteMode mode : Arrays.asList(TPaimonWriteMode.OVERWRITE, TPaimonWriteMode.CHANGELOG)) {
            Assert.assertNotNull(PaimonCppWriteSupport.unsupportedReason(table(Collections.emptyMap()), COLUMNS, mode));
        }
    }

    @Test
    public void testUnsupportedRoutingAndStorage() {
        Assert.assertNotNull(PaimonCppWriteSupport.unsupportedReason(
                table(Collections.emptyMap(), Collections.singletonList("id"), Collections.emptyList(), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(PaimonCppWriteSupport.unsupportedReason(
                table(Collections.emptyMap(), Collections.emptyList(), Collections.singletonList("id"), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(PaimonCppWriteSupport.unsupportedReason(
                table(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), "s3://bucket/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(PaimonCppWriteSupport.unsupportedReason(
                table(Collections.emptyMap()), Arrays.asList("name", "id"), TPaimonWriteMode.APPEND));
    }
}

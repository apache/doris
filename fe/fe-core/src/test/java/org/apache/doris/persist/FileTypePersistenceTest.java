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

package org.apache.doris.persist;

import org.apache.doris.backup.BackupMeta;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FileType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Writable;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FileTypePersistenceTest {
    private static final long TABLE_ID = 1000;
    private static final long INDEX_ID = 1001;
    private static final int TRAILER = 0x12345678;
    private MetaContext previousMetaContext;

    @BeforeEach
    public void setUp() {
        previousMetaContext = MetaContext.get();
        MetaContext context = new MetaContext();
        context.setMetaVersion(FeConstants.meta_version);
        context.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        if (previousMetaContext == null) {
            MetaContext.remove();
        } else {
            previousMetaContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testBackupMetaWriteReadPreservesFileSchema() throws Exception {
        OlapTable table = table();
        BackupMeta backup = new BackupMeta(Collections.singletonList(table), Collections.emptyList());
        try (DataInputStream in = serializedInput(backup)) {
            BackupMeta restored = BackupMeta.read(in);
            Assertions.assertEquals(1, restored.getTables().size());
            Assertions.assertSame(restored.getTable(TABLE_ID), restored.getTable(table.getName()));
            assertTable(table, (OlapTable) restored.getTable(TABLE_ID));
            assertEnd(in);
        }
    }

    @Test
    public void testCreateTableInfoWriteReadPreservesFileSchema() throws Exception {
        OlapTable table = table();
        CreateTableInfo info = new CreateTableInfo("file_persistence_db", 123L, table);
        try (DataInputStream in = serializedInput(info)) {
            CreateTableInfo restored = CreateTableInfo.read(in);
            Assertions.assertEquals(123L, restored.getDbId());
            Assertions.assertEquals("file_persistence_db", restored.getDbName());
            Assertions.assertEquals(table.getName(), restored.getTblName());
            assertTable(table, (OlapTable) restored.getTable());
            assertEnd(in);
        }
    }

    @Test
    public void testLightSchemaChangePayloadWriteReadPreservesFileSchema() throws Exception {
        LinkedList<Column> schema = schema();
        TableAddOrDropColumnsInfo info = new TableAddOrDropColumnsInfo(
                "ALTER TABLE file_persistence ADD COLUMN f FILE", 123L, TABLE_ID, INDEX_ID,
                Collections.singletonMap(INDEX_ID, schema),
                Collections.singletonMap(INDEX_ID, Collections.singletonList(schema.get(0))),
                Collections.singletonMap("file_persistence", INDEX_ID), Collections.emptyList(), 456L);
        try (DataInputStream in = serializedInput(info)) {
            TableAddOrDropColumnsInfo restored = TableAddOrDropColumnsInfo.read(in);
            Assertions.assertEquals(123L, restored.getDbId());
            Assertions.assertEquals(TABLE_ID, restored.getTableId());
            Assertions.assertEquals(456L, restored.getJobId());
            Assertions.assertEquals(Collections.singleton(INDEX_ID), restored.getIndexSchemaMap().keySet());
            assertSchema(schema, restored.getIndexSchemaMap().get(INDEX_ID));
            assertEnd(in);
        }
    }

    private static LinkedList<Column> schema() {
        LinkedList<Column> columns = new LinkedList<>(Arrays.asList(
                new Column("id", Type.INT, false),
                new Column("f", FileType.create(), true),
                new Column("files", new ArrayType(FileType.create(), true), true),
                new Column("details", new StructType(
                        new StructField("payload", FileType.create()),
                        new StructField("grouped", new ArrayType(new MapType(Type.STRING, FileType.create())))), true),
                new Column("by_name", new MapType(Type.STRING, FileType.create()), true)));
        columns.get(0).setIsKey(true);
        for (int i = 0; i < columns.size(); i++) {
            columns.get(i).setUniqueId(10 + i);
        }
        return columns;
    }

    private static OlapTable table() {
        List<Column> schema = schema();
        RandomDistributionInfo distribution = new RandomDistributionInfo(1);
        OlapTable table = new OlapTable(TABLE_ID, "file_persistence", schema, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), distribution);
        table.setBaseIndexId(INDEX_ID);
        table.setIndexMeta(INDEX_ID, table.getName(), schema, 1, 100, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.addPartition(new Partition(2000L, table.getName(),
                new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL), distribution));
        return table;
    }

    private static DataInputStream serializedInput(Writable value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            value.write(out);
            out.writeInt(TRAILER);
        }
        return new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    }

    private static void assertEnd(DataInputStream in) throws IOException {
        Assertions.assertEquals(TRAILER, in.readInt());
        Assertions.assertEquals(-1, in.read());
    }

    private static void assertTable(OlapTable expected, OlapTable actual) {
        Assertions.assertNotNull(actual);
        Assertions.assertNotSame(expected, actual);
        Assertions.assertEquals(TABLE_ID, actual.getId());
        Assertions.assertEquals(expected.getName(), actual.getName());
        Assertions.assertEquals(INDEX_ID, actual.getBaseIndexId());
        assertSchema(expected.getFullSchema(), actual.getFullSchema());
        assertSchema(expected.getSchemaByIndexId(INDEX_ID), actual.getSchemaByIndexId(INDEX_ID));
    }

    private static void assertSchema(List<Column> expected, List<Column> actual) {
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(5, actual.size());
        int files = 0;
        for (int i = 0; i < expected.size(); i++) {
            files += assertColumn(expected.get(i), actual.get(i));
        }
        Assertions.assertEquals(5, files, "Every top-level and nested FILE must retain its identity");
    }

    private static int assertColumn(Column expected, Column actual) {
        Assertions.assertEquals(expected.getName(), actual.getName());
        Assertions.assertEquals(expected.getUniqueId(), actual.getUniqueId());
        Assertions.assertEquals(expected.isAllowNull(), actual.isAllowNull());
        Assertions.assertEquals(expected.getType().getClass(), actual.getType().getClass());
        Assertions.assertEquals(expected.getType(), actual.getType());
        if (expected.getType().isFileType()) {
            assertCanonicalFile(actual);
            return 1;
        }
        if (expected.getChildren() == null) {
            Assertions.assertNull(actual.getChildren());
            return 0;
        }
        Assertions.assertNotNull(actual.getChildren());
        Assertions.assertEquals(expected.getChildren().size(), actual.getChildren().size());
        int files = 0;
        for (int i = 0; i < expected.getChildren().size(); i++) {
            files += assertColumn(expected.getChildren().get(i), actual.getChildren().get(i));
        }
        return files;
    }

    private static void assertCanonicalFile(Column column) {
        Assertions.assertEquals(FileType.class, column.getType().getClass());
        Assertions.assertEquals(PrimitiveType.FILE, column.getDataType());
        Assertions.assertFalse(column.getType().isStructType());
        Assertions.assertNotNull(column.getChildren());
        Assertions.assertEquals(6, column.getChildren().size());
        List<StructField> fields = ((FileType) column.getType()).getFields();
        Assertions.assertEquals(6, fields.size());
        List<String> names = Arrays.asList("uri", "offset", "size", "content_type", "checksum", "inline");
        List<PrimitiveType> types = Arrays.asList(PrimitiveType.VARCHAR, PrimitiveType.BIGINT,
                PrimitiveType.BIGINT, PrimitiveType.VARCHAR, PrimitiveType.VARCHAR, PrimitiveType.VARBINARY);
        for (int i = 0; i < 6; i++) {
            Column child = column.getChildren().get(i);
            Assertions.assertEquals(names.get(i), child.getName());
            Assertions.assertEquals(i, child.getUniqueId());
            Assertions.assertTrue(child.isAllowNull());
            Assertions.assertEquals(types.get(i), child.getDataType());
            Assertions.assertNull(child.getChildren());
            Assertions.assertEquals(names.get(i), fields.get(i).getName());
            Assertions.assertTrue(fields.get(i).getContainsNull());
            Assertions.assertEquals(child.getType(), fields.get(i).getType());
        }
        Assertions.assertEquals(65533, column.getChildren().get(0).getType().getLength());
        Assertions.assertEquals(1024, column.getChildren().get(3).getType().getLength());
        Assertions.assertEquals(1024, column.getChildren().get(4).getType().getLength());
    }
}

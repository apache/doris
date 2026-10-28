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

package org.apache.doris.common.jni.vec;

import org.apache.doris.common.jni.utils.OffHeap;

import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VectorColumnFileTest {
    @BeforeClass
    public static void setUpClass() {
        OffHeap.setTesting();
    }

    @Test
    public void testFileTypeInsideNestedTypes() {
        ColumnType file = ColumnType.parseType("image", "file");
        Assert.assertTrue(file.isFile());
        Assert.assertFalse(file.isStruct());
        Assert.assertTrue(file.isComplexType());
        Assert.assertEquals(Arrays.asList("uri", "offset", "size", "content_type", "checksum", "inline"),
                file.getChildNames());
        Assert.assertEquals(FileValue.FIELD_COUNT, file.getChildTypes().size());
        Assert.assertEquals(23, file.metaSize());
        Assert.assertTrue(ColumnType.parseType("a", "array<file>").getChildTypes().get(0).isFile());
        Assert.assertTrue(ColumnType.parseType("m", "map<string,file>").getChildTypes().get(1).isFile());
        Assert.assertTrue(ColumnType.parseType("s", "struct<image:file>").getChildTypes().get(0).isFile());
        Assert.assertTrue(ColumnType.parseTypeWithEncodedStructFields("s", "struct<$aW1hZ2U=:file>")
                .getChildTypes().get(0).isFile());
    }

    @Test
    public void testAppendInlineDescriptorNullAndEmptyFile() {
        new MockUp<OffHeap>() {
            @Mock
            public long allocateMemory(Invocation invocation, long size) {
                // Unsafe.allocateMemory(0) masks the production malloc(0) allocation.
                Assert.assertTrue("Empty payloads must not allocate memory", size > 0);
                return invocation.proceed(size);
            }
        };
        ColumnType fileType = ColumnType.parseType("image", "file");
        VectorTable table = VectorTable.createWritableTable(
                new ColumnType[] {fileType}, new String[] {"image"}, 1);
        try {
            VectorColumn column = table.getColumn(0);
            byte[] payload = "hello world".getBytes(StandardCharsets.UTF_8);
            column.appendFile(new FileValue(null, null, null, null, null, payload));
            column.appendFile(new FileValue("s3://bucket/data.blob", 17L, 11L, "image/png", "crc", null));
            column.appendValue(null);
            column.appendFile(new FileValue(null, null, null, null, null, new byte[0]));

            Assert.assertArrayEquals(payload, column.getFile(0).getInline());
            Assert.assertEquals("image/png", column.getFile(1).getContentType());
            Assert.assertEquals("crc", column.getFile(1).getChecksum());
            Assert.assertNull(column.getFile(2));
            Assert.assertArrayEquals(new byte[0], column.getFile(3).getInline());
            long meta = table.getMetaAddress();
            Assert.assertEquals(4, OffHeap.getLong(null, meta));
            Assert.assertArrayEquals(new boolean[] {false, false, true, false},
                    OffHeap.getBoolean(null, OffHeap.getLong(null, meta + 8), 4));
            long uriMeta = childMeta(table, FileValue.URI_FIELD_INDEX);
            Assert.assertArrayEquals(new boolean[] {true, false, true, true},
                    OffHeap.getBoolean(null, OffHeap.getLong(null, uriMeta), 4));
            long offsetData = OffHeap.getLong(null, childMeta(table, FileValue.OFFSET_FIELD_INDEX) + 8);
            long sizeData = OffHeap.getLong(null, childMeta(table, FileValue.SIZE_FIELD_INDEX) + 8);
            Assert.assertEquals(17L, OffHeap.getLong(null, offsetData + 8));
            Assert.assertEquals(11L, OffHeap.getLong(null, sizeData + 8));
            long inlineMeta = childMeta(table, FileValue.INLINE_FIELD_INDEX);
            Assert.assertArrayEquals(new boolean[] {false, true, true, false},
                    OffHeap.getBoolean(null, OffHeap.getLong(null, inlineMeta), 4));
            // Java -> BE VARBINARY uses [length, pointer], including short payloads.
            long inlineData = OffHeap.getLong(null, inlineMeta + 8);
            Assert.assertEquals(payload.length, OffHeap.getLong(null, inlineData));
            Assert.assertArrayEquals(payload,
                    OffHeap.getByte(null, OffHeap.getLong(null, inlineData + 8), payload.length));
            for (int i = 1; i < 4; i++) {
                Assert.assertEquals(0, OffHeap.getLong(null, inlineData + i * 16));
                Assert.assertEquals(0, OffHeap.getLong(null, inlineData + i * 16 + 8));
            }

            table.reset();
            column.appendFile(new FileValue("file:/new.blob", 0L, null, null, null, null));
            Assert.assertEquals(1, OffHeap.getLong(null, table.getMetaAddress()));
            Assert.assertFalse(column.isNullAt(0));
            Assert.assertTrue(OffHeap.getBoolean(null,
                    OffHeap.getLong(null, childMeta(table, FileValue.INLINE_FIELD_INDEX))));
        } finally {
            table.close();
        }
    }

    @Test
    public void testObjectBatchPreservesNullableMetadata() {
        VectorTable table = VectorTable.createWritableTable(
                new ColumnType[] {ColumnType.parseType("image", "file")}, new String[] {"image"}, 1);
        try {
            FileValue descriptor = new FileValue("file:/a.blob", 0L, null, null, null, null);
            table.appendData(0, new FileValue[] {descriptor, null}, true);
            FileValue[] values = (FileValue[]) table.getColumn(0).getObjectColumn(0, 2);
            Assert.assertEquals(descriptor.getUri(), values[0].getUri());
            Assert.assertEquals(Long.valueOf(0), values[0].getOffset());
            Assert.assertNull(values[0].getSize());
            Assert.assertNull(values[0].getInline());
            Assert.assertNull(values[1]);
            Assert.assertThrows(RuntimeException.class,
                    () -> table.appendData(0, new FileValue[] {null}, false));
        } finally {
            table.close();
        }
    }

    @Test
    public void testFileInsideArrayMapAndStruct() {
        ColumnType[] types = new ColumnType[] {
                ColumnType.parseType("a", "array<file>"),
                ColumnType.parseType("m", "map<string,file>"),
                ColumnType.parseType("s", "struct<image:file>")};
        VectorTable table = VectorTable.createWritableTable(types, new String[] {"a", "m", "s"}, 1);
        try {
            byte[] bytes = new byte[] {0, 42, (byte) 255};
            FileValue file = new FileValue(null, null, null, null, null, bytes);
            table.appendData(0, new List[] {Arrays.asList(file, null)}, true);
            table.appendData(1, new Map[] {Collections.singletonMap("image", file)}, true);
            table.appendData(2, new Map[] {Collections.singletonMap("image", file)}, true);
            List<?> array = (List<?>) table.getColumn(0).getObjectColumn(0, 1)[0];
            Assert.assertArrayEquals(bytes, ((FileValue) array.get(0)).getInline());
            Assert.assertNull(array.get(1));
            Map<?, ?> map = (Map<?, ?>) table.getColumn(1).getObjectColumn(0, 1)[0];
            Map<?, ?> struct = (Map<?, ?>) table.getColumn(2).getObjectColumn(0, 1)[0];
            Assert.assertArrayEquals(bytes, ((FileValue) map.get("image")).getInline());
            Assert.assertArrayEquals(bytes, ((FileValue) struct.get("image")).getInline());
        } finally {
            table.close();
        }
    }

    @Test
    public void testReadFileFromBeStringViewLayout() {
        ColumnType fileType = ColumnType.parseType("image", "file");
        // BE -> Java includes a const flag per column. Model a FILE with all-null
        // metadata and a short VARBINARY payload stored inline in a C++ StringView.
        long meta = OffHeap.allocateMemory(fileType.metaSize() * 8L);
        long nullMaps = OffHeap.allocateMemory(7);
        long payload = OffHeap.allocateMemory(16);
        long emptyData = OffHeap.allocateMemory(8);
        try {
            OffHeap.setMemory(meta, (byte) 0, fileType.metaSize() * 8L);
            OffHeap.setMemory(nullMaps, (byte) 1, 7);
            OffHeap.putByte(null, nullMaps, (byte) 0);
            OffHeap.putByte(null, nullMaps + 6, (byte) 0);
            OffHeap.setMemory(emptyData, (byte) 0, 8);
            OffHeap.setMemory(payload, (byte) 0, 16);
            byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
            OffHeap.putInt(null, payload, bytes.length);
            OffHeap.copyMemory(bytes, OffHeap.BYTE_ARRAY_OFFSET, null, payload + 4, bytes.length);
            OffHeap.putLong(null, meta + 8, nullMaps);
            long child = meta + 16;
            for (int i = 0; i < FileValue.FIELD_COUNT; i++) {
                OffHeap.putLong(null, child + 8, nullMaps + i + 1);
                OffHeap.putLong(null, child + 16, emptyData);
                if (fileType.getChildTypes().get(i).isStringType()) {
                    OffHeap.putLong(null, child + 24, emptyData);
                }
                if (i == FileValue.INLINE_FIELD_INDEX) {
                    OffHeap.putLong(null, child + 16, payload);
                }
                child += fileType.getChildTypes().get(i).metaSize() * 8L;
            }
            VectorColumn column = VectorColumn.createReadableColumn(fileType, 1, meta);
            FileValue file = column.getFile(0);
            Assert.assertNull(file.getUri());
            Assert.assertNull(file.getSize());
            Assert.assertArrayEquals(bytes, file.getInline());
            OffHeap.putLong(null, meta, 1L); // constant FILE owns one physical row
            VectorColumn constant = VectorColumn.createReadableColumn(fileType, 5, meta);
            Assert.assertArrayEquals(bytes, constant.getFile(4).getInline());
        } finally {
            // Readable columns borrow BE buffers; the owner releases them.
            OffHeap.freeMemory(emptyData);
            OffHeap.freeMemory(payload);
            OffHeap.freeMemory(nullMaps);
            OffHeap.freeMemory(meta);
        }
    }

    private long childMeta(VectorTable table, int childIndex) {
        long address = table.getMetaAddress() + 16; // row count and FILE null map
        for (int i = 0; i < childIndex; i++) {
            // Java -> BE omits the const flags used by BE -> Java.
            address += (table.getColumn(0).getColumnType().getChildTypes().get(i).metaSize() - 1) * 8L;
        }
        return address;
    }
}

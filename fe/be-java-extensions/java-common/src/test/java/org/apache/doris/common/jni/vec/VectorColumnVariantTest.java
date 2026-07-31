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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class VectorColumnVariantTest {
    private static final byte[] EMPTY_METADATA = new byte[] {1, 0, 0};
    private static final byte[] ONE_KEY_METADATA = new byte[] {1, 1, 0, 1, 'a'};

    @BeforeClass
    public static void setUpClass() {
        OffHeap.setTesting();
    }

    @Test
    public void testVariantTypeAndEncodedLayout() {
        ColumnType variantType = ColumnType.parseType("v", "variant");
        Assert.assertEquals(ColumnType.Type.VARIANT, variantType.getType());
        Assert.assertEquals(8, variantType.metaSize());

        VectorTable table = VectorTable.createWritableTable(
                new ColumnType[] {variantType}, new String[] {"v"}, 2);
        try {
            VectorColumn column = table.getColumn(0);
            column.appendVariant(EMPTY_METADATA, new byte[] {0});
            column.appendVariant(EMPTY_METADATA.clone(), new byte[] {4});
            column.appendVariant(ONE_KEY_METADATA, new byte[] {8});

            long meta = table.getMetaAddress();
            Assert.assertEquals(3L, OffHeap.getLong(null, meta));
            Assert.assertArrayEquals(new boolean[] {false, false, false},
                    OffHeap.getBoolean(null, OffHeap.getLong(null, meta + 8), 3));
            Assert.assertEquals(2L, OffHeap.getLong(null, meta + 16));

            long metadataOffsets = OffHeap.getLong(null, meta + 24);
            long metadataBytes = OffHeap.getLong(null, meta + 32);
            long metadataIds = OffHeap.getLong(null, meta + 40);
            long valueOffsets = OffHeap.getLong(null, meta + 48);
            long valueBytes = OffHeap.getLong(null, meta + 56);

            Assert.assertArrayEquals(new int[] {0, 3, 8},
                    OffHeap.getInt(null, metadataOffsets, 3));
            Assert.assertArrayEquals(
                    new byte[] {1, 0, 0, 1, 1, 0, 1, 'a'},
                    OffHeap.getByte(null, metadataBytes, 8));
            Assert.assertArrayEquals(new int[] {0, 0, 1},
                    OffHeap.getInt(null, metadataIds, 3));
            Assert.assertArrayEquals(new int[] {0, 1, 2, 3},
                    OffHeap.getInt(null, valueOffsets, 4));
            Assert.assertArrayEquals(new byte[] {0, 4, 8},
                    OffHeap.getByte(null, valueBytes, 3));
        } finally {
            table.close();
        }
    }

    @Test
    public void testSqlNullUsesValidVariantNullPlaceholder() {
        ColumnType variantType = ColumnType.parseType("v", "variant");
        VectorTable table = VectorTable.createWritableTable(
                new ColumnType[] {variantType}, new String[] {"v"}, 1);
        try {
            VectorColumn column = table.getColumn(0);
            column.appendVariant(EMPTY_METADATA, new byte[] {4});
            column.appendNull(ColumnType.Type.VARIANT);

            long meta = table.getMetaAddress();
            long nullMap = OffHeap.getLong(null, meta + 8);
            long metadataIds = OffHeap.getLong(null, meta + 40);
            long valueOffsets = OffHeap.getLong(null, meta + 48);
            long valueBytes = OffHeap.getLong(null, meta + 56);

            Assert.assertArrayEquals(new boolean[] {false, true},
                    OffHeap.getBoolean(null, nullMap, 2));
            Assert.assertEquals(1L, OffHeap.getLong(null, meta + 16));
            Assert.assertArrayEquals(new int[] {0, 0},
                    OffHeap.getInt(null, metadataIds, 2));
            Assert.assertArrayEquals(new int[] {0, 1, 2},
                    OffHeap.getInt(null, valueOffsets, 3));
            Assert.assertArrayEquals(new byte[] {4, 0},
                    OffHeap.getByte(null, valueBytes, 2));
        } finally {
            table.close();
        }
    }

    @Test
    public void testResetRebuildsMetadataDictionary() {
        ColumnType variantType = ColumnType.parseType("v", "variant");
        VectorTable table = VectorTable.createWritableTable(
                new ColumnType[] {variantType}, new String[] {"v"}, 1);
        try {
            table.getColumn(0).appendVariant(EMPTY_METADATA, new byte[] {0});
            table.reset();
            table.getColumn(0).appendVariant(ONE_KEY_METADATA, new byte[] {4});

            long meta = table.getMetaAddress();
            Assert.assertEquals(1L, OffHeap.getLong(null, meta));
            Assert.assertEquals(1L, OffHeap.getLong(null, meta + 16));
            Assert.assertArrayEquals(new int[] {0, 5},
                    OffHeap.getInt(null, OffHeap.getLong(null, meta + 24), 2));
            Assert.assertArrayEquals(new int[] {0},
                    OffHeap.getInt(null, OffHeap.getLong(null, meta + 40), 1));
        } finally {
            table.close();
        }
    }
}

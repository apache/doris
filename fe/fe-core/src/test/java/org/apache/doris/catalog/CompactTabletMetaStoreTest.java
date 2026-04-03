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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TStorageMedium;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class CompactTabletMetaStoreTest {

    private CompactTabletMetaStore store;

    @Before
    public void setUp() {
        store = new CompactTabletMetaStore(4);
    }

    @Test
    public void testAddAndGet() {
        TabletMeta meta = new TabletMeta(1L, 2L, 3L, 4L, 100, TStorageMedium.HDD);
        store.add(10001L, meta);

        Assert.assertEquals(1, store.size());
        Assert.assertTrue(store.containsKey(10001L));
        Assert.assertEquals(1L, store.getDbId(10001L));
        Assert.assertEquals(2L, store.getTableId(10001L));
        Assert.assertEquals(3L, store.getPartitionId(10001L));
        Assert.assertEquals(4L, store.getIndexId(10001L));
        Assert.assertEquals(100, store.getOldSchemaHash(10001L));
        Assert.assertEquals(TStorageMedium.HDD, store.getStorageMedium(10001L));
    }

    @Test
    public void testGetTabletMeta() {
        TabletMeta meta = new TabletMeta(10L, 20L, 30L, 40L, 200, TStorageMedium.SSD);
        store.add(10002L, meta);

        TabletMeta result = store.getTabletMeta(10002L);
        Assert.assertNotNull(result);
        Assert.assertEquals(10L, result.getDbId());
        Assert.assertEquals(20L, result.getTableId());
        Assert.assertEquals(30L, result.getPartitionId());
        Assert.assertEquals(40L, result.getIndexId());
        Assert.assertEquals(200, result.getOldSchemaHash());
        Assert.assertEquals(TStorageMedium.SSD, result.getStorageMedium());
    }

    @Test
    public void testNonExistentKey() {
        Assert.assertFalse(store.containsKey(99999L));
        Assert.assertNull(store.getTabletMeta(99999L));
        Assert.assertNull(store.getStorageMedium(99999L));
        Assert.assertEquals(TabletInvertedIndex.NOT_EXIST_VALUE, store.getDbId(99999L));
        Assert.assertEquals(TabletInvertedIndex.NOT_EXIST_VALUE, store.getTableId(99999L));
        Assert.assertEquals(TabletInvertedIndex.NOT_EXIST_VALUE, store.getPartitionId(99999L));
        Assert.assertEquals(TabletInvertedIndex.NOT_EXIST_VALUE, store.getIndexId(99999L));
        Assert.assertEquals(TabletInvertedIndex.NOT_EXIST_VALUE, store.getOldSchemaHash(99999L));
    }

    @Test
    public void testDuplicateAddIsIgnored() {
        TabletMeta meta1 = new TabletMeta(1L, 2L, 3L, 4L, 100, TStorageMedium.HDD);
        TabletMeta meta2 = new TabletMeta(10L, 20L, 30L, 40L, 200, TStorageMedium.SSD);

        store.add(10001L, meta1);
        store.add(10001L, meta2);

        Assert.assertEquals(1, store.size());
        // original values preserved
        Assert.assertEquals(1L, store.getDbId(10001L));
        Assert.assertEquals(TStorageMedium.HDD, store.getStorageMedium(10001L));
    }

    @Test
    public void testRemove() {
        TabletMeta meta = new TabletMeta(1L, 2L, 3L, 4L, 100, TStorageMedium.HDD);
        store.add(10001L, meta);
        Assert.assertEquals(1, store.size());

        store.remove(10001L);
        Assert.assertEquals(0, store.size());
        Assert.assertFalse(store.containsKey(10001L));
        Assert.assertNull(store.getTabletMeta(10001L));
    }

    @Test
    public void testRemoveNonExistent() {
        // should not throw
        store.remove(99999L);
        Assert.assertEquals(0, store.size());
    }

    @Test
    public void testSetStorageMedium() {
        TabletMeta meta = new TabletMeta(1L, 2L, 3L, 4L, 100, TStorageMedium.HDD);
        store.add(10001L, meta);
        Assert.assertEquals(TStorageMedium.HDD, store.getStorageMedium(10001L));

        store.setStorageMedium(10001L, TStorageMedium.SSD);
        Assert.assertEquals(TStorageMedium.SSD, store.getStorageMedium(10001L));

        store.setStorageMedium(10001L, TStorageMedium.S3);
        Assert.assertEquals(TStorageMedium.S3, store.getStorageMedium(10001L));

        store.setStorageMedium(10001L, TStorageMedium.REMOTE_CACHE);
        Assert.assertEquals(TStorageMedium.REMOTE_CACHE, store.getStorageMedium(10001L));
    }

    @Test
    public void testSetStorageMediumNonExistent() {
        // should not throw
        store.setStorageMedium(99999L, TStorageMedium.SSD);
    }

    @Test
    public void testFreeListReuse() {
        TabletMeta meta1 = new TabletMeta(1L, 2L, 3L, 4L, 100, TStorageMedium.HDD);
        TabletMeta meta2 = new TabletMeta(10L, 20L, 30L, 40L, 200, TStorageMedium.SSD);
        TabletMeta meta3 = new TabletMeta(100L, 200L, 300L, 400L, 300, TStorageMedium.S3);

        store.add(10001L, meta1);
        store.add(10002L, meta2);
        store.remove(10001L);

        // meta3 should reuse the freed slot
        store.add(10003L, meta3);
        Assert.assertEquals(2, store.size());
        Assert.assertEquals(100L, store.getDbId(10003L));
        Assert.assertEquals(TStorageMedium.S3, store.getStorageMedium(10003L));

        // meta2 should still be intact
        Assert.assertEquals(10L, store.getDbId(10002L));
    }

    @Test
    public void testGrowBeyondInitialCapacity() {
        // initial capacity is 4, add more than that
        for (int i = 0; i < 20; i++) {
            TabletMeta meta = new TabletMeta(i, i + 100, i + 200, i + 300, i + 400, TStorageMedium.HDD);
            store.add(50000L + i, meta);
        }

        Assert.assertEquals(20, store.size());

        // verify all entries are accessible
        for (int i = 0; i < 20; i++) {
            Assert.assertTrue(store.containsKey(50000L + i));
            Assert.assertEquals(i, store.getDbId(50000L + i));
            Assert.assertEquals(i + 100, store.getTableId(50000L + i));
        }
    }

    @Test
    public void testToMap() {
        store.add(10001L, new TabletMeta(1L, 2L, 3L, 4L, 100, TStorageMedium.HDD));
        store.add(10002L, new TabletMeta(10L, 20L, 30L, 40L, 200, TStorageMedium.SSD));

        Map<Long, TabletMeta> map = store.toMap();
        Assert.assertEquals(2, map.size());
        Assert.assertTrue(map.containsKey(10001L));
        Assert.assertTrue(map.containsKey(10002L));
        Assert.assertEquals(1L, map.get(10001L).getDbId());
        Assert.assertEquals(10L, map.get(10002L).getDbId());
    }

    @Test
    public void testClear() {
        store.add(10001L, new TabletMeta(1L, 2L, 3L, 4L, 100, TStorageMedium.HDD));
        store.add(10002L, new TabletMeta(10L, 20L, 30L, 40L, 200, TStorageMedium.SSD));
        Assert.assertEquals(2, store.size());

        store.clear();
        Assert.assertEquals(0, store.size());
        Assert.assertFalse(store.containsKey(10001L));
        Assert.assertFalse(store.containsKey(10002L));

        // can add again after clear
        store.add(10003L, new TabletMeta(100L, 200L, 300L, 400L, 300, TStorageMedium.S3));
        Assert.assertEquals(1, store.size());
        Assert.assertEquals(100L, store.getDbId(10003L));
    }

    @Test
    public void testAllStorageMediumValues() {
        store.add(1L, new TabletMeta(1L, 1L, 1L, 1L, 1, TStorageMedium.HDD));
        store.add(2L, new TabletMeta(2L, 2L, 2L, 2L, 2, TStorageMedium.SSD));
        store.add(3L, new TabletMeta(3L, 3L, 3L, 3L, 3, TStorageMedium.S3));
        store.add(4L, new TabletMeta(4L, 4L, 4L, 4L, 4, TStorageMedium.REMOTE_CACHE));

        Assert.assertEquals(TStorageMedium.HDD, store.getStorageMedium(1L));
        Assert.assertEquals(TStorageMedium.SSD, store.getStorageMedium(2L));
        Assert.assertEquals(TStorageMedium.S3, store.getStorageMedium(3L));
        Assert.assertEquals(TStorageMedium.REMOTE_CACHE, store.getStorageMedium(4L));
    }

    @Test
    public void testMultipleDeletesAndReuse() {
        store.add(1L, new TabletMeta(10L, 10L, 10L, 10L, 10, TStorageMedium.HDD));
        store.add(2L, new TabletMeta(20L, 20L, 20L, 20L, 20, TStorageMedium.SSD));
        store.add(3L, new TabletMeta(30L, 30L, 30L, 30L, 30, TStorageMedium.S3));

        // delete in different order
        store.remove(2L);
        store.remove(1L);
        Assert.assertEquals(1, store.size());

        // add new entries - should reuse freed slots
        store.add(4L, new TabletMeta(40L, 40L, 40L, 40L, 40, TStorageMedium.HDD));
        store.add(5L, new TabletMeta(50L, 50L, 50L, 50L, 50, TStorageMedium.SSD));
        Assert.assertEquals(3, store.size());

        // verify all data is correct
        Assert.assertEquals(30L, store.getDbId(3L));
        Assert.assertEquals(40L, store.getDbId(4L));
        Assert.assertEquals(50L, store.getDbId(5L));
    }
}

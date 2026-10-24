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

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Memory-compact storage for TabletMeta using Structure-of-Arrays layout.
 *
 * Instead of storing one TabletMeta object per tablet (each with a 16-byte Java object header),
 * this class stores each field in a parallel primitive array indexed by an internal slot.
 * A Long2IntOpenHashMap maps tabletId to the slot index.
 *
 * Deleted slots are reused via a free list embedded in the dbIds array.
 *
 * Thread safety: callers must hold appropriate locks (provided by TabletInvertedIndex).
 */
public class CompactTabletMetaStore {

    private static final int INITIAL_CAPACITY = 1024;
    private static final int ABSENT = -1;

    // tabletId -> slot index
    private Long2IntOpenHashMap tabletIdToSlot;

    // parallel arrays indexed by slot
    private long[] dbIds;
    private long[] tableIds;
    private long[] partitionIds;
    private long[] indexIds;
    private int[] oldSchemaHashes;
    private byte[] storageMediumOrdinals;

    // free list head; ABSENT means empty
    private int freeHead = ABSENT;
    // next never-used slot index
    private int highWaterMark = 0;
    // number of live entries
    private int size = 0;
    // allocated length of arrays
    private int capacity;

    private static final TStorageMedium[] MEDIUM_VALUES = TStorageMedium.values();

    public CompactTabletMetaStore() {
        this(INITIAL_CAPACITY);
    }

    public CompactTabletMetaStore(int initialCapacity) {
        this.capacity = Math.max(initialCapacity, 4);
        this.tabletIdToSlot = new Long2IntOpenHashMap(this.capacity);
        this.tabletIdToSlot.defaultReturnValue(ABSENT);
        this.dbIds = new long[this.capacity];
        this.tableIds = new long[this.capacity];
        this.partitionIds = new long[this.capacity];
        this.indexIds = new long[this.capacity];
        this.oldSchemaHashes = new int[this.capacity];
        this.storageMediumOrdinals = new byte[this.capacity];
    }

    public boolean add(long tabletId, TabletMeta meta) {
        if (tabletIdToSlot.containsKey(tabletId)) {
            return false;
        }
        int slot = allocateSlot();
        tabletIdToSlot.put(tabletId, slot);
        dbIds[slot] = meta.getDbId();
        tableIds[slot] = meta.getTableId();
        partitionIds[slot] = meta.getPartitionId();
        indexIds[slot] = meta.getIndexId();
        oldSchemaHashes[slot] = meta.getOldSchemaHash();
        storageMediumOrdinals[slot] = (byte) meta.getStorageMedium().getValue();
        size++;
        return true;
    }

    public void remove(long tabletId) {
        int slot = tabletIdToSlot.remove(tabletId);
        if (slot == ABSENT) {
            return;
        }
        freeSlot(slot);
        size--;
    }

    public boolean containsKey(long tabletId) {
        return tabletIdToSlot.containsKey(tabletId);
    }

    public long getDbId(long tabletId) {
        int slot = tabletIdToSlot.get(tabletId);
        return slot == ABSENT ? TabletInvertedIndex.NOT_EXIST_VALUE : dbIds[slot];
    }

    public long getTableId(long tabletId) {
        int slot = tabletIdToSlot.get(tabletId);
        return slot == ABSENT ? TabletInvertedIndex.NOT_EXIST_VALUE : tableIds[slot];
    }

    public long getPartitionId(long tabletId) {
        int slot = tabletIdToSlot.get(tabletId);
        return slot == ABSENT ? TabletInvertedIndex.NOT_EXIST_VALUE : partitionIds[slot];
    }

    public long getIndexId(long tabletId) {
        int slot = tabletIdToSlot.get(tabletId);
        return slot == ABSENT ? TabletInvertedIndex.NOT_EXIST_VALUE : indexIds[slot];
    }

    public int getOldSchemaHash(long tabletId) {
        int slot = tabletIdToSlot.get(tabletId);
        return slot == ABSENT ? TabletInvertedIndex.NOT_EXIST_VALUE : oldSchemaHashes[slot];
    }

    public TStorageMedium getStorageMedium(long tabletId) {
        int slot = tabletIdToSlot.get(tabletId);
        if (slot == ABSENT) {
            return null;
        }
        return MEDIUM_VALUES[storageMediumOrdinals[slot]];
    }

    public void setStorageMedium(long tabletId, TStorageMedium medium) {
        int slot = tabletIdToSlot.get(tabletId);
        if (slot != ABSENT) {
            storageMediumOrdinals[slot] = (byte) medium.getValue();
        }
    }

    /**
     * Construct a TabletMeta on demand for backward compatibility.
     * Returns null if the tabletId is not present.
     */
    public TabletMeta getTabletMeta(long tabletId) {
        int slot = tabletIdToSlot.get(tabletId);
        if (slot == ABSENT) {
            return null;
        }
        return new TabletMeta(
                dbIds[slot],
                tableIds[slot],
                partitionIds[slot],
                indexIds[slot],
                oldSchemaHashes[slot],
                MEDIUM_VALUES[storageMediumOrdinals[slot]]);
    }

    /**
     * Build a full Map for backward compatibility (test-only usage).
     */
    public Map<Long, TabletMeta> toMap() {
        Map<Long, TabletMeta> map = new HashMap<>(size * 4 / 3 + 1);
        for (Long2IntOpenHashMap.Entry entry : tabletIdToSlot.long2IntEntrySet()) {
            long tabletId = entry.getLongKey();
            int slot = entry.getIntValue();
            map.put(tabletId, new TabletMeta(
                    dbIds[slot],
                    tableIds[slot],
                    partitionIds[slot],
                    indexIds[slot],
                    oldSchemaHashes[slot],
                    MEDIUM_VALUES[storageMediumOrdinals[slot]]));
        }
        return map;
    }

    public int size() {
        return size;
    }

    public void clear() {
        tabletIdToSlot.clear();
        freeHead = ABSENT;
        highWaterMark = 0;
        size = 0;
    }

    private int allocateSlot() {
        if (freeHead != ABSENT) {
            int slot = freeHead;
            freeHead = (int) dbIds[freeHead];
            return slot;
        }
        if (highWaterMark == capacity) {
            grow();
        }
        return highWaterMark++;
    }

    private void freeSlot(int slot) {
        dbIds[slot] = freeHead;
        freeHead = slot;
    }

    private void grow() {
        int newCapacity = capacity * 2;
        dbIds = Arrays.copyOf(dbIds, newCapacity);
        tableIds = Arrays.copyOf(tableIds, newCapacity);
        partitionIds = Arrays.copyOf(partitionIds, newCapacity);
        indexIds = Arrays.copyOf(indexIds, newCapacity);
        oldSchemaHashes = Arrays.copyOf(oldSchemaHashes, newCapacity);
        storageMediumOrdinals = Arrays.copyOf(storageMediumOrdinals, newCapacity);
        capacity = newCapacity;
    }
}

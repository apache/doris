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

import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.ColocatePersistInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * maintain the colocate table related indexes and meta
 */
public class ColocateTableIndex implements Writable {
    private transient ReentrantReadWriteLock lock;

    // group_id -> table_ids
    private Multimap<Long, Long> group2Tables;
    // table_id -> group_id
    private Map<Long, Long> table2Group;
    // group_id -> db_id
    private Map<Long, Long> group2DB;
    // group_id -> bucketSeq -> backend ids
    private Map<Long, List<List<Long>>> group2BackendsPerBucketSeq;
    // the colocate group is balancing
    private Set<Long> balancingGroups;

    public ColocateTableIndex() {
        lock = new ReentrantReadWriteLock();

        group2Tables = ArrayListMultimap.create();
        table2Group = Maps.newHashMap();
        group2DB = Maps.newHashMap();
        group2BackendsPerBucketSeq = Maps.newHashMap();
        balancingGroups = new CopyOnWriteArraySet<Long>();
    }

    private final void readLock() {
        this.lock.readLock().lock();
    }

    private final void readUnlock() {
        this.lock.readLock().unlock();
    }

    private final void writeLock() {
        this.lock.writeLock().lock();
    }

    private final void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public void addTableToGroup(long dbId, long tableId, long groupId) {
        writeLock();
        try {
            group2Tables.put(groupId, tableId);
            group2DB.put(groupId, dbId);
            table2Group.put(tableId, groupId);
        } finally {
            writeUnlock();
        }
    }

    public void addBackendsPerBucketSeq(long groupId, List<List<Long>> backendsPerBucketSeq) {
        writeLock();
        try {
            group2BackendsPerBucketSeq.put(groupId, backendsPerBucketSeq);
        } finally {
            writeUnlock();
        }
    }

    public void markGroupBalancing(long groupId) {
        balancingGroups.add(groupId);
    }

    public void markGroupStable(long groupId) {
        balancingGroups.remove(groupId);
    }

    public boolean removeTable(long tableId) {
        long groupId;
        readLock();
        try {
            if (!table2Group.containsKey(tableId)) {
                return false;
            }
            groupId = table2Group.get(tableId);
        } finally {
            readUnlock();
        }

        removeTableFromGroup(tableId, groupId);
        return true;
    }

    private void removeTableFromGroup(long tableId, long groupId) {
        writeLock();
        try {
            if (groupId == tableId) {
                //for parent table
                for(Long id: group2Tables.get(groupId)) {
                    table2Group.remove(id);
                }

                group2Tables.removeAll(groupId);
                group2BackendsPerBucketSeq.remove(groupId);
                group2DB.remove(groupId);
                balancingGroups.remove(groupId);
            } else {
                //for child table
                group2Tables.remove(groupId, tableId);
                table2Group.remove(tableId);
            }

        } finally {
            writeUnlock();
        }
    }

    public boolean isGroupBalancing(long groupId) {
        return balancingGroups.contains(groupId);
    }

    public boolean isColocateParentTable(long tableId) {
        readLock();
        try {
            return group2Tables.containsKey(tableId);
        } finally {
            readUnlock();
        }

    }

    public boolean isColocateChildTable(long tableId) {
        readLock();
        try {
            return table2Group.containsKey(tableId) && !group2Tables.containsKey(tableId);
        } finally {
            readUnlock();
        }

    }

    public boolean isColocateTable(long tableId) {
        readLock();
        try {
            return table2Group.containsKey(tableId);
        } finally {
            readUnlock();
        }
    }

    public boolean isGroupExist(long groupId) {
        readLock();
        try {
            return group2DB.containsKey(groupId);
        } finally {
            readUnlock();
        }
    }

    public boolean isSameGroup(long table1, long table2) {
        readLock();
        try {
            if (table2Group.containsKey(table1) && table2Group.containsKey(table2)) {
                return table2Group.get(table1).equals(table2Group.get(table2));
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    public Multimap<Long, Long> getGroup2Tables() {
        readLock();
        try {
            return group2Tables;
        } finally {
            readUnlock();
        }
    }

    public Map<Long, Long> getTable2Group() {
        readLock();
        try {
            return table2Group;
        } finally {
            readUnlock();
        }
    }

    public Map<Long, Long> getGroup2DB() {
        readLock();
        try {
            return group2DB;
        } finally {
            readUnlock();
        }
    }

    public Map<Long, List<List<Long>>> getGroup2BackendsPerBucketSeq() {
        readLock();
        try {
            return group2BackendsPerBucketSeq;
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getBalancingGroupIds() {
        return balancingGroups;
    }

    public long getGroup(long tableId) {
        readLock();
        try {
            Preconditions.checkState(table2Group.containsKey(tableId));
            return table2Group.get(tableId);
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getAllGroupIds() {
        readLock();
        try {
            return group2Tables.keySet();
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getBackendsByGroup(long groupId) {
        readLock();
        try {
            Set<Long> allBackends = new HashSet<>();
            List<List<Long>> BackendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            for (List<Long> bes : BackendsPerBucketSeq) {
                allBackends.addAll(bes);
            }
            return allBackends;
        } finally {
            readUnlock();
        }
    }

    public Long getDB(long group) {
        readLock();
        try {
            Preconditions.checkState(group2DB.containsKey(group));
            return group2DB.get(group);
        } finally {
            readUnlock();
        }
    }

    public List<Long> getAllTableIds(long groupId) {
        readLock();
        try {
            Preconditions.checkState(group2Tables.containsKey(groupId));
            return new ArrayList(group2Tables.get(groupId));
        } finally {
            readUnlock();
        }
    }

    public List<List<Long>> getBackendsPerBucketSeq(long groupId) {
        readLock();
        try {
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            if (backendsPerBucketSeq == null) {
                return new ArrayList<List<Long>>();
            }
            return backendsPerBucketSeq;
        } finally {
            readUnlock();
        }
    }

    public void replayAddTableToGroup(ColocatePersistInfo info) {
        addTableToGroup(info.getDbId(), info.getTableId(), info.getGroupId());
        //for parent table
        if (info.getBackendsPerBucketSeq().size() > 0) {
            addBackendsPerBucketSeq(info.getGroupId(), info.getBackendsPerBucketSeq());
        }
    }

    public void replayAddBackendsPerBucketSeq(ColocatePersistInfo info) {
        addBackendsPerBucketSeq(info.getGroupId(), info.getBackendsPerBucketSeq());
    }

    public void replayMarkGroupBalancing(ColocatePersistInfo info) {
        markGroupBalancing(info.getGroupId());
    }

    public void replayMarkGroupStable(ColocatePersistInfo info) {
        markGroupStable(info.getGroupId());
    }

    public void replayRemoveTable(ColocatePersistInfo info) {
        removeTable(info.getTableId());
    }

    // only for test
    public void clear() {
        writeLock();
        try {
            group2Tables.clear();
            table2Group.clear();
            group2DB.clear();
            group2BackendsPerBucketSeq.clear();
            balancingGroups.clear();
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int size = group2Tables.asMap().size();
        out.writeInt(size);
        for (Map.Entry<Long, Collection<Long>> entry : group2Tables.asMap().entrySet()) {
            out.writeLong(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Long tableId : entry.getValue()) {
                out.writeLong(tableId);
            }
        }

        size = table2Group.size();
        out.writeInt(size);
        for (Map.Entry<Long, Long> entry : table2Group.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeLong(entry.getValue());
        }

        size = group2DB.size();
        out.writeInt(size);
        for (Map.Entry<Long, Long> entry : group2DB.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeLong(entry.getValue());
        }

        size = group2BackendsPerBucketSeq.size();
        out.writeInt(size);
        for (Map.Entry<Long, List<List<Long>>> entry : group2BackendsPerBucketSeq.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (List<Long> bucket2BEs : entry.getValue()) {
                out.writeInt(bucket2BEs.size());
                for (Long be : bucket2BEs) {
                    out.writeLong(be);
                }
            }
        }

        size = balancingGroups.size();
        out.writeInt(size);
        for (Long group : balancingGroups) {
            out.writeLong(group);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long group = in.readLong();
            int tableSize = in.readInt();
            List<Long> tables = new ArrayList<>();
            for (int j = 0; j < tableSize; j++) {
                tables.add(in.readLong());
            }
            group2Tables.putAll(group, tables);
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long table = in.readLong();
            long group = in.readLong();
            table2Group.put(table, group);
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long group = in.readLong();
            long db = in.readLong();
            group2DB.put(group, db);
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long group = in.readLong();
            List<List<Long>> bucketBeLists = new ArrayList<>();
            int bucketBeListsSize = in.readInt();
            for (int j = 0; j < bucketBeListsSize; j++) {
                int beListSize = in.readInt();
                List<Long> beLists = new ArrayList<>();
                for (int k = 0; k < beListSize; k++) {
                    beLists.add(in.readLong());
                }
                bucketBeLists.add(beLists);
            }
            group2BackendsPerBucketSeq.put(group, bucketBeLists);
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long group = in.readLong();
            balancingGroups.add(group);
        }
    }
}

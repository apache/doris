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

import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.ColocatePersistInfo;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * maintain the colocate table related indexes and meta
 */
public class ColocateTableIndex implements Writable {
    private static final Logger LOG = LogManager.getLogger(ColocateTableIndex.class);

    public static class GroupId implements Writable {
        public Long dbId;
        public Long grpId;

        private GroupId() {
        }

        public GroupId(long dbId, long grpId) {
            this.dbId = dbId;
            this.grpId = grpId;
        }

        public static GroupId read(DataInput in) throws IOException {
            GroupId groupId = new GroupId();
            groupId.readFields(in);
            return groupId;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(dbId);
            out.writeLong(grpId);
        }

        public void readFields(DataInput in) throws IOException {
            dbId = in.readLong();
            grpId = in.readLong();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof GroupId)) {
                return false;
            }
            GroupId other = (GroupId) obj;
            return dbId.equals(other.dbId) && grpId.equals(other.grpId);
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + dbId.hashCode();
            result = 31 * result + grpId.hashCode();
            return result;
        }
        
        @Override
        public String toString() {
            return dbId + "." + grpId;
        }
    }

    // group_name -> group_id
    private Map<String, GroupId> groupName2Id = Maps.newHashMap();
    // group_id -> table_ids
    private Multimap<GroupId, Long> group2Tables = ArrayListMultimap.create();
    // table_id -> group_id
    private Map<Long, GroupId> table2Group = Maps.newHashMap();
    // group id -> group schema
    private Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
    // group_id -> bucketSeq -> backend ids
    private Map<GroupId, List<List<Long>>> group2BackendsPerBucketSeq = Maps.newHashMap();
    // the colocate group is unstable
    private Set<GroupId> unstableGroups = Sets.newHashSet();

    private transient ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ColocateTableIndex() {

    }

    private void readLock() {
        this.lock.readLock().lock();
    }

    private void readUnlock() {
        this.lock.readLock().unlock();
    }

    private void writeLock() {
        this.lock.writeLock().lock();
    }

    private void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    // NOTICE: call 'addTableToGroup()' will not modify 'group2BackendsPerBucketSeq'
    // 'group2BackendsPerBucketSeq' need to be set manually before or after, if necessary.
    public GroupId addTableToGroup(long dbId, OlapTable tbl, String groupName, GroupId assignedGroupId) {
        writeLock();
        try {
            GroupId groupId = null;
            String fullGroupName = dbId + "_" + groupName;
            if (groupName2Id.containsKey(fullGroupName)) {
                groupId = groupName2Id.get(fullGroupName);
            } else {
                if (assignedGroupId != null) {
                    // use the given group id, eg, in replay process
                    groupId = assignedGroupId;
                } else {
                    // generate a new one
                    groupId = new GroupId(dbId, Catalog.getCurrentCatalog().getNextId());
                }
                HashDistributionInfo distributionInfo = (HashDistributionInfo) tbl.getDefaultDistributionInfo();
                ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId,
                        distributionInfo.getDistributionColumns(), distributionInfo.getBucketNum(),
                        tbl.getDefaultReplicationNum());
                groupName2Id.put(fullGroupName, groupId);
                group2Schema.put(groupId, groupSchema);
            }
            group2Tables.put(groupId, tbl.getId());
            table2Group.put(tbl.getId(), groupId);
            return groupId;
        } finally {
            writeUnlock();
        }
    }

    public void addBackendsPerBucketSeq(GroupId groupId, List<List<Long>> backendsPerBucketSeq) {
        writeLock();
        try {
            group2BackendsPerBucketSeq.put(groupId, backendsPerBucketSeq);
        } finally {
            writeUnlock();
        }
    }

    public void markGroupUnstable(GroupId groupId, boolean needEditLog) {
        writeLock();
        try {
            if (!group2Tables.containsKey(groupId)) {
                return;
            }
            if (unstableGroups.add(groupId)) {
                if (needEditLog) {
                    ColocatePersistInfo info = ColocatePersistInfo.createForMarkUnstable(groupId);
                    Catalog.getCurrentCatalog().getEditLog().logColocateMarkUnstable(info);
                }
                LOG.info("mark group {} as unstable", groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public void markGroupStable(GroupId groupId, boolean needEditLog) {
        writeLock();
        try {
            if (!group2Tables.containsKey(groupId)) {
                return;
            }
            if (unstableGroups.remove(groupId)) {
                if (needEditLog) {
                    ColocatePersistInfo info = ColocatePersistInfo.createForMarkStable(groupId);
                    Catalog.getCurrentCatalog().getEditLog().logColocateMarkStable(info);
                }
                LOG.info("mark group {} as stable", groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public boolean removeTable(long tableId) {
        writeLock();
        try {
            if (!table2Group.containsKey(tableId)) {
                return false;
            }

            GroupId groupId = table2Group.remove(tableId);
            group2Tables.remove(groupId, tableId);
            if (!group2Tables.containsKey(groupId)) {
                // all tables of this group are removed, remove the group
                group2BackendsPerBucketSeq.remove(groupId);
                group2Schema.remove(groupId);
                unstableGroups.remove(groupId);
                String fullGroupName = null;
                for (Map.Entry<String, GroupId> entry : groupName2Id.entrySet()) {
                    if (entry.getValue().equals(groupId)) {
                        fullGroupName = entry.getKey();
                        break;
                    }
                }
                if (fullGroupName != null) {
                    groupName2Id.remove(fullGroupName);
                }
            }
        } finally {
            writeUnlock();
        }

        return true;
    }

    public boolean isGroupUnstable(GroupId groupId) {
        readLock();
        try {
            return unstableGroups.contains(groupId);
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

    public boolean isGroupExist(GroupId groupId) {
        readLock();
        try {
            return group2Schema.containsKey(groupId);
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

    public Set<GroupId> getUnstableGroupIds() {
        readLock();
        try {
            return Sets.newHashSet(unstableGroups);
        } finally {
            readUnlock();
        }
    }

    public GroupId getGroup(long tableId) {
        readLock();
        try {
            Preconditions.checkState(table2Group.containsKey(tableId));
            return table2Group.get(tableId);
        } finally {
            readUnlock();
        }
    }

    public Set<GroupId> getAllGroupIds() {
        readLock();
        try {
            return group2Tables.keySet();
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getBackendsByGroup(GroupId groupId) {
        readLock();
        try {
            Set<Long> allBackends = new HashSet<>();
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            // if create colocate table with empty partition or create colocate table
            // with dynamic_partition will cause backendsPerBucketSeq == null
            if (backendsPerBucketSeq != null) {
                for (List<Long> bes : backendsPerBucketSeq) {
                    allBackends.addAll(bes);
                }
            }
            return allBackends;
        } finally {
            readUnlock();
        }
    }

    public List<Long> getAllTableIds(GroupId groupId) {
        readLock();
        try {
            if (!group2Tables.containsKey(groupId)) {
                return Lists.newArrayList();
            }
            return Lists.newArrayList(group2Tables.get(groupId));
        } finally {
            readUnlock();
        }
    }

    public List<List<Long>> getBackendsPerBucketSeq(GroupId groupId) {
        readLock();
        try {
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            if (backendsPerBucketSeq == null) {
                return Lists.newArrayList();
            }
            return backendsPerBucketSeq;
        } finally {
            readUnlock();
        }
    }

    public List<Set<Long>> getBackendsPerBucketSeqSet(GroupId groupId) {
        readLock();
        try {
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            if (backendsPerBucketSeq == null) {
                return Lists.newArrayList();
            }
            List<Set<Long>> sets = Lists.newArrayList();
            for (List<Long> backends : backendsPerBucketSeq) {
                sets.add(Sets.newHashSet(backends));
            }
            return sets;
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getTabletBackendsByGroup(GroupId groupId, int tabletOrderIdx) {
        readLock();
        try {
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            if (backendsPerBucketSeq == null) {
                return Sets.newHashSet();
            }
            if (tabletOrderIdx >= backendsPerBucketSeq.size()) {
                return Sets.newHashSet();
            }

            return Sets.newHashSet(backendsPerBucketSeq.get(tabletOrderIdx));
        } finally {
            readUnlock();
        }
    }

    public ColocateGroupSchema getGroupSchema(String fullGroupName) {
        readLock();
        try {
            if (!groupName2Id.containsKey(fullGroupName)) {
                return null;
            }
            return group2Schema.get(groupName2Id.get(fullGroupName));
        } finally {
            readUnlock();
        }
    }

    public ColocateGroupSchema getGroupSchema(GroupId groupId) {
        readLock();
        try {
            return group2Schema.get(groupId);
        } finally {
            readUnlock();
        }
    }

    public long getTableIdByGroup(String fullGroupName) {
        readLock();
        try {
            if (groupName2Id.containsKey(fullGroupName)) {
                GroupId groupId = groupName2Id.get(fullGroupName);
                Optional<Long> tblId = group2Tables.get(groupId).stream().findFirst();
                return tblId.isPresent() ? tblId.get() : -1;
            }
        } finally {
            readUnlock();
        }
        return -1;
    }

    public GroupId changeGroup(long dbId, OlapTable tbl, String oldGroup, String newGroup, GroupId assignedGroupId) {
        writeLock();
        try {
            if (!Strings.isNullOrEmpty(oldGroup)) {
                // remove from old group
                removeTable(tbl.getId());
            }
            return addTableToGroup(dbId, tbl, newGroup, assignedGroupId);
        } finally {
            writeUnlock();
        }
    }

    public void replayAddTableToGroup(ColocatePersistInfo info) {
        Database db = Catalog.getCurrentCatalog().getDb(info.getGroupId().dbId);
        Preconditions.checkNotNull(db);
        OlapTable tbl = (OlapTable)db.getTable(info.getTableId());
        Preconditions.checkNotNull(tbl);
        
        writeLock();
        try {
            if (!group2BackendsPerBucketSeq.containsKey(info.getGroupId())) {
                group2BackendsPerBucketSeq.put(info.getGroupId(), info.getBackendsPerBucketSeq());
            }

            addTableToGroup(info.getGroupId().dbId, tbl, tbl.getColocateGroup(), info.getGroupId());
        } finally {
            writeUnlock();
        }
    }

    public void replayAddBackendsPerBucketSeq(ColocatePersistInfo info) {
        addBackendsPerBucketSeq(info.getGroupId(), info.getBackendsPerBucketSeq());
    }

    public void replayMarkGroupUnstable(ColocatePersistInfo info) {
        markGroupUnstable(info.getGroupId(), false);
    }

    public void replayMarkGroupStable(ColocatePersistInfo info) {
        markGroupStable(info.getGroupId(), false);
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
            group2BackendsPerBucketSeq.clear();
            group2Schema.clear();
            unstableGroups.clear();
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> getInfos() {
        List<List<String>> infos = Lists.newArrayList();
        readLock();
        try {
            for (Map.Entry<String, GroupId> entry : groupName2Id.entrySet()) {
                List<String> info = Lists.newArrayList();
                GroupId groupId = entry.getValue();
                info.add(groupId.toString());
                info.add(entry.getKey());
                info.add(Joiner.on(", ").join(group2Tables.get(groupId)));
                ColocateGroupSchema groupSchema = group2Schema.get(groupId);
                info.add(String.valueOf(groupSchema.getBucketsNum()));
                info.add(String.valueOf(groupSchema.getReplicationNum()));
                List<String> cols = groupSchema.getDistributionColTypes().stream().map(
                        e -> e.toSql()).collect(Collectors.toList());
                info.add(Joiner.on(", ").join(cols));
                info.add(String.valueOf(!unstableGroups.contains(groupId)));
                infos.add(info);
            }
        } finally {
            readUnlock();
        }
        return infos;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int size = groupName2Id.size();
        out.writeInt(size);
        for (Map.Entry<String, GroupId> entry : groupName2Id.entrySet()) {
            Text.writeString(out, entry.getKey()); // group name
            entry.getValue().write(out); // group id
            Collection<Long> tableIds = group2Tables.get(entry.getValue());
            out.writeInt(tableIds.size());
            for (Long tblId : tableIds) {
                out.writeLong(tblId); // table ids
            }
            ColocateGroupSchema groupSchema = group2Schema.get(entry.getValue());
            groupSchema.write(out); // group schema

            // backend seq
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(entry.getValue());
            out.writeInt(backendsPerBucketSeq.size());
            for (List<Long> bucket2BEs : backendsPerBucketSeq) {
                out.writeInt(bucket2BEs.size());
                for (Long be : bucket2BEs) {
                    out.writeLong(be);
                }
            }
        }

        size = unstableGroups.size();
        out.writeInt(size);
        for (GroupId groupId : unstableGroups) {
            groupId.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_55) {
            Multimap<Long, Long> tmpGroup2Tables = ArrayListMultimap.create();
            Map<Long, Long> tmpTable2Group = Maps.newHashMap();
            Map<Long, Long> tmpGroup2Db = Maps.newHashMap();
            Map<Long, List<List<Long>>> tmpGroup2BackendsPerBucketSeq = Maps.newHashMap();
            Set<Long> tmpBalancingGroups = Sets.newHashSet();

            for (int i = 0; i < size; i++) {
                long group = in.readLong();
                int tableSize = in.readInt();
                List<Long> tables = new ArrayList<>();
                for (int j = 0; j < tableSize; j++) {
                    tables.add(in.readLong());
                }
                tmpGroup2Tables.putAll(group, tables);
            }

            size = in.readInt();
            for (int i = 0; i < size; i++) {
                long table = in.readLong();
                long group = in.readLong();
                tmpTable2Group.put(table, group);
            }

            size = in.readInt();
            for (int i = 0; i < size; i++) {
                long group = in.readLong();
                long db = in.readLong();
                tmpGroup2Db.put(group, db);
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
                tmpGroup2BackendsPerBucketSeq.put(group, bucketBeLists);
            }

            size = in.readInt();
            for (int i = 0; i < size; i++) {
                long group = in.readLong();
                tmpBalancingGroups.add(group);
            }

            convertedToNewMembers(tmpGroup2Tables, tmpTable2Group, tmpGroup2Db, tmpGroup2BackendsPerBucketSeq,
                    tmpBalancingGroups);
        } else {
            for (int i = 0; i < size; i++) {
                String fullGrpName = Text.readString(in);
                GroupId grpId = GroupId.read(in);
                groupName2Id.put(fullGrpName, grpId);
                int tableSize = in.readInt();
                for (int j = 0; j < tableSize; j++) {
                    long tblId = in.readLong();
                    group2Tables.put(grpId, tblId);
                    table2Group.put(tblId, grpId);
                }
                ColocateGroupSchema groupSchema = ColocateGroupSchema.read(in);
                group2Schema.put(grpId, groupSchema);

                List<List<Long>> backendsPerBucketSeq = Lists.newArrayList();
                int beSize = in.readInt();
                for (int j = 0; j < beSize; j++) {
                    int seqSize = in.readInt();
                    List<Long> seq = Lists.newArrayList();
                    for (int k = 0; k < seqSize; k++) {
                        long beId = in.readLong();
                        seq.add(beId);
                    }
                    backendsPerBucketSeq.add(seq);
                }
                group2BackendsPerBucketSeq.put(grpId, backendsPerBucketSeq);
            }

            size = in.readInt();
            for (int i = 0; i < size; i++) {
                unstableGroups.add(GroupId.read(in));
            }
        }
    }

    private void convertedToNewMembers(Multimap<Long, Long> tmpGroup2Tables, Map<Long, Long> tmpTable2Group,
            Map<Long, Long> tmpGroup2Db, Map<Long, List<List<Long>>> tmpGroup2BackendsPerBucketSeq,
            Set<Long> tmpBalancingGroups) {

        LOG.debug("debug: tmpGroup2Tables {}", tmpGroup2Tables);
        LOG.debug("debug: tmpTable2Group {}", tmpTable2Group);
        LOG.debug("debug: tmpGroup2Db {}", tmpGroup2Db);
        LOG.debug("debug: tmpGroup2BackendsPerBucketSeq {}", tmpGroup2BackendsPerBucketSeq);
        LOG.debug("debug: tmpBalancingGroups {}", tmpBalancingGroups);

        for (Map.Entry<Long, Long> entry : tmpGroup2Db.entrySet()) {
            GroupId groupId = new GroupId(entry.getValue(), entry.getKey());
            Database db = Catalog.getCurrentCatalog().getDb(groupId.dbId);
            if (db == null) {
                continue;
            }
            Collection<Long> tableIds = tmpGroup2Tables.get(groupId.grpId);

            for (Long tblId : tableIds) {
                OlapTable tbl = (OlapTable) db.getTable(tblId);
                if (tbl == null) {
                    continue;
                }
                tbl.readLock();
                try {
                    if (tblId.equals(groupId.grpId)) {
                        // this is a parent table, use its name as group name
                        groupName2Id.put(groupId.dbId + "_" + tbl.getName(), groupId);

                        ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId,
                                ((HashDistributionInfo)tbl.getDefaultDistributionInfo()).getDistributionColumns(),
                                tbl.getDefaultDistributionInfo().getBucketNum(),
                                tbl.getPartitionInfo().idToReplicationNum.values().stream().findFirst().get());
                        group2Schema.put(groupId, groupSchema);
                        group2BackendsPerBucketSeq.put(groupId, tmpGroup2BackendsPerBucketSeq.get(groupId.grpId));
                    }
                } finally {
                    tbl.readUnlock();
                }

                group2Tables.put(groupId, tblId);
                table2Group.put(tblId, groupId);
            }
        }
    }

    public void setBackendsSetByIdxForGroup(GroupId groupId, int tabletOrderIdx, Set<Long> newBackends) {
        writeLock();
        try {
            List<List<Long>> backends = group2BackendsPerBucketSeq.get(groupId);
            if (backends == null) {
                return;
            }
            Preconditions.checkState(tabletOrderIdx < backends.size(), tabletOrderIdx + " vs. " + backends.size());
            backends.set(tabletOrderIdx, Lists.newArrayList(newBackends));
            ColocatePersistInfo info = ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backends);
            Catalog.getCurrentCatalog().getEditLog().logColocateBackendsPerBucketSeq(info);
        } finally {
            writeUnlock();
        }
    }
}

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

import org.apache.doris.analysis.AlterColocateGroupStmt;
import org.apache.doris.clone.ColocateTableCheckerAndBalancer;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * maintain the colocation table related indexes and meta
 */
public class ColocateTableIndex implements Writable {
    private static final Logger LOG = LogManager.getLogger(ColocateTableIndex.class);

    public static class GroupId implements Writable, GsonPostProcessable {
        public static final String GLOBAL_COLOCATE_PREFIX = "__global__";

        @SerializedName(value = "dbId")
        public Long dbId;
        @SerializedName(value = "grpId")
        public Long grpId;
        // only available when dbId = 0
        // because for global colocate table, the dbId is 0, so we do not know which db the table belongs to,
        // so we use tblId2DbId to record the dbId of each table
        @SerializedName(value = "tblId2DbId")
        private Map<Long, Long> tblId2DbId = Maps.newHashMap();

        private GroupId() {
        }

        public GroupId(long dbId, long grpId) {
            this.dbId = dbId;
            this.grpId = grpId;
        }

        public void addTblId2DbId(long tblId, long dbId) {
            Preconditions.checkState(this.dbId == 0);
            tblId2DbId.put(tblId, dbId);
        }

        public void removeTblId2DbId(long tblId) {
            tblId2DbId.remove(tblId);
        }

        public long getDbIdByTblId(long tblId) {
            return tblId2DbId.get(tblId);
        }

        public int getTblId2DbIdSize() {
            return tblId2DbId.size();
        }

        public static GroupId read(DataInput in) throws IOException {
            if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_105) {
                GroupId groupId = new GroupId();
                groupId.readFields(in);
                return groupId;
            } else {
                String json = Text.readString(in);
                return GsonUtils.GSON.fromJson(json, GroupId.class);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        @Deprecated
        private void readFields(DataInput in) throws IOException {
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
        public void gsonPostProcess() throws IOException {
            if (tblId2DbId == null) {
                tblId2DbId = Maps.newHashMap();
            }
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

        public static String getFullGroupName(long dbId, String colocateGroup) {
            if (colocateGroup.startsWith(GLOBAL_COLOCATE_PREFIX)) {
                return colocateGroup;
            } else {
                return dbId + "_" + colocateGroup;
            }
        }

        public static boolean isGlobalGroupName(String groupName) {
            return groupName.startsWith(GLOBAL_COLOCATE_PREFIX);
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
    private Table<GroupId, Tag, List<List<Long>>> group2BackendsPerBucketSeq = HashBasedTable.create();
    // the colocate group is unstable
    private Set<GroupId> unstableGroups = Sets.newHashSet();
    // save some error msg of the group for show. no need to persist
    private Map<GroupId, String> group2ErrMsgs = Maps.newHashMap();

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
    public GroupId addTableToGroup(long dbId, OlapTable tbl, String fullGroupName, GroupId assignedGroupId) {
        writeLock();
        try {
            GroupId groupId = null;
            if (groupName2Id.containsKey(fullGroupName)) {
                groupId = groupName2Id.get(fullGroupName);
            } else {
                if (assignedGroupId != null) {
                    // use the given group id, eg, in replay process
                    groupId = assignedGroupId;
                } else {
                    // generate a new one
                    if (GroupId.isGlobalGroupName(fullGroupName)) {
                        groupId = new GroupId(0, Env.getCurrentEnv().getNextId());
                    } else {
                        groupId = new GroupId(dbId, Env.getCurrentEnv().getNextId());
                    }
                }
                HashDistributionInfo distributionInfo = (HashDistributionInfo) tbl.getDefaultDistributionInfo();
                ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId,
                        distributionInfo.getDistributionColumns(), distributionInfo.getBucketNum(),
                        tbl.getDefaultReplicaAllocation());
                groupName2Id.put(fullGroupName, groupId);
                group2Schema.put(groupId, groupSchema);
                group2ErrMsgs.put(groupId, "");
            }
            // for global colocate table, dbId is 0, and we need to save the real dbId of the table
            if (groupId.dbId == 0) {
                groupId.addTblId2DbId(tbl.getId(), dbId);
            }
            group2Tables.put(groupId, tbl.getId());
            table2Group.put(tbl.getId(), groupId);
            return groupId;
        } finally {
            writeUnlock();
        }
    }

    public void addBackendsPerBucketSeq(GroupId groupId, Map<Tag, List<List<Long>>> backendsPerBucketSeq) {
        writeLock();
        try {
            for (Map.Entry<Tag, List<List<Long>>> entry : backendsPerBucketSeq.entrySet()) {
                group2BackendsPerBucketSeq.put(groupId, entry.getKey(), entry.getValue());
            }
        } finally {
            writeUnlock();
        }
    }

    public void setBackendsPerBucketSeq(GroupId groupId, Map<Tag, List<List<Long>>> backendsPerBucketSeq) {
        writeLock();
        try {
            Map<Tag, List<List<Long>>> backendsPerBucketSeqMap = group2BackendsPerBucketSeq.row(groupId);
            if (backendsPerBucketSeqMap != null) {
                backendsPerBucketSeqMap.clear();
            }
            for (Map.Entry<Tag, List<List<Long>>> entry : backendsPerBucketSeq.entrySet()) {
                group2BackendsPerBucketSeq.put(groupId, entry.getKey(), entry.getValue());
            }
        } finally {
            writeUnlock();
        }
    }

    public boolean addBackendsPerBucketSeqByTag(GroupId groupId, Tag tag, List<List<Long>> backendsPerBucketSeq,
            ReplicaAllocation originReplicaAlloc) {
        writeLock();
        try {
            ColocateGroupSchema groupSchema = group2Schema.get(groupId);
            // replica allocation has outdate
            if (groupSchema != null && !originReplicaAlloc.equals(groupSchema.getReplicaAlloc())) {
                LOG.info("replica allocation has outdate for group {}, old replica alloc {}, new replica alloc {}",
                        groupId, originReplicaAlloc.getAllocMap(), groupSchema.getReplicaAlloc());
                return false;
            }
            group2BackendsPerBucketSeq.put(groupId, tag, backendsPerBucketSeq);
            return true;
        } finally {
            writeUnlock();
        }
    }

    public void markGroupUnstable(GroupId groupId, String reason, boolean needEditLog) {
        writeLock();
        try {
            if (!group2Tables.containsKey(groupId)) {
                return;
            }
            if (unstableGroups.add(groupId)) {
                group2ErrMsgs.put(groupId, Strings.nullToEmpty(reason));
                if (needEditLog) {
                    ColocatePersistInfo info = ColocatePersistInfo.createForMarkUnstable(groupId);
                    Env.getCurrentEnv().getEditLog().logColocateMarkUnstable(info);
                }
                LOG.info("mark group {} as unstable", groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public void markGroupStable(GroupId groupId, boolean needEditLog, ReplicaAllocation originReplicaAlloc) {
        writeLock();
        try {
            if (!group2Tables.containsKey(groupId)) {
                return;
            }
            // replica allocation is outdate
            ColocateGroupSchema groupSchema = group2Schema.get(groupId);
            if (groupSchema != null && originReplicaAlloc != null
                    && !originReplicaAlloc.equals(groupSchema.getReplicaAlloc())) {
                LOG.warn("mark group {} failed, replica alloc has outdate, old replica alloc {}, new replica alloc {}",
                        groupId, originReplicaAlloc.getAllocMap(), groupSchema.getReplicaAlloc());
                return;
            }
            if (unstableGroups.remove(groupId)) {
                group2ErrMsgs.put(groupId, "");
                if (needEditLog) {
                    ColocatePersistInfo info = ColocatePersistInfo.createForMarkStable(groupId);
                    Env.getCurrentEnv().getEditLog().logColocateMarkStable(info);
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
            groupId.removeTblId2DbId(tableId);
            group2Tables.remove(groupId, tableId);
            if (!group2Tables.containsKey(groupId)) {
                // all tables of this group are removed, remove the group
                group2BackendsPerBucketSeq.rowMap().remove(groupId);
                group2Schema.remove(groupId);
                group2ErrMsgs.remove(groupId);
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
            return Sets.newHashSet(group2Tables.keySet());
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getBackendsByGroup(GroupId groupId, Tag tag) {
        readLock();
        try {
            Set<Long> allBackends = new HashSet<>();
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId, tag);
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

    public Map<Tag, List<List<Long>>> getBackendsPerBucketSeq(GroupId groupId) {
        readLock();
        try {
            Map<Tag, List<List<Long>>> backendsPerBucketSeq = group2BackendsPerBucketSeq.row(groupId);
            if (backendsPerBucketSeq == null) {
                return Maps.newHashMap();
            }
            return backendsPerBucketSeq;
        } finally {
            readUnlock();
        }
    }

    public List<List<Long>> getBackendsPerBucketSeqByTag(GroupId groupId, Tag tag) {
        readLock();
        try {
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId, tag);
            if (backendsPerBucketSeq == null) {
                return Lists.newArrayList();
            }
            return backendsPerBucketSeq;
        } finally {
            readUnlock();
        }
    }

    // Get all backend ids except for the given tag
    public Set<Long> getBackendIdsExceptForTag(GroupId groupId, Tag tag) {
        Set<Long> beIds = Sets.newHashSet();
        readLock();
        try {
            Map<Tag, List<List<Long>>> backendsPerBucketSeq = group2BackendsPerBucketSeq.row(groupId);
            if (backendsPerBucketSeq == null) {
                return beIds;
            }

            for (Map.Entry<Tag, List<List<Long>>> entry : backendsPerBucketSeq.entrySet()) {
                if (entry.getKey().equals(tag)) {
                    continue;
                }
                for (List<Long> list : entry.getValue()) {
                    beIds.addAll(list);
                }
            }
            return beIds;
        } finally {
            readUnlock();
        }
    }


    public List<Set<Long>> getBackendsPerBucketSeqSet(GroupId groupId) {
        readLock();
        try {
            Map<Tag, List<List<Long>>> backendsPerBucketSeqMap = group2BackendsPerBucketSeq.row(groupId);
            if (backendsPerBucketSeqMap == null) {
                return Lists.newArrayList();
            }
            List<Set<Long>> list = Lists.newArrayList();

            // Merge backend ids of all tags
            for (Map.Entry<Tag, List<List<Long>>> backendsPerBucketSeq : backendsPerBucketSeqMap.entrySet()) {
                for (int i = 0; i < backendsPerBucketSeq.getValue().size(); ++i) {
                    if (list.size() == i) {
                        list.add(Sets.newHashSet());
                    }
                    list.get(i).addAll(backendsPerBucketSeq.getValue().get(i));
                }
            }
            return list;
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getTabletBackendsByGroup(GroupId groupId, int tabletOrderIdx) {
        readLock();
        try {
            Map<Tag, List<List<Long>>> backendsPerBucketSeqMap = group2BackendsPerBucketSeq.row(groupId);
            if (backendsPerBucketSeqMap == null) {
                return Sets.newHashSet();
            }

            // Merge backend ids of all tags
            Set<Long> beIds = Sets.newHashSet();
            for (Map.Entry<Tag, List<List<Long>>> backendsPerBucketSeq : backendsPerBucketSeqMap.entrySet()) {
                if (tabletOrderIdx >= backendsPerBucketSeq.getValue().size()) {
                    return Sets.newHashSet();
                }
                beIds.addAll(backendsPerBucketSeq.getValue().get(tabletOrderIdx));
            }

            return beIds;
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
            String fullNewGroupName = GroupId.getFullGroupName(dbId, newGroup);
            return addTableToGroup(dbId, tbl, fullNewGroupName, assignedGroupId);
        } finally {
            writeUnlock();
        }
    }

    public void replayAddTableToGroup(ColocatePersistInfo info) throws MetaNotFoundException {
        long dbId = info.getGroupId().dbId;
        if (dbId == 0) {
            dbId = info.getGroupId().getDbIdByTblId(info.getTableId());
        }
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        OlapTable tbl = (OlapTable) db.getTableOrMetaException(info.getTableId(),
                org.apache.doris.catalog.Table.TableType.OLAP);
        writeLock();
        try {
            Map<Tag, List<List<Long>>> map = info.getBackendsPerBucketSeq();
            for (Map.Entry<Tag, List<List<Long>>> entry : map.entrySet()) {
                group2BackendsPerBucketSeq.put(info.getGroupId(), entry.getKey(), entry.getValue());
            }
            String fullGroupName = GroupId.getFullGroupName(dbId, tbl.getColocateGroup());
            addTableToGroup(dbId, tbl, fullGroupName, info.getGroupId());
        } finally {
            writeUnlock();
        }
    }

    public void replayAddBackendsPerBucketSeq(ColocatePersistInfo info) {
        addBackendsPerBucketSeq(info.getGroupId(), info.getBackendsPerBucketSeq());
    }

    public void replayMarkGroupUnstable(ColocatePersistInfo info) {
        markGroupUnstable(info.getGroupId(), "replay mark group unstable", false);
    }

    public void replayMarkGroupStable(ColocatePersistInfo info) {
        markGroupStable(info.getGroupId(), false, null);
    }

    public void replayRemoveTable(ColocatePersistInfo info) {
        removeTable(info.getTableId());
    }

    public void replayModifyReplicaAlloc(ColocatePersistInfo info) throws UserException {
        writeLock();
        try {
            modifyColocateGroupReplicaAllocation(info.getGroupId(), info.getReplicaAlloc(),
                    info.getBackendsPerBucketSeq(), false);
        } finally {
            writeUnlock();
        }
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
                String dbName = "";
                if (groupId.dbId != 0) {
                    Database db = Env.getCurrentInternalCatalog().getDbNullable(groupId.dbId);
                    if (db != null) {
                        dbName = db.getFullName();
                        int index = dbName.indexOf(":");
                        if (index > 0) {
                            dbName = dbName.substring(index + 1); //use short db name
                        }
                    }
                }
                String groupName = entry.getKey();
                if (!GroupId.isGlobalGroupName(groupName)) {
                    groupName = dbName + "." + groupName.substring(groupName.indexOf("_") + 1);
                }
                info.add(groupName);
                info.add(Joiner.on(", ").join(group2Tables.get(groupId)));
                ColocateGroupSchema groupSchema = group2Schema.get(groupId);
                info.add(String.valueOf(groupSchema.getBucketsNum()));
                info.add(String.valueOf(groupSchema.getReplicaAlloc().toCreateStmt()));
                List<String> cols = groupSchema.getDistributionColTypes().stream().map(
                        e -> e.toSql()).collect(Collectors.toList());
                info.add(Joiner.on(", ").join(cols));
                info.add(String.valueOf(!unstableGroups.contains(groupId)));
                info.add(Strings.nullToEmpty(group2ErrMsgs.get(groupId)));
                infos.add(info);
            }
        } finally {
            readUnlock();
        }
        return infos;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeLock();
        try {
            int size = groupName2Id.size();
            out.writeInt(size);
            for (Map.Entry<String, GroupId> entry : ImmutableMap.copyOf(groupName2Id).entrySet()) {
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
                Map<Tag, List<List<Long>>> backendsPerBucketSeq = group2BackendsPerBucketSeq.row(entry.getValue());
                out.writeInt(backendsPerBucketSeq.size());
                for (Map.Entry<Tag, List<List<Long>>> tag2Bucket2BEs : backendsPerBucketSeq.entrySet()) {
                    tag2Bucket2BEs.getKey().write(out);
                    out.writeInt(tag2Bucket2BEs.getValue().size());
                    for (List<Long> beIds : tag2Bucket2BEs.getValue()) {
                        out.writeInt(beIds.size());
                        for (Long be : beIds) {
                            out.writeLong(be);
                        }
                    }
                }
            }

            size = unstableGroups.size();
            out.writeInt(size);
            for (GroupId groupId : unstableGroups) {
                groupId.write(out);
            }
        } finally {
            writeUnlock();
        }

    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
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

            // backends seqs
            if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_105) {
                List<List<Long>> bucketsSeq = Lists.newArrayList();
                int beSize = in.readInt();
                for (int j = 0; j < beSize; j++) {
                    int seqSize = in.readInt();
                    List<Long> seq = Lists.newArrayList();
                    for (int k = 0; k < seqSize; k++) {
                        long beId = in.readLong();
                        seq.add(beId);
                    }
                    bucketsSeq.add(seq);
                }
                group2BackendsPerBucketSeq.put(grpId, Tag.DEFAULT_BACKEND_TAG, bucketsSeq);
            } else {
                int tagSize = in.readInt();
                for (int j = 0; j < tagSize; j++) {
                    Tag tag = Tag.read(in);
                    int bucketSize = in.readInt();
                    List<List<Long>> bucketsSeq = Lists.newArrayList();
                    for (int k = 0; k < bucketSize; k++) {
                        List<Long> beIds = Lists.newArrayList();
                        int beSize = in.readInt();
                        for (int l = 0; l < beSize; l++) {
                            beIds.add(in.readLong());
                        }
                        bucketsSeq.add(beIds);
                    }
                    group2BackendsPerBucketSeq.put(grpId, tag, bucketsSeq);
                }
            }
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            unstableGroups.add(GroupId.read(in));
        }
    }

    public void setErrMsgForGroup(GroupId groupId, String message) {
        group2ErrMsgs.put(groupId, message);
    }

    // just for ut
    public Map<Long, GroupId> getTable2Group() {
        return table2Group;
    }

    public void alterColocateGroup(AlterColocateGroupStmt stmt) throws UserException {
        writeLock();
        try {
            Map<String, String> properties = stmt.getProperties();
            String dbName = stmt.getColocateGroupName().getDb();
            String groupName = stmt.getColocateGroupName().getGroup();
            long dbId = 0;
            if (!GroupId.isGlobalGroupName(groupName)) {
                Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(dbName);
                dbId = db.getId();
            }
            String fullGroupName = GroupId.getFullGroupName(dbId, groupName);
            ColocateGroupSchema groupSchema = getGroupSchema(fullGroupName);
            if (groupSchema == null) {
                throw new DdlException("Not found colocate group " + stmt.getColocateGroupName().toSql());
            }

            GroupId groupId = groupSchema.getGroupId();

            if (properties.size() > 1) {
                throw new DdlException("Can only set one colocate group property at a time");
            }

            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)
                    || properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION)) {
                if (Config.isCloudMode()) {
                    throw new DdlException("Cann't modify colocate group replication in cloud mode");
                }

                ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
                Preconditions.checkState(!replicaAlloc.isNotSet());
                Env.getCurrentSystemInfo().checkReplicaAllocation(replicaAlloc);
                Map<Tag, List<List<Long>>> backendsPerBucketSeq = getBackendsPerBucketSeq(groupId);
                Map<Tag, List<List<Long>>> newBackendsPerBucketSeq = Maps.newHashMap();
                for (Map.Entry<Tag, List<List<Long>>> entry : backendsPerBucketSeq.entrySet()) {
                    List<List<Long>> newList = Lists.newArrayList();
                    for (List<Long> backends : entry.getValue()) {
                        newList.add(Lists.newArrayList(backends));
                    }
                    newBackendsPerBucketSeq.put(entry.getKey(), newList);
                }
                try {
                    ColocateTableCheckerAndBalancer.modifyGroupReplicaAllocation(replicaAlloc,
                            newBackendsPerBucketSeq, groupSchema.getBucketsNum());
                } catch (Exception e) {
                    LOG.warn("modify group [{}, {}] to replication allocation {} failed, bucket seq {}",
                            fullGroupName, groupId, replicaAlloc, backendsPerBucketSeq, e);
                    throw new DdlException(e.getMessage());
                }
                backendsPerBucketSeq = newBackendsPerBucketSeq;
                Preconditions.checkState(backendsPerBucketSeq.size() == replicaAlloc.getAllocMap().size());
                modifyColocateGroupReplicaAllocation(groupSchema.getGroupId(), replicaAlloc,
                        backendsPerBucketSeq, true);
            } else {
                throw new DdlException("Unknown colocate group property: " + properties.keySet());
            }
        } finally {
            writeUnlock();
        }
    }

    private void modifyColocateGroupReplicaAllocation(GroupId groupId, ReplicaAllocation replicaAlloc,
            Map<Tag, List<List<Long>>> backendsPerBucketSeq, boolean needEditLog) throws UserException {
        ColocateGroupSchema groupSchema = getGroupSchema(groupId);
        if (groupSchema == null) {
            LOG.warn("not found group {}", groupId);
            return;
        }

        List<Long> tableIds = getAllTableIds(groupId);
        for (Long tableId : tableIds) {
            long dbId = groupId.dbId;
            if (dbId == 0) {
                dbId = groupId.getDbIdByTblId(tableId);
            }
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null || !isColocateTable(table.getId())) {
                continue;
            }
            table.writeLock();
            try {
                Map<String, String> tblProperties = Maps.newHashMap();
                tblProperties.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION,
                        replicaAlloc.toCreateStmt());
                table.setReplicaAllocation(tblProperties);
                if (table.dynamicPartitionExists()) {
                    TableProperty tableProperty = table.getTableProperty();
                    // Merge the new properties with origin properties, and then analyze them
                    Map<String, String> origDynamicProperties = tableProperty.getOriginDynamicPartitionProperty();
                    origDynamicProperties.put(DynamicPartitionProperty.REPLICATION_ALLOCATION,
                            replicaAlloc.toCreateStmt());
                    Map<String, String> analyzedDynamicPartition = DynamicPartitionUtil.analyzeDynamicPartition(
                            origDynamicProperties, table, db);
                    tableProperty.modifyTableProperties(analyzedDynamicPartition);
                    tableProperty.buildDynamicProperty();
                }
                for (ReplicaAllocation alloc : table.getPartitionInfo().getPartitionReplicaAllocations().values()) {
                    Map<Tag, Short> allocMap = alloc.getAllocMap();
                    allocMap.clear();
                    allocMap.putAll(replicaAlloc.getAllocMap());
                }
            } finally {
                table.writeUnlock();
            }
        }

        if (!backendsPerBucketSeq.equals(group2BackendsPerBucketSeq.row(groupId))) {
            markGroupUnstable(groupId, "change replica allocation", false);
        }
        groupSchema.setReplicaAlloc(replicaAlloc);
        setBackendsPerBucketSeq(groupId, backendsPerBucketSeq);

        if (needEditLog) {
            ColocatePersistInfo info = ColocatePersistInfo.createForModifyReplicaAlloc(groupId,
                    replicaAlloc, backendsPerBucketSeq);
            Env.getCurrentEnv().getEditLog().logColocateModifyRepliaAlloc(info);
        }
        LOG.info("modify group {} replication allocation to {}, is replay {}", groupId, replicaAlloc, !needEditLog);
    }
}

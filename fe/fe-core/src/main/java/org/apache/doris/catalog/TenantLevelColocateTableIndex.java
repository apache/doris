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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.ModifyTenantLevelColocateMapInfo;
import org.apache.doris.persist.TenantLevelColocateData;
import org.apache.doris.persist.TenantLevelColocateGroupInfo;
import org.apache.doris.persist.TenantLevelColocateStableInfo;
import org.apache.doris.persist.TenantLevelColocateTableInfo;
import org.apache.doris.resource.Tag;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * maintain the tenant-level colocation table related indexes and meta
 */
public class TenantLevelColocateTableIndex implements Writable {
    private static final Logger LOG = LogManager.getLogger(TenantLevelColocateTableIndex.class);

    // group_name -> tag-> group_id
    private final Table<String, Tag, Long> groupName2Id = HashBasedTable.create();
    // group id -> group schema
    private final Map<Long, TenantLevelColocateGroupSchema> group2Schema = Maps.newHashMap();
    // group_id -> bucketSeq -> backend ids
    private final Map<Long, List<List<Long>>> group2BackendsPerBucketSeq = Maps.newHashMap();

    // master data
    // group_id -> table_ids
    private final Multimap<Long, Long> masterGroup2Tables = ArrayListMultimap.create();
    // table_id -> group_id
    private final Table<Long, Tag, Long> table2MasterGroup = HashBasedTable.create();

    // the colocate group is unstable
    private final Set<Long> unstableMasterGroups = Sets.newHashSet();
    // save some error msg of the group for show. no need to persist
    private final Map<Long, String> masterGroup2ErrMsgs = Maps.newHashMap();

    // slave data
    // group_id -> table_ids
    private final Multimap<Long, Long> slaveGroup2Tables = ArrayListMultimap.create();
    // table_id -> group_id
    private final Table<Long, Tag, Long> table2SlaveGroup = HashBasedTable.create();
    // the colocate group is unstable
    private final Set<Long> unstableSlaveGroups = Sets.newHashSet();
    // save some error msg of the group for show. no need to persist
    private final Map<Long, String> slaveGroup2ErrMsgs = Maps.newHashMap();

    private final transient ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public TenantLevelColocateTableIndex() {

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
    public TenantLevelColocateGroupSchema addTableToMasterGroup(OlapTable tbl, String groupName, Tag tag,
            Long assignedGroupId) {
        writeLock();
        try {
            TenantLevelColocateGroupSchema groupSchema;
            if (groupName2Id.contains(groupName, tag)) {
                Long groupId = groupName2Id.get(groupName, tag);
                groupSchema = group2Schema.get(groupId);
                Preconditions.checkNotNull(groupSchema);
            } else {
                final long groupId;
                if (assignedGroupId != null) {
                    // use the given group id, eg, in replay process
                    groupId = assignedGroupId;
                } else {
                    // generate a new one
                    groupId = Env.getCurrentEnv().getNextId();
                }
                HashDistributionInfo distributionInfo = (HashDistributionInfo) tbl.getDefaultDistributionInfo();
                ReplicaAllocation tblReplicaAlloc = tbl.getDefaultReplicaAllocation();
                groupSchema = new TenantLevelColocateGroupSchema(groupId, groupName, tag,
                        distributionInfo.getDistributionColumns(), distributionInfo.getBucketNum(),
                        tblReplicaAlloc.getReplicaNumByTag(tag));
                groupName2Id.put(groupName, tag, groupId);
                group2Schema.put(groupId, groupSchema);
                masterGroup2ErrMsgs.put(groupId, "");
            }
            masterGroup2Tables.put(groupSchema.getGroupId(), tbl.getId());
            table2MasterGroup.put(tbl.getId(), tag, groupSchema.getGroupId());
            return groupSchema;
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

    public boolean addBackendsPerBucketSeq(long groupId, List<List<Long>> backendsPerBucketSeq,
            ReplicaAllocation originReplicaAlloc) {
        writeLock();
        try {
            TenantLevelColocateGroupSchema groupSchema = group2Schema.get(groupId);
            // replica allocation has outdate
            if (groupSchema != null && !originReplicaAlloc.equals(groupSchema.getReplicaAlloc())) {
                LOG.info("replica allocation has outdate for group {}, old replica alloc {}, new replica alloc {}",
                        groupId, originReplicaAlloc.getAllocMap(), groupSchema.getReplicaAlloc());
                return false;
            }
            group2BackendsPerBucketSeq.put(groupId,  backendsPerBucketSeq);
            return true;
        } finally {
            writeUnlock();
        }
    }

    public void markMasterGroupUnstable(long groupId, String reason, boolean needEditLog) {
        writeLock();
        try {
            if (!masterGroup2Tables.containsKey(groupId)) {
                return;
            }
            if (unstableMasterGroups.add(groupId)) {
                if (needEditLog) {
                    TenantLevelColocateStableInfo info = new TenantLevelColocateStableInfo(
                            Collections.singleton(groupId));
                    Env.getCurrentEnv().getEditLog().logTenantLevelColocateMarkMasterUnstable(info);
                }
                LOG.info("mark group {} as unstable, reason:{}", groupId, reason);
            }
            //update unstable reason every time not just when it was first added to group
            if (unstableMasterGroups.contains(groupId)) {
                masterGroup2ErrMsgs.put(groupId, Strings.nullToEmpty(reason));
            }
        } finally {
            writeUnlock();
        }
    }

    public void markMasterGroupStable(long groupId, boolean needEditLog) {
        writeLock();
        try {
            if (!masterGroup2Tables.containsKey(groupId)) {
                return;
            }
            if (unstableMasterGroups.remove(groupId)) {
                masterGroup2ErrMsgs.put(groupId, "");
                if (needEditLog) {
                    TenantLevelColocateStableInfo info = new TenantLevelColocateStableInfo(
                            Collections.singleton(groupId));
                    Env.getCurrentEnv().getEditLog().logTenantLevelColocateMarkMasterStable(info);
                }
                LOG.info("mark group {} as stable", groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public void removeTable(long tableId) {
        removeMasterTable(tableId);
        removeSlaveTable(tableId);
    }

    private void removeMasterTable(long tableId) {
        writeLock();
        try {
            Map<Tag, Long> groups = new HashMap<>(table2MasterGroup.row(tableId));
            for (Map.Entry<Tag, Long> tagGroupIdEntry : groups.entrySet()) {
                Tag tag = tagGroupIdEntry.getKey();
                Long groupId = table2MasterGroup.remove(tableId, tag);
                Preconditions.checkNotNull(groupId);
                masterGroup2Tables.remove(groupId, tableId);
                if (!slaveGroup2Tables.containsKey(groupId)) {
                    removeUnusedMasterGroup(groupId);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void removeMasterTable(long tableId, Tag tag) {
        writeLock();
        try {
            Long groupId = table2MasterGroup.remove(tableId, tag);
            Preconditions.checkNotNull(groupId);
            masterGroup2Tables.remove(groupId, tableId);
            if (!slaveGroup2Tables.containsKey(groupId)) {
                removeUnusedMasterGroup(groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    private void removeUnusedMasterGroup(long groupId) {
        if (!masterGroup2Tables.containsKey(groupId)) {
            // all tables of this group are removed, remove the group
            TenantLevelColocateGroupSchema groupSchema = group2Schema.remove(groupId);
            group2BackendsPerBucketSeq.remove(groupId);
            masterGroup2ErrMsgs.remove(groupId);
            unstableMasterGroups.remove(groupId);
            groupName2Id.remove(groupSchema.getName(), groupSchema.getTag());
        }
    }

    public Map<Tag, Long> getStableGroup(long table) {
        Map<Tag, Long> result = new HashMap<>();
        readLock();
        try {
            Map<Tag, Long> masterGroups = table2MasterGroup.row(table);
            for (Map.Entry<Tag, Long> entry : masterGroups.entrySet()) {
                if (unstableMasterGroups.contains(entry.getValue())) {
                    continue;
                }
                result.put(entry.getKey(), entry.getValue());
            }
            Map<Tag, Long> slaveGroups = table2SlaveGroup.row(table);
            for (Map.Entry<Tag, Long> entry : slaveGroups.entrySet()) {
                if (unstableSlaveGroups.contains(entry.getValue())) {
                    continue;
                }
                result.put(entry.getKey(), entry.getValue());
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    public Map<Tag, List<List<Long>>> getStableGroupMap(long table) {
        Map<Tag, List<List<Long>>> result = new HashMap<>();
        readLock();
        try {
            Map<Tag, Long> masterGroups = table2MasterGroup.row(table);
            for (Map.Entry<Tag, Long> entry : masterGroups.entrySet()) {
                if (unstableMasterGroups.contains(entry.getValue())) {
                    continue;
                }
                result.put(entry.getKey(), group2BackendsPerBucketSeq.get(entry.getValue()));
            }
            Map<Tag, Long> groups = table2SlaveGroup.row(table);
            for (Map.Entry<Tag, Long> entry : groups.entrySet()) {
                if (unstableSlaveGroups.contains(entry.getValue())) {
                    continue;
                }
                result.put(entry.getKey(), group2BackendsPerBucketSeq.get(entry.getValue()));
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    public boolean isColocateTable(long tableId) {
        return isColocateMasterTable(tableId) || isColocateSlaveTable(tableId);
    }

    public boolean isColocateMasterTable(long tableId) {
        readLock();
        try {
            return table2MasterGroup.containsRow(tableId);
        } finally {
            readUnlock();
        }
    }

    @VisibleForTesting
    public Set<Long> getUnstableMasterGroupIds() {
        readLock();
        try {
            return Sets.newHashSet(unstableMasterGroups);
        } finally {
            readUnlock();
        }
    }

    public Map<Tag, TenantLevelColocateGroupSchema> getMasterGroupByTable(long tableId) {
        Map<Tag, TenantLevelColocateGroupSchema> result = new HashMap<>();
        readLock();
        try {
            for (Map.Entry<Tag, Long> entry : table2MasterGroup.row(tableId).entrySet()) {
                TenantLevelColocateGroupSchema groupSchema = group2Schema.get(entry.getValue());
                result.put(entry.getKey(), groupSchema);
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    public Map<Tag, String> getMasterGroupNameMapByTable(long tableId) {
        readLock();
        try {
            Map<Tag, Long> map = table2MasterGroup.row(tableId);
            Map<Tag, String> result = new HashMap<>();
            for (Map.Entry<Tag, Long> entry : map.entrySet()) {
                TenantLevelColocateGroupSchema groupSchema = group2Schema.get(entry.getValue());
                result.put(entry.getKey(), groupSchema.getName());
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    public long getMasterGroupByTable(long tableId, Tag tag) {
        readLock();
        try {
            Long groupId = table2MasterGroup.get(tableId, tag);
            Preconditions.checkNotNull(groupId);
            return groupId;
        } finally {
            readUnlock();
        }
    }

    public boolean hasMasterGroup(long tableId, Tag tag) {
        readLock();
        try {
            return table2MasterGroup.contains(tableId, tag);
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getAllGroupIds() {
        readLock();
        try {
            return new HashSet<>(group2Schema.keySet());
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getBackendsByGroup(long groupId) {
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

    public Set<Long> getBackendsByTable(long tableId) {
        Set<Long> result = new HashSet<>();
        result.addAll(getBackendsByMasterTable(tableId));
        result.addAll(getBackendsBySlaveTable(tableId));
        return result;
    }

    private Set<Long> getBackendsByMasterTable(long tableId) {
        readLock();
        try {
            Set<Long> result = new HashSet<>();
            Map<Tag, Long> map = table2MasterGroup.row(tableId);
            for (Map.Entry<Tag, Long> entry : map.entrySet()) {
                Long groupId = entry.getValue();
                List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
                if (backendsPerBucketSeq == null) {
                    continue;
                }
                for (List<Long> backends : backendsPerBucketSeq) {
                    result.addAll(backends);
                }
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    public List<Long> getAllTableIds(long groupId) {
        List<Long> result = new ArrayList<>();
        result.addAll(getAllMasterTableIds(groupId));
        result.addAll(getAllSlaveTableIds(groupId));
        return result;
    }

    public List<Long> getAllMasterTableIds(long groupId) {
        readLock();
        try {
            if (!masterGroup2Tables.containsKey(groupId)) {
                return Lists.newArrayList();
            }
            return Lists.newArrayList(masterGroup2Tables.get(groupId));
        } finally {
            readUnlock();
        }
    }

    public Map<Tag, List<List<Long>>> getBackendsPerBucketSeqByTable(Long tableId, int bucketNum) {
        Map<Tag, List<List<Long>>> result = new HashMap<>();
        readLock();
        try {
            Map<Tag, Long> masterGroups = table2MasterGroup.row(tableId);
            for (Map.Entry<Tag, Long> entry : masterGroups.entrySet()) {
                Long groupId = entry.getValue();
                Tag tag = entry.getKey();
                List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
                if (backendsPerBucketSeq == null) {
                    continue;
                }
                result.put(tag, backendsPerBucketSeq);
            }
            Map<Tag, Long> slaveGroups = table2SlaveGroup.row(tableId);
            for (Map.Entry<Tag, Long> entry : slaveGroups.entrySet()) {
                Long groupId = entry.getValue();
                Tag tag = entry.getKey();
                List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
                if (backendsPerBucketSeq == null) {
                    continue;
                }
                result.put(tag, getSlaveBackendsPerBucketSeq(backendsPerBucketSeq, bucketNum));
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    public List<List<Long>> getBackendsPerBucketSeqByGroup(long groupId) {
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

    public List<Set<Long>> getBackendsPerBucketSeqSet(long groupId) {
        readLock();
        try {
            List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
            if (backendsPerBucketSeq == null) {
                return Lists.newArrayList();
            }
            List<Set<Long>> list = Lists.newArrayList();
            // Merge backend ids of all tags
            for (int i = 0; i < backendsPerBucketSeq.size(); ++i) {
                list.add(Sets.newHashSet(backendsPerBucketSeq.get(i)));
            }
            return list;
        } finally {
            readUnlock();
        }
    }

    public Set<Long> getTabletBackendsByTable(long tableId, int tabletOrderIdx) {
        Set<Long> result = new HashSet<>();
        readLock();
        try {
            Map<Tag, Long> masterGroups = table2MasterGroup.row(tableId);
            for (Map.Entry<Tag, Long> entry : masterGroups.entrySet()) {
                Long groupId = entry.getValue();
                List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
                Preconditions.checkState(backendsPerBucketSeq != null && tabletOrderIdx < backendsPerBucketSeq.size());
                // Merge backend ids of all tags
                result.addAll(backendsPerBucketSeq.get(tabletOrderIdx));
            }
            Map<Tag, Long> slaveGroups = table2SlaveGroup.row(tableId);
            for (Map.Entry<Tag, Long> entry : slaveGroups.entrySet()) {
                Long groupId = entry.getValue();
                List<List<Long>> backendsPerBucketSeq = group2BackendsPerBucketSeq.get(groupId);
                Preconditions.checkState(backendsPerBucketSeq != null && !backendsPerBucketSeq.isEmpty());
                // Merge backend ids of all tags
                result.addAll(backendsPerBucketSeq.get(tabletOrderIdx % backendsPerBucketSeq.size()));
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    public TenantLevelColocateGroupSchema getGroupSchema(String groupName, Tag tag) {
        readLock();
        try {
            if (!groupName2Id.contains(groupName, tag)) {
                return null;
            }
            return group2Schema.get(groupName2Id.get(groupName, tag));
        } finally {
            readUnlock();
        }
    }

    public TenantLevelColocateGroupSchema getGroupSchema(long groupId) {
        readLock();
        try {
            return group2Schema.get(groupId);
        } finally {
            readUnlock();
        }
    }

    public TenantLevelColocateGroupSchema changeMasterGroup(OlapTable tbl, String oldGroup, String newGroup, Tag tag,
            Long assignedGroupId) {
        writeLock();
        try {
            if (!Strings.isNullOrEmpty(oldGroup)) {
                // remove from old group
                removeMasterTable(tbl.getId(), tag);
            }
            return addTableToMasterGroup(tbl, newGroup, tag, assignedGroupId);
        } finally {
            writeUnlock();
        }
    }

    public void checkPartitionDistributionAndReplica(Long tableId, int defaultBucketNum,
            DistributionInfo partitionDistributionInfo, ReplicaAllocation replicaAlloc) throws DdlException {
        checkMasterDistributionAndReplica(tableId, partitionDistributionInfo, replicaAlloc);
        checkSlaveDistributionAndReplica(tableId, defaultBucketNum, partitionDistributionInfo, replicaAlloc);
    }

    private void checkMasterDistributionAndReplica(Long tableId, DistributionInfo distributionInfo,
            ReplicaAllocation replicaAlloc) throws DdlException {
        readLock();
        try {
            Map<Tag, Long> map = table2MasterGroup.row(tableId);
            for (Map.Entry<Tag, Long> entry : map.entrySet()) {
                Long groupId = entry.getValue();
                TenantLevelColocateGroupSchema groupSchema = group2Schema.get(groupId);
                Preconditions.checkNotNull(groupSchema);
                groupSchema.checkMasterDistribution(distributionInfo);
                groupSchema.checkReplicaAllocation(replicaAlloc);
            }
        } finally {
            readUnlock();
        }
    }

    public void checkReplica(Long tableId, ReplicaAllocation replicaAlloc) throws DdlException {
        checkMasterReplica(tableId, replicaAlloc);
        checkSlaveReplica(tableId, replicaAlloc);
    }

    private void checkMasterReplica(Long tableId, ReplicaAllocation replicaAlloc) throws DdlException {
        readLock();
        try {
            Map<Tag, Long> map = table2MasterGroup.row(tableId);
            for (Map.Entry<Tag, Long> entry : map.entrySet()) {
                Long groupId = entry.getValue();
                TenantLevelColocateGroupSchema groupSchema = group2Schema.get(groupId);
                Preconditions.checkNotNull(groupSchema);
                groupSchema.checkReplicaAllocation(replicaAlloc);
            }
        } finally {
            readUnlock();
        }
    }

    public void replayAddTableToMasterGroup(TenantLevelColocateTableInfo info) throws MetaNotFoundException {
        long dbId = info.getDbId();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        OlapTable tbl = (OlapTable) db.getTableOrMetaException(info.getTableId(),
                org.apache.doris.catalog.Table.TableType.OLAP);
        writeLock();
        try {
            for (Entry<Long, TenantLevelColocateGroupInfo> entry : info.getGroupMap().entrySet()) {
                Long groupId = entry.getKey();
                TenantLevelColocateGroupInfo groupInfo = entry.getValue();
                group2BackendsPerBucketSeq.put(groupId, groupInfo.getBackendsPerBucketSeq());
                addTableToMasterGroup(tbl, groupInfo.getName(), groupInfo.getTag(), groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayAddBackendsPerBucketSeq(ModifyTenantLevelColocateMapInfo info) {
        addBackendsPerBucketSeq(info.getBackendsPerBucketSeq());
    }

    public void replayMarkMasterGroupUnstable(TenantLevelColocateStableInfo info) {
        String reason = "replay mark group unstable";
        writeLock();
        try {
            for (Long groupId : info.getGroupSet()) {
                if (!group2Schema.containsKey(groupId)) {
                    continue;
                }
                unstableMasterGroups.add(groupId);
                //update unstable reason every time not just when it was first added to group
                if (unstableMasterGroups.contains(groupId)) {
                    masterGroup2ErrMsgs.put(groupId, Strings.nullToEmpty(reason));
                }
            }
            LOG.info("mark group {} as unstable", info.getGroupSet());
        } finally {
            writeUnlock();
        }
    }

    public void replayMarkMasterGroupStable(TenantLevelColocateStableInfo info) {
        writeLock();
        try {
            for (Long groupId : info.getGroupSet()) {
                if (!group2Schema.containsKey(groupId)) {
                    continue;
                }
                if (unstableMasterGroups.remove(groupId)) {
                    masterGroup2ErrMsgs.put(groupId, "");
                }
            }
            LOG.info("mark group {} as stable", info.getGroupSet());
        } finally {
            writeUnlock();
        }
    }

    public void replayRemoveMasterTable(TenantLevelColocateTableInfo info) {
        removeMasterTable(info.getTableId());
    }

    public void addBackendsPerBucketSeq(Map<Long, List<List<Long>>> backendsPerBucketSeq) {
        writeLock();
        try {
            for (Map.Entry<Long, List<List<Long>>> entry : backendsPerBucketSeq.entrySet()) {
                Long groupId = entry.getKey();
                group2BackendsPerBucketSeq.put(groupId, entry.getValue());
            }
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> getInfos() {
        List<List<String>> infos = Lists.newArrayList();
        readLock();
        try {
            Set<Cell<String, Tag, Long>> cells = groupName2Id.cellSet();
            for (Cell<String, Tag, Long> cell : cells) {
                Long groupId = cell.getValue();
                TenantLevelColocateGroupSchema groupSchema = group2Schema.get(groupId);
                infos.add(getGroupInfo(groupId, masterGroup2Tables.get(groupId), true,
                        !unstableMasterGroups.contains(groupId), masterGroup2ErrMsgs.get(groupId), groupSchema));
            }
            for (Map.Entry<Long, Collection<Long>> entry : slaveGroup2Tables.asMap().entrySet()) {
                Long groupId = entry.getKey();
                TenantLevelColocateGroupSchema groupSchema = group2Schema.get(groupId);
                infos.add(getGroupInfo(groupId, entry.getValue(), false, !unstableSlaveGroups.contains(groupId),
                        slaveGroup2ErrMsgs.get(groupId), groupSchema));
            }
        } finally {
            readUnlock();
        }
        return infos;
    }

    private static List<String> getGroupInfo(long groupId, Collection<Long> tables, boolean isMater,
            boolean stable, String errMsg, TenantLevelColocateGroupSchema groupSchema) {
        List<String> info = Lists.newArrayList();
        Tag tag = groupSchema.getTag();
        String name = groupSchema.getName();
        info.add("0." + groupId);
        info.add(name + "/" + tag.value);
        info.add(Joiner.on(", ").join(tables));
        info.add(String.valueOf(groupSchema.getBucketsNum()));
        info.add(String.valueOf(groupSchema.getReplicaCreateStmt()));
        List<String> cols = groupSchema.getDistributionColTypes().stream().map(Type::toSql)
                .collect(Collectors.toList());
        info.add(Joiner.on(", ").join(cols));
        info.add(String.valueOf(isMater));
        info.add(String.valueOf(stable));
        info.add(Strings.nullToEmpty(errMsg));
        return info;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeLock();
        try {
            if (group2Schema.isEmpty()) {
                return;
            }
            // group schema
            TenantLevelColocateData data = new TenantLevelColocateData();
            data.setGroupSchemaList(new ArrayList<>(group2Schema.values()));
            // master data
            data.setGroupMapData(group2BackendsPerBucketSeq);
            data.setTableToMasterGroupMap(table2MasterGroup.rowMap());
            data.setUnstableMasterGroup(unstableMasterGroups);

            // slave data
            data.setTableToSlaveGroupMap(table2SlaveGroup.rowMap());
            data.setUnstableSlaveGroup(unstableSlaveGroups);
            data.write(out);
        } finally {
            writeUnlock();
        }
    }

    public void read(DataInput in) throws IOException {
        TenantLevelColocateData data = TenantLevelColocateData.read(in);
        for (TenantLevelColocateGroupSchema groupSchema : data.getGroupSchemaList()) {
            group2Schema.put(groupSchema.getGroupId(), groupSchema);
            groupName2Id.put(groupSchema.getName(), groupSchema.getTag(), groupSchema.getGroupId());
        }
        // master data
        group2BackendsPerBucketSeq.putAll(data.getGroupMapData());
        for (Map.Entry<Long, Map<Tag, Long>> tableEntry : data.getTableToMasterGroupMap().entrySet()) {
            Long tableId = tableEntry.getKey();
            for (Map.Entry<Tag, Long> tagGroupIdEntry : tableEntry.getValue().entrySet()) {
                Long groupId = tagGroupIdEntry.getValue();
                TenantLevelColocateGroupSchema groupSchema = group2Schema.get(groupId);
                table2MasterGroup.put(tableId, groupSchema.getTag(), groupId);
                masterGroup2Tables.put(groupId, tableId);
            }
        }
        unstableMasterGroups.addAll(data.getUnstableMasterGroup());

        // slave data
        for (Map.Entry<Long, Map<Tag, Long>> tableMap : data.getTableToSlaveGroupMap().entrySet()) {
            for (Map.Entry<Tag, Long> tagMap : tableMap.getValue().entrySet()) {
                table2SlaveGroup.put(tableMap.getKey(), tagMap.getKey(), tagMap.getValue());
                slaveGroup2Tables.put(tagMap.getValue(), tableMap.getKey());
            }
        }
        unstableSlaveGroups.addAll(data.getUnstableSlaveGroup());
    }

    public void setErrMsgForGroup(long groupId, String message) {
        masterGroup2ErrMsgs.put(groupId, message);
    }

    public Set<Tag> getAllTagByTable(long tableId) {
        Set<Tag> tags = new HashSet<>();
        tags.addAll(getAllMasterTagByTable(tableId));
        tags.addAll(getAllSlaveTagByTable(tableId));
        return tags;
    }


    public Set<Tag> getAllMasterTagByTable(long tableId) {
        readLock();
        try {
            Map<Tag, Long> map = table2MasterGroup.row(tableId);
            return new HashSet<>(map.keySet());
        } finally {
            readUnlock();
        }
    }

    public void addTableToSlaveGroup(OlapTable tbl, long groupId) {
        writeLock();
        try {
            slaveGroup2Tables.put(groupId, tbl.getId());
            TenantLevelColocateGroupSchema groupSchema = group2Schema.get(groupId);
            table2SlaveGroup.put(tbl.getId(), groupSchema.getTag(), groupId);
        } finally {
            writeUnlock();
        }
    }

    public Map<Tag, String> getSlaveGroupNameMap(long tableId) {
        readLock();
        try {
            Map<Tag, Long> map = table2SlaveGroup.row(tableId);
            Map<Tag, String> result = new HashMap<>();
            for (Map.Entry<Tag, Long> entry : map.entrySet()) {
                TenantLevelColocateGroupSchema groupSchema = group2Schema.get(entry.getValue());
                result.put(entry.getKey(), groupSchema.getName());
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    public void changeSlaveGroup(OlapTable tbl, String oldGroup, long groupId) {
        writeLock();
        try {
            if (!Strings.isNullOrEmpty(oldGroup)) {
                // remove from old group
                TenantLevelColocateGroupSchema groupSchema = group2Schema.get(groupId);
                removeSlaveTable(tbl.getId(), groupSchema.getTag());
            }
            addTableToSlaveGroup(tbl, groupId);
        } finally {
            writeUnlock();
        }
    }

    public void removeSlaveTable(long tableId, Tag tag) {
        writeLock();
        try {
            Long groupId = table2SlaveGroup.remove(tableId, tag);
            Preconditions.checkNotNull(groupId);
            slaveGroup2Tables.remove(groupId, tableId);
            if (!slaveGroup2Tables.containsKey(groupId)) {
                // all tables of this group are removed, remove the group
                slaveGroup2ErrMsgs.remove(groupId);
                unstableSlaveGroups.remove(groupId);
                removeUnusedMasterGroup(groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public void markSlaveGroupUnstable(long groupId, String reason, boolean needEditLog) {
        writeLock();
        try {
            if (!slaveGroup2Tables.containsKey(groupId)) {
                return;
            }
            if (unstableSlaveGroups.add(groupId)) {
                if (needEditLog) {
                    TenantLevelColocateStableInfo info = new TenantLevelColocateStableInfo(
                            Collections.singleton(groupId));
                    Env.getCurrentEnv().getEditLog().logColocateMarkUnstableSlave(info);
                }
                LOG.info("mark group {} as unstable", groupId);
            }
            //update unstable reason every time not just when it was first added to group
            if (unstableSlaveGroups.contains(groupId)) {
                slaveGroup2ErrMsgs.put(groupId, Strings.nullToEmpty(reason));
            }
        } finally {
            writeUnlock();
        }
    }

    public void markSlaveGroupUnstable(Set<Long> groups, String reason, boolean needEditLog) {
        Set<Long> added = new HashSet<>();
        writeLock();
        try {
            for (Long groupId : groups) {
                if (!slaveGroup2Tables.containsKey(groupId)) {
                    continue;
                }
                if (unstableSlaveGroups.add(groupId)) {
                    added.add(groupId);
                }
                //update unstable reason every time not just when it was first added to group
                if (unstableSlaveGroups.contains(groupId)) {
                    slaveGroup2ErrMsgs.put(groupId, Strings.nullToEmpty(reason));
                }
            }
            LOG.info("mark group {} as unstable", groups);
        } finally {
            writeUnlock();
        }
        if (needEditLog) {
            TenantLevelColocateStableInfo info = new TenantLevelColocateStableInfo(added);
            Env.getCurrentEnv().getEditLog().logColocateMarkUnstableSlave(info);
        }
    }

    public void replayAddTableToSlaveGroup(TenantLevelColocateTableInfo info) throws MetaNotFoundException {
        long dbId = info.getDbId();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        OlapTable tbl = (OlapTable) db.getTableOrMetaException(info.getTableId(),
                org.apache.doris.catalog.Table.TableType.OLAP);
        writeLock();
        try {
            for (Entry<Long, TenantLevelColocateGroupInfo> entry : info.getGroupMap().entrySet()) {
                Long groupId = entry.getKey();
                addTableToSlaveGroup(tbl, groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public boolean isColocateSlaveTable(long tableId) {
        readLock();
        try {
            return table2SlaveGroup.containsRow(tableId);
        } finally {
            readUnlock();
        }
    }

    private Set<Long> getBackendsBySlaveTable(long tableId) {
        Map<Tag, TenantLevelColocateGroupSchema> map = getSlaveGroupByTable(tableId);
        Set<Long> result = new HashSet<>();
        for (Entry<Tag, TenantLevelColocateGroupSchema> entry : map.entrySet()) {
            Long groupId = entry.getValue().getGroupId();
            List<List<Long>> backendsPerBucketSeq = getBackendsPerBucketSeqByGroup(groupId);
            if (backendsPerBucketSeq == null) {
                continue;
            }
            for (List<Long> backends : backendsPerBucketSeq) {
                result.addAll(backends);
            }
        }
        return result;
    }

    private void checkSlaveDistributionAndReplica(Long tableId, int defaultBucketNum,
            DistributionInfo distributionInfo, ReplicaAllocation replicaAlloc) throws DdlException {
        Map<Tag, TenantLevelColocateGroupSchema> map = getSlaveGroupByTable(tableId);
        for (Entry<Tag, TenantLevelColocateGroupSchema> entry : map.entrySet()) {
            TenantLevelColocateGroupSchema groupSchema = entry.getValue();
            Preconditions.checkNotNull(groupSchema);
            groupSchema.checkSlaveDistribution(defaultBucketNum, distributionInfo);
            groupSchema.checkReplicaAllocation(replicaAlloc);
        }
    }

    public Set<Tag> getAllSlaveTagByTable(long tableId) {
        readLock();
        try {
            Map<Tag, Long> map = table2SlaveGroup.row(tableId);
            return new HashSet<>(map.keySet());
        } finally {
            readUnlock();
        }
    }

    public void markSlaveGroupStable(long groupId, boolean needEditLog) {
        writeLock();
        try {
            if (!slaveGroup2Tables.containsKey(groupId)) {
                return;
            }
            if (unstableSlaveGroups.remove(groupId)) {
                slaveGroup2ErrMsgs.put(groupId, "");
                if (needEditLog) {
                    TenantLevelColocateStableInfo info = new TenantLevelColocateStableInfo(
                            Collections.singleton(groupId));
                    Env.getCurrentEnv().getEditLog().logColocateMarkStableSlave(info);
                }
                LOG.info("mark group {} as stable", groupId);
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayMarkSlaveGroupStable(TenantLevelColocateStableInfo info) {
        markSlaveGroupStable(info.getGroupSet(), false);
    }

    public void markSlaveGroupStable(Set<Long> groups, boolean needEditLog) {
        writeLock();
        try {
            for (Long groupId : groups) {
                if (!slaveGroup2Tables.containsKey(groupId)) {
                    continue;
                }
                if (unstableSlaveGroups.remove(groupId)) {
                    slaveGroup2ErrMsgs.put(groupId, "");
                    if (needEditLog) {
                        TenantLevelColocateStableInfo info = new TenantLevelColocateStableInfo(
                                Collections.singleton(groupId));
                        Env.getCurrentEnv().getEditLog().logColocateMarkStableSlave(info);
                    }
                }
            }
            LOG.info("mark group {} as stable", groups);
        } finally {
            writeUnlock();
        }
    }

    public void replayRemoveSlaveTable(TenantLevelColocateTableInfo info) {
        removeSlaveTable(info.getTableId());
    }

    private void removeSlaveTable(long tableId) {
        writeLock();
        try {
            Map<Tag, Long> groups = new HashMap<>(table2SlaveGroup.row(tableId));
            for (Map.Entry<Tag, Long> tagGroupIdEntry : groups.entrySet()) {
                Tag tag = tagGroupIdEntry.getKey();
                Long groupId = table2SlaveGroup.remove(tableId, tag);
                Preconditions.checkNotNull(groupId);
                slaveGroup2Tables.remove(groupId, tableId);
                if (!slaveGroup2Tables.containsKey(groupId)) {
                    // all tables of this group are removed, remove the group
                    slaveGroup2ErrMsgs.remove(groupId);
                    unstableSlaveGroups.remove(groupId);
                    removeUnusedMasterGroup(groupId);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    private void checkSlaveReplica(Long tableId, ReplicaAllocation replicaAlloc) throws DdlException {
        Map<Tag, TenantLevelColocateGroupSchema> map = getSlaveGroupByTable(tableId);
        for (Map.Entry<Tag, TenantLevelColocateGroupSchema> entry : map.entrySet()) {
            long groupId = entry.getValue().getGroupId();
            TenantLevelColocateGroupSchema groupSchema = getGroupSchema(groupId);
            Preconditions.checkNotNull(groupSchema);
            groupSchema.checkReplicaAllocation(replicaAlloc);
        }
    }

    public Map<Tag, TenantLevelColocateGroupSchema> getSlaveGroupByTable(long tableId) {
        Map<Tag, TenantLevelColocateGroupSchema> result = new HashMap<>();
        readLock();
        try {
            for (Map.Entry<Tag, Long> entry : table2SlaveGroup.row(tableId).entrySet()) {
                TenantLevelColocateGroupSchema groupSchema = getGroupSchema(entry.getValue());
                result.put(entry.getKey(), groupSchema);
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    public void replayMarkSlaveGroupUnstable(TenantLevelColocateStableInfo info) {
        markSlaveGroupUnstable(info.getGroupSet(), "replay mark group unstable", false);
    }

    public boolean hasSlaveGroup(long tableId, Tag tag) {
        readLock();
        try {
            return table2SlaveGroup.contains(tableId, tag);
        } finally {
            readUnlock();
        }
    }

    public List<Long> getAllSlaveTableIds(long groupId) {
        readLock();
        try {
            if (!slaveGroup2Tables.containsKey(groupId)) {
                return Lists.newArrayList();
            }
            return Lists.newArrayList(slaveGroup2Tables.get(groupId));
        } finally {
            readUnlock();
        }
    }

    private static List<List<Long>> getSlaveBackendsPerBucketSeq(List<List<Long>> backendsPerBucketSeq, int bucketNum) {
        if (backendsPerBucketSeq == null) {
            return Lists.newArrayList();
        }
        List<List<Long>> result = new ArrayList<>(bucketNum);
        for (int i = 0; i < bucketNum; i++) {
            result.add(new ArrayList<>(backendsPerBucketSeq.get(i % backendsPerBucketSeq.size())));
        }
        return result;
    }

    public static <T extends Iterable<Long>> List<Set<Long>> getSlaveBackendsPerBucketSeqSet(
            List<T> backendsPerBucketSeq, int bucketNum) {
        if (backendsPerBucketSeq == null || backendsPerBucketSeq.isEmpty()) {
            return Lists.newArrayList();
        }
        Preconditions.checkArgument(
                bucketNum >= backendsPerBucketSeq.size() && bucketNum % backendsPerBucketSeq.size() == 0);
        List<Set<Long>> list = Lists.newArrayList();
        // Merge backend ids of all tags
        for (int i = 0; i < bucketNum; ++i) {
            list.add(Sets.newHashSet(backendsPerBucketSeq.get(i % backendsPerBucketSeq.size())));
        }
        return list;
    }
}

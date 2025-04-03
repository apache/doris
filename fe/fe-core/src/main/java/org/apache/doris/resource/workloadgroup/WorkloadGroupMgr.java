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

package org.apache.doris.resource.workloadgroup;

import org.apache.doris.analysis.AlterWorkloadGroupStmt;
import org.apache.doris.analysis.CreateWorkloadGroupStmt;
import org.apache.doris.analysis.DropWorkloadGroupStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.DropWorkloadGroupOperationLog;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TUserIdentity;
import org.apache.doris.thrift.TopicInfo;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadGroupMgr extends MasterDaemon implements Writable, GsonPostProcessable {

    public static final String DEFAULT_GROUP_NAME = "normal";

    public static final Long DEFAULT_GROUP_ID = 1L;

    public static final ImmutableList<String> WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Id").add("Name").add(WorkloadGroup.CPU_SHARE).add(WorkloadGroup.MEMORY_LIMIT)
            .add(WorkloadGroup.ENABLE_MEMORY_OVERCOMMIT)
            .add(WorkloadGroup.WRITE_BUFFER_RATIO)
            .add(WorkloadGroup.SLOT_MEMORY_POLICY)
            .add(WorkloadGroup.MAX_CONCURRENCY).add(WorkloadGroup.MAX_QUEUE_SIZE)
            .add(WorkloadGroup.QUEUE_TIMEOUT).add(WorkloadGroup.CPU_HARD_LIMIT)
            .add(WorkloadGroup.SCAN_THREAD_NUM).add(WorkloadGroup.MAX_REMOTE_SCAN_THREAD_NUM)
            .add(WorkloadGroup.MIN_REMOTE_SCAN_THREAD_NUM)
            .add(WorkloadGroup.MEMORY_LOW_WATERMARK).add(WorkloadGroup.MEMORY_HIGH_WATERMARK)
            .add(WorkloadGroup.COMPUTE_GROUP)
            .add(WorkloadGroup.READ_BYTES_PER_SECOND).add(WorkloadGroup.REMOTE_READ_BYTES_PER_SECOND)
            .add(QueryQueue.RUNNING_QUERY_NUM).add(QueryQueue.WAITING_QUERY_NUM)
            .build();

    private static final Logger LOG = LogManager.getLogger(WorkloadGroupMgr.class);
    @SerializedName(value = "idToWorkloadGroup")
    private final Map<Long, WorkloadGroup> idToWorkloadGroup = Maps.newHashMap();
    private final Map<Pair<String, String>, WorkloadGroup> nameToWorkloadGroup = Maps.newHashMap();
    private final Map<Long, QueryQueue> idToQueryQueue = Maps.newHashMap();
    private final ResourceProcNode procNode = new ResourceProcNode();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static final String EMPTY_COMPUTE_GROUP = "";

    @Override
    protected void runAfterCatalogReady() {
        try {
            resetQueryQueueProp();
        } catch (Throwable e) {
            LOG.warn("reset query queue failed ", e);
        }
    }

    public void resetQueryQueueProp() {
        List<QueryQueue> newPropList = new ArrayList<>();
        Map<Long, QueryQueue> currentQueueCopyMap = new HashMap<>();
        readLock();
        try {
            for (Map.Entry<Long, WorkloadGroup> entry : idToWorkloadGroup.entrySet()) {
                WorkloadGroup wg = entry.getValue();
                QueryQueue tmpQ = new QueryQueue(wg.getId(), wg.getMaxConcurrency(),
                        wg.getMaxQueueSize(), wg.getQueueTimeout(), wg.getVersion());
                newPropList.add(tmpQ);
            }
            for (Map.Entry<Long, QueryQueue> entry : idToQueryQueue.entrySet()) {
                currentQueueCopyMap.put(entry.getKey(), entry.getValue());
            }
        } finally {
            readUnlock();
        }

        for (QueryQueue newPropQq : newPropList) {
            QueryQueue currentQueryQueue = currentQueueCopyMap.get(newPropQq.getWgId());
            if (currentQueryQueue == null) {
                continue;
            }
            if (newPropQq.getPropVersion() > currentQueryQueue.getPropVersion()) {
                currentQueryQueue.resetQueueProperty(newPropQq.getMaxConcurrency(), newPropQq.getMaxQueueSize(),
                        newPropQq.getQueueTimeout(), newPropQq.getPropVersion());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(currentQueryQueue.debugString()); // for test debug
            }
        }
    }

    public WorkloadGroupMgr() {
        super("workload-group-thread", Config.query_queue_update_interval_ms);
    }

    public static WorkloadGroupMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        WorkloadGroupMgr ret = GsonUtils.GSON.fromJson(json, WorkloadGroupMgr.class);
        return ret;
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private WorkloadGroup getWorkloadGroupByComputeGroupUnlock(String cgName, String wgName)
            throws DdlException {
        WorkloadGroup wg = nameToWorkloadGroup.get(Pair.of(cgName, wgName));
        if (wg == null) {
            throw new DdlException(
                    "Can not find workload group " + cgName + " in compute group " + wgName + ".");
        }
        return wg;
    }

    public List<TPipelineWorkloadGroup> getWorkloadGroup(ConnectContext context) throws UserException {
        String wgName = getWorkloadGroupNameAndCheckPriv(context);
        Set<String> cgNames = context.getComputeGroup().getNames();

        List<TPipelineWorkloadGroup> workloadGroups = Lists.newArrayList();
        readLock();
        try {
            for (String cgName : cgNames) {
                WorkloadGroup workloadGroup = getWorkloadGroupByComputeGroupUnlock(cgName, wgName);
                workloadGroups.add(workloadGroup.toThrift());
            }
            context.setWorkloadGroupName(wgName);
        } finally {
            readUnlock();
        }
        return workloadGroups;
    }

    public List<TPipelineWorkloadGroup> getTWorkloadGroupById(long wgId) {
        List<TPipelineWorkloadGroup> tWorkloadGroups = Lists.newArrayList();
        readLock();
        try {
            WorkloadGroup wg = idToWorkloadGroup.get(wgId);
            if (wg != null) {
                tWorkloadGroups.add(wg.toThrift());
            }
        } finally {
            readUnlock();
        }
        return tWorkloadGroups;
    }

    public List<TopicInfo> getPublishTopicInfo() {
        List<TopicInfo> workloadGroups = new ArrayList();
        readLock();
        try {
            for (WorkloadGroup wg : idToWorkloadGroup.values()) {
                workloadGroups.add(wg.toTopicInfo());
            }
        } finally {
            readUnlock();
        }
        return workloadGroups;
    }

    public QueryQueue getWorkloadGroupQueryQueue(Set<Long> wgIdSet) throws UserException {
        writeLock();
        try {
            QueryQueue queryQueue = null;
            for (long wgId : wgIdSet) {
                WorkloadGroup wg = idToWorkloadGroup.get(wgId);
                if (wg == null) {
                    continue;
                }
                QueryQueue tmpQueue = idToQueryQueue.get(wg.getId());
                if (tmpQueue == null) {
                    tmpQueue = new QueryQueue(wg.getId(), wg.getMaxConcurrency(), wg.getMaxQueueSize(),
                            wg.getQueueTimeout(), wg.getVersion());
                    idToQueryQueue.put(wg.getId(), tmpQueue);
                    queryQueue = tmpQueue;
                    break;
                }
                if (queryQueue == null) {
                    queryQueue = tmpQueue;
                } else {
                    Pair<Integer, Integer> detail1 = queryQueue.getQueryQueueDetail();
                    Pair<Integer, Integer> detail2 = tmpQueue.getQueryQueueDetail();
                    if (detail2.first < detail1.first) {
                        queryQueue = tmpQueue;
                    }
                }
            }
            if (queryQueue == null) {
                throw new DdlException("Can not find query queue for workload group: " + wgIdSet);
            }
            return queryQueue;
        } finally {
            writeUnlock();
        }
    }

    public Map<String, List<String>> getWorkloadGroupQueryDetail() {
        Map<String, List<String>> ret = Maps.newHashMap();
        readLock();
        try {
            for (Map.Entry<Long, WorkloadGroup> entry : idToWorkloadGroup.entrySet()) {
                Long wgId = entry.getKey();
                WorkloadGroup wg = entry.getValue();
                QueryQueue qq = idToQueryQueue.get(wgId);
                List<String> valueList = new ArrayList<>(2);
                if (qq == null) {
                    valueList.add("0");
                    valueList.add("0");
                } else {
                    Pair<Integer, Integer> qdtail = qq.getQueryQueueDetail();
                    valueList.add(String.valueOf(qdtail.first));
                    valueList.add(String.valueOf(qdtail.second));
                }
                ret.put(wg.getName(), valueList);
            }
        } finally {
            readUnlock();
        }
        return ret;
    }

    private String getWorkloadGroupNameAndCheckPriv(ConnectContext context) throws AnalysisException {
        String groupName = context.getSessionVariable().getWorkloadGroup();
        if (Strings.isNullOrEmpty(groupName)) {
            groupName = Env.getCurrentEnv().getAuth().getWorkloadGroup(context.getQualifiedUser());
        }
        if (Strings.isNullOrEmpty(groupName)) {
            groupName = DEFAULT_GROUP_NAME;
        }
        if (!Env.getCurrentEnv().getAccessManager().checkWorkloadGroupPriv(context, groupName, PrivPredicate.USAGE)) {
            ErrorReport.reportAnalysisException(
                    "Access denied; you need (at least one of) the %s privilege(s) to use workload group '%s'.",
                    ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "USAGE/ADMIN", groupName);
        }
        return groupName;
    }

    public void createWorkloadGroup(String computeGroup, WorkloadGroup workloadGroup, boolean isIfNotExists)
            throws DdlException {
        String workloadGroupName = workloadGroup.getName();
        writeLock();
        try {
            Pair<String, String> key = Pair.of(computeGroup, workloadGroupName);
            if (nameToWorkloadGroup.containsKey(key)) {
                if (isIfNotExists) {
                    return;
                }
                throw new DdlException(
                        "Compute group " + key.first + " already has workload group " + key.second + ".");
            }
            if (idToWorkloadGroup.size() >= Config.workload_group_max_num) {
                throw new DdlException(
                        "Workload group number can not be exceed " + Config.workload_group_max_num);
            }
            checkGlobalUnlock(workloadGroup, null);
            nameToWorkloadGroup.put(key, workloadGroup);
            idToWorkloadGroup.put(workloadGroup.getId(), workloadGroup);
            Env.getCurrentEnv().getEditLog().logCreateWorkloadGroup(workloadGroup);
        } finally {
            writeUnlock();
        }
        LOG.info("Create workload group {} for compute group {} success.", workloadGroup, computeGroup);
    }

    public void createWorkloadGroup(CreateWorkloadGroupStmt stmt) throws DdlException {
        throw new DdlException("Unsupported create statement");
    }

    // NOTE: used for checking sum value of 100%  for cpu_hard_limit and memory_limit
    //  when create/alter workload group with same tag.
    //  when oldWg is null it means caller is an alter stmt.
    private void checkGlobalUnlock(WorkloadGroup newWg, WorkloadGroup oldWg) throws DdlException {
        String newWgCg = newWg.getComputeGroup();

        double sumOfAllMemLimit = 0;
        int sumOfAllCpuHardLimit = 0;

        // 1 get sum value of all wg which has same tag without current wg
        for (Map.Entry<Long, WorkloadGroup> entry : idToWorkloadGroup.entrySet()) {
            WorkloadGroup wg = entry.getValue();
            String curWgCg = wg.getComputeGroup();

            if (oldWg != null && entry.getKey() == oldWg.getId()) {
                continue;
            }

            if (!newWgCg.equals(curWgCg)) {
                continue;
            }

            if (wg.getCpuHardLimitWhenCalSum() > 0) {
                sumOfAllCpuHardLimit += wg.getCpuHardLimitWhenCalSum();
            }
            if (wg.getMemoryLimitPercentWhenCalSum() > 0) {
                sumOfAllMemLimit += wg.getMemoryLimitPercentWhenCalSum();
            }
        }

        // 2 sum current wg value
        sumOfAllMemLimit += newWg.getMemoryLimitPercentWhenCalSum();
        sumOfAllCpuHardLimit += newWg.getCpuHardLimitWhenCalSum();

        // 3 check total sum
        if (sumOfAllMemLimit > 100.0 + 1e-6) {
            throw new DdlException(
                    "The sum of all workload group " + WorkloadGroup.MEMORY_LIMIT + " within compute group " + (
                            newWgCg)
                            + " can not be greater than 100.0%. current sum val:" + sumOfAllMemLimit);
        }

        if (sumOfAllCpuHardLimit > 100) {
            throw new DdlException(
                    "The sum of all workload group " + WorkloadGroup.CPU_HARD_LIMIT + " within compute group " + newWgCg
                            + " can not be greater than 100%. current sum val:" + sumOfAllCpuHardLimit);
        }
    }

    public void alterWorkloadGroup(AlterWorkloadGroupStmt stmt) throws DdlException {
        throw new DdlException("Unsupported alter statement");
    }

    public void alterWorkloadGroup(String computeGroup, String workloadGroupName, Map<String, String> properties)
            throws DdlException {
        if (properties.size() == 0) {
            throw new DdlException("Alter workload group should contain at least one property");
        }

        WorkloadGroup newWorkloadGroup;
        writeLock();
        try {
            WorkloadGroup currentWorkloadGroup = getWorkloadGroupByComputeGroupUnlock(computeGroup, workloadGroupName);
            newWorkloadGroup = WorkloadGroup.copyAndUpdate(currentWorkloadGroup, properties);
            checkGlobalUnlock(newWorkloadGroup, currentWorkloadGroup);
            nameToWorkloadGroup.put(Pair.of(computeGroup, workloadGroupName), newWorkloadGroup);
            idToWorkloadGroup.put(newWorkloadGroup.getId(), newWorkloadGroup);
            Env.getCurrentEnv().getEditLog().logAlterWorkloadGroup(newWorkloadGroup);
        } finally {
            writeUnlock();
        }
        LOG.info("Alter workload group {} for compute group {} success: {}", newWorkloadGroup, computeGroup);
    }

    public void dropWorkloadGroup(DropWorkloadGroupStmt stmt) throws DdlException {
        throw new DdlException("Unsupported drop statement.");
    }

    public void dropWorkloadGroup(String computeGroup, String workloadGroupName, boolean ifExists)
            throws DdlException {
        if (Tag.DEFAULT_BACKEND_TAG.value.equals(computeGroup) && DEFAULT_GROUP_NAME.equals(workloadGroupName)) {
            throw new DdlException(
                    "Dropping workload group " + workloadGroupName + " for " + computeGroup + "is not allowed.");
        }

        // if a workload group exists in user property, it should not be dropped
        // user need to reset user property first
        Pair<Boolean, String> ret = Env.getCurrentEnv().getAuth().isWorkloadGroupInUse(workloadGroupName);
        if (ret.first) {
            throw new DdlException("Workload group " + workloadGroupName + " is set for user " + ret.second
                    + ", you can reset the user's property(eg, "
                    + "set property for " + ret.second + " 'default_workload_group'='xxx'; ), "
                    + "then you can drop the group.");
        }

        // A group with related policies should not be deleted.
        Long wgId = null;
        readLock();
        try {
            WorkloadGroup wg = nameToWorkloadGroup.get(Pair.of(computeGroup, workloadGroupName));
            if (wg != null) {
                wgId = wg.getId();
            }
        } finally {
            readUnlock();
        }
        if (wgId != null) {
            boolean groupHasPolicy = Env.getCurrentEnv().getWorkloadSchedPolicyMgr()
                    .checkWhetherGroupHasPolicy(wgId.longValue());
            if (groupHasPolicy) {
                throw new DdlException(
                        "Workload group " + workloadGroupName + " can't be dropped, because it has related policy");
            }
        }

        Pair<String, String> key = Pair.of(computeGroup, workloadGroupName);
        writeLock();
        try {
            if (!nameToWorkloadGroup.containsKey(key)) {
                if (ifExists) {
                    return;
                }
                throw new DdlException(
                        "Can not find workload group " + workloadGroupName + " in compute group " + computeGroup + ".");
            }
            WorkloadGroup workloadGroup = nameToWorkloadGroup.get(key);
            long groupId = workloadGroup.getId();
            idToWorkloadGroup.remove(groupId);
            nameToWorkloadGroup.remove(key);
            idToQueryQueue.remove(groupId);
            Env.getCurrentEnv().getEditLog().logDropWorkloadGroup(new DropWorkloadGroupOperationLog(groupId));
        } finally {
            writeUnlock();
        }
        LOG.info("Drop workload group success: {} for compute group {}", workloadGroupName, computeGroup);
    }

    private void insertWorkloadGroup(WorkloadGroup workloadGroup) {
        writeLock();
        try {
            LOG.info("[init_wg] before,  name map, {}, id map: {}, name map: {}", "replay",
                    idToWorkloadGroup, nameToWorkloadGroup);
            idToWorkloadGroup.put(workloadGroup.getId(), workloadGroup);
            Pair<String, String> key = Pair.of(workloadGroup.getComputeGroup(), workloadGroup.getName());
            nameToWorkloadGroup.put(key, workloadGroup);
            LOG.info("[init_wg] after,  name map, {}, id map: {}, name map: {}", "replay",
                    idToWorkloadGroup, nameToWorkloadGroup);
        } finally {
            writeUnlock();
        }
    }

    public boolean isWorkloadGroupExists(String workloadGroupName) {
        readLock();
        try {
            return nameToWorkloadGroup.containsKey(workloadGroupName);
        } finally {
            readUnlock();
        }
    }

    public void replayCreateWorkloadGroup(WorkloadGroup workloadGroup) {
        insertWorkloadGroup(workloadGroup);
    }

    public void replayAlterWorkloadGroup(WorkloadGroup workloadGroup) {
        insertWorkloadGroup(workloadGroup);
    }

    public void replayDropWorkloadGroup(DropWorkloadGroupOperationLog operationLog) {
        long id = operationLog.getId();
        writeLock();
        try {
            if (!idToWorkloadGroup.containsKey(id)) {
                return;
            }
            WorkloadGroup workloadGroup = idToWorkloadGroup.get(id);
            nameToWorkloadGroup.remove(workloadGroup.getName());
            idToWorkloadGroup.remove(id);
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> getResourcesInfo(PatternMatcher matcher) {
        UserIdentity currentUserIdentity = ConnectContext.get().getCurrentUserIdentity();
        List<List<String>> rows = procNode.fetchResult(currentUserIdentity).getRows();
        for (Iterator<List<String>> it = rows.iterator(); it.hasNext(); ) {
            List<String> row = it.next();
            if (matcher != null && !matcher.match(row.get(1))) {
                it.remove();
            }
        }
        return rows;
    }

    public List<List<String>> getResourcesInfo(TUserIdentity tCurrentUserIdentity) {
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(tCurrentUserIdentity);
        return procNode.fetchResult(currentUserIdentity).getRows();
    }

    public Map<Long, String> getIdToNameMap() {
        Map<Long, String> ret = Maps.newHashMap();
        readLock();
        try {
            for (Map.Entry<Long, WorkloadGroup> entry : idToWorkloadGroup.entrySet()) {
                ret.put(entry.getKey(), entry.getValue().getName());
            }
            return ret;
        } finally {
            readUnlock();
        }
    }

    public String getWorkloadGroupNameById(Long id) {
        readLock();
        try {
            WorkloadGroup wg = idToWorkloadGroup.get(id);
            if (wg == null) {
                return null;
            }
            return wg.getName();
        } finally {
            readUnlock();
        }
    }

    // for ut
    public Map<Pair<String, String>, WorkloadGroup> getNameToWorkloadGroup() {
        return nameToWorkloadGroup;
    }

    // for ut
    public Map<Long, WorkloadGroup> getIdToWorkloadGroup() {
        return idToWorkloadGroup;
    }

    // for ut
    public Map<Long, QueryQueue> getIdToQueryQueue() {
        return idToQueryQueue;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        LOG.info("[init_wg] before,  name map, {}, id map: {}, name map: {}", "gson",
                idToWorkloadGroup, nameToWorkloadGroup);
        for (Map.Entry<Long, WorkloadGroup> entry : idToWorkloadGroup.entrySet()) {
            String computeGroupName = entry.getValue().getComputeGroup();
            nameToWorkloadGroup.put(Pair.of(computeGroupName, entry.getValue().getName()), entry.getValue());
        }
        LOG.info("[init_wg] after,  name map, {}, id map: {}, name map: {}", "gson",
                idToWorkloadGroup, nameToWorkloadGroup);
    }

    public class ResourceProcNode {
        public ProcResult fetchResult(UserIdentity currentUserIdentity) {
            BaseProcResult result = new BaseProcResult();
            result.setNames(WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES);
            readLock();
            try {
                for (WorkloadGroup workloadGroup : idToWorkloadGroup.values()) {
                    if (!Env.getCurrentEnv().getAccessManager().checkWorkloadGroupPriv(currentUserIdentity,
                            workloadGroup.getName(), PrivPredicate.SHOW_WORKLOAD_GROUP)) {
                        continue;
                    }
                    workloadGroup.getProcNodeData(result, idToQueryQueue.get(workloadGroup.getId()));
                }
            } finally {
                readUnlock();
            }
            return result;
        }
    }


    public List<WorkloadGroup> getOldWorkloadGroup() {
        List<WorkloadGroup> oldWgList = Lists.newArrayList();
        readLock();
        try {
            for (Map.Entry<Pair<String, String>, WorkloadGroup> entry : nameToWorkloadGroup.entrySet()) {
                if (EMPTY_COMPUTE_GROUP.equals(entry.getKey().first)) {
                    oldWgList.add(entry.getValue());
                }
            }
        } finally {
            readUnlock();
        }
        return oldWgList;
    }

    public void tryCreateNormalWorkloadGroup() {
        writeLock();
        try {
            LOG.info("[init_wg] before create normal wg, id map: {}, name map: {}", idToWorkloadGroup,
                    nameToWorkloadGroup);
            if (idToWorkloadGroup.isEmpty()) {
                String defaultCgName = Config.isCloudMode() ? Tag.VALUE_DEFAULT_COMPUTE_GROUP_NAME
                        : Tag.VALUE_DEFAULT_TAG;
                Map<String, String> properties = Maps.newHashMap();
                properties.put(WorkloadGroup.ENABLE_MEMORY_OVERCOMMIT, "true");
                properties.put(WorkloadGroup.COMPUTE_GROUP, defaultCgName);
                WorkloadGroup defaultWg = new WorkloadGroup(Env.getCurrentEnv().getNextId(), DEFAULT_GROUP_NAME,
                        properties);
                idToWorkloadGroup.put(defaultWg.getId(), defaultWg);
                nameToWorkloadGroup.put(Pair.of(defaultWg.getComputeGroup(), defaultWg.getName()), defaultWg);
                Env.getCurrentEnv().getEditLog().logCreateWorkloadGroup(defaultWg);
                LOG.info("[init_wg]Create default workload group success: {}", defaultWg);
            } else {
                LOG.info("[init_wg]This is not a new cluster, skip create default wg");
            }
            LOG.info("[init_wg] after create normal wg, id map: {}, name map: {}", idToWorkloadGroup,
                    nameToWorkloadGroup);
        } finally {
            writeUnlock();
        }
    }

    public void bindWorkloadGroupToComputeGroup(Set<String> cgSet, WorkloadGroup oldWg) {
        writeLock();
        try {
            Pair<String, String> oldKey = Pair.of(EMPTY_COMPUTE_GROUP, oldWg.getName());
            // it means old compute group has been dropped, just return;
            if (!nameToWorkloadGroup.containsKey(oldKey)) {
                LOG.info("[init_wg]Old workload group {} has been dropped, skip it.", oldWg.getName());
                return;
            }
            // create new workload group for all compute group.
            for (String computeGroup : cgSet) {
                Pair<String, String> newKey = Pair.of(computeGroup, oldWg.getName());
                if (nameToWorkloadGroup.containsKey(newKey)) {
                    LOG.info("[init_wg]Workload group {} already exists in compute group {}, skip it.",
                            oldWg.getName(), computeGroup);
                    continue;
                }
                Map<String, String> newProp = Maps.newHashMap();
                for (Map.Entry<String, String> entry : oldWg.getProperties().entrySet()) {
                    newProp.put(entry.getKey(), entry.getValue());
                }
                newProp.put(WorkloadGroup.COMPUTE_GROUP, computeGroup);
                WorkloadGroup newWg = new WorkloadGroup(Env.getCurrentEnv().getNextId(), oldWg.getName(),
                        newProp);
                nameToWorkloadGroup.put(newKey, newWg);
                idToWorkloadGroup.put(newWg.getId(), newWg);
                Env.getCurrentEnv().getEditLog().logCreateWorkloadGroup(newWg);
                LOG.info("[init_wg]Create workload group {} for compute group {} success.", oldWg.getName(),
                        computeGroup);
            }

            // drop old workload group
            nameToWorkloadGroup.remove(oldKey);
            idToWorkloadGroup.remove(oldWg.getId());
            idToQueryQueue.remove(oldWg.getId());
            Env.getCurrentEnv().getEditLog().logDropWorkloadGroup(new DropWorkloadGroupOperationLog(oldWg.getId()));
            LOG.info("[init_wg]Drop old workload group {} success.", oldWg);
        } catch (Throwable t) {
            LOG.error("[init_wg]Error happens when drop old workload group, {}, {}", cgSet, oldWg.getName(), t);
        } finally {
            writeUnlock();
        }
    }

}

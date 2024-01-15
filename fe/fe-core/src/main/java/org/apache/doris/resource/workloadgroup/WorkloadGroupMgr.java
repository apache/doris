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
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.DropWorkloadGroupOperationLog;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadGroupMgr implements Writable, GsonPostProcessable {

    public static final String DEFAULT_GROUP_NAME = "normal";
    public static final ImmutableList<String> WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Id").add("Name").add(WorkloadGroup.CPU_SHARE).add(WorkloadGroup.MEMORY_LIMIT)
            .add(WorkloadGroup.ENABLE_MEMORY_OVERCOMMIT)
            .add(WorkloadGroup.MAX_CONCURRENCY).add(WorkloadGroup.MAX_QUEUE_SIZE)
            .add(WorkloadGroup.QUEUE_TIMEOUT).add(WorkloadGroup.CPU_HARD_LIMIT)
            .add(QueryQueue.RUNNING_QUERY_NUM).add(QueryQueue.WAITING_QUERY_NUM)
            .build();

    private static final Logger LOG = LogManager.getLogger(WorkloadGroupMgr.class);
    @SerializedName(value = "idToWorkloadGroup")
    private final Map<Long, WorkloadGroup> idToWorkloadGroup = Maps.newHashMap();
    private final Map<String, WorkloadGroup> nameToWorkloadGroup = Maps.newHashMap();
    private final Map<Long, QueryQueue> idToQueryQueue = Maps.newHashMap();
    private final ResourceProcNode procNode = new ResourceProcNode();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Thread updatePropThread;

    public void startUpdateThread() {
        WorkloadGroupMgr wgMgr = this;
        updatePropThread = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        wgMgr.resetQueryQueueProp();
                        Thread.sleep(Config.query_queue_update_interval_ms);
                    } catch (Throwable e) {
                        LOG.warn("reset query queue failed ", e);
                    }
                }
            }
        });
        updatePropThread.start();
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
            LOG.debug(currentQueryQueue.debugString()); // for test debug
        }
    }

    public WorkloadGroupMgr() {
    }

    public static WorkloadGroupMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WorkloadGroupMgr.class);
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

    public void init() {
        if (Config.enable_workload_group || Config.use_fuzzy_session_variable /* for github workflow */) {
            checkAndCreateDefaultGroup();
        }
    }

    public List<TPipelineWorkloadGroup> getWorkloadGroup(ConnectContext context) throws UserException {
        String groupName = getWorkloadGroupNameAndCheckPriv(context);
        List<TPipelineWorkloadGroup> workloadGroups = Lists.newArrayList();
        readLock();
        try {
            WorkloadGroup workloadGroup = nameToWorkloadGroup.get(groupName);
            if (workloadGroup == null) {
                throw new UserException("Workload group " + groupName + " does not exist");
            }
            workloadGroups.add(workloadGroup.toThrift());
            context.setWorkloadGroupName(groupName);
        } finally {
            readUnlock();
        }
        return workloadGroups;
    }

    public WorkloadGroup getWorkloadGroupById(long wgId) {
        readLock();
        try {
            return idToWorkloadGroup.get(wgId);
        } finally {
            readUnlock();
        }
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

    public QueryQueue getWorkloadGroupQueryQueue(ConnectContext context) throws UserException {
        String groupName = getWorkloadGroupNameAndCheckPriv(context);
        writeLock();
        try {
            WorkloadGroup wg = nameToWorkloadGroup.get(groupName);
            if (wg == null) {
                throw new UserException("Workload group " + groupName + " does not exist");
            }
            QueryQueue queryQueue = idToQueryQueue.get(wg.getId());
            if (queryQueue == null) {
                queryQueue = new QueryQueue(wg.getId(), wg.getMaxConcurrency(), wg.getMaxQueueSize(),
                        wg.getQueueTimeout(), wg.getVersion());
                idToQueryQueue.put(wg.getId(), queryQueue);
            }
            return queryQueue;
        } finally {
            writeUnlock();
        }
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

    private void checkAndCreateDefaultGroup() {
        WorkloadGroup defaultWorkloadGroup = null;
        writeLock();
        try {
            if (nameToWorkloadGroup.containsKey(DEFAULT_GROUP_NAME)) {
                return;
            }
            Map<String, String> properties = Maps.newHashMap();
            properties.put(WorkloadGroup.CPU_SHARE, "1024");
            properties.put(WorkloadGroup.MEMORY_LIMIT, "30%");
            properties.put(WorkloadGroup.ENABLE_MEMORY_OVERCOMMIT, "true");
            defaultWorkloadGroup = WorkloadGroup.create(DEFAULT_GROUP_NAME, properties);
            nameToWorkloadGroup.put(DEFAULT_GROUP_NAME, defaultWorkloadGroup);
            idToWorkloadGroup.put(defaultWorkloadGroup.getId(), defaultWorkloadGroup);
            Env.getCurrentEnv().getEditLog().logCreateWorkloadGroup(defaultWorkloadGroup);
        } catch (DdlException e) {
            LOG.warn("Create workload group " + DEFAULT_GROUP_NAME + " fail");
        } finally {
            writeUnlock();
        }
        LOG.info("Create workload group success: {}", defaultWorkloadGroup);
    }

    public void createWorkloadGroup(CreateWorkloadGroupStmt stmt) throws DdlException {
        WorkloadGroup workloadGroup = WorkloadGroup.create(stmt.getWorkloadGroupName(), stmt.getProperties());
        String workloadGroupName = workloadGroup.getName();
        writeLock();
        try {
            if (nameToWorkloadGroup.containsKey(workloadGroupName)) {
                if (stmt.isIfNotExists()) {
                    return;
                }
                throw new DdlException("workload group " + workloadGroupName + " already exist");
            }
            if (idToWorkloadGroup.size() >= Config.workload_group_max_num) {
                throw new DdlException(
                        "workload group number can not be exceed " + Config.workload_group_max_num);
            }
            checkGlobalUnlock(workloadGroup, null);
            nameToWorkloadGroup.put(workloadGroupName, workloadGroup);
            idToWorkloadGroup.put(workloadGroup.getId(), workloadGroup);
            Env.getCurrentEnv().getEditLog().logCreateWorkloadGroup(workloadGroup);
        } finally {
            writeUnlock();
        }
        LOG.info("Create workload group success: {}", workloadGroup);
    }

    private void checkGlobalUnlock(WorkloadGroup workloadGroup, WorkloadGroup old) throws DdlException {
        double totalMemoryLimit = idToWorkloadGroup.values().stream().mapToDouble(WorkloadGroup::getMemoryLimitPercent)
                .sum() + workloadGroup.getMemoryLimitPercent();
        if (!Objects.isNull(old)) {
            totalMemoryLimit -= old.getMemoryLimitPercent();
        }
        if (totalMemoryLimit > 100.0 + 1e-6) {
            throw new DdlException(
                    "The sum of all workload group " + WorkloadGroup.MEMORY_LIMIT + " cannot be greater than 100.0%.");
        }

        // 1, check new group
        int newGroupCpuHardLimit = workloadGroup.getCpuHardLimit();
        if (newGroupCpuHardLimit > 100 || newGroupCpuHardLimit < 0) {
            throw new DdlException(
                    "new group's " + WorkloadGroup.CPU_HARD_LIMIT
                            + " value can not be greater than 100% or less than or equal 0%");
        }

        // 2, check sum of all cpu hard limit
        int sumOfAllCpuHardLimit = 0;
        for (Map.Entry<Long, WorkloadGroup> entry : idToWorkloadGroup.entrySet()) {
            if (old != null && entry.getKey() == old.getId()) {
                continue;
            }
            sumOfAllCpuHardLimit += entry.getValue().getCpuHardLimit();
        }
        sumOfAllCpuHardLimit += newGroupCpuHardLimit;

        if (sumOfAllCpuHardLimit > 100) {
            throw new DdlException("sum of all workload group " + WorkloadGroup.CPU_HARD_LIMIT
                    + " can not be greater than 100% ");
        }
    }

    public void alterWorkloadGroup(AlterWorkloadGroupStmt stmt) throws DdlException {
        String workloadGroupName = stmt.getWorkloadGroupName();
        Map<String, String> properties = stmt.getProperties();
        WorkloadGroup newWorkloadGroup;
        writeLock();
        try {
            if (!nameToWorkloadGroup.containsKey(workloadGroupName)) {
                throw new DdlException("workload group(" + workloadGroupName + ") does not exist.");
            }
            WorkloadGroup currentWorkloadGroup = nameToWorkloadGroup.get(workloadGroupName);
            newWorkloadGroup = WorkloadGroup.copyAndUpdate(currentWorkloadGroup, properties);
            checkGlobalUnlock(newWorkloadGroup, currentWorkloadGroup);
            nameToWorkloadGroup.put(workloadGroupName, newWorkloadGroup);
            idToWorkloadGroup.put(newWorkloadGroup.getId(), newWorkloadGroup);
            Env.getCurrentEnv().getEditLog().logAlterWorkloadGroup(newWorkloadGroup);
        } finally {
            writeUnlock();
        }
        LOG.info("Alter resource success: {}", newWorkloadGroup);
    }

    public void dropWorkloadGroup(DropWorkloadGroupStmt stmt) throws DdlException {
        String workloadGroupName = stmt.getWorkloadGroupName();
        if (DEFAULT_GROUP_NAME.equals(workloadGroupName)) {
            throw new DdlException("Dropping default workload group " + workloadGroupName + " is not allowed");
        }

        // if a workload group exists in user property, it should not be dropped
        // user need to reset user property first
        Pair<Boolean, String> ret = Env.getCurrentEnv().getAuth().isWorkloadGroupInUse(workloadGroupName);
        if (ret.first) {
            throw new DdlException("workload group " + workloadGroupName + " is set for user " + ret.second);
        }

        writeLock();
        try {
            if (!nameToWorkloadGroup.containsKey(workloadGroupName)) {
                if (stmt.isIfExists()) {
                    return;
                }
                throw new DdlException("workload group " + workloadGroupName + " does not exist");
            }
            WorkloadGroup workloadGroup = nameToWorkloadGroup.get(workloadGroupName);
            long groupId = workloadGroup.getId();
            idToWorkloadGroup.remove(groupId);
            nameToWorkloadGroup.remove(workloadGroupName);
            idToQueryQueue.remove(groupId);
            Env.getCurrentEnv().getEditLog().logDropWorkloadGroup(new DropWorkloadGroupOperationLog(groupId));
        } finally {
            writeUnlock();
        }
        LOG.info("Drop workload group success: {}", workloadGroupName);
    }

    private void insertWorkloadGroup(WorkloadGroup workloadGroup) {
        writeLock();
        try {
            nameToWorkloadGroup.put(workloadGroup.getName(), workloadGroup);
            idToWorkloadGroup.put(workloadGroup.getId(), workloadGroup);
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

    public List<List<String>> getResourcesInfo() {
        UserIdentity currentUserIdentity = ConnectContext.get().getCurrentUserIdentity();
        return procNode.fetchResult(currentUserIdentity).getRows();
    }

    public List<List<String>> getResourcesInfo(TUserIdentity tcurrentUserIdentity) {
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(tcurrentUserIdentity);
        return procNode.fetchResult(currentUserIdentity).getRows();
    }

    public Long getWorkloadGroupIdByName(String name) {
        readLock();
        try {
            WorkloadGroup wg = nameToWorkloadGroup.get(name);
            if (wg == null) {
                return null;
            }
            return wg.getId();
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
    public Map<String, WorkloadGroup> getNameToWorkloadGroup() {
        return nameToWorkloadGroup;
    }

    // for ut
    public Map<Long, WorkloadGroup> getIdToWorkloadGroup() {
        return idToWorkloadGroup;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        idToWorkloadGroup.forEach(
                (id, workloadGroup) -> nameToWorkloadGroup.put(workloadGroup.getName(), workloadGroup));
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
}

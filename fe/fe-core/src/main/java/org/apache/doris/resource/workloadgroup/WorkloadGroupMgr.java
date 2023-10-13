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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadGroupMgr implements Writable, GsonPostProcessable {

    public static final String DEFAULT_GROUP_NAME = "normal";
    public static final ImmutableList<String> WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Id").add("Name").add("Item").add("Value")
            .build();

    private static final Logger LOG = LogManager.getLogger(WorkloadGroupMgr.class);
    @SerializedName(value = "idToWorkloadGroup")
    private final Map<Long, WorkloadGroup> idToWorkloadGroup = Maps.newHashMap();
    private final Map<String, WorkloadGroup> nameToWorkloadGroup = Maps.newHashMap();
    private final ResourceProcNode procNode = new ResourceProcNode();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static final String QUERY_CPU_HARD_LIMIT = "query_cpu_hard_limit";
    private int queryCPUHardLimit = 0;
    // works when user not set cpu hard limit, we fill a default value
    private int cpuHardLimitDefaultVal = 0;

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

    private void checkWorkloadGroupEnabled() throws DdlException {
        if (!Config.enable_workload_group) {
            throw new DdlException(
                    "WorkloadGroup is disabled, you can set config enable_workload_group = true to enable it");
        }
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
            // note(wb) -1 to tell be no need to update cgroup
            int thriftVal = -1;
            if (Config.enable_cpu_hard_limit) {
                // reset cpu_share according to cpu hard limit
                int cpuHardLimitShare = workloadGroup.getCpuHardLimit() == 0
                        ? this.cpuHardLimitDefaultVal : workloadGroup.getCpuHardLimit();
                workloadGroups.get(0).getProperties()
                        .put(WorkloadGroup.CPU_SHARE, String.valueOf(cpuHardLimitShare));

                // reset sum of all groups cpu hard limit
                thriftVal = this.queryCPUHardLimit;
            }
            workloadGroups.get(0).getProperties().put(QUERY_CPU_HARD_LIMIT, String.valueOf(thriftVal));
            context.setWorkloadGroupName(groupName);
        } finally {
            readUnlock();
        }
        return workloadGroups;
    }

    public QueryQueue getWorkloadGroupQueryQueue(ConnectContext context) throws UserException {
        String groupName = getWorkloadGroupNameAndCheckPriv(context);
        readLock();
        try {
            WorkloadGroup workloadGroup = nameToWorkloadGroup.get(groupName);
            if (workloadGroup == null) {
                throw new UserException("Workload group " + groupName + " does not exist");
            }
            return workloadGroup.getQueryQueue();
        } finally {
            readUnlock();
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
            properties.put(WorkloadGroup.CPU_SHARE, "10");
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
        checkWorkloadGroupEnabled();

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
            checkGlobalUnlock(workloadGroup, null);
            nameToWorkloadGroup.put(workloadGroupName, workloadGroup);
            idToWorkloadGroup.put(workloadGroup.getId(), workloadGroup);
            calQueryCPUHardLimit();
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

        // 2, calculate new query hard cpu limit
        int tmpCpuHardLimit = 0;
        int zeroCpuHardLimitCount = 0;
        for (Map.Entry<Long, WorkloadGroup> entry : idToWorkloadGroup.entrySet()) {
            if (old != null && entry.getKey() == old.getId()) {
                continue;
            }
            int cpuHardLimit = entry.getValue().getCpuHardLimit();
            if (cpuHardLimit == 0) {
                zeroCpuHardLimitCount++;
            }
            tmpCpuHardLimit += cpuHardLimit;
        }
        if (newGroupCpuHardLimit == 0) {
            zeroCpuHardLimitCount++;
        }
        tmpCpuHardLimit += newGroupCpuHardLimit;

        if (tmpCpuHardLimit > 100) {
            throw new DdlException("sum of all workload group " + WorkloadGroup.CPU_HARD_LIMIT
                    + " can not be greater than 100% ");
        }

        if (tmpCpuHardLimit == 100 && zeroCpuHardLimitCount > 0) {
            throw new DdlException("some workload group may not be assigned "
                    + "cpu hard limit but all query cpu hard limit exceeds 100%");
        }

        int leftCpuHardLimitVal = 100 - tmpCpuHardLimit;
        if (zeroCpuHardLimitCount != 0) {
            int tmpCpuHardLimitDefaultVal = leftCpuHardLimitVal / zeroCpuHardLimitCount;
            if (tmpCpuHardLimitDefaultVal == 0) {
                throw new DdlException("remaining cpu can not be assigned to the "
                        + "workload group without cpu hard limit value; "
                        + leftCpuHardLimitVal + "%," + newGroupCpuHardLimit
                        + "%," + zeroCpuHardLimitCount);
            }
        }
    }

    public void alterWorkloadGroup(AlterWorkloadGroupStmt stmt) throws DdlException {
        checkWorkloadGroupEnabled();

        String workloadGroupName = stmt.getWorkloadGroupName();
        Map<String, String> properties = stmt.getProperties();
        WorkloadGroup newWorkloadGroup;
        writeLock();
        try {
            if (!nameToWorkloadGroup.containsKey(workloadGroupName)) {
                throw new DdlException("workload group(" + workloadGroupName + ") does not exist.");
            }
            WorkloadGroup workloadGroup = nameToWorkloadGroup.get(workloadGroupName);
            newWorkloadGroup = WorkloadGroup.copyAndUpdate(workloadGroup, properties);
            checkGlobalUnlock(newWorkloadGroup, workloadGroup);
            nameToWorkloadGroup.put(workloadGroupName, newWorkloadGroup);
            idToWorkloadGroup.put(newWorkloadGroup.getId(), newWorkloadGroup);
            calQueryCPUHardLimit();
            Env.getCurrentEnv().getEditLog().logAlterWorkloadGroup(newWorkloadGroup);
        } finally {
            writeUnlock();
        }
        LOG.info("Alter resource success: {}", newWorkloadGroup);
    }

    public void dropWorkloadGroup(DropWorkloadGroupStmt stmt) throws DdlException {
        checkWorkloadGroupEnabled();

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
            calQueryCPUHardLimit();
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
            calQueryCPUHardLimit();
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
            calQueryCPUHardLimit();
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

    // for ut
    public Map<String, WorkloadGroup> getNameToWorkloadGroup() {
        return nameToWorkloadGroup;
    }

    // for ut
    public Map<Long, WorkloadGroup> getIdToWorkloadGroup() {
        return idToWorkloadGroup;
    }

    private void calQueryCPUHardLimit() {
        int zeroCpuHardLimitCount = 0;
        int ret = 0;
        for (Map.Entry<Long, WorkloadGroup> entry : idToWorkloadGroup.entrySet()) {
            if (entry.getValue().getCpuHardLimit() == 0) {
                zeroCpuHardLimitCount++;
            }
            ret += entry.getValue().getCpuHardLimit();
        }
        this.queryCPUHardLimit = ret;
        if (zeroCpuHardLimitCount != 0) {
            this.cpuHardLimitDefaultVal = (100 - this.queryCPUHardLimit) / zeroCpuHardLimitCount;
        }
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
        calQueryCPUHardLimit();
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
                    workloadGroup.getProcNodeData(result);
                }
            } finally {
                readUnlock();
            }
            return result;
        }
    }
}

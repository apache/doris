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
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TUserIdentity;
import org.apache.doris.thrift.TopicInfo;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
            .add(WorkloadGroup.MAX_CONCURRENCY).add(WorkloadGroup.MAX_QUEUE_SIZE)
            .add(WorkloadGroup.QUEUE_TIMEOUT).add(WorkloadGroup.CPU_HARD_LIMIT)
            .add(WorkloadGroup.SCAN_THREAD_NUM).add(WorkloadGroup.MAX_REMOTE_SCAN_THREAD_NUM)
            .add(WorkloadGroup.MIN_REMOTE_SCAN_THREAD_NUM)
            .add(WorkloadGroup.SPILL_THRESHOLD_LOW_WATERMARK).add(WorkloadGroup.SPILL_THRESHOLD_HIGH_WATERMARK)
            .add(WorkloadGroup.TAG)
            .add(WorkloadGroup.READ_BYTES_PER_SECOND).add(WorkloadGroup.REMOTE_READ_BYTES_PER_SECOND)
            .add(QueryQueue.RUNNING_QUERY_NUM).add(QueryQueue.WAITING_QUERY_NUM)
            .build();

    private static final Logger LOG = LogManager.getLogger(WorkloadGroupMgr.class);
    @SerializedName(value = "idToWorkloadGroup")
    private final Map<Long, WorkloadGroup> idToWorkloadGroup = Maps.newHashMap();
    private final Map<String, WorkloadGroup> nameToWorkloadGroup = Maps.newHashMap();
    private final Map<Long, QueryQueue> idToQueryQueue = Maps.newHashMap();
    private final ResourceProcNode procNode = new ResourceProcNode();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

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
        // if no fe image exist, we should append internal group here.
        appendInternalWorkloadGroup();
    }

    public static WorkloadGroupMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        WorkloadGroupMgr ret = GsonUtils.GSON.fromJson(json, WorkloadGroupMgr.class);
        ret.appendInternalWorkloadGroup();
        return ret;
    }

    public void appendInternalWorkloadGroup() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(WorkloadGroup.CPU_SHARE, "1024");
        properties.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        properties.put(WorkloadGroup.ENABLE_MEMORY_OVERCOMMIT, "true");
        WorkloadGroup defaultValWg = new WorkloadGroup(DEFAULT_GROUP_ID.longValue(), DEFAULT_GROUP_NAME,
                properties);

        // when doris version is 2.0, user create a normal group with id 12345
        // when doris upgrade from 2.0 to 2.1.2, Doris may create a workload id with 1
        // then doris could contain two normal workload group with id 12345 and 1
        // so we should check duplicate workload group when Fe starts
        // and remove invalid workload group.
        // case 1: no images exist or has an image but has no normal wg,
        //         insert a normal group with id 1 and default value directly.
        // case 2: image exits and has a normal group, then do nothing.
        Set<Long> invalidNormalWg = new HashSet<>();
        for (WorkloadGroup curWg : idToWorkloadGroup.values()) {
            if (DEFAULT_GROUP_NAME.equals(curWg.getName()) && DEFAULT_GROUP_ID.longValue() != curWg.getId()) {
                invalidNormalWg.add(curWg.getId());
            }
        }
        for (Long wgId : invalidNormalWg) {
            idToWorkloadGroup.remove(wgId);
        }

        WorkloadGroup curNormalWg = idToWorkloadGroup.get(DEFAULT_GROUP_ID);
        if (curNormalWg == null) {
            curNormalWg = defaultValWg;
            idToWorkloadGroup.put(curNormalWg.getId(), curNormalWg);
        }
        nameToWorkloadGroup.put(curNormalWg.getName(), curNormalWg);

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

    public long getWorkloadGroup(UserIdentity currentUser, String groupName) throws UserException {
        Long workloadId = getWorkloadGroupIdByName(groupName);
        if (workloadId == null) {
            throw new UserException("Workload group " + groupName + " does not exist");
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkWorkloadGroupPriv(currentUser, groupName, PrivPredicate.USAGE)) {
            ErrorReport.reportAnalysisException(
                    "Access denied; you need (at least one of) the %s privilege(s) to use workload group '%s'.",
                    ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "USAGE/ADMIN", groupName);
        }
        return workloadId.longValue();
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

    public List<TPipelineWorkloadGroup> getWorkloadGroupByUser(UserIdentity user, boolean checkAuth)
            throws UserException {
        String groupName = Env.getCurrentEnv().getAuth().getWorkloadGroup(user.getQualifiedUser());
        List<TPipelineWorkloadGroup> ret = new ArrayList<>();
        WorkloadGroup wg = null;
        readLock();
        try {
            if (groupName == null || groupName.isEmpty()) {
                wg = nameToWorkloadGroup.get(DEFAULT_GROUP_NAME);
                if (wg == null) {
                    throw new RuntimeException("can not find normal workload group for user " + user);
                }
            } else {
                wg = nameToWorkloadGroup.get(groupName);
                if (wg == null) {
                    throw new UserException(
                            "can not find workload group " + groupName + " for user " + user);
                }
            }
            if (checkAuth && !Env.getCurrentEnv().getAccessManager()
                    .checkWorkloadGroupPriv(user, wg.getName(), PrivPredicate.USAGE)) {
                ErrorReport.reportAnalysisException(
                        "Access denied; you need (at least one of) the %s privilege(s) to use workload group '%s'."
                                + " used id=(%s)",
                        ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "USAGE/ADMIN", wg.getName(), user.toString());
            }
            ret.add(wg.toThrift());
        } finally {
            readUnlock();
        }
        return ret;
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

    // NOTE: used for checking sum value of 100%  for cpu_hard_limit and memory_limit
    //  when create/alter workload group with same tag.
    //  when oldWg is null it means caller is an alter stmt.
    private void checkGlobalUnlock(WorkloadGroup newWg, WorkloadGroup oldWg) throws DdlException {
        String wgTag = newWg.getTag();
        double sumOfAllMemLimit = 0;
        int sumOfAllCpuHardLimit = 0;
        for (Map.Entry<Long, WorkloadGroup> entry : idToWorkloadGroup.entrySet()) {
            WorkloadGroup wg = entry.getValue();
            if (!StringUtils.equals(wgTag, wg.getTag())) {
                continue;
            }

            if (oldWg != null && entry.getKey() == oldWg.getId()) {
                continue;
            }

            if (wg.getCpuHardLimit() > 0) {
                sumOfAllCpuHardLimit += wg.getCpuHardLimit();
            }
            if (wg.getMemoryLimitPercent() > 0) {
                sumOfAllMemLimit += wg.getMemoryLimitPercent();
            }
        }

        sumOfAllMemLimit += newWg.getMemoryLimitPercent();
        sumOfAllCpuHardLimit += newWg.getCpuHardLimit();

        if (sumOfAllMemLimit > 100.0 + 1e-6) {
            throw new DdlException(
                    "The sum of all workload group " + WorkloadGroup.MEMORY_LIMIT + " within tag " + wgTag
                            + " cannot be greater than 100.0%.");
        }

        if (sumOfAllCpuHardLimit > 100) {
            throw new DdlException(
                    "sum of all workload group " + WorkloadGroup.CPU_HARD_LIMIT + " within tag "
                            + wgTag + " can not be greater than 100% ");
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
            // NOTE: used for regression test query queue
            if (Config.enable_alter_queue_prop_sync) {
                resetQueryQueueProp();
            }
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

        // A group with related policies should not be deleted.
        Long wgId = getWorkloadGroupIdByName(workloadGroupName);
        if (wgId != null) {
            boolean groupHasPolicy = Env.getCurrentEnv().getWorkloadSchedPolicyMgr()
                    .checkWhetherGroupHasPolicy(wgId.longValue());
            if (groupHasPolicy) {
                throw new DdlException(
                        "workload group " + workloadGroupName + " can't be dropped, because it has related policy");
            }
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
            // when wg named normal but id is not DEFAULT_GROUP_ID,
            // then we should abort it to avoid duplicate normal group
            if (DEFAULT_GROUP_NAME.equals(workloadGroup.getName())
                    && DEFAULT_GROUP_ID.longValue() != workloadGroup.getId()) {
                return;
            }

            idToWorkloadGroup.put(workloadGroup.getId(), workloadGroup);
            nameToWorkloadGroup.put(workloadGroup.getName(), workloadGroup);
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

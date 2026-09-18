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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TCompareOperator;
import org.apache.doris.thrift.TUserIdentity;
import org.apache.doris.thrift.TWorkloadActionType;
import org.apache.doris.thrift.TWorkloadMetricType;
import org.apache.doris.thrift.TopicInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadSchedPolicyMgr implements Writable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(WorkloadSchedPolicyMgr.class);

    @SerializedName(value = "idToPolicy")
    private ConcurrentMap<Long, WorkloadSchedPolicy> idToPolicy = Maps.newConcurrentMap();
    private Map<String, WorkloadSchedPolicy> nameToPolicy = Maps.newHashMap();

    private PolicyProcNode policyProcNode = new PolicyProcNode();

    public static final ImmutableList<String> WORKLOAD_SCHED_POLICY_NODE_TITLE_NAMES
            = new ImmutableList.Builder<String>()
            .add("Id").add("Name").add("Condition").add("Action").add("Priority").add("Enabled").add("Version")
            .add("WorkloadGroup")
            .build();

    public static final ImmutableMap<WorkloadConditionOperator, TCompareOperator> OP_MAP
            = new ImmutableMap.Builder<WorkloadConditionOperator, TCompareOperator>()
            .put(WorkloadConditionOperator.EQUAL, TCompareOperator.EQUAL)
            .put(WorkloadConditionOperator.GREATER, TCompareOperator.GREATER)
            .put(WorkloadConditionOperator.GREATER_EQUAL, TCompareOperator.GREATER_EQUAL)
            .put(WorkloadConditionOperator.LESS, TCompareOperator.LESS)
            .put(WorkloadConditionOperator.LESS_EQUAl, TCompareOperator.LESS_EQUAL).build();

    public static final ImmutableSet<WorkloadActionType> BE_ACTION_SET
            = new ImmutableSet.Builder<WorkloadActionType>().add(WorkloadActionType.MOVE_QUERY_TO_GROUP)
            .add(WorkloadActionType.CANCEL_QUERY).build();

    public static final ImmutableSet<WorkloadMetricType> BE_METRIC_SET
            = new ImmutableSet.Builder<WorkloadMetricType>().add(WorkloadMetricType.BE_SCAN_ROWS)
            .add(WorkloadMetricType.BE_SCAN_BYTES)
            // Treat remote scan bytes as a BE-only runtime metric.
            .add(WorkloadMetricType.BE_SCAN_BYTES_FROM_REMOTE_STORAGE).add(WorkloadMetricType.QUERY_TIME)
            .add(WorkloadMetricType.QUERY_BE_MEMORY_BYTES).add(WorkloadMetricType.USERNAME).build();

    // used for convert fe type to thrift type
    public static final ImmutableMap<WorkloadMetricType, TWorkloadMetricType> METRIC_MAP
            = new ImmutableMap.Builder<WorkloadMetricType, TWorkloadMetricType>()
            .put(WorkloadMetricType.QUERY_TIME, TWorkloadMetricType.QUERY_TIME)
            .put(WorkloadMetricType.BE_SCAN_ROWS, TWorkloadMetricType.BE_SCAN_ROWS)
            .put(WorkloadMetricType.BE_SCAN_BYTES, TWorkloadMetricType.BE_SCAN_BYTES)
            // Map the new FE metric enum to the appended thrift metric enum.
            .put(WorkloadMetricType.BE_SCAN_BYTES_FROM_REMOTE_STORAGE,
                    TWorkloadMetricType.BE_SCAN_BYTES_FROM_REMOTE_STORAGE)
            .put(WorkloadMetricType.QUERY_BE_MEMORY_BYTES, TWorkloadMetricType.QUERY_BE_MEMORY_BYTES)
            .put(WorkloadMetricType.USERNAME, TWorkloadMetricType.USERNAME).build();
    public static final ImmutableMap<WorkloadActionType, TWorkloadActionType> ACTION_MAP
            = new ImmutableMap.Builder<WorkloadActionType, TWorkloadActionType>()
            .put(WorkloadActionType.MOVE_QUERY_TO_GROUP, TWorkloadActionType.MOVE_QUERY_TO_GROUP)
            .put(WorkloadActionType.CANCEL_QUERY, TWorkloadActionType.CANCEL_QUERY).build();

    public static final Map<String, WorkloadMetricType> STRING_METRIC_MAP = new HashMap<>();
    public static final Map<String, WorkloadActionType> STRING_ACTION_MAP = new HashMap<>();

    static {
        for (WorkloadMetricType metricType : BE_METRIC_SET) {
            STRING_METRIC_MAP.put(metricType.toString(), metricType);
        }
        for (WorkloadActionType actionType : BE_ACTION_SET) {
            STRING_ACTION_MAP.put(actionType.toString(), actionType);
        }
    }

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static Comparator<WorkloadSchedPolicy> policyComparator = new Comparator<WorkloadSchedPolicy>() {
        @Override
        public int compare(WorkloadSchedPolicy p1, WorkloadSchedPolicy p2) {
            return p2.getPriority() - p1.getPriority();
        }
    };

    public void createWorkloadSchedPolicy(String policyName, boolean isIfNotExists,
            List<WorkloadConditionMeta> originConditions, List<WorkloadActionMeta> originActions,
            Map<String, String> propMap) throws UserException {
        // 1 create condition
        List<WorkloadCondition> policyConditionList = new ArrayList<>();
        for (WorkloadConditionMeta cm : originConditions) {
            WorkloadCondition cond = WorkloadCondition.createWorkloadCondition(cm);
            policyConditionList.add(cond);
        }
        checkPolicyCondition(policyConditionList);

        // 2 create action
        List<WorkloadAction> policyActionList = new ArrayList<>();
        for (WorkloadActionMeta workloadActionMeta : originActions) {
            // todo(wb) support move action
            WorkloadAction ret = WorkloadAction.createWorkloadAction(workloadActionMeta);
            policyActionList.add(ret);
        }
        checkPolicyAction(policyActionList);

        // 3 create policy
        if (propMap == null) {
            propMap = new HashMap<>();
        }
        List<Long> wgIdList = new ArrayList<>();
        if (propMap.size() != 0) {
            checkProperties(propMap, wgIdList);
        }
        writeLock();
        try {
            if (nameToPolicy.containsKey(policyName)) {
                if (isIfNotExists) {
                    return;
                } else {
                    throw new UserException("workload schedule policy " + policyName + " already exists ");
                }
            }
            if (idToPolicy.size() >= Config.workload_max_policy_num) {
                throw new UserException(
                        "workload scheduler policy num can not exceed " + Config.workload_max_policy_num);
            }
            long id = Env.getCurrentEnv().getNextId();
            WorkloadSchedPolicy policy = new WorkloadSchedPolicy(id, policyName,
                    policyConditionList, policyActionList, propMap, wgIdList);
            policy.setConditionMeta(originConditions);
            policy.setActionMeta(originActions);
            Env.getCurrentEnv().getEditLog().logCreateWorkloadSchedPolicy(policy);
            idToPolicy.put(id, policy);
            nameToPolicy.put(policyName, policy);
        } finally {
            writeUnlock();
        }
    }

    private void checkPolicyCondition(List<WorkloadCondition> conditionList) throws UserException {
        if (conditionList.size() > Config.workload_max_condition_num_in_policy) {
            throw new UserException(
                    "condition num in a policy can not exceed " + Config.workload_max_condition_num_in_policy);
        }
    }

    private void checkPolicyAction(List<WorkloadAction> actionList) throws UserException {
        if (actionList.size() > Config.workload_max_action_num_in_policy) {
            throw new UserException(
                    "action num in one policy can not exceed " + Config.workload_max_action_num_in_policy);
        }

        Set<WorkloadActionType> actionTypeSet = new HashSet<>();
        for (WorkloadAction action : actionList) {
            if (!actionTypeSet.add(action.getWorkloadActionType())) {
                throw new UserException("duplicate action in one policy");
            }
        }

        if (actionTypeSet.contains(WorkloadActionType.CANCEL_QUERY) && actionTypeSet.contains(
                WorkloadActionType.MOVE_QUERY_TO_GROUP)) {
            throw new UserException(String.format("%s and %s can not exist in one policy at same time",
                    WorkloadActionType.CANCEL_QUERY, WorkloadActionType.MOVE_QUERY_TO_GROUP));
        }
    }

    List<WorkloadSchedPolicy> pickPolicy(Map<WorkloadActionType, Queue<WorkloadSchedPolicy>> policyMap) {
        // NOTE(wb) currently all action share the same comparator which use priority.
        // But later we may design every action type's own comparator,
        // such as if two move group action has the same priority but move to different group,
        // then we may pick group by resource usage and query statistics.

        // 1 only need one policy with move action which has the highest priority
        WorkloadSchedPolicy policyWithMoveAction = null;
        Queue<WorkloadSchedPolicy> moveActionQueue = policyMap.get(WorkloadActionType.MOVE_QUERY_TO_GROUP);
        if (moveActionQueue != null) {
            policyWithMoveAction = moveActionQueue.peek();
        }

        // 2 only need one policy with cancel action which has the highest priority
        WorkloadSchedPolicy policyWithCancelQueryAction = null;
        Queue<WorkloadSchedPolicy> canelQueryActionQueue = policyMap.get(WorkloadActionType.CANCEL_QUERY);
        if (canelQueryActionQueue != null) {
            policyWithCancelQueryAction = canelQueryActionQueue.peek();
        }

        // 3 compare policy with move action and cancel action
        List<WorkloadSchedPolicy> ret = new ArrayList<>();
        if (policyWithMoveAction != null && policyWithCancelQueryAction != null) {
            if (policyWithMoveAction.getPriority() > policyWithCancelQueryAction.getPriority()) {
                ret.add(policyWithMoveAction);
            } else {
                ret.add(policyWithCancelQueryAction);
            }
        } else {
            if (policyWithCancelQueryAction != null) {
                ret.add(policyWithCancelQueryAction);
            } else if (policyWithMoveAction != null) {
                ret.add(policyWithMoveAction);
            }
        }

        // 4 add no-conflict policy
        for (Map.Entry<WorkloadActionType, Queue<WorkloadSchedPolicy>> entry : policyMap.entrySet()) {
            WorkloadActionType type = entry.getKey();
            Queue<WorkloadSchedPolicy> policyQueue = entry.getValue();
            if (!WorkloadActionType.CANCEL_QUERY.equals(type) && !WorkloadActionType.MOVE_QUERY_TO_GROUP.equals(type)
                    && policyQueue != null && policyQueue.size() > 0) {
                WorkloadSchedPolicy pickedPolicy = policyQueue.peek();
                ret.add(pickedPolicy);
            }
        }

        Preconditions.checkArgument(ret.size() > 0, "should pick at least one policy");
        return ret;
    }

    @VisibleForTesting
    void checkProperties(Map<String, String> properties, List<Long> wgIdList) throws UserException {
        Set<String> allInputPropKeySet = new HashSet<>();
        allInputPropKeySet.addAll(properties.keySet());

        allInputPropKeySet.removeAll(WorkloadSchedPolicy.POLICY_PROPERTIES);
        if (allInputPropKeySet.size() > 0) {
            throw new UserException("illegal policy properties " + String.join(",", allInputPropKeySet));
        }

        String enabledStr = properties.get(WorkloadSchedPolicy.ENABLED);
        if (enabledStr != null) {
            if (!"true".equals(enabledStr) && !"false".equals(enabledStr)) {
                throw new UserException("invalid enabled property value, it can only true or false with lower case");
            }
        }

        String priorityStr = properties.get(WorkloadSchedPolicy.PRIORITY);
        if (priorityStr != null) {
            try {
                Long prioLongVal = Long.parseLong(priorityStr);
                if (prioLongVal < 0 || prioLongVal > 100) {
                    throw new UserException("policy's priority can only between 0 ~ 100");
                }
            } catch (NumberFormatException e) {
                throw new UserException(
                        "invalid priority property value, it must be a number, input value=" + priorityStr);
            }
        }

        String workloadGroupNameStr = properties.get(WorkloadSchedPolicy.WORKLOAD_GROUP);
        if (workloadGroupNameStr != null && !workloadGroupNameStr.isEmpty()) {
            // Use limit=-1 so trailing empty segments are preserved; otherwise inputs like
            // "wg." would silently collapse to ["wg"] and slip past the length check, and
            // ".wg" would pass the length==2 check with an empty compute-group component.
            String[] ss = workloadGroupNameStr.split("\\.", -1);
            String cg;
            String wg;
            if (Config.isCloudMode()) {
                // Cloud mode requires the fully-qualified "<compute_group>.<workload_group>" form
                // so the binding is unambiguous across multiple compute groups.
                if (ss.length != 2 || ss[0].isEmpty() || ss[1].isEmpty()) {
                    throw new UserException("workload_group must be '<compute_group>.<workload_group>' "
                            + "in cloud mode, got: " + workloadGroupNameStr);
                }
                cg = ss[0];
                wg = ss[1];
            } else {
                // Non-cloud mode also accepts the '<compute_group>.<workload_group>' form for
                // grammar consistency with cloud mode, but the prefix here actually refers to
                // a resource group (Tag), not a real compute group. The bare '<workload_group>'
                // form is also accepted and defaults the resource group to Tag.VALUE_DEFAULT_TAG.
                if (ss.length == 1) {
                    if (ss[0].isEmpty()) {
                        throw new UserException("workload_group must be '<workload_group>' or "
                                + "'<resource_group>.<workload_group>' in non-cloud mode, got: "
                                + workloadGroupNameStr);
                    }
                    cg = Tag.VALUE_DEFAULT_TAG;
                    wg = ss[0];
                } else if (ss.length == 2) {
                    if (ss[0].isEmpty() || ss[1].isEmpty()) {
                        throw new UserException("workload_group must be '<workload_group>' or "
                                + "'<resource_group>.<workload_group>' in non-cloud mode, got: "
                                + workloadGroupNameStr);
                    }
                    cg = ss[0];
                    wg = ss[1];
                } else {
                    throw new UserException("workload_group must be '<workload_group>' or "
                            + "'<resource_group>.<workload_group>' in non-cloud mode, got: "
                            + workloadGroupNameStr);
                }
            }
            ConnectContext tmpCtx = new ConnectContext();
            tmpCtx.setComputeGroup(
                    Env.getCurrentEnv().getComputeGroupMgr().getComputeGroupByName(cg));
            // In cloud mode ConnectContext#getComputeGroup() re-resolves the compute group via
            // getCloudCluster() and ignores setComputeGroup(), so propagate the name through the
            // session variable as well to make sure the downstream lookup uses the chosen cg.
            if (Config.isCloudMode()) {
                tmpCtx.getSessionVariable().setCloudCluster(cg);
            }
            tmpCtx.getSessionVariable().setWorkloadGroup(wg);
            tmpCtx.setCurrentUserIdentity(UserIdentity.ROOT);
            Long wgId = Env.getCurrentEnv().getWorkloadGroupMgr().getWorkloadGroup(tmpCtx)
                    .get(0).getId();
            wgIdList.add(wgId);
        }
    }

    public void alterWorkloadSchedPolicy(String policyName, Map<String, String> properties) throws UserException {
        writeLock();
        try {
            WorkloadSchedPolicy policy = nameToPolicy.get(policyName);
            if (policy == null) {
                throw new UserException("can not find workload schedule policy " + policyName);
            }

            List<Long> wgIdList = new ArrayList<>();
            checkProperties(properties, wgIdList);
            policy.updatePropertyIfNotNull(properties, wgIdList);
            policy.incrementVersion();
            Env.getCurrentEnv().getEditLog().logAlterWorkloadSchedPolicy(policy);
        } finally {
            writeUnlock();
        }
    }

    public void dropWorkloadSchedPolicy(String policyName, boolean isExists) throws UserException {
        writeLock();
        try {
            WorkloadSchedPolicy schedPolicy = nameToPolicy.get(policyName);
            if (schedPolicy == null) {
                if (isExists) {
                    return;
                } else {
                    throw new UserException("workload schedule policy " + policyName + " not exists");
                }
            }

            long id = schedPolicy.getId();
            idToPolicy.remove(id);
            nameToPolicy.remove(policyName);
            Env.getCurrentEnv().getEditLog().dropWorkloadSchedPolicy(id);
        } finally {
            writeUnlock();
        }
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

    public List<TopicInfo> getPublishTopicInfoList() {
        List<TopicInfo> topicInfoList = new ArrayList();
        readLock();
        try {
            for (Map.Entry<Long, WorkloadSchedPolicy> entry : idToPolicy.entrySet()) {
                TopicInfo tInfo = entry.getValue().toTopicInfo();
                if (tInfo != null) {
                    topicInfoList.add(tInfo);
                }
            }
        } finally {
            readUnlock();
        }
        return topicInfoList;
    }

    public void replayCreateWorkloadSchedPolicy(WorkloadSchedPolicy policy) {
        insertWorkloadSchedPolicy(policy);
    }

    public void replayAlterWorkloadSchedPolicy(WorkloadSchedPolicy policy) {
        insertWorkloadSchedPolicy(policy);
    }

    public void replayDropWorkloadSchedPolicy(long policyId) {
        writeLock();
        try {
            WorkloadSchedPolicy policy = idToPolicy.get(policyId);
            if (policy == null) {
                return;
            }
            idToPolicy.remove(policyId);
            nameToPolicy.remove(policy.getName());
        } finally {
            writeUnlock();
        }
    }

    private void insertWorkloadSchedPolicy(WorkloadSchedPolicy policy) {
        writeLock();
        try {
            idToPolicy.put(policy.getId(), policy);
            nameToPolicy.put(policy.getName(), policy);
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> getShowPolicyInfo() {
        UserIdentity currentUserIdentity = ConnectContext.get().getCurrentUserIdentity();
        return policyProcNode.fetchResult(currentUserIdentity).getRows();
    }

    public List<List<String>> getWorkloadSchedPolicyTvfInfo(TUserIdentity tcurrentUserIdentity) {
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(tcurrentUserIdentity);
        return policyProcNode.fetchResult(currentUserIdentity).getRows();
    }

    public boolean checkWhetherGroupHasPolicy(long wgId) {
        readLock();
        try {
            for (Map.Entry<Long, WorkloadSchedPolicy> entry : idToPolicy.entrySet()) {
                if (entry.getValue().getWorkloadGroupIdList().contains(wgId)) {
                    return true;
                }
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    public class PolicyProcNode {
        public ProcResult fetchResult(UserIdentity currentUserIdentity) {
            BaseProcResult result = new BaseProcResult();
            result.setNames(WORKLOAD_SCHED_POLICY_NODE_TITLE_NAMES);
            Map<Long, String> idToNameMap = Env.getCurrentEnv().getWorkloadGroupMgr().getIdToNameMap();
            readLock();
            try {
                for (WorkloadSchedPolicy policy : idToPolicy.values()) {
                    if (!Env.getCurrentEnv().getAccessManager().checkWorkloadGroupPriv(currentUserIdentity,
                            policy.getName(), PrivPredicate.SHOW_WORKLOAD_GROUP)) {
                        continue;
                    }

                    List<String> row = new ArrayList<>();
                    String pName = policy.getName();
                    row.add(String.valueOf(policy.getId()));
                    row.add(pName);

                    List<WorkloadConditionMeta> conditionList = policy.getConditionMetaList();
                    StringBuilder cmStr = new StringBuilder();
                    for (WorkloadConditionMeta cm : conditionList) {
                        cmStr.append(cm.toString()).append(";");
                    }
                    String retStr = cmStr.toString().toLowerCase();
                    row.add(retStr.substring(0, retStr.length() - 1));

                    List<WorkloadActionMeta> actionList = policy.getActionMetaList();
                    StringBuilder actionStr = new StringBuilder();
                    for (WorkloadActionMeta am : actionList) {
                        actionStr.append(am.toString()).append(";");
                    }
                    String retStr2 = actionStr.toString().toLowerCase();
                    row.add(retStr2.substring(0, retStr2.length() - 1));

                    row.add(String.valueOf(policy.getPriority()));
                    row.add(String.valueOf(policy.isEnabled()));
                    row.add(String.valueOf(policy.getVersion()));

                    List<Long> wgIdList = policy.getWorkloadGroupIdList();
                    if (wgIdList.size() == 0) {
                        row.add("");
                    } else {
                        Long wgId = wgIdList.get(0);
                        String wgName = idToNameMap.get(wgId);
                        if (wgName == null) {
                            row.add("null");
                        } else {
                            row.add(wgName);
                        }
                    }
                    result.addRow(row);
                }
            } finally {
                readUnlock();
            }
            return result;
        }
    }

    public static WorkloadSchedPolicyMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WorkloadSchedPolicyMgr.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        idToPolicy.forEach(
                (id, schedPolicy) -> nameToPolicy.put(schedPolicy.getName(), schedPolicy));
    }
}

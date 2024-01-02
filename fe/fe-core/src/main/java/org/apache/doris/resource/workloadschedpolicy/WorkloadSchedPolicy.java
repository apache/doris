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

import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TCompareOperator;
import org.apache.doris.thrift.TWorkloadAction;
import org.apache.doris.thrift.TWorkloadActionType;
import org.apache.doris.thrift.TWorkloadCondition;
import org.apache.doris.thrift.TWorkloadMetricType;
import org.apache.doris.thrift.TWorkloadSchedPolicy;
import org.apache.doris.thrift.TopicInfo;

import com.esotericsoftware.minlog.Log;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WorkloadSchedPolicy implements Writable, GsonPostProcessable {

    public static final String ENABLED = "enabled";
    public static final String PRIORITY = "priority";
    public static final ImmutableSet<String> POLICY_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(ENABLED).add(PRIORITY).build();

    // used for convert fe type to thrift type
    private static ImmutableMap<WorkloadMetricType, TWorkloadMetricType> METRIC_MAP
            = new ImmutableMap.Builder<WorkloadMetricType, TWorkloadMetricType>()
            .put(WorkloadMetricType.QUERY_TIME, TWorkloadMetricType.QUERY_TIME).build();
    private static ImmutableMap<WorkloadActionType, TWorkloadActionType> ACTION_MAP
            = new ImmutableMap.Builder<WorkloadActionType, TWorkloadActionType>()
            .put(WorkloadActionType.MOVE_QUERY_TO_GROUP, TWorkloadActionType.MOVE_QUERY_TO_GROUP)
            .put(WorkloadActionType.CANCEL_QUERY, TWorkloadActionType.CANCEL_QUERY).build();

    private static ImmutableMap<WorkloadConditionOperator, TCompareOperator> OP_MAP
            = new ImmutableMap.Builder<WorkloadConditionOperator, TCompareOperator>()
            .put(WorkloadConditionOperator.EQUAL, TCompareOperator.EQUAL)
            .put(WorkloadConditionOperator.GREATER, TCompareOperator.GREATER)
            .put(WorkloadConditionOperator.GREATER_EQUAL, TCompareOperator.GREATER_EQUAL)
            .put(WorkloadConditionOperator.LESS, TCompareOperator.LESS)
            .put(WorkloadConditionOperator.LESS_EQUAl, TCompareOperator.LESS_EQUAL).build();

    @SerializedName(value = "id")
    long id;
    @SerializedName(value = "name")
    String name;
    @SerializedName(value = "version")
    int version;

    @SerializedName(value = "enabled")
    private volatile boolean enabled;
    @SerializedName(value = "priority")
    private volatile int priority;

    @SerializedName(value = "conditionMetaList")
    List<WorkloadConditionMeta> conditionMetaList;
    // we regard action as a command, map's key is command, map's value is args, so it's a command list
    @SerializedName(value = "actionMetaList")
    List<WorkloadActionMeta> actionMetaList;

    private List<WorkloadCondition> workloadConditionList;
    private List<WorkloadAction> workloadActionList;

    private Boolean isFePolicy = null;

    // for ut
    public WorkloadSchedPolicy() {
    }

    // for ut
    public void setWorkloadConditionList(List<WorkloadCondition> workloadConditionList) {
        this.workloadConditionList = workloadConditionList;
    }

    public WorkloadSchedPolicy(long id, String name, List<WorkloadCondition> workloadConditionList,
            List<WorkloadAction> workloadActionList, Map<String, String> properties) throws UserException {
        this.id = id;
        this.name = name;
        this.workloadConditionList = workloadConditionList;
        this.workloadActionList = workloadActionList;
        // set enable and priority
        parseAndSetProperties(properties);
        this.version = 0;
    }

    // return true, this means all conditions in policy can match queryInfo's data
    // return false,
    //    1 metric not match
    //    2 condition value not match query info's value
    public boolean isMatch(WorkloadQueryInfo queryInfo) {
        for (WorkloadCondition condition : workloadConditionList) {
            WorkloadMetricType metricType = condition.getMetricType();
            String value = queryInfo.metricMap.get(metricType);
            if (value == null) {
                return false; // query info's metric must match all condition's metric
            }
            if (!condition.eval(value)) {
                return false;
            }
        }
        return true;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getPriority() {
        return priority;
    }

    public void execAction(WorkloadQueryInfo queryInfo) {
        for (WorkloadAction action : workloadActionList) {
            action.exec(queryInfo);
        }
    }

    // move > log, cancel > log
    // move and cancel can not exist at same time
    public WorkloadActionType getFirstActionType() {
        WorkloadActionType retType = null;
        for (WorkloadAction action : workloadActionList) {
            WorkloadActionType currentActionType = action.getWorkloadActionType();
            if (retType == null) {
                retType = currentActionType;
                continue;
            }

            if (currentActionType == WorkloadActionType.MOVE_QUERY_TO_GROUP
                    || currentActionType == WorkloadActionType.CANCEL_QUERY) {
                return currentActionType;
            }
        }
        return retType;
    }

    public void parseAndSetProperties(Map<String, String> properties) throws UserException {
        String enabledStr = properties.get(ENABLED);
        this.enabled = enabledStr == null ? true : Boolean.parseBoolean(enabledStr);

        String priorityStr = properties.get(PRIORITY);
        this.priority = priorityStr == null ? 0 : Integer.parseInt(priorityStr);
    }

    void incrementVersion() {
        this.version++;
    }

    public void setConditionMeta(List<WorkloadConditionMeta> conditionMeta) {
        this.conditionMetaList = conditionMeta;
    }

    public void setActionMeta(List<WorkloadActionMeta> actionMeta) {
        this.actionMetaList = actionMeta;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getVersion() {
        return version;
    }

    public List<WorkloadConditionMeta> getConditionMetaList() {
        return conditionMetaList;
    }

    public List<WorkloadActionMeta> getActionMetaList() {
        return actionMetaList;
    }

    // true, current policy can only run in FE;
    // false, current policy can only run in BE
    public boolean isFePolicy() {
        if (isFePolicy == null) {
            isFePolicy = false;
            for (WorkloadAction action : workloadActionList) {
                if (WorkloadSchedPolicyMgr.FE_ACTION_SET.contains(action.getWorkloadActionType())) {
                    isFePolicy = true;
                    break;
                }
            }
        }
        return isFePolicy;
    }

    public TopicInfo toTopicInfo() {
        TWorkloadSchedPolicy tPolicy = new TWorkloadSchedPolicy();
        tPolicy.setId(id);
        tPolicy.setName(name);
        tPolicy.setVersion(version);
        tPolicy.setPriority(priority);
        tPolicy.setEnabled(enabled);

        List<TWorkloadCondition> condList = new ArrayList();
        for (WorkloadConditionMeta cond : conditionMetaList) {
            TWorkloadCondition tCond = new TWorkloadCondition();
            TWorkloadMetricType metricType = METRIC_MAP.get(cond.metricName);
            if (metricType == null) {
                return null;
            }
            tCond.setMetricName(metricType);
            tCond.setOp(OP_MAP.get(cond.op));
            tCond.setValue(cond.value);
            condList.add(tCond);
        }

        List<TWorkloadAction> actionList = new ArrayList();
        for (WorkloadActionMeta action : actionMetaList) {
            TWorkloadAction tAction = new TWorkloadAction();
            TWorkloadActionType tActionType = ACTION_MAP.get(action.action);
            if (tActionType == null) {
                return null;
            }
            tAction.setAction(tActionType);
            tAction.setActionArgs(action.actionArgs);
            actionList.add(tAction);
        }

        tPolicy.setConditionList(condList);
        tPolicy.setActionList(actionList);

        TopicInfo topicInfo = new TopicInfo();
        topicInfo.setWorkloadSchedPolicy(tPolicy);

        return topicInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static WorkloadSchedPolicy read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WorkloadSchedPolicy.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        List<WorkloadCondition> policyConditionList = new ArrayList<>();
        for (WorkloadConditionMeta cm : conditionMetaList) {
            try {
                WorkloadCondition cond = WorkloadCondition.createWorkloadCondition(cm);
                policyConditionList.add(cond);
            } catch (UserException ue) {
                Log.error("unexpected condition data error when replay log ", ue);
            }
        }
        this.workloadConditionList = policyConditionList;

        List<WorkloadAction> actionList = new ArrayList<>();
        for (WorkloadActionMeta actionMeta : actionMetaList) {
            try {
                WorkloadAction ret = WorkloadAction.createWorkloadAction(actionMeta);
                actionList.add(ret);
            } catch (UserException ue) {
                Log.error("unexpected action data error when replay log ", ue);
            }
        }
        this.workloadActionList = actionList;

    }
}

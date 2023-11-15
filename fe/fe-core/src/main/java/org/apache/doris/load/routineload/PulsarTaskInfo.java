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


package org.apache.doris.load.routineload;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TPulsarLoadInfo;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PulsarTaskInfo extends RoutineLoadTaskInfo {
    private RoutineLoadManager routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();

    private List<String> partitions;
    private Map<String, Long> initialPositions = Maps.newHashMap();

    public PulsarTaskInfo(UUID id, long jobId, String clusterName, List<String> partitions,
                          Map<String, Long> initialPositions, long tastTimeoutMs, boolean isMultiTable) {
        super(id, jobId, clusterName, tastTimeoutMs, isMultiTable);
        this.partitions = partitions;
        this.initialPositions.putAll(initialPositions);
    }

    public PulsarTaskInfo(PulsarTaskInfo pulsarTaskInfo, Map<String, Long> initialPositions, boolean isMultiTable) {
        super(UUID.randomUUID(), pulsarTaskInfo.getJobId(), pulsarTaskInfo.getClusterName(),
                pulsarTaskInfo.getTimeoutMs(), pulsarTaskInfo.getBeId(), isMultiTable);
        this.partitions = pulsarTaskInfo.getPartitions();
        this.initialPositions.putAll(initialPositions);
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public Map<String, Long> getInitialPositions() {
        return initialPositions;
    }

    @Override
    public TRoutineLoadTask createRoutineLoadTask() throws UserException {
        PulsarRoutineLoadJob routineLoadJob = (PulsarRoutineLoadJob) routineLoadManager.getJob(jobId);

        // init tRoutineLoadTask and create plan fragment
        TRoutineLoadTask tRoutineLoadTask = new TRoutineLoadTask();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tRoutineLoadTask.setId(queryId);
        tRoutineLoadTask.setJobId(jobId);
        tRoutineLoadTask.setTxnId(txnId);
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(routineLoadJob.getDbId());
        tRoutineLoadTask.setDb(database.getFullName());
        Table tbl = database.getTableNullable(routineLoadJob.getTableId());
        if (tbl == null) {
            throw new MetaNotFoundException("table " + routineLoadJob.getTableId() + " does not exist");
        }
        // label = job_name+job_id+task_id+txn_id
        String label = Joiner.on("-").join(routineLoadJob.getName(),
                routineLoadJob.getId(), DebugUtil.printId(id), txnId);
        tRoutineLoadTask.setTbl(tbl.getName());
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuthCode(routineLoadJob.getAuthCode());
        TPulsarLoadInfo tPulsarLoadInfo = new TPulsarLoadInfo();
        tPulsarLoadInfo.setServiceUrl((routineLoadJob).getServiceUrl());
        tPulsarLoadInfo.setTopic((routineLoadJob).getTopic());
        tPulsarLoadInfo.setSubscription((routineLoadJob).getSubscription());
        tPulsarLoadInfo.setPartitions(getPartitions());
        if (!initialPositions.isEmpty()) {
            tPulsarLoadInfo.setInitialPositions(getInitialPositions());
        }
        tPulsarLoadInfo.setProperties(routineLoadJob.getConvertedCustomProperties());
        tRoutineLoadTask.setPulsarLoadInfo(tPulsarLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.PULSAR);
        tRoutineLoadTask.setIsMultiTable(isMultiTable);
        tRoutineLoadTask.setParams(plan(routineLoadJob));
        tRoutineLoadTask.setMaxIntervalS(routineLoadJob.getMaxBatchIntervalS());
        tRoutineLoadTask.setMaxBatchRows(routineLoadJob.getMaxBatchRows());
        tRoutineLoadTask.setMaxBatchSize(routineLoadJob.getMaxBatchSizeBytes());
        if (!routineLoadJob.getFormat().isEmpty() && routineLoadJob.getFormat().equalsIgnoreCase("json")) {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_JSON);
        } else {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_CSV_PLAIN);
        }
        //if (Math.abs(routineLoadJob.getMaxFilterRatio() - 1) > 0.001) {
        //    tRoutineLoadTask.setMaxFilterRatio(routineLoadJob.getMaxFilterRatio());
        //}
        return tRoutineLoadTask;
    }

    @Override
    protected String getTaskDataSourceProperties() {
        StringBuilder result = new StringBuilder();

        Gson gson = new Gson();
        result.append("Partitions: " + gson.toJson(partitions));
        if (!initialPositions.isEmpty()) {
            result.append("InitialPositisons: " + gson.toJson(initialPositions));
        }

        return result.toString();
    }

    @Override
    boolean hasMoreDataToConsume() {
        PulsarRoutineLoadJob routineLoadJob = (PulsarRoutineLoadJob) routineLoadManager.getJob(jobId);
        return routineLoadJob.hasMoreDataToConsume(id, partitions, initialPositions);
    }

    @Override
    public String toString() {
        return "Task id: " + getId() + ", partitions: " + partitions + ", initial positions: " + initialPositions;
    }

    private TExecPlanFragmentParams plan(RoutineLoadJob routineLoadJob) throws UserException {
        TUniqueId loadId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        // plan for each task, in case table has change(rollup or schema change)
        TExecPlanFragmentParams tExecPlanFragmentParams = routineLoadJob.plan(loadId, txnId);
        TPlanFragment tPlanFragment = tExecPlanFragmentParams.getFragment();
        tPlanFragment.getOutputSink().getOlapTableSink().setTxnId(txnId);
        return tExecPlanFragmentParams;
    }
}

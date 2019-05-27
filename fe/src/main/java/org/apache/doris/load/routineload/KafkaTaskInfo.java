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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TKafkaLoadInfo;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Joiner;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaTaskInfo extends RoutineLoadTaskInfo {

    private RoutineLoadManager routineLoadManager = Catalog.getCurrentCatalog().getRoutineLoadManager();

    // <partitionId, beginOffsetOfPartitionId>
    private Map<Integer, Long> partitionIdToOffset;

    public KafkaTaskInfo(UUID id, long jobId, String clusterName, Map<Integer, Long> partitionIdToOffset) {
        super(id, jobId, clusterName);
        this.partitionIdToOffset = partitionIdToOffset;
    }

    public KafkaTaskInfo(KafkaTaskInfo kafkaTaskInfo, Map<Integer, Long> partitionIdToOffset) {
        super(UUID.randomUUID(), kafkaTaskInfo.getJobId(), kafkaTaskInfo.getClusterName(), kafkaTaskInfo.getBeId());
        this.partitionIdToOffset = partitionIdToOffset;
    }

    public List<Integer> getPartitions() {
        return new ArrayList<>(partitionIdToOffset.keySet());
    }

    @Override
    public TRoutineLoadTask createRoutineLoadTask() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = (KafkaRoutineLoadJob) routineLoadManager.getJob(jobId);

        // init tRoutineLoadTask and create plan fragment
        TRoutineLoadTask tRoutineLoadTask = new TRoutineLoadTask();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tRoutineLoadTask.setId(queryId);
        tRoutineLoadTask.setJob_id(jobId);
        tRoutineLoadTask.setTxn_id(txnId);
        Database database = Catalog.getCurrentCatalog().getDb(routineLoadJob.getDbId());
        if (database == null) {
            throw new MetaNotFoundException("database " + routineLoadJob.getDbId() + " does not exist");
        }
        tRoutineLoadTask.setDb(database.getFullName());
        tRoutineLoadTask.setTbl(database.getTable(routineLoadJob.getTableId()).getName());
        // label = job_name+job_id+task_id+txn_id
        String label = Joiner.on("-").join(routineLoadJob.getName(), routineLoadJob.getId(), DebugUtil.printId(id), txnId);
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuth_code(routineLoadJob.getAuthCode());
        TKafkaLoadInfo tKafkaLoadInfo = new TKafkaLoadInfo();
        tKafkaLoadInfo.setTopic((routineLoadJob).getTopic());
        tKafkaLoadInfo.setBrokers((routineLoadJob).getBrokerList());
        tKafkaLoadInfo.setPartition_begin_offset(partitionIdToOffset);
        tKafkaLoadInfo.setProperties((routineLoadJob).getProperties());
        tRoutineLoadTask.setKafka_load_info(tKafkaLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.KAFKA);
        tRoutineLoadTask.setParams(updateTExecPlanFragmentParams(routineLoadJob));
        tRoutineLoadTask.setMax_interval_s(routineLoadJob.getMaxBatchIntervalS());
        tRoutineLoadTask.setMax_batch_rows(routineLoadJob.getMaxBatchRows());
        tRoutineLoadTask.setMax_batch_size(routineLoadJob.getMaxBatchSizeBytes());
        return tRoutineLoadTask;
    }

    @Override
    protected String getTaskDataSourceProperties() {
        Gson gson = new Gson();
        return gson.toJson(partitionIdToOffset);
    }

    private TExecPlanFragmentParams updateTExecPlanFragmentParams(RoutineLoadJob routineLoadJob) throws UserException {
        // plan for each task, in case table has change(rollup or schema change)
        TExecPlanFragmentParams tExecPlanFragmentParams = routineLoadJob.plan();
        TPlanFragment tPlanFragment = tExecPlanFragmentParams.getFragment();
        // we use task id as both query id(TPlanFragmentExecParams) and load id(olap table sink/scan range desc)
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tPlanFragment.getOutput_sink().getOlap_table_sink().setLoad_id(queryId);
        tPlanFragment.getOutput_sink().getOlap_table_sink().setTxn_id(this.txnId);
        tExecPlanFragmentParams.getParams().setQuery_id(queryId);
        tExecPlanFragmentParams.getParams().getPer_node_scan_ranges().values().stream()
                .forEach(entity -> entity.get(0).getScan_range().getBroker_scan_range().getRanges().get(0).setLoad_id(queryId));
        return tExecPlanFragmentParams;
    }
}

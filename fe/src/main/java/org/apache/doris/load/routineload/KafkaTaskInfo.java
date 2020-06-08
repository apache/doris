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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFileFormatType;
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

    public KafkaTaskInfo(UUID id, long jobId, String clusterName, long timeoutMs, Map<Integer, Long> partitionIdToOffset) {
        super(id, jobId, clusterName, timeoutMs);
        this.partitionIdToOffset = partitionIdToOffset;
    }

    public KafkaTaskInfo(KafkaTaskInfo kafkaTaskInfo, Map<Integer, Long> partitionIdToOffset) {
        super(UUID.randomUUID(), kafkaTaskInfo.getJobId(), kafkaTaskInfo.getClusterName(),
                kafkaTaskInfo.getTimeoutMs(), kafkaTaskInfo.getBeId());
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
        Table tbl = database.getTable(routineLoadJob.getTableId());
        if (tbl == null) {
            throw new MetaNotFoundException("table " + routineLoadJob.getTableId() + " does not exist");
        }
        tRoutineLoadTask.setTbl(tbl.getName());
        // label = job_name+job_id+task_id+txn_id
        String label = Joiner.on("-").join(routineLoadJob.getName(), routineLoadJob.getId(), DebugUtil.printId(id), txnId);
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuth_code(routineLoadJob.getAuthCode());
        TKafkaLoadInfo tKafkaLoadInfo = new TKafkaLoadInfo();
        tKafkaLoadInfo.setTopic((routineLoadJob).getTopic());
        tKafkaLoadInfo.setBrokers((routineLoadJob).getBrokerList());
        tKafkaLoadInfo.setPartition_begin_offset(partitionIdToOffset);
        tKafkaLoadInfo.setProperties(routineLoadJob.getConvertedCustomProperties());
        tRoutineLoadTask.setKafka_load_info(tKafkaLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.KAFKA);
        tRoutineLoadTask.setParams(rePlan(routineLoadJob));
        tRoutineLoadTask.setMax_interval_s(routineLoadJob.getMaxBatchIntervalS());
        tRoutineLoadTask.setMax_batch_rows(routineLoadJob.getMaxBatchRows());
        tRoutineLoadTask.setMax_batch_size(routineLoadJob.getMaxBatchSizeBytes());
        if (!routineLoadJob.getFormat().isEmpty() && routineLoadJob.getFormat().equalsIgnoreCase("json")) {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_JSON);
        } else {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_CSV_PLAIN);
        }
        return tRoutineLoadTask;
    }

    @Override
    protected String getTaskDataSourceProperties() {
        Gson gson = new Gson();
        return gson.toJson(partitionIdToOffset);
    }

    private TExecPlanFragmentParams rePlan(RoutineLoadJob routineLoadJob) throws UserException {
        TUniqueId loadId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        // plan for each task, in case table has change(rollup or schema change)
        TExecPlanFragmentParams tExecPlanFragmentParams = routineLoadJob.plan(loadId, txnId);
        TPlanFragment tPlanFragment = tExecPlanFragmentParams.getFragment();
        tPlanFragment.getOutput_sink().getOlap_table_sink().setTxn_id(txnId);
        return tExecPlanFragmentParams;
    }
}

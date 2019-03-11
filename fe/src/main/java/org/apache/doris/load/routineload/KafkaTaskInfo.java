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

import com.google.common.base.Joiner;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TKafkaLoadInfo;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaTaskInfo extends RoutineLoadTaskInfo {

    private RoutineLoadManager routineLoadManager = Catalog.getCurrentCatalog().getRoutineLoadManager();

    private List<Integer> partitions;

    public KafkaTaskInfo(UUID id, long jobId) {
        super(id, jobId);
        this.partitions = new ArrayList<>();
    }

    public KafkaTaskInfo(KafkaTaskInfo kafkaTaskInfo) throws LabelAlreadyUsedException,
            BeginTransactionException, AnalysisException {
        super(UUID.randomUUID(), kafkaTaskInfo.getJobId(), kafkaTaskInfo.getBeId());
        this.partitions = kafkaTaskInfo.getPartitions();
    }

    public void addKafkaPartition(int partition) {
        partitions.add(partition);
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    // todo: reuse plan fragment of stream load
    @Override
    public TRoutineLoadTask createRoutineLoadTask() throws LoadException, UserException {
        KafkaRoutineLoadJob routineLoadJob = (KafkaRoutineLoadJob) routineLoadManager.getJob(jobId);
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        for (Integer partitionId : partitions) {
            KafkaProgress kafkaProgress = (KafkaProgress) routineLoadJob.getProgress();
            if (!kafkaProgress.getPartitionIdToOffset().containsKey(partitionId)) {
                kafkaProgress.getPartitionIdToOffset().put(partitionId, 0L);
            }
            partitionIdToOffset.put(partitionId, kafkaProgress.getPartitionIdToOffset().get(partitionId));
        }

        // init tRoutineLoadTask and create plan fragment
        TRoutineLoadTask tRoutineLoadTask = new TRoutineLoadTask();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tRoutineLoadTask.setId(queryId);
        tRoutineLoadTask.setJob_id(jobId);
        tRoutineLoadTask.setTxn_id(txnId);
        Database database = Catalog.getCurrentCatalog().getDb(routineLoadJob.getDbId());
        tRoutineLoadTask.setDb(database.getFullName());
        tRoutineLoadTask.setTbl(database.getTable(routineLoadJob.getTableId()).getName());
        StringBuilder stringBuilder = new StringBuilder();
        // label = (serviceAddress_topic_partition1:offset_partition2:offset).hashcode()
        String label = String.valueOf(stringBuilder.append(routineLoadJob.getBrokerList()).append("_")
                                              .append(routineLoadJob.getTopic()).append("_")
                                              .append(Joiner.on("_").withKeyValueSeparator(":")
                                                              .join(partitionIdToOffset)).toString().hashCode());
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuth_code(routineLoadJob.getAuthCode());
        TKafkaLoadInfo tKafkaLoadInfo = new TKafkaLoadInfo();
        tKafkaLoadInfo.setTopic((routineLoadJob).getTopic());
        tKafkaLoadInfo.setBrokers((routineLoadJob).getBrokerList());
        tKafkaLoadInfo.setPartition_begin_offset(partitionIdToOffset);
        tRoutineLoadTask.setKafka_load_info(tKafkaLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.KAFKA);
        tRoutineLoadTask.setParams(updateTExecPlanFragmentParams(routineLoadJob));
        return tRoutineLoadTask;
    }


    private TExecPlanFragmentParams updateTExecPlanFragmentParams(RoutineLoadJob routineLoadJob) throws UserException {
        TExecPlanFragmentParams tExecPlanFragmentParams = routineLoadJob.gettExecPlanFragmentParams().deepCopy();
        TPlanFragment tPlanFragment = tExecPlanFragmentParams.getFragment();
        tPlanFragment.getOutput_sink().getOlap_table_sink().setTxn_id(this.txnId);
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tPlanFragment.getOutput_sink().getOlap_table_sink().setLoad_id(queryId);
        tExecPlanFragmentParams.getParams().setQuery_id(queryId);
        tExecPlanFragmentParams.getParams().getPer_node_scan_ranges().values().stream()
                .forEach(entity -> entity.get(0).getScan_range().getBroker_scan_range().getRanges().get(0).setLoad_id(queryId));
        return tExecPlanFragmentParams;
    }
}

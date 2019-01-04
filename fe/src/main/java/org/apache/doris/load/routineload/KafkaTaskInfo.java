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

import com.google.common.collect.Maps;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LoadException;
import org.apache.doris.task.KafkaRoutineLoadTask;
import org.apache.doris.task.RoutineLoadTask;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.LabelAlreadyExistsException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaTaskInfo extends RoutineLoadTaskInfo {

    private RoutineLoadManager routineLoadManager = Catalog.getCurrentCatalog().getRoutineLoadManager();

    private List<Integer> partitions;

    public KafkaTaskInfo(String id, String jobId) throws LabelAlreadyExistsException,
            BeginTransactionException, AnalysisException {
        super(id, jobId);
        this.partitions = new ArrayList<>();
    }

    public KafkaTaskInfo(KafkaTaskInfo kafkaTaskInfo) throws LabelAlreadyExistsException,
            BeginTransactionException, AnalysisException {
        super(UUID.randomUUID().toString(), kafkaTaskInfo.getJobId());
        this.partitions = kafkaTaskInfo.getPartitions();
    }

    public void addKafkaPartition(int partition) {
        partitions.add(partition);
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    @Override
    public RoutineLoadTask createStreamLoadTask(long beId) throws LoadException {
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(jobId);
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        this.getPartitions().parallelStream()
                .forEach(entity -> partitionIdToOffset.put(entity, ((KafkaProgress) routineLoadJob.getProgress())
                        .getPartitionIdToOffset().get(entity)));
        RoutineLoadTask routineLoadTask = new KafkaRoutineLoadTask(routineLoadJob.getResourceInfo(),
                                                                   beId, routineLoadJob.getDbId(),
                                                                   routineLoadJob.getTableId(),
                                                                   id, txnId, partitionIdToOffset);
        if (routineLoadJob.getRoutineLoadDesc() != null) {
            routineLoadTask.setRoutineLoadDesc(routineLoadJob.getRoutineLoadDesc());
        }
        return routineLoadTask;
    }
}

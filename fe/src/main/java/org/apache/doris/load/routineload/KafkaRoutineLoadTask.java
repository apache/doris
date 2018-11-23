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

import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

import java.util.HashMap;
import java.util.Map;


public class KafkaRoutineLoadTask extends RoutineLoadTask {

    private Map<Integer, Long> partitionIdToOffset;

    public KafkaRoutineLoadTask(TResourceInfo resourceInfo, long backendId,
                                long dbId, long tableId, long partitionId, long indexId, long tabletId,
                                String columns, String where, String columnSeparator,
                                KafkaTaskInfo kafkaTaskInfo, KafkaProgress kafkaProgress) {
        super(resourceInfo, backendId, TTaskType.STREAM_LOAD, dbId, tableId, partitionId, indexId, tabletId,
                kafkaTaskInfo.getId(), columns, where, columnSeparator, RoutineLoadJob.DataSourceType.KAFKA);
        this.partitionIdToOffset = new HashMap<>();
        kafkaTaskInfo.getPartitions().parallelStream().forEach(entity ->
                partitionIdToOffset.put(entity, kafkaProgress.getPartitionIdToOffset().get(entity)));
    }

    public Map<Integer, Long> getPartitionIdToOffset() {
        return partitionIdToOffset;
    }
}

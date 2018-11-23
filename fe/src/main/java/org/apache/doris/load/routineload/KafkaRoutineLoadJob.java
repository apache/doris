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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.SystemIdGenerator;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJob.class);

    private static final String FE_GROUP_ID = "fe_fetch_partitions";
    private static final int FETCH_PARTITIONS_TIMEOUT = 10;

    private String serverAddress;
    private String topic;
    // optional, user want to load partitions.
    private List<Integer> kafkaPartitions;

    public KafkaRoutineLoadJob() {
    }

    public KafkaRoutineLoadJob(String id, String name, String userName, long dbId, long tableId,
                               String partitions, String columns, String where, String columnSeparator,
                               int desireTaskConcurrentNum, JobState state, DataSourceType dataSourceType,
                               int maxErrorNum, TResourceInfo resourceInfo, String serverAddress, String topic) {
        super(id, name, userName, dbId, tableId, partitions, columns, where,
                columnSeparator, desireTaskConcurrentNum, state, dataSourceType, maxErrorNum, resourceInfo);
        this.serverAddress = serverAddress;
        this.topic = topic;
    }

    public KafkaProgress getProgress() {
        Gson gson = new Gson();
        return gson.fromJson(this.progress, KafkaProgress.class);
    }

    @Override
    public void divideRoutineLoadJob(int currentConcurrentTaskNum) {
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULER) {
                // divide kafkaPartitions into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUID.randomUUID().toString(), id);
                    routineLoadTaskInfoList.add(kafkaTaskInfo);
                    needSchedulerTaskInfoList.add(kafkaTaskInfo);
                }
                for (int i = 0; i < kafkaPartitions.size(); i++) {
                    ((KafkaTaskInfo) routineLoadTaskInfoList.get(i % currentConcurrentTaskNum))
                            .addKafkaPartition(kafkaPartitions.get(i));
                }
                // change job state to running
                state = JobState.RUNNING;
            } else {
                LOG.debug("Ignore to divide routine load job while job state {}", state);
            }
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        updatePartitions();
        SystemInfoService systemInfoService = Catalog.getCurrentSystemInfo();
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            LOG.warn("db {} is not exists from job {}", dbId, id);
            throw new MetaNotFoundException("db " + dbId + " is not exists from job " + id);
        }
        String clusterName = db.getClusterName();
        if (Strings.isNullOrEmpty(clusterName)) {
            LOG.debug("database {} has no cluster name", dbId);
            clusterName = SystemInfoService.DEFAULT_CLUSTER;
        }
        int aliveBeNum = systemInfoService.getClusterBackendIds(clusterName, true).size();
        int partitionNum = kafkaPartitions.size();

        LOG.info("current concurrent task number is min "
                        + "(current size of partition {}, desire task concurrent num {}, alive be num {})",
                partitionNum, desireTaskConcurrentNum, aliveBeNum);
        return Math.min(partitionNum, Math.min(desireTaskConcurrentNum, aliveBeNum));
    }

    @Override
    public RoutineLoadTask createTask(RoutineLoadTaskInfo routineLoadTaskInfo, long beId) {
        return new KafkaRoutineLoadTask(getResourceInfo(),
                beId, getDbId(), getTableId(),
                0L, 0L, 0L, getColumns(), getWhere(), getColumnSeparator(),
                (KafkaTaskInfo) routineLoadTaskInfo,
                getProgress());
    }

    private void updatePartitions() {
        // fetch all of kafkaPartitions in topic
        if (kafkaPartitions == null || kafkaPartitions.size() == 0) {
            kafkaPartitions = new ArrayList<>();
            Properties props = new Properties();
            props.put("bootstrap.servers", this.serverAddress);
            props.put("group.id", FE_GROUP_ID);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            List<PartitionInfo> partitionList = consumer.partitionsFor(
                    topic, Duration.ofSeconds(FETCH_PARTITIONS_TIMEOUT));
            for (PartitionInfo partitionInfo : partitionList) {
                kafkaPartitions.add(partitionInfo.partition());
            }
        }
    }
}

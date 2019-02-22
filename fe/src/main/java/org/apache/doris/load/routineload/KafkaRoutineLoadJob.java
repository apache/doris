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

import com.google.common.collect.Sets;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.transaction.BeginTransactionException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * KafkaRoutineLoadJob is a kind of RoutineLoadJob which fetch data from kafka.
 * The progress which is super class property is seems like "{"partition1": offset1, "partition2": offset2}"
 */
public class KafkaRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJob.class);

    private static final String FE_GROUP_ID = "fe_fetch_partitions";
    private static final int FETCH_PARTITIONS_TIMEOUT = 10;

    private String serverAddress;
    private String topic;
    // optional, user want to load partitions.
    private List<Integer> customKafkaPartitions;
    // current kafka partitions is the actually partition which will be fetched
    private List<Integer> currentKafkaPartitions;

    private KafkaConsumer<String, String> consumer;

    public KafkaRoutineLoadJob(String name, long dbId, long tableId, String serverAddress, String topic) {
        super(name, dbId, tableId, LoadDataSourceType.KAFKA);
        this.serverAddress = serverAddress;
        this.topic = topic;
        this.progress = new KafkaProgress();
        this.customKafkaPartitions = new ArrayList<>();
        this.currentKafkaPartitions = new ArrayList<>();
        setConsumer();
    }

    // TODO(ml): I will change it after ut.
    @VisibleForTesting
    public KafkaRoutineLoadJob(String id, String name, long dbId, long tableId,
                               RoutineLoadDesc routineLoadDesc,
                               int desireTaskConcurrentNum, int maxErrorNum,
                               String serverAddress, String topic, KafkaProgress kafkaProgress) {
        super(id, name, dbId, tableId, routineLoadDesc,
              desireTaskConcurrentNum, LoadDataSourceType.KAFKA,
              maxErrorNum);
        this.serverAddress = serverAddress;
        this.topic = topic;
        this.progress = kafkaProgress;
        this.customKafkaPartitions = new ArrayList<>();
        this.currentKafkaPartitions = new ArrayList<>();
        setConsumer();
    }

    private void setCustomKafkaPartitions(List<Integer> kafkaPartitions) throws LoadException {
        writeLock();
        try {
            if (this.customKafkaPartitions.size() != 0) {
                throw new LoadException("Kafka partitions have been initialized");
            }
            this.customKafkaPartitions = kafkaPartitions;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public List<RoutineLoadTaskInfo> divideRoutineLoadJob(int currentConcurrentTaskNum) {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULER) {
                // divide kafkaPartitions into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    try {
                        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUID.randomUUID().toString(), id);
                        routineLoadTaskInfoList.add(kafkaTaskInfo);
                        needSchedulerTaskInfoList.add(kafkaTaskInfo);
                        result.add(kafkaTaskInfo);
                    } catch (UserException e) {
                        LOG.error("failed to begin txn for kafka routine load task, change job state to failed");
                        state = JobState.CANCELLED;
                        // TODO(ml): edit log
                        break;
                    }
                }
                if (result.size() != 0) {
                    for (int i = 0; i < currentKafkaPartitions.size(); i++) {
                        ((KafkaTaskInfo) routineLoadTaskInfoList.get(i % currentConcurrentTaskNum))
                                .addKafkaPartition(currentKafkaPartitions.get(i));
                    }
                    // change job state to running
                    // TODO(ml): edit log
                    state = JobState.RUNNING;
                }
            } else {
                LOG.debug("Ignore to divide routine load job while job state {}", state);
            }
        } finally {
            writeUnlock();
        }
        return result;
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        updateCurrentKafkaPartitions();
        SystemInfoService systemInfoService = Catalog.getCurrentSystemInfo();
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            LOG.warn("db {} is not exists from job {}", dbId, id);
            throw new MetaNotFoundException("db " + dbId + " is not exists from job " + id);
        }
        int aliveBeNum = systemInfoService.getBackendIds(true).size();
        int partitionNum = currentKafkaPartitions.size();
        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = partitionNum;
        }

        LOG.info("current concurrent task number is min "
                         + "(current size of partition {}, desire task concurrent num {}, alive be num {})",
                 partitionNum, desireTaskConcurrentNum, aliveBeNum);
        return Math.min(partitionNum, Math.min(desireTaskConcurrentNum, aliveBeNum));
    }

    @Override
    protected void updateProgress(RoutineLoadProgress progress) {
        this.progress.update(progress);
    }

    @Override
    protected RoutineLoadTaskInfo reNewTask(RoutineLoadTaskInfo routineLoadTaskInfo) throws AnalysisException,
            LabelAlreadyUsedException, BeginTransactionException {
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo((KafkaTaskInfo) routineLoadTaskInfo);
        routineLoadTaskInfoList.add(kafkaTaskInfo);
        needSchedulerTaskInfoList.add(kafkaTaskInfo);
        return kafkaTaskInfo;
    }

    // if customKafkaPartition is not null, then return false immediately
    // else if kafka partitions of topic has been changed, return true.
    // else return false
    @Override
    protected boolean needRescheduler() {
        if (customKafkaPartitions != null && customKafkaPartitions.size() != 0) {
            return false;
        } else {
            List<Integer> newCurrentKafkaPartition = getAllKafkaPartitions();
            if (currentKafkaPartitions.containsAll(newCurrentKafkaPartition)) {
                if (currentKafkaPartitions.size() > newCurrentKafkaPartition.size()) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }

        }
    }

    private List<Integer> getAllKafkaPartitions() {
        List<Integer> result = new ArrayList<>();
        List<PartitionInfo> partitionList = consumer.partitionsFor(
                topic, Duration.ofSeconds(FETCH_PARTITIONS_TIMEOUT));
        for (PartitionInfo partitionInfo : partitionList) {
            result.add(partitionInfo.partition());
        }
        return result;
    }

    public static KafkaRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws AnalysisException,
            LoadException {
        checkCreate(stmt);
        // find dbId
        Database database = Catalog.getCurrentCatalog().getDb(stmt.getDBTableName().getDb());
        Table table;
        database.readLock();
        try {
            table = database.getTable(stmt.getDBTableName().getTbl());
        } finally {
            database.readUnlock();
        }

        // init kafka routine load job
        KafkaRoutineLoadJob kafkaRoutineLoadJob =
                new KafkaRoutineLoadJob(stmt.getName(), database.getId(), table.getId(),
                                        stmt.getKafkaEndpoint(),
                                        stmt.getKafkaTopic());
        kafkaRoutineLoadJob.setOptional(stmt);

        return kafkaRoutineLoadJob;
    }

    // current kafka partitions = customKafkaPartitions == 0 ? all of partition of kafka topic : customKafkaPartitions
    private void updateCurrentKafkaPartitions() {
        if (customKafkaPartitions == null || customKafkaPartitions.size() == 0) {
            LOG.debug("All of partitions which belong to topic will be load for {} routine load job", name);
            // fetch all of kafkaPartitions in topic
            currentKafkaPartitions.addAll(getAllKafkaPartitions());
        } else {
            currentKafkaPartitions = customKafkaPartitions;
        }
        for (Integer kafkaPartition : currentKafkaPartitions) {
            try {
                ((KafkaProgress) progress).getPartitionIdToOffset().get(kafkaPartition);
            } catch (NullPointerException e) {
                ((KafkaProgress) progress).getPartitionIdToOffset().put(kafkaPartition, 0L);
            }
        }
    }

    private void setConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.serverAddress);
        props.put("group.id", FE_GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    private void setOptional(CreateRoutineLoadStmt stmt) throws LoadException {
        if (stmt.getRoutineLoadDesc() != null) {
            setRoutineLoadDesc(stmt.getRoutineLoadDesc());
        }
        if (stmt.getDesiredConcurrentNum() != 0) {
            setDesireTaskConcurrentNum(stmt.getDesiredConcurrentNum());
        }
        if (stmt.getMaxErrorNum() != 0) {
            setMaxErrorNum(stmt.getMaxErrorNum());
        }
        if (stmt.getKafkaPartitions() != null) {
            setCustomKafkaPartitions(stmt.getKafkaPartitions());
        }
    }
}

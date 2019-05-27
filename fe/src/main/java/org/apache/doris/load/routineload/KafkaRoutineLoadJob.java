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

import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * KafkaRoutineLoadJob is a kind of RoutineLoadJob which fetch data from kafka.
 * The progress which is super class property is seems like "{"partition1": offset1, "partition2": offset2}"
 */
public class KafkaRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJob.class);

    private static final int FETCH_PARTITIONS_TIMEOUT_SECOND = 5;

    private String brokerList;
    private String topic;
    // optional, user want to load partitions.
    private List<Integer> customKafkaPartitions = Lists.newArrayList();
    // current kafka partitions is the actually partition which will be fetched
    private List<Integer> currentKafkaPartitions = Lists.newArrayList();
    //kafka properties ，property prefix will be mapped to kafka custom parameters, which can be extended in the future
    private Map<String, String> customKafkaProperties = Maps.newHashMap();

    // this is the kafka consumer which is used to fetch the number of partitions
    private KafkaConsumer<String, String> consumer;

    public KafkaRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.KAFKA);
    }

    public KafkaRoutineLoadJob(Long id, String name, String clusterName, long dbId, long tableId, String brokerList,
            String topic) {
        super(id, name, clusterName, dbId, tableId, LoadDataSourceType.KAFKA);
        this.brokerList = brokerList;
        this.topic = topic;
        this.progress = new KafkaProgress();
        setConsumer();
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public Map<String, String> getProperties() {
        return customKafkaProperties;
    }

    @Override
    public void divideRoutineLoadJob(int currentConcurrentTaskNum) throws UserException {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULE) {
                // divide kafkaPartitions into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    Map<Integer, Long> taskKafkaProgress = Maps.newHashMap();
                    for (int j = 0; j < currentKafkaPartitions.size(); j++) {
                        if (j % currentConcurrentTaskNum == i) {
                            int kafkaPartition = currentKafkaPartitions.get(j);
                            taskKafkaProgress.put(kafkaPartition,
                                    ((KafkaProgress) progress).getOffsetByPartition(kafkaPartition));
                        }
                    }
                    KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), id, clusterName, taskKafkaProgress);
                    routineLoadTaskInfoList.add(kafkaTaskInfo);
                    result.add(kafkaTaskInfo);
                }
                // change job state to running
                if (result.size() != 0) {
                    unprotectUpdateState(JobState.RUNNING, null, false);
                }
            } else {
                LOG.debug("Ignore to divide routine load job while job state {}", state);
            }
            // save task into queue of needScheduleTasks
            Catalog.getCurrentCatalog().getRoutineLoadTaskScheduler().addTasksInQueue(result);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        SystemInfoService systemInfoService = Catalog.getCurrentSystemInfo();
        int aliveBeNum = systemInfoService.getClusterBackendIds(clusterName, true).size();
        int partitionNum = currentKafkaPartitions.size();
        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        LOG.info("current concurrent task number is min"
                + "(partition num: {}, desire task concurrent num: {}, alive be num: {}, config: {})",
                partitionNum, desireTaskConcurrentNum, aliveBeNum, Config.max_routine_load_task_concurrent_num);
        currentTaskConcurrentNum = 
                Math.min(Math.min(partitionNum, Math.min(desireTaskConcurrentNum, aliveBeNum)),
                        Config.max_routine_load_task_concurrent_num);
        return currentTaskConcurrentNum;
    }

    // partitionIdToOffset must be not empty when loaded rows > 0
    // situation1: be commit txn but fe throw error when committing txn,
    //             fe rollback txn without partitionIdToOffset by itself
    //             this task should not be commit
    //             otherwise currentErrorNum and currentTotalNum is updated when progress is not updated
    @Override
    protected boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment) {
        if (rlTaskTxnCommitAttachment.getLoadedRows() > 0
                && (!((KafkaProgress) rlTaskTxnCommitAttachment.getProgress()).hasPartition())) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()))
                             .add("job_id", id)
                             .add("loaded_rows", rlTaskTxnCommitAttachment.getLoadedRows())
                             .add("progress_partition_offset_size", 0)
                             .add("msg", "commit attachment info is incorrect"));
            return false;
        }
        return true;
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        super.updateProgress(attachment);
        this.progress.update(attachment.getProgress());
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        this.progress.update(attachment.getProgress());
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo routineLoadTaskInfo) {
        KafkaTaskInfo oldKafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
        // add new task
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(oldKafkaTaskInfo,
                ((KafkaProgress) progress).getPartitionIdToOffset(oldKafkaTaskInfo.getPartitions()));
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(kafkaTaskInfo);
        return kafkaTaskInfo;
    }

    @Override
    protected void unprotectUpdateProgress() {
        updateNewPartitionProgress();
    }

    // if customKafkaPartition is not null, then return false immediately
    // else if kafka partitions of topic has been changed, return true.
    // else return false
    // update current kafka partition at the same time
    // current kafka partitions = customKafkaPartitions == 0 ? all of partition of kafka topic : customKafkaPartitions
    @Override
    protected boolean unprotectNeedReschedule() throws UserException {
        // only running and need_schedule job need to be changed current kafka partitions
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            if (customKafkaPartitions != null && customKafkaPartitions.size() != 0) {
                currentKafkaPartitions = customKafkaPartitions;
                return false;
            } else {
                List<Integer> newCurrentKafkaPartition;
                try {
                    newCurrentKafkaPartition = getAllKafkaPartitions();
                } catch (Exception e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                     .add("error_msg", "Job failed to fetch all current partition with error " + e.getMessage())
                                     .build(), e);
                    if (this.state == JobState.NEED_SCHEDULE) {
                        unprotectUpdateState(JobState.PAUSED,
                                "Job failed to fetch all current partition with error " + e.getMessage(),
                                false /* not replay */);
                    }
                    return false;
                }
                if (currentKafkaPartitions.containsAll(newCurrentKafkaPartition)) {
                    if (currentKafkaPartitions.size() > newCurrentKafkaPartition.size()) {
                        currentKafkaPartitions = newCurrentKafkaPartition;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                              .add("current_kafka_partitions", Joiner.on(",").join(currentKafkaPartitions))
                                              .add("msg", "current kafka partitions has been change")
                                              .build());
                        }
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    currentKafkaPartitions = newCurrentKafkaPartition;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                          .add("current_kafka_partitions", Joiner.on(",").join(currentKafkaPartitions))
                                          .add("msg", "current kafka partitions has been change")
                                          .build());
                    }
                    return true;
                }
            }
        } else {
            return false;
        }
    }

    @Override
    protected String getStatistic() {
        Map<String, Object> summary = Maps.newHashMap();
        summary.put("totalRows", Long.valueOf(totalRows));
        summary.put("loadedRows", Long.valueOf(totalRows - errorRows - unselectedRows));
        summary.put("errorRows", Long.valueOf(errorRows));
        summary.put("unselectedRows", Long.valueOf(unselectedRows));
        summary.put("receivedBytes", Long.valueOf(receivedBytes));
        summary.put("taskExecuteTimeMs", Long.valueOf(totalTaskExcutionTimeMs));
        summary.put("receivedBytesRate", Long.valueOf(receivedBytes / totalTaskExcutionTimeMs * 1000));
        summary.put("loadRowsRate", Long.valueOf((totalRows - errorRows - unselectedRows) / totalTaskExcutionTimeMs * 1000));
        summary.put("committedTaskNum", Long.valueOf(committedTaskNum));
        summary.put("abortedTaskNum", Long.valueOf(abortedTaskNum));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    private List<Integer> getAllKafkaPartitions() throws LoadException {
        List<Integer> result = new ArrayList<>();
        try {
            List<PartitionInfo> partitionList = consumer.partitionsFor(topic,
                    Duration.ofSeconds(FETCH_PARTITIONS_TIMEOUT_SECOND));
            for (PartitionInfo partitionInfo : partitionList) {
                result.add(partitionInfo.partition());
            }
        } catch (Exception e) {
            throw new LoadException("failed to get partitions for topic: " + topic + ". " + e.getMessage());
        }
        return result;
    }

    public static KafkaRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws UserException {
        // check db and table
        Database db = Catalog.getCurrentCatalog().getDb(stmt.getDBName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDBName());
        }

        long tableId = -1L;
        db.readLock();
        try {
            unprotectedCheckMeta(db, stmt.getTableName(), stmt.getRoutineLoadDesc());
            tableId = db.getTable(stmt.getTableName()).getId();
        } finally {
            db.readUnlock();
        }

        // TODO(ml): check partition

        // init kafka routine load job
        long id = Catalog.getInstance().getNextId();
        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(id, stmt.getName(),
                db.getClusterName(), db.getId(), tableId, stmt.getKafkaBrokerList(), stmt.getKafkaTopic());
        kafkaRoutineLoadJob.setOptional(stmt);

        return kafkaRoutineLoadJob;
    }

    private void updateNewPartitionProgress() {
        // update the progress of new partitions
        for (Integer kafkaPartition : currentKafkaPartitions) {
            if (!((KafkaProgress) progress).containsPartition(kafkaPartition)) {
                // if offset is not assigned, start from OFFSET_END
                ((KafkaProgress) progress).addPartitionOffset(Pair.create(kafkaPartition, KafkaProgress.OFFSET_END_VAL));
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                      .add("kafka_partition_id", kafkaPartition)
                                      .add("begin_offset", KafkaProgress.OFFSET_END)
                                      .add("msg", "The new partition has been added in job"));
                }
            }
        }
    }

    private void setConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.brokerList);
        props.put("group.id", UUID.randomUUID().toString());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        super.setOptional(stmt);

        if (!stmt.getKafkaPartitionOffsets().isEmpty()) {
            setCustomKafkaPartitions(stmt.getKafkaPartitionOffsets());
        }
        if (!stmt.getCustomKafkaProperties().isEmpty()) {
            setCustomKafkaProperties(stmt.getCustomKafkaProperties());
        }
    }

    // this is a unprotected method which is called in the initialization function
    private void setCustomKafkaPartitions(List<Pair<Integer, Long>> kafkaPartitionOffsets) throws LoadException {
        // check if custom kafka partition is valid
        List<Integer> allKafkaPartitions = getAllKafkaPartitions();
        for (Pair<Integer, Long> partitionOffset : kafkaPartitionOffsets) {
            if (!allKafkaPartitions.contains(partitionOffset.first)) {
                throw new LoadException("there is a custom kafka partition " + partitionOffset.first
                        + " which is invalid for topic " + topic);
            }
            this.customKafkaPartitions.add(partitionOffset.first);
            ((KafkaProgress) progress).addPartitionOffset(partitionOffset);
        }
    }

    private void setCustomKafkaProperties(Map<String, String> kafkaProperties) {
        this.customKafkaProperties = kafkaProperties;
    }

    @Override
    protected String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("brokerList", brokerList);
        dataSourceProperties.put("topic", topic);
        List<Integer> sortedPartitions = Lists.newArrayList(currentKafkaPartitions);
        Collections.sort(sortedPartitions);
        dataSourceProperties.put("currentKafkaPartitions", Joiner.on(",").join(sortedPartitions));
        for (Map.Entry<String, String> property : customKafkaProperties.entrySet()) {
            dataSourceProperties.put(property.getKey(), property.getValue());
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, brokerList);
        Text.writeString(out, topic);

        out.writeInt(customKafkaPartitions.size());
        for (Integer partitionId : customKafkaPartitions) {
            out.writeInt(partitionId);
        }

        out.writeInt(customKafkaProperties.size());
        for (Map.Entry<String, String> property : customKafkaProperties.entrySet()) {
            Text.writeString(out, property.getKey());
            Text.writeString(out, property.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        brokerList = Text.readString(in);
        topic = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            customKafkaPartitions.add(in.readInt());
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_51) {
            int count = in.readInt();
            for (int i = 0 ;i < count ;i ++) {
                this.customKafkaProperties.put(Text.readString(in), Text.readString(in));
            }
        }

        setConsumer();
    }
}

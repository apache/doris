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

import org.apache.doris.analysis.AlterRoutineLoadStmt;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.kafka.KafkaUtil;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 * KafkaRoutineLoadJob is a kind of RoutineLoadJob which fetch data from kafka.
 * The progress which is super class property is seems like "{"partition1": offset1, "partition2": offset2}"
 */
public class KafkaRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJob.class);

    public static final String KAFKA_FILE_CATALOG = "kafka";
    public static final String PROP_GROUP_ID = "group.id";

    private String brokerList;
    private String topic;
    // optional, user want to load partitions.
    private List<Integer> customKafkaPartitions = Lists.newArrayList();
    // current kafka partitions is the actual partition which will be fetched
    private List<Integer> currentKafkaPartitions = Lists.newArrayList();
    // optional, user want to set default offset when new partition add or offset not set.
    // kafkaDefaultOffSet has two formats, one is the time format, eg: "2021-10-10 11:00:00",
    // the other is string value, including OFFSET_END and OFFSET_BEGINNING.
    // We should check it by calling isOffsetForTimes() method before use it.
    private String kafkaDefaultOffSet = "";
    // kafka properties ，property prefix will be mapped to kafka custom parameters, which can be extended in the future
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();

    // The latest offset of each partition fetched from kafka server.
    // Will be updated periodically by calling hasMoreDataToConsume()
    private Map<Integer, Long> cachedPartitionWithLatestOffsets = Maps.newConcurrentMap();

    // The kafka partition fetch from kafka server.
    // Will be updated periodically by calling updateKafkaPartitions();
    private List<Integer> newCurrentKafkaPartition = Lists.newArrayList();

    public KafkaRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.KAFKA);
    }

    public KafkaRoutineLoadJob(Long id, String name,
                               long dbId, long tableId, String brokerList, String topic,
                               UserIdentity userIdentity) {
        super(id, name, dbId, tableId, LoadDataSourceType.KAFKA, userIdentity);
        this.brokerList = brokerList;
        this.topic = topic;
        this.progress = new KafkaProgress();
    }

    public KafkaRoutineLoadJob(Long id, String name,
                               long dbId, String brokerList, String topic,
                               UserIdentity userIdentity, boolean isMultiTable) {
        super(id, name, dbId, LoadDataSourceType.KAFKA, userIdentity);
        this.brokerList = brokerList;
        this.topic = topic;
        this.progress = new KafkaProgress();
        setMultiTable(isMultiTable);
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public Map<String, String> getConvertedCustomProperties() {
        return convertedCustomProperties;
    }

    private boolean isOffsetForTimes() {
        long offset = TimeUtils.timeStringToLong(this.kafkaDefaultOffSet);
        return offset != -1;
    }

    private long convertedDefaultOffsetToTimestamp() {
        TimeZone timeZone = TimeUtils.getOrSystemTimeZone(getTimezone());
        return TimeUtils.timeStringToLong(this.kafkaDefaultOffSet, timeZone);
    }

    private long convertedDefaultOffsetToLong() {
        if (this.kafkaDefaultOffSet.isEmpty()) {
            return KafkaProgress.OFFSET_END_VAL;
        } else {
            if (isOffsetForTimes()) {
                return convertedDefaultOffsetToTimestamp();
            } else if (this.kafkaDefaultOffSet.equalsIgnoreCase(KafkaProgress.OFFSET_BEGINNING)) {
                return KafkaProgress.OFFSET_BEGINNING_VAL;
            } else if (this.kafkaDefaultOffSet.equalsIgnoreCase(KafkaProgress.OFFSET_END)) {
                return KafkaProgress.OFFSET_END_VAL;
            } else {
                return KafkaProgress.OFFSET_END_VAL;
            }
        }
    }

    @Override
    public void prepare() throws UserException {
        // should reset converted properties each time the job being prepared.
        // because the file info can be changed anytime.
        convertCustomProperties(true);
    }

    private void convertCustomProperties(boolean rebuild) throws DdlException {
        if (customProperties.isEmpty()) {
            return;
        }

        if (!rebuild && !convertedCustomProperties.isEmpty()) {
            return;
        }

        if (rebuild) {
            convertedCustomProperties.clear();
        }

        SmallFileMgr smallFileMgr = Env.getCurrentEnv().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                // convert FILE:file_name -> FILE:file_id:md5
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                SmallFile smallFile = smallFileMgr.getSmallFile(dbId, KAFKA_FILE_CATALOG, file, true);
                convertedCustomProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                convertedCustomProperties.put(entry.getKey(), entry.getValue());
            }
        }

        // This is mainly for compatibility. In the previous version, we directly obtained the value of the
        // KAFKA_DEFAULT_OFFSETS attribute. In the new version, we support date time as the value of
        // KAFKA_DEFAULT_OFFSETS, and this attribute will be converted into a timestamp during the analyzing phase,
        // thus losing some information. So we use KAFKA_ORIGIN_DEFAULT_OFFSETS to store the original datetime
        // formatted KAFKA_DEFAULT_OFFSETS value
        if (convertedCustomProperties.containsKey(KafkaConfiguration.KAFKA_ORIGIN_DEFAULT_OFFSETS.getName())) {
            kafkaDefaultOffSet = convertedCustomProperties
                    .remove(KafkaConfiguration.KAFKA_ORIGIN_DEFAULT_OFFSETS.getName());
            return;
        }
        if (convertedCustomProperties.containsKey(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName())) {
            kafkaDefaultOffSet = convertedCustomProperties.remove(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName());
        }
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
                    for (int j = i; j < currentKafkaPartitions.size(); j = j + currentConcurrentTaskNum) {
                        int kafkaPartition = currentKafkaPartitions.get(j);
                        taskKafkaProgress.put(kafkaPartition,
                                ((KafkaProgress) progress).getOffsetByPartition(kafkaPartition));
                    }
                    KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), id,
                            maxBatchIntervalS * Config.routine_load_task_timeout_multiplier * 1000,
                            taskKafkaProgress, isMultiTable(), -1, false);
                    routineLoadTaskInfoList.add(kafkaTaskInfo);
                    result.add(kafkaTaskInfo);
                }
                // change job state to running
                if (result.size() != 0) {
                    unprotectUpdateState(JobState.RUNNING, null, false);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignore to divide routine load job while job state {}", state);
                }
            }
            // save task into queue of needScheduleTasks
            Env.getCurrentEnv().getRoutineLoadTaskScheduler().addTasksInQueue(result);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() {
        int partitionNum = currentKafkaPartitions.size();
        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("current concurrent task number is min"
                            + "(partition num: {}, desire task concurrent num: {} config: {})",
                    partitionNum, desireTaskConcurrentNum, Config.max_routine_load_task_concurrent_num);
        }
        currentTaskConcurrentNum = Math.min(partitionNum, Math.min(desireTaskConcurrentNum,
                Config.max_routine_load_task_concurrent_num));
        return currentTaskConcurrentNum;
    }

    // Through the transaction status and attachment information, to determine whether the progress needs to be updated.
    @Override
    protected boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
                                      TransactionState txnState,
                                      TransactionState.TxnStatusChangeReason txnStatusChangeReason) {
        if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            // For committed txn, update the progress.
            return true;
        }

        // Running here, the status of the transaction should be ABORTED,
        // and it is caused by other errors. In this case, we should not update the offset.
        if (LOG.isDebugEnabled()) {
            LOG.debug("no need to update the progress of kafka routine load. txn status: {}, "
                            + "txnStatusChangeReason: {}, task: {}, job: {}",
                    txnState.getTransactionStatus(), txnStatusChangeReason,
                    DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        }
        return false;
    }

    private void updateProgressAndOffsetsCache(RLTaskTxnCommitAttachment attachment) {
        ((KafkaProgress) attachment.getProgress()).getOffsetByPartition().entrySet().stream()
                .forEach(entity -> {
                    if (cachedPartitionWithLatestOffsets.containsKey(entity.getKey())
                            && cachedPartitionWithLatestOffsets.get(entity.getKey()) < entity.getValue() + 1) {
                        cachedPartitionWithLatestOffsets.put(entity.getKey(), entity.getValue() + 1);
                    }
                });
        this.progress.update(attachment);
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        updateProgressAndOffsetsCache(attachment);
        super.updateProgress(attachment);
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        updateProgressAndOffsetsCache(attachment);
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo routineLoadTaskInfo) {
        KafkaTaskInfo oldKafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
        // add new task
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(oldKafkaTaskInfo,
                ((KafkaProgress) progress).getPartitionIdToOffset(oldKafkaTaskInfo.getPartitions()), isMultiTable());
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(kafkaTaskInfo);
        return kafkaTaskInfo;
    }

    @Override
    protected void unprotectUpdateProgress() throws UserException {
        updateNewPartitionProgress();
    }

    @Override
    protected void preCheckNeedSchedule() throws UserException {
        // If user does not specify kafka partition,
        // We will fetch partition from kafka server periodically
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            if (customKafkaPartitions != null && !customKafkaPartitions.isEmpty()) {
                return;
            }
            updateKafkaPartitions();
        }
    }

    private void updateKafkaPartitions() throws UserException {
        try {
            this.newCurrentKafkaPartition = getAllKafkaPartitions();
        } catch (Exception e) {
            String msg = e.getMessage()
                        + " may be Kafka properties set in job is error"
                        + " or no partition in this topic that should check Kafka";
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("error_msg", msg)
                    .build(), e);
            if (this.state == JobState.NEED_SCHEDULE) {
                unprotectUpdateState(JobState.PAUSED,
                        new ErrorReason(InternalErrorCode.PARTITIONS_ERR, msg),
                        false /* not replay */);
            }
        }
    }

    // if customKafkaPartition is not null, then return false immediately
    // else if kafka partitions of topic has been changed, return true.
    // else return false
    // update current kafka partition at the same time
    // current kafka partitions = customKafkaPartitions == 0 ? all partition of kafka topic : customKafkaPartitions
    @Override
    protected boolean unprotectNeedReschedule() throws UserException {
        // only running and need_schedule job need to be changed current kafka partitions
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            if (CollectionUtils.isNotEmpty(customKafkaPartitions)) {
                currentKafkaPartitions = customKafkaPartitions;
                return false;
            }
            // the newCurrentKafkaPartition should be already updated in preCheckNeedScheduler()
            Preconditions.checkNotNull(this.newCurrentKafkaPartition);
            if (new HashSet<>(currentKafkaPartitions).containsAll(this.newCurrentKafkaPartition)) {
                if (currentKafkaPartitions.size() > this.newCurrentKafkaPartition.size()) {
                    currentKafkaPartitions = this.newCurrentKafkaPartition;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                .add("current_kafka_partitions", Joiner.on(",").join(currentKafkaPartitions))
                                .add("msg", "current kafka partitions has been change")
                                .build());
                    }
                    return true;
                } else {
                    // if the partitions of currentKafkaPartitions and progress are inconsistent,
                    // We should also update the progress
                    for (Integer kafkaPartition : currentKafkaPartitions) {
                        if (!((KafkaProgress) progress).containsPartition(kafkaPartition)) {
                            return true;
                        }
                    }
                    return false;
                }
            } else {
                currentKafkaPartitions = this.newCurrentKafkaPartition;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("current_kafka_partitions", Joiner.on(",").join(currentKafkaPartitions))
                            .add("msg", "current kafka partitions has been change")
                            .build());
                }
                return true;
            }

        }
        if (this.state == JobState.PAUSED) {
            return ScheduleRule.isNeedAutoSchedule(this);
        }
        return false;

    }

    @Override
    protected String getStatistic() {
        Map<String, Object> summary = this.jobStatistic.summary();
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    private List<Integer> getAllKafkaPartitions() throws UserException {
        convertCustomProperties(false);
        return KafkaUtil.getAllKafkaPartitions(brokerList, topic, convertedCustomProperties);
    }

    public static KafkaRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws UserException {
        // check db and table
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(stmt.getDBName());

        long id = Env.getCurrentEnv().getNextId();
        KafkaDataSourceProperties kafkaProperties = (KafkaDataSourceProperties) stmt.getDataSourceProperties();
        KafkaRoutineLoadJob kafkaRoutineLoadJob;
        if (kafkaProperties.isMultiTable()) {
            kafkaRoutineLoadJob = new KafkaRoutineLoadJob(id, stmt.getName(),
                    db.getId(),
                    kafkaProperties.getBrokerList(), kafkaProperties.getTopic(), stmt.getUserInfo(), true);
        } else {
            OlapTable olapTable = db.getOlapTableOrDdlException(stmt.getTableName());
            checkMeta(olapTable, stmt.getRoutineLoadDesc());
            long tableId = olapTable.getId();
            // init kafka routine load job
            kafkaRoutineLoadJob = new KafkaRoutineLoadJob(id, stmt.getName(),
                    db.getId(), tableId,
                    kafkaProperties.getBrokerList(), kafkaProperties.getTopic(), stmt.getUserInfo());
        }
        kafkaRoutineLoadJob.setOptional(stmt);
        kafkaRoutineLoadJob.checkCustomProperties();
        kafkaRoutineLoadJob.checkCustomPartition();

        return kafkaRoutineLoadJob;
    }

    private void checkCustomPartition() throws UserException {
        if (customKafkaPartitions.isEmpty()) {
            return;
        }
        List<Integer> allKafkaPartitions = getAllKafkaPartitions();
        for (Integer customPartition : customKafkaPartitions) {
            if (!allKafkaPartitions.contains(customPartition)) {
                throw new LoadException("there is a custom kafka partition " + customPartition
                        + " which is invalid for topic " + topic);
            }
        }
    }

    private void checkCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = Env.getCurrentEnv().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                // check file
                if (!smallFileMgr.containsFile(dbId, KAFKA_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with catalog: " + KAFKA_FILE_CATALOG);
                }
            }
        }
    }

    private void updateNewPartitionProgress() throws UserException {
        // update the progress of new partitions
        try {
            for (Integer kafkaPartition : currentKafkaPartitions) {
                if (!((KafkaProgress) progress).containsPartition(kafkaPartition)) {
                    List<Integer> newPartitions = Lists.newArrayList();
                    newPartitions.add(kafkaPartition);
                    List<Pair<Integer, Long>> newPartitionsOffsets
                            = getNewPartitionOffsetsFromDefaultOffset(newPartitions);
                    Preconditions.checkState(newPartitionsOffsets.size() == 1);
                    for (Pair<Integer, Long> partitionOffset : newPartitionsOffsets) {
                        ((KafkaProgress) progress).addPartitionOffset(partitionOffset);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                    .add("kafka_partition_id", partitionOffset.first)
                                    .add("begin_offset", partitionOffset.second)
                                    .add("msg", "The new partition has been added in job"));
                        }
                    }
                }
            }
        } catch (UserException e) {
            unprotectUpdateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.PARTITIONS_ERR, e.getMessage()), false /* not replay */);
            throw e;
        }
    }

    private List<Pair<Integer, Long>> getNewPartitionOffsetsFromDefaultOffset(List<Integer> newPartitions)
            throws UserException {
        List<Pair<Integer, Long>> partitionOffsets = Lists.newArrayList();
        // get default offset
        long beginOffset = convertedDefaultOffsetToLong();
        for (Integer kafkaPartition : newPartitions) {
            partitionOffsets.add(Pair.of(kafkaPartition, beginOffset));
        }
        try {
            if (isOffsetForTimes()) {
                partitionOffsets = KafkaUtil.getOffsetsForTimes(this.brokerList,
                        this.topic, convertedCustomProperties, partitionOffsets);
            } else {
                partitionOffsets = KafkaUtil.getRealOffsets(this.brokerList,
                        this.topic, convertedCustomProperties, partitionOffsets);
            }
        } catch (LoadException e) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("partition:", Joiner.on(",").join(partitionOffsets))
                    .add("error_msg", "Job failed to fetch current offsets with error " + e.getMessage())
                    .build(), e);
            throw new UserException(e);
        }
        return partitionOffsets;
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        super.setOptional(stmt);
        KafkaDataSourceProperties kafkaDataSourceProperties
                = (KafkaDataSourceProperties) stmt.getDataSourceProperties();
        if (CollectionUtils.isNotEmpty(kafkaDataSourceProperties.getKafkaPartitionOffsets())) {
            setCustomKafkaPartitions(kafkaDataSourceProperties);
        }
        if (MapUtils.isNotEmpty(kafkaDataSourceProperties.getCustomKafkaProperties())) {
            setCustomKafkaProperties(kafkaDataSourceProperties.getCustomKafkaProperties());
        }
        // set group id if not specified
        this.customProperties.putIfAbsent(PROP_GROUP_ID, name + "_" + UUID.randomUUID());
    }

    // this is an unprotected method which is called in the initialization function
    private void setCustomKafkaPartitions(KafkaDataSourceProperties kafkaDataSourceProperties) throws LoadException {

        List<Pair<Integer, Long>> kafkaPartitionOffsets = kafkaDataSourceProperties.getKafkaPartitionOffsets();
        boolean isForTimes = kafkaDataSourceProperties.isOffsetsForTimes();
        if (isForTimes) {
            // the offset is set by date time, we need to get the real offset by time
            kafkaPartitionOffsets = KafkaUtil.getOffsetsForTimes(kafkaDataSourceProperties.getBrokerList(),
                    kafkaDataSourceProperties.getTopic(),
                    convertedCustomProperties, kafkaDataSourceProperties.getKafkaPartitionOffsets());
        } else {
            kafkaPartitionOffsets = KafkaUtil.getRealOffsets(kafkaDataSourceProperties.getBrokerList(),
                    kafkaDataSourceProperties.getTopic(),
                    convertedCustomProperties, kafkaDataSourceProperties.getKafkaPartitionOffsets());
        }

        for (Pair<Integer, Long> partitionOffset : kafkaPartitionOffsets) {
            this.customKafkaPartitions.add(partitionOffset.first);
            ((KafkaProgress) progress).addPartitionOffset(partitionOffset);
        }
    }

    private void setCustomKafkaProperties(Map<String, String> kafkaProperties) {
        this.customProperties = kafkaProperties;
    }

    @Override
    protected String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("brokerList", brokerList);
        dataSourceProperties.put("topic", topic);
        List<Integer> sortedPartitions = Lists.newArrayList(currentKafkaPartitions);
        Collections.sort(sortedPartitions);
        dataSourceProperties.put("currentKafkaPartitions", Joiner.on(",").join(sortedPartitions));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    protected String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(customProperties);
    }

    @Override
    protected Map<String, String> getDataSourceProperties() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("kafka_broker_list", brokerList);
        dataSourceProperties.put("kafka_topic", topic);
        return dataSourceProperties;
    }

    @Override
    protected Map<String, String> getCustomProperties() {
        Map<String, String> ret = new HashMap<>();
        customProperties.forEach((k, v) -> ret.put("property." + k, v));
        return ret;
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

        out.writeInt(customProperties.size());
        for (Map.Entry<String, String> property : customProperties.entrySet()) {
            Text.writeString(out, "property." + property.getKey());
            Text.writeString(out, property.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        brokerList = Text.readString(in);
        topic = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            customKafkaPartitions.add(in.readInt());
        }

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String propertyKey = Text.readString(in);
            String propertyValue = Text.readString(in);
            if (propertyKey.startsWith("property.")) {
                this.customProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
            }
        }
    }

    @Override
    public void modifyProperties(AlterRoutineLoadStmt stmt) throws UserException {
        Map<String, String> jobProperties = stmt.getAnalyzedJobProperties();
        KafkaDataSourceProperties dataSourceProperties = (KafkaDataSourceProperties) stmt.getDataSourceProperties();
        if (null != dataSourceProperties) {
            // if the partition offset is set by timestamp, convert it to real offset
            convertOffset(dataSourceProperties);
        }

        writeLock();
        try {
            if (getState() != JobState.PAUSED) {
                throw new DdlException("Only supports modification of PAUSED jobs");
            }

            modifyPropertiesInternal(jobProperties, dataSourceProperties);

            AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(this.id,
                    jobProperties, dataSourceProperties);
            Env.getCurrentEnv().getEditLog().logAlterRoutineLoadJob(log);
        } finally {
            writeUnlock();
        }
    }

    private void convertOffset(KafkaDataSourceProperties dataSourceProperties) throws UserException {
        List<Pair<Integer, Long>> partitionOffsets = dataSourceProperties.getKafkaPartitionOffsets();
        if (partitionOffsets.isEmpty()) {
            return;
        }
        List<Pair<Integer, Long>> newOffsets;
        if (dataSourceProperties.isOffsetsForTimes()) {
            newOffsets = KafkaUtil.getOffsetsForTimes(brokerList, topic, convertedCustomProperties, partitionOffsets);
        } else {
            newOffsets = KafkaUtil.getRealOffsets(brokerList, topic, convertedCustomProperties, partitionOffsets);
        }
        dataSourceProperties.setKafkaPartitionOffsets(newOffsets);
    }

    private void modifyPropertiesInternal(Map<String, String> jobProperties,
                                          KafkaDataSourceProperties dataSourceProperties)
            throws DdlException {
        if (null != dataSourceProperties) {
            List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();
            Map<String, String> customKafkaProperties = Maps.newHashMap();

            if (MapUtils.isNotEmpty(dataSourceProperties.getOriginalDataSourceProperties())) {
                kafkaPartitionOffsets = dataSourceProperties.getKafkaPartitionOffsets();
                customKafkaProperties = dataSourceProperties.getCustomKafkaProperties();
            }

            // convertCustomProperties and check partitions before reset progress to make modify operation atomic
            if (!customKafkaProperties.isEmpty()) {
                this.customProperties.putAll(customKafkaProperties);
                convertCustomProperties(true);
            }

            if (!kafkaPartitionOffsets.isEmpty()) {
                ((KafkaProgress) progress).checkPartitions(kafkaPartitionOffsets);
            }

            // It is necessary to reset the Kafka progress cache if topic change,
            // and should reset cache before modifying partition offset.
            if (!Strings.isNullOrEmpty(dataSourceProperties.getTopic())) {
                this.topic = dataSourceProperties.getTopic();
                this.progress = new KafkaProgress();
            }

            // modify partition offset
            if (!kafkaPartitionOffsets.isEmpty()) {
                // we can only modify the partition that is being consumed
                ((KafkaProgress) progress).modifyOffset(kafkaPartitionOffsets);
            }

            // modify broker list
            if (!Strings.isNullOrEmpty(dataSourceProperties.getBrokerList())) {
                this.brokerList = dataSourceProperties.getBrokerList();
            }
        }
        if (!jobProperties.isEmpty()) {
            Map<String, String> copiedJobProperties = Maps.newHashMap(jobProperties);
            modifyCommonJobProperties(copiedJobProperties);
            this.jobProperties.putAll(copiedJobProperties);
            if (jobProperties.containsKey(CreateRoutineLoadStmt.PARTIAL_COLUMNS)) {
                this.isPartialUpdate = BooleanUtils.toBoolean(jobProperties.get(CreateRoutineLoadStmt.PARTIAL_COLUMNS));
            }
        }
        LOG.info("modify the properties of kafka routine load job: {}, jobProperties: {}, datasource properties: {}",
                this.id, jobProperties, dataSourceProperties);
    }

    @Override
    public void replayModifyProperties(AlterRoutineLoadJobOperationLog log) {
        try {
            modifyPropertiesInternal(log.getJobProperties(), (KafkaDataSourceProperties) log.getDataSourceProperties());
        } catch (DdlException e) {
            // should not happen
            LOG.error("failed to replay modify kafka routine load job: {}", id, e);
        }
    }

    // check if given partitions has more data to consume.
    // 'partitionIdToOffset' to the offset to be consumed.
    public boolean hasMoreDataToConsume(UUID taskId, Map<Integer, Long> partitionIdToOffset) throws UserException {
        boolean needUpdateCache = false;
        // it is need check all partitions, for some partitions offset may be out of time
        for (Map.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            if (cachedPartitionWithLatestOffsets.containsKey(entry.getKey())
                    && entry.getValue() < cachedPartitionWithLatestOffsets.get(entry.getKey())) {
                // "entry.getValue()" is the offset to be consumed.
                // "cachedPartitionWithLatestOffsets.get(entry.getKey())" is the "next" offset of this partition.
                // (because librdkafa's query_watermark_offsets() will return the next offset.
                //  For example, there 4 msg in partition with offset 0,1,2,3,
                //  query_watermark_offsets() will return 4.)
                if (LOG.isDebugEnabled()) {
                    LOG.debug("has more data to consume. offsets to be consumed: {}, "
                                    + "latest offsets: {}, task {}, job {}",
                            partitionIdToOffset, cachedPartitionWithLatestOffsets, taskId, id);
                }
            } else {
                needUpdateCache = true;
                break;
            }
        }
        if (needUpdateCache == false) {
            return true;
        }

        try {
            // all offsets to be consumed are newer than offsets in cachedPartitionWithLatestOffsets,
            // maybe the cached offset is out-of-date, fetch from kafka server again
            List<Pair<Integer, Long>> tmp = KafkaUtil.getLatestOffsets(id, taskId, getBrokerList(),
                    getTopic(), getConvertedCustomProperties(), Lists.newArrayList(partitionIdToOffset.keySet()));
            for (Pair<Integer, Long> pair : tmp) {
                if (pair.second >= cachedPartitionWithLatestOffsets.getOrDefault(pair.first, Long.MIN_VALUE)) {
                    cachedPartitionWithLatestOffsets.put(pair.first, pair.second);
                } else {
                    LOG.warn("Kafka offset fallback. partition: {}, cache offset: {}"
                                + " get latest offset: {}, task {}, job {}",
                                pair.first, cachedPartitionWithLatestOffsets.getOrDefault(pair.first, Long.MIN_VALUE),
                                pair.second, taskId, id);
                }
            }
        } catch (Exception e) {
            // It needs to pause job when can not get partition meta.
            // To ensure the stability of the routine load,
            // the scheduler will automatically pull up routine load job in this scenario,
            // to avoid some network and Kafka exceptions causing the routine load job to stop
            updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.PARTITIONS_ERR,
                        "failed to get latest partition offset. {}" + e.getMessage()),
                        false /* not replay */);
            return false;
        }

        // check again
        for (Map.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            Integer partitionId = entry.getKey();
            if (cachedPartitionWithLatestOffsets.containsKey(partitionId)) {
                long partitionLatestOffset = cachedPartitionWithLatestOffsets.get(partitionId);
                long recordPartitionOffset = entry.getValue();
                if (recordPartitionOffset < partitionLatestOffset) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("has more data to consume. offsets to be consumed: {},"
                                + " latest offsets: {}, task {}, job {}",
                                partitionIdToOffset, cachedPartitionWithLatestOffsets, taskId, id);
                    }
                    return true;
                } else if (recordPartitionOffset > partitionLatestOffset) {
                    String msg = "offset set in job: " + recordPartitionOffset
                                + " is greater than kafka latest offset: "
                                + partitionLatestOffset + " partition id: "
                                + partitionId;
                    throw new UserException(msg);
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("no more data to consume. offsets to be consumed: {}, latest offsets: {}, task {}, job {}",
                    partitionIdToOffset, cachedPartitionWithLatestOffsets, taskId, id);
        }
        return false;
    }

    @Override
    protected String getLag() {
        Map<Integer, Long> partitionIdToOffsetLag = ((KafkaProgress) progress).getLag(cachedPartitionWithLatestOffsets);
        Gson gson = new Gson();
        return gson.toJson(partitionIdToOffsetLag);
    }

    @Override
    public TFileCompressType getCompressType() {
        return TFileCompressType.PLAIN;
    }

    @Override
    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }
}

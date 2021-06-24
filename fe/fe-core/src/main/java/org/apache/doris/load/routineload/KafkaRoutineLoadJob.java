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
import org.apache.doris.analysis.RoutineLoadDataSourceProperties;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.KafkaUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.UUID;

/**
 * KafkaRoutineLoadJob is a kind of RoutineLoadJob which fetch data from kafka.
 * The progress which is super class property is seems like "{"partition1": offset1, "partition2": offset2}"
 */
public class KafkaRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJob.class);

    public static final String KAFKA_FILE_CATALOG = "kafka";

    private String brokerList;
    private String topic;
    // optional, user want to load partitions.
    private List<Integer> customKafkaPartitions = Lists.newArrayList();
    // current kafka partitions is the actually partition which will be fetched
    private List<Integer> currentKafkaPartitions = Lists.newArrayList();
    // optional, user want to set default offset when new partition add or offset not set.
    // kafkaDefaultOffSet has two formats, one is the time format, eg: "2021-10-10 11:00:00",
    // the other is string value, including OFFSET_END and OFFSET_BEGINNING.
    // We should check it by calling isOffsetForTimes() method before use it.
    private String kafkaDefaultOffSet = "";
    // kafka properties ，property prefix will be mapped to kafka custom parameters, which can be extended in the future
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();

    public KafkaRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.KAFKA);
    }

    public KafkaRoutineLoadJob(Long id, String name, String clusterName,
                               long dbId, long tableId, String brokerList, String topic) {
        super(id, name, clusterName, dbId, tableId, LoadDataSourceType.KAFKA);
        this.brokerList = brokerList;
        this.topic = topic;
        this.progress = new KafkaProgress();
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

    @Override
    public void prepare() throws UserException {
        super.prepare();
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

        SmallFileMgr smallFileMgr = Catalog.getCurrentCatalog().getSmallFileMgr();
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
        // KAFKA_DEFAULT_OFFSETS attribute. In the new version, we support date time as the value of KAFKA_DEFAULT_OFFSETS,
        // and this attribute will be converted into a timestamp during the analyzing phase, thus losing some information.
        // So we use KAFKA_ORIGIN_DEFAULT_OFFSETS to store the original datetime formatted KAFKA_DEFAULT_OFFSETS value
        if (convertedCustomProperties.containsKey(CreateRoutineLoadStmt.KAFKA_ORIGIN_DEFAULT_OFFSETS)) {
            kafkaDefaultOffSet = convertedCustomProperties.remove(CreateRoutineLoadStmt.KAFKA_ORIGIN_DEFAULT_OFFSETS);
        } else if (convertedCustomProperties.containsKey(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS)) {
            kafkaDefaultOffSet = convertedCustomProperties.remove(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS);
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
                    for (int j = 0; j < currentKafkaPartitions.size(); j++) {
                        if (j % currentConcurrentTaskNum == i) {
                            int kafkaPartition = currentKafkaPartitions.get(j);
                            taskKafkaProgress.put(kafkaPartition,
                                    ((KafkaProgress) progress).getOffsetByPartition(kafkaPartition));
                        }
                    }
                    KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), id, clusterName,
                            maxBatchIntervalS * 2 * 1000, taskKafkaProgress);
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

        LOG.debug("current concurrent task number is min"
                        + "(partition num: {}, desire task concurrent num: {}, alive be num: {}, config: {})",
                partitionNum, desireTaskConcurrentNum, aliveBeNum, Config.max_routine_load_task_concurrent_num);
        currentTaskConcurrentNum = Math.min(Math.min(partitionNum, Math.min(desireTaskConcurrentNum, aliveBeNum)),
                Config.max_routine_load_task_concurrent_num);
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

        if (txnStatusChangeReason != null && txnStatusChangeReason == TransactionState.TxnStatusChangeReason.NO_PARTITIONS) {
            // Because the max_filter_ratio of routine load task is always 1.
            // Therefore, under normal circumstances, routine load task will not return the error "too many filtered rows".
            // If no data is imported, the error "all partitions have no load data" may only be returned.
            // In this case, the status of the transaction is ABORTED,
            // but we still need to update the offset to skip these error lines.
            Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.ABORTED, txnState.getTransactionStatus());
            return true;
        }

        // Running here, the status of the transaction should be ABORTED,
        // and it is caused by other errors. In this case, we should not update the offset.
        LOG.debug("no need to update the progress of kafka routine load. txn status: {}, " +
                        "txnStatusChangeReason: {}, task: {}, job: {}",
                txnState.getTransactionStatus(), txnStatusChangeReason,
                DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        return false;
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        super.updateProgress(attachment);
        this.progress.update(attachment);
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        this.progress.update(attachment);
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
    protected void unprotectUpdateProgress() throws UserException {
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
                                new ErrorReason(InternalErrorCode.PARTITIONS_ERR,
                                        "Job failed to fetch all current partition with error " + e.getMessage()),
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
        } else if (this.state == JobState.PAUSED) {
            return ScheduleRule.isNeedAutoSchedule(this);
        } else {
            return false;
        }
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
        Database db = Catalog.getCurrentCatalog().getDb(stmt.getDBName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDBName());
        }

        checkMeta(db, stmt.getTableName(), stmt.getRoutineLoadDesc());
        Table table = db.getTable(stmt.getTableName());
        long tableId = table.getId();

        // init kafka routine load job
        long id = Catalog.getCurrentCatalog().getNextId();
        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(id, stmt.getName(),
                db.getClusterName(), db.getId(), tableId,
                stmt.getKafkaBrokerList(), stmt.getKafkaTopic());
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
        SmallFileMgr smallFileMgr = Catalog.getCurrentCatalog().getSmallFileMgr();
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

    private void updateNewPartitionProgress() throws LoadException {
        // update the progress of new partitions
        for (Integer kafkaPartition : currentKafkaPartitions) {
            if (!((KafkaProgress) progress).containsPartition(kafkaPartition)) {
                // if offset is not assigned, start from OFFSET_END
                long beginOffSet = KafkaProgress.OFFSET_END_VAL;
                if (!kafkaDefaultOffSet.isEmpty()) {
                    if (isOffsetForTimes()) {
                        // get offset by time
                        List<Pair<Integer, Long>> offsets = Lists.newArrayList();
                        offsets.add(Pair.create(kafkaPartition, convertedDefaultOffsetToTimestamp()));
                        offsets = KafkaUtil.getOffsetsForTimes(this.brokerList, this.topic, convertedCustomProperties, offsets);
                        Preconditions.checkState(offsets.size() == 1);
                        beginOffSet = offsets.get(0).second;
                    } else if (kafkaDefaultOffSet.equalsIgnoreCase(KafkaProgress.OFFSET_BEGINNING)) {
                        beginOffSet = KafkaProgress.OFFSET_BEGINNING_VAL;
                    } else if (kafkaDefaultOffSet.equalsIgnoreCase(KafkaProgress.OFFSET_END)) {
                        beginOffSet = KafkaProgress.OFFSET_END_VAL;
                    } else {
                        beginOffSet = KafkaProgress.OFFSET_END_VAL;
                    }
                }
                ((KafkaProgress) progress).addPartitionOffset(Pair.create(kafkaPartition, beginOffSet));
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("kafka_partition_id", kafkaPartition)
                            .add("begin_offset", beginOffSet)
                            .add("msg", "The new partition has been added in job"));
                }
            }
        }
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        super.setOptional(stmt);

        if (!stmt.getKafkaPartitionOffsets().isEmpty()) {
            setCustomKafkaPartitions(stmt);
        }
        if (!stmt.getCustomKafkaProperties().isEmpty()) {
            setCustomKafkaProperties(stmt.getCustomKafkaProperties());
        }
    }

    // this is a unprotected method which is called in the initialization function
    private void setCustomKafkaPartitions(CreateRoutineLoadStmt stmt) throws LoadException {
        List<Pair<Integer, Long>> kafkaPartitionOffsets = stmt.getKafkaPartitionOffsets();
        boolean isForTimes = stmt.isOffsetsForTimes();
        if (isForTimes) {
            // the offset is set by date time, we need to get the real offset by time
            kafkaPartitionOffsets = KafkaUtil.getOffsetsForTimes(stmt.getKafkaBrokerList(), stmt.getKafkaTopic(),
                    convertedCustomProperties, stmt.getKafkaPartitionOffsets());
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

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_51) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                String propertyKey = Text.readString(in);
                String propertyValue = Text.readString(in);
                if (propertyKey.startsWith("property.")) {
                    this.customProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
                }
            }
        }
    }

    @Override
    public void modifyProperties(AlterRoutineLoadStmt stmt) throws UserException {
        Map<String, String> jobProperties = stmt.getAnalyzedJobProperties();
        RoutineLoadDataSourceProperties dataSourceProperties = stmt.getDataSourceProperties();
        if (dataSourceProperties.isOffsetsForTimes()) {
            // if the partition offset is set by timestamp, convert it to real offset
            convertTimestampToOffset(dataSourceProperties);
        }

        writeLock();
        try {
            if (getState() != JobState.PAUSED) {
                throw new DdlException("Only supports modification of PAUSED jobs");
            }


            modifyPropertiesInternal(jobProperties, dataSourceProperties);

            AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(this.id,
                    jobProperties, dataSourceProperties);
            Catalog.getCurrentCatalog().getEditLog().logAlterRoutineLoadJob(log);
        } finally {
            writeUnlock();
        }
    }

    private void convertTimestampToOffset(RoutineLoadDataSourceProperties dataSourceProperties) throws UserException {
        List<Pair<Integer, Long>> partitionOffsets = dataSourceProperties.getKafkaPartitionOffsets();
        if (partitionOffsets.isEmpty()) {
            return;
        }
        List<Pair<Integer, Long>> newOffsets = KafkaUtil.getOffsetsForTimes(brokerList, topic,
                convertedCustomProperties, partitionOffsets);
        dataSourceProperties.setKafkaPartitionOffsets(newOffsets);
    }

    private void modifyPropertiesInternal(Map<String, String> jobProperties,
                                          RoutineLoadDataSourceProperties dataSourceProperties)
            throws DdlException {

        List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();
        Map<String, String> customKafkaProperties = Maps.newHashMap();

        if (dataSourceProperties.hasAnalyzedProperties()) {
            kafkaPartitionOffsets = dataSourceProperties.getKafkaPartitionOffsets();
            customKafkaProperties = dataSourceProperties.getCustomKafkaProperties();
        }

        // modify partition offset first
        if (!kafkaPartitionOffsets.isEmpty()) {
            // we can only modify the partition that is being consumed
            ((KafkaProgress) progress).modifyOffset(kafkaPartitionOffsets);
        }

        if (!jobProperties.isEmpty()) {
            Map<String, String> copiedJobProperties = Maps.newHashMap(jobProperties);
            modifyCommonJobProperties(copiedJobProperties);
            this.jobProperties.putAll(copiedJobProperties);
        }

        if (!customKafkaProperties.isEmpty()) {
            this.customProperties.putAll(customKafkaProperties);
            convertCustomProperties(true);
        }

        LOG.info("modify the properties of kafka routine load job: {}, jobProperties: {}, datasource properties: {}",
                this.id, jobProperties, dataSourceProperties);
    }

    @Override
    public void replayModifyProperties(AlterRoutineLoadJobOperationLog log) {
        try {
            modifyPropertiesInternal(log.getJobProperties(), log.getDataSourceProperties());
        } catch (DdlException e) {
            // should not happen
            LOG.error("failed to replay modify kafka routine load job: {}", id, e);
        }
    }
}

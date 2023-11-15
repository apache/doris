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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.KafkaUtil;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TKafkaLoadInfo;
import org.apache.doris.thrift.TKafkaTvfTask;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;

public class KafkaTableValuedFunction extends ExternalFileTableValuedFunction {
    public static final String NAME = "kafka";
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;

    private String brokerList;

    private String topic;

    // for kafka properties
    private KafkaDataSourceProperties kafkaDataSourceProperties;
    private Map<String, String> customKafkaProperties = Maps.newHashMap();
    private final List<Integer> kafkaPartitions = Lists.newArrayList();
    private Boolean hasCustomPartitions = true;

    // for jobProperties
    private final Map<String, Long> jobProperties = Maps.newHashMap();

    private Long dbId = -1L;

    @Getter
    private final Map<Long, TKafkaTvfTask> kafkaTvfTaskMap = Maps.newHashMap();

    public static final String PROP_GROUP_ID = "group.id";
    public static final String KAFKA_FILE_CATALOG = "kafka";

    public KafkaTableValuedFunction(Map<String, String> properties) throws UserException {
        // 1. parse and analyze common properties
        Map<String, String> otherProperties = super.parseCommonProperties(properties);

        // 2. parse and analyze kafka properties
        parseAndAnalyzeKafkaProperties(otherProperties);

        // 3. parse and analyze job properties
        parseAndAnalyzeJobProperties(otherProperties);

        // 4. divide partitions
        int concurrentTaskNum = calculateConcurrentTaskNum();
        getKafkaTvfTaskInfoList(concurrentTaskNum);

        // 5. transfer partition list to be
        transferPartitionListToBe();
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_KAFKA;
    }

    @Override
    public String getFilePath() {
        return null;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("KafkaTvfBroker", StorageBackend.StorageType.KAFKA, locationProperties);
    }

    @Override
    public String getTableName() {
        return "KafkaTableValuedFunction";
    }

    protected void setOptional() throws UserException {
        // set custom kafka partitions
        if (CollectionUtils.isNotEmpty(kafkaDataSourceProperties.getKafkaPartitionOffsets())) {
            setKafkaPartitions();
        }
        // set kafka customProperties
        if (MapUtils.isNotEmpty(kafkaDataSourceProperties.getCustomKafkaProperties())) {
            setCustomKafkaProperties(kafkaDataSourceProperties.getCustomKafkaProperties());
        }
        // set group id if not specified
        this.customKafkaProperties.putIfAbsent(PROP_GROUP_ID, "_" + UUID.randomUUID());
    }

    private void setKafkaPartitions() throws LoadException {
        // get kafka partition offsets
        List<Pair<Integer, Long>> kafkaPartitionOffsets = kafkaDataSourceProperties.getKafkaPartitionOffsets();
        boolean isForTimes = kafkaDataSourceProperties.isOffsetsForTimes();
        if (isForTimes) {
            // if the offset is set by date time, we need to get the real offset by time
            // need to communicate with be
            // TODO not need ssl file?
            kafkaPartitionOffsets = KafkaUtil.getOffsetsForTimes(kafkaDataSourceProperties.getBrokerList(),
                    kafkaDataSourceProperties.getTopic(),
                    customKafkaProperties, kafkaDataSourceProperties.getKafkaPartitionOffsets());
        }

        // get partition number list, eg:[0,1,2]
        for (Pair<Integer, Long> partitionOffset : kafkaPartitionOffsets) {
            this.kafkaPartitions.add(partitionOffset.first);
        }
    }

    private void setCustomKafkaProperties(Map<String, String> kafkaProperties) {
        this.customKafkaProperties = kafkaProperties;
    }

    private void checkCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = Env.getCurrentEnv().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customKafkaProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                if (dbId == -1L) {
                    throw new DdlException("No db specified for storing ssl files");
                }

                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                // check file
                if (!smallFileMgr.containsFile(dbId, KAFKA_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with catalog: " + KAFKA_FILE_CATALOG);
                }
            }
        }
    }

    private void checkPartition() throws UserException {
        // user not define kafka partitions and be return no partitions
        if (kafkaPartitions.isEmpty()) {
            throw new AnalysisException("there is no available kafka partition");
        }
        if (!hasCustomPartitions) {
            return;
        }

        // get all kafka partitions from be
        List<Integer> allKafkaPartitions = getAllKafkaPartitions();

        for (Integer customPartition : kafkaPartitions) {
            if (!allKafkaPartitions.contains(customPartition)) {
                throw new LoadException("there is a custom kafka partition " + customPartition
                        + " which is invalid for topic " + kafkaDataSourceProperties.getTopic());
            }
        }
    }

    private List<Integer> getAllKafkaPartitions() throws UserException {
        convertCustomProperties();
        return KafkaUtil.getAllKafkaPartitions(brokerList, topic, customKafkaProperties);
    }

    private void convertCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = Env.getCurrentEnv().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customKafkaProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                // convert FILE:file_name -> FILE:file_id:md5
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                SmallFileMgr.SmallFile smallFile = smallFileMgr.getSmallFile(dbId, KAFKA_FILE_CATALOG, file, true);
                customKafkaProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                customKafkaProperties.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void parseAndAnalyzeKafkaProperties(Map<String, String> properties) throws UserException {
        // kafka partition offset may be time-data format
        // get time zone, to convert time into timestamps during the analysis phase
        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        }
        timezone = TimeUtils.checkTimeZoneValidAndStandardize(properties.getOrDefault(
                LoadStmt.TIMEZONE, timezone));

        topic = properties.getOrDefault(KafkaConfiguration.KAFKA_TOPIC.getName(), "");
        brokerList = properties.getOrDefault(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), "");
        Preconditions.checkState(!Strings.isNullOrEmpty(topic), "topic must be set before analyzing");
        Preconditions.checkState(!Strings.isNullOrEmpty(brokerList), "broker list must be set before analyzing");

        String partitionStr = properties.getOrDefault(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "");
        if (partitionStr.isEmpty()) {
            hasCustomPartitions = false;
            // get all kafka partitions from be
            List<Integer> allKafkaPartitions = getAllKafkaPartitions();
            partitionStr = allKafkaPartitions.stream()
                           .map(String::valueOf)
                           .collect(Collectors.joining(","));
            properties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), partitionStr);
        }
        // parse and analyze kafka properties, broker list and topic are required, others are optional
        this.kafkaDataSourceProperties = new KafkaDataSourceProperties(properties);
        this.kafkaDataSourceProperties.setTimezone(this.timezone);
        this.kafkaDataSourceProperties.analyze();
        // get ssl file db
        String tableName = properties.getOrDefault(KAFKA_FILE_CATALOG, "");
        if (!tableName.isEmpty()) {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(tableName);
            dbId = db.getId();
        }

        // set custom properties and partition, include some converted operations
        setOptional();
        checkCustomProperties();
        checkPartition();
    }

    public void parseAndAnalyzeJobProperties(Map<String, String> properties) throws AnalysisException {
        // Copy the properties, because we will remove the key from properties.
        Map<String, String> copiedProps = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        copiedProps.putAll(properties);

        // get job properties and check range
        long desiredConcurrentNum = Util.getLongPropertyOrDefault(
                copiedProps.get(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY),
                Config.max_routine_load_task_concurrent_num,
                CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PRED,
                CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY + " should > 0");
        long maxBatchIntervalS = Util.getLongPropertyOrDefault(copiedProps.get(
                CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_INTERVAL_SECOND, CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_PRED,
                CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY + " should between 1 and 60");
        long maxBatchRows = Util.getLongPropertyOrDefault(copiedProps.get(
                CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_BATCH_ROWS, CreateRoutineLoadStmt.MAX_BATCH_ROWS_PRED,
                CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY + " should > 200000");
        long maxBatchSizeBytes = Util.getLongPropertyOrDefault(copiedProps.get(
                CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_BATCH_SIZE, CreateRoutineLoadStmt.MAX_BATCH_SIZE_PRED,
                CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY + " should between 100MB and 1GB");

        // put into jobProperties
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, desiredConcurrentNum);
        jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY, maxBatchIntervalS);
        jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY, maxBatchRows);
        jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY, maxBatchSizeBytes);
    }

    public void getKafkaTvfTaskInfoList(int currentConcurrentTaskNum) throws AnalysisException {
        // select be by policy
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needQueryAvailable().needLoadAvailable().build();
        List<Long> backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, currentConcurrentTaskNum);
        if (backendIds.isEmpty() || backendIds.size() != currentConcurrentTaskNum) {
            throw new AnalysisException("No available backends or incorrect number of backends"
                    + ", policy: " + policy);
        }

        for (int i = 0; i < currentConcurrentTaskNum; i++) {
            TKafkaTvfTask tKafkaLoadInfo = createTKafkaTvfTask(ConnectContext.get().queryId(),
                    currentConcurrentTaskNum, i);
            kafkaTvfTaskMap.put(backendIds.get(i), tKafkaLoadInfo);
        }

    }

    public int calculateConcurrentTaskNum() throws AnalysisException {
        int aliveBeNums = Env.getCurrentSystemInfo().getAllBackendIds(true).size();
        int desireTaskConcurrentNum = jobProperties.get(
                CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY).intValue();
        int partitionNum = kafkaPartitions.size();

        int[] numbers = {aliveBeNums, desireTaskConcurrentNum, partitionNum,
                Config.max_routine_load_task_concurrent_num};
        int concurrentTaskNum = Arrays.stream(numbers).min().getAsInt();
        if (concurrentTaskNum == 0) {
            throw new AnalysisException("concurrent task number is 0");
        }

        return concurrentTaskNum;
    }

    private TKafkaTvfTask createTKafkaTvfTask(TUniqueId uniqueId, int currentConcurrentTaskNum, int i) {
        Map<Integer, Long> taskKafkaProgress = Maps.newHashMap();
        List<Pair<Integer, Long>> kafkaPartitionOffsets = kafkaDataSourceProperties.getKafkaPartitionOffsets();
        for (int j = i; j < kafkaPartitionOffsets.size(); j = j + currentConcurrentTaskNum) {
            Pair<Integer, Long> pair = kafkaPartitionOffsets.get(j);
            taskKafkaProgress.put(pair.key(), pair.value());
        }

        TKafkaLoadInfo tKafkaLoadInfo  = new TKafkaLoadInfo();
        tKafkaLoadInfo.setBrokers(kafkaDataSourceProperties.getBrokerList());
        tKafkaLoadInfo.setTopic(kafkaDataSourceProperties.getTopic());
        tKafkaLoadInfo.setProperties(kafkaDataSourceProperties.getCustomKafkaProperties());
        tKafkaLoadInfo.setPartitionBeginOffset(taskKafkaProgress);

        TKafkaTvfTask tKafkaTvfTask = new TKafkaTvfTask(TLoadSourceType.KAFKA, uniqueId, tKafkaLoadInfo);
        tKafkaTvfTask.setMaxIntervalS(jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY));
        tKafkaTvfTask.setMaxBatchRows(jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY));
        tKafkaTvfTask.setMaxBatchSize(jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY));

        return tKafkaTvfTask;
    }

    public void transferPartitionListToBe() throws AnalysisException {
        for (Entry<Long, TKafkaTvfTask> entry : kafkaTvfTaskMap.entrySet()) {
            Long beId = entry.getKey();
            TKafkaTvfTask task = entry.getValue();

            Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
            if (backend == null) {
                throw new AnalysisException("failed to send tasks to backend " + beId + " because not exist");
            }

            TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());

            boolean ok = false;
            BackendService.Client client = null;
            try {
                client = ClientPool.backendPool.borrowObject(address);
                TStatus tStatus = client.sendKafkaTvfTask(task);
                ok = true;

                if (tStatus.getStatusCode() != TStatusCode.OK) {
                    throw new AnalysisException("failed to send task. error code: " + tStatus.getStatusCode()
                            + ", msg: " + (tStatus.getErrorMsgsSize() > 0 ? tStatus.getErrorMsgs().get(0) : "NaN"));
                }
                LOG.debug("send kafka tvf task {} to BE: {}", DebugUtil.printId(task.id), beId);
            } catch (Exception e) {
                throw new AnalysisException("failed to send task: " + e.getMessage(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }

        }
    }
}

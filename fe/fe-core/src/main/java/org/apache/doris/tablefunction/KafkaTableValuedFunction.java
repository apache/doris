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
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.KafkaUtil;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaTableValuedFunction extends ExternalFileTableValuedFunction {
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private final KafkaDataSourceProperties kafkaProp;

    private Map<String, String> customProperties = Maps.newHashMap();

    private List<Integer> customKafkaPartitions = Lists.newArrayList();

    private Long dbId;

    public static final String PROP_GROUP_ID = "group.id";
    public static final String PROP_TABLE_NAME = "group.id";
    public static final String KAFKA_FILE_CATALOG = "kafka";

    public KafkaTableValuedFunction(Map<String, String> properties) throws UserException {
        // 1. analyze common properties
        Map<String, String> otherProperties = super.parseCommonProperties(properties);

        // 2. get time zone, to convert time into timestamps during the analysis phase
        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        }
        timezone = TimeUtils.checkTimeZoneValidAndStandardize(otherProperties.getOrDefault(
                LoadStmt.TIMEZONE, timezone));

        // 3. parse and analyze kafka properties
        this.kafkaProp = new KafkaDataSourceProperties(otherProperties);
        this.kafkaProp.setTimezone(this.timezone);
        this.kafkaProp.analyze();

        // TODO:parse and analyze job properties

        // get ssl file db
        String tableName = otherProperties.getOrDefault(PROP_TABLE_NAME, "");
        if (!tableName.isEmpty()) {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(tableName);
            dbId = db.getId();
        }

        // 3. convert offsets, check the ssl files, check the partitions
        setOptional();
        checkCustomProperties();
        checkCustomPartition();

        // 4. divide partitions

        // 5. transfer partition list to be


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
        if (CollectionUtils.isNotEmpty(kafkaProp.getKafkaPartitionOffsets())) {
            setCustomKafkaPartitions();
        }
        // set kafka customProperties
        if (MapUtils.isNotEmpty(kafkaProp.getCustomKafkaProperties())) {
            setCustomKafkaProperties(kafkaProp.getCustomKafkaProperties());
        }
        // set group id if not specified
        this.customProperties.putIfAbsent(PROP_GROUP_ID, "_" + UUID.randomUUID());
    }

    private void setCustomKafkaPartitions() throws LoadException {
        // get kafka partition offsets
        List<Pair<Integer, Long>> kafkaPartitionOffsets = kafkaProp.getKafkaPartitionOffsets();
        boolean isForTimes = kafkaProp.isOffsetsForTimes();
        if (isForTimes) {
            // if the offset is set by date time, we need to get the real offset by time
            // need to communicate with be
            // not need ssl file?
            kafkaPartitionOffsets = KafkaUtil.getOffsetsForTimes(kafkaProp.getBrokerList(),
                kafkaProp.getTopic(),
                customProperties, kafkaProp.getKafkaPartitionOffsets());
        }

        // get partition number list, eg:[0,1,2]
        for (Pair<Integer, Long> partitionOffset : kafkaPartitionOffsets) {
            this.customKafkaPartitions.add(partitionOffset.first);
        }
    }

    private void setCustomKafkaProperties(Map<String, String> kafkaProperties) {
        this.customProperties = kafkaProperties;
    }

    private void checkCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = Env.getCurrentEnv().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                if (dbId == 0) {
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

    private void checkCustomPartition() throws UserException {
        if (customKafkaPartitions.isEmpty()) {
            return;
        }
        List<Integer> allKafkaPartitions = getAllKafkaPartitions();
        for (Integer customPartition : customKafkaPartitions) {
            if (!allKafkaPartitions.contains(customPartition)) {
                throw new LoadException("there is a custom kafka partition " + customPartition
                    + " which is invalid for topic " + kafkaProp.getTopic());
            }
        }
    }

    private List<Integer> getAllKafkaPartitions() throws UserException {
        convertCustomProperties();
        return KafkaUtil.getAllKafkaPartitions(kafkaProp.getBrokerList(), kafkaProp.getTopic(), customProperties);
    }

    private void convertCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = Env.getCurrentEnv().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                // convert FILE:file_name -> FILE:file_id:md5
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                SmallFileMgr.SmallFile smallFile = smallFileMgr.getSmallFile(dbId, KAFKA_FILE_CATALOG, file, true);
                customProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                customProperties.put(entry.getKey(), entry.getValue());
            }
        }
    }

}

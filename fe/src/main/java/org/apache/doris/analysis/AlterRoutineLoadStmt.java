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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.routineload.KafkaProgress;
import org.apache.doris.load.routineload.LoadDataSourceType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AlterRoutineLoadStmt extends DdlStmt {

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";
    
    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY)
            .add(CreateRoutineLoadStmt.JSONPATHS)
            .add(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY)
            .add(LoadStmt.STRICT_MODE)
            .add(LoadStmt.TIMEZONE)
            .build();
    
    private static final ImmutableSet<String> CONFIGURABLE_KAFKA_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)
            .build();

    private final LabelName labelName;
    private final Map<String, String> jobProperties;
    private final String typeName;
    private final Map<String, String> dataSourceProperties;

    private int desiredConcurrentNum = 1;
    private long maxErrorNum = -1;
    private long maxBatchIntervalS = -1;
    private long maxBatchRows = -1;
    private long maxBatchSizeBytes = -1;
    private Boolean strictMode = null;
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;

    private String jsonPaths = "";
    private Boolean stripOuterArray = false;

    private List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();

    // save analyzed properties
    private Map<String, String> analyzedProperties = Maps.newHashMap();
    // save analyzed kafka properties
    private Map<String, String> customKafkaProperties = Maps.newHashMap();

    public AlterRoutineLoadStmt(LabelName labelName, Map<String, String> jobProperties, String typeName,
            Map<String, String> dataSourceProperties) {

        this.labelName = labelName;
        this.jobProperties = jobProperties;
        this.typeName = typeName;
        this.dataSourceProperties = dataSourceProperties;
    }
    
    /*
    boolean hasDesiredConcurrentNum() { return desiredConcurrentNum > 0; }
    boolean hasMaxErrorNum() { return maxErrorNum >=0; }
    boolean hasMaxBatchIntervalS() { return maxBatchIntervalS > 0; }
    boolean hasMaxBatchRows() { return maxBatchRows > 0; }
    boolean hasMaxBatchSizeBytes() { return maxBatchSizeBytes > 0; }
    boolean hasStrictMode() { return strictMode != null; }
    boolean hasJsonPath() { return !Strings.isNu}
    */
    

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        labelName.analyze(analyzer);
        FeNameFormat.checkCommonName(NAME_TYPE, labelName.getLabelName());
        // check routine load job properties include desired concurrent number etc.
        checkJobProperties();
        // check data source properties
        checkDataSourceProperties();
    }

    private void checkJobProperties() throws UserException {
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !CONFIGURABLE_PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)) {
            long desiredConcurrentNum = ((Long) Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY),
                    -1, CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PRED,
                    CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY + " should > 0")).intValue();
            analyzedProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY,
                    String.valueOf(desiredConcurrentNum));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY)) {
            long maxErrorNum = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PRED,
                    CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY + " should >= 0");
            analyzedProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY,
                    String.valueOf(maxErrorNum));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)) {
            long maxBatchIntervalS = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_PRED,
                    CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY + " should between 5 and 60");
            analyzedProperties.put(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY,
                    String.valueOf(maxBatchIntervalS));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)) {
            long maxBatchRows = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_BATCH_ROWS_PRED,
                    CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY + " should > 200000");
            analyzedProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY,
                    String.valueOf(maxBatchRows));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY)) {
            long maxBatchSizeBytes = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_BATCH_SIZE_PRED,
                    CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY + " should between 100MB and 1GB");
            analyzedProperties.put(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY,
                    String.valueOf(maxBatchSizeBytes));
        }

        if (jobProperties.containsKey(LoadStmt.STRICT_MODE)) {
            strictMode = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.STRICT_MODE),
                    null, LoadStmt.STRICT_MODE + " should be a boolean");
            analyzedProperties.put(LoadStmt.STRICT_MODE, String.valueOf(strictMode));
        }

        if (jobProperties.containsKey(LoadStmt.TIMEZONE)) {
            timezone = TimeUtils.checkTimeZoneValidAndStandardize(jobProperties.get(LoadStmt.TIMEZONE));
            analyzedProperties.put(LoadStmt.TIMEZONE, timezone);
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.JSONPATHS)) {
            analyzedProperties.put(CreateRoutineLoadStmt.JSONPATHS, jobProperties.get(CreateRoutineLoadStmt.JSONPATHS));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY)) {
            stripOuterArray = Boolean.valueOf(jobProperties.get(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY));
            analyzedProperties.put(jobProperties.get(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY),
                    String.valueOf(stripOuterArray));
        }
    }

    private void checkDataSourceProperties() throws AnalysisException {
        LoadDataSourceType type;
        try {
            type = LoadDataSourceType.valueOf(typeName);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("routine load job does not support this type " + typeName);
        }
        switch (type) {
            case KAFKA:
                checkKafkaProperties();
                break;
            default:
                break;
        }
    }

    // this is mostly a copy method from CreateRoutineLoadStmt
    private void checkKafkaProperties() throws AnalysisException {
        Optional<String> optional = dataSourceProperties.keySet().stream().filter(
                entity -> !CONFIGURABLE_KAFKA_PROPERTIES_SET.contains(entity)).filter(
                        entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid kafka custom property");
        }

        // check partitions
        final String kafkaPartitionsString = dataSourceProperties.get(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY);
        if (kafkaPartitionsString != null) {
            kafkaPartitionsString.replaceAll(" ", "");
            if (kafkaPartitionsString.isEmpty()) {
                throw new AnalysisException(
                        CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY + " could not be a empty string");
            }
            String[] kafkaPartionsStringList = kafkaPartitionsString.split(",");
            for (String s : kafkaPartionsStringList) {
                try {
                    kafkaPartitionOffsets.add(
                            Pair.create(CreateRoutineLoadStmt.getIntegerValueFromString(s,
                                    CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY),
                                    KafkaProgress.OFFSET_END_VAL));
                } catch (AnalysisException e) {
                    throw new AnalysisException(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY
                            + " must be a number string with comma-separated");
                }
            }
        }

        // check offset
        final String kafkaOffsetsString = dataSourceProperties.get(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY);
        if (kafkaOffsetsString != null) {
            kafkaOffsetsString.replaceAll(" ", "");
            if (kafkaOffsetsString.isEmpty()) {
                throw new AnalysisException(
                        CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY + " could not be a empty string");
            }
            String[] kafkaOffsetsStringList = kafkaOffsetsString.split(",");
            if (kafkaOffsetsStringList.length != kafkaPartitionOffsets.size()) {
                throw new AnalysisException("Partitions number should be equals to offsets number");
            }

            for (int i = 0; i < kafkaOffsetsStringList.length; i++) {
                // defined in librdkafka/rdkafkacpp.h
                // OFFSET_BEGINNING: -2
                // OFFSET_END: -1
                try {
                    kafkaPartitionOffsets.get(i).second = CreateRoutineLoadStmt.getLongValueFromString(
                            kafkaOffsetsStringList[i],
                            CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY);
                    if (kafkaPartitionOffsets.get(i).second < 0) {
                        throw new AnalysisException("Cannot specify offset smaller than 0");
                    }
                } catch (AnalysisException e) {
                    if (kafkaOffsetsStringList[i].equalsIgnoreCase(KafkaProgress.OFFSET_BEGINNING)) {
                        kafkaPartitionOffsets.get(i).second = KafkaProgress.OFFSET_BEGINNING_VAL;
                    } else if (kafkaOffsetsStringList[i].equalsIgnoreCase(KafkaProgress.OFFSET_END)) {
                        kafkaPartitionOffsets.get(i).second = KafkaProgress.OFFSET_END_VAL;
                    } else {
                        throw e;
                    }
                }
            }
        }

        // check custom kafka property
        for (Map.Entry<String, String> dataSourceProperty : dataSourceProperties.entrySet()) {
            if (dataSourceProperty.getKey().startsWith("property.")) {
                String propertyKey = dataSourceProperty.getKey();
                String propertyValue = dataSourceProperty.getValue();
                String propertyValueArr[] = propertyKey.split("\\.");
                if (propertyValueArr.length < 2) {
                    throw new AnalysisException("kafka property value could not be a empty string");
                }
                customKafkaProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
            }
            // can be extended in the future which other prefix
        }
    }
}

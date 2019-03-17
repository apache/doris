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
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.routineload.LoadDataSourceType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/*
 Create routine Load statement,  continually load data from a streaming app

 syntax:
      CREATE ROUTINE LOAD name ON database.table
      [load properties]
      [PROPERTIES
      (
          desired_concurrent_number = xxx,
          max_error_number = xxx,
          k1 = v1,
          ...
          kn = vn
      )]
      FROM type of routine load
      [(
          k1 = v1,
          ...
          kn = vn
      )]

      load properties:
          load property [[,] load property] ...

      load property:
          column separator | columns_mapping | partitions | where

      column separator:
          COLUMNS TERMINATED BY xxx
      columns_mapping:
          COLUMNS (c1, c2, c3 = c1 + c2)
      partitions:
          PARTITIONS (p1, p2, p3)
      where:
          WHERE c1 > 1

      type of routine load:
          KAFKA
*/
public class CreateRoutineLoadStmt extends DdlStmt {
    // routine load properties
    public static final String DESIRED_CONCURRENT_NUMBER_PROPERTY = "desired_concurrent_number";
    // max error number in ten thousand records
    public static final String MAX_ERROR_NUMBER_PROPERTY = "max_error_number";
    // the following 3 properties limit the time and batch size of a single routine load task
    public static final String MAX_BATCH_INTERVAL_SECOND = "max_batch_interval";
    public static final String MAX_BATCH_ROWS = "max_batch_rows";
    public static final String MAX_BATCH_SIZE = "max_batch_size";

    // kafka type properties
    public static final String KAFKA_BROKER_LIST_PROPERTY = "kafka_broker_list";
    public static final String KAFKA_TOPIC_PROPERTY = "kafka_topic";
    // optional
    public static final String KAFKA_PARTITIONS_PROPERTY = "kafka_partitions";
    public static final String KAFKA_OFFSETS_PROPERTY = "kafka_offsets";

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";
    private static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(MAX_ERROR_NUMBER_PROPERTY)
            .add(MAX_BATCH_INTERVAL_SECOND)
            .add(MAX_BATCH_ROWS)
            .add(MAX_BATCH_SIZE)
            .build();

    private static final ImmutableSet<String> KAFKA_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(KAFKA_BROKER_LIST_PROPERTY)
            .add(KAFKA_TOPIC_PROPERTY)
            .add(KAFKA_PARTITIONS_PROPERTY)
            .add(KAFKA_OFFSETS_PROPERTY)
            .build();

    private final String name;
    private final TableName dbTableName;
    private final List<ParseNode> loadPropertyList;
    private final Map<String, String> jobProperties;
    private final String typeName;
    private final Map<String, String> dataSourceProperties;

    // the following variables will be initialized after analyze
    // -1 as unset, the default value will set in RoutineLoadJob
    private RoutineLoadDesc routineLoadDesc;
    private int desiredConcurrentNum = 1;
    private int maxErrorNum = -1;
    private int maxBatchIntervalS = -1;
    private int maxBatchRows = -1;
    private int maxBatchSizeBytes = -1;

    // kafka related properties
    private String kafkaBrokerList;
    private String kafkaTopic;
    // pair<partition id, offset>
    private List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();

    public CreateRoutineLoadStmt(String name, TableName dbTableName, List<ParseNode> loadPropertyList,
                                 Map<String, String> jobProperties,
                                 String typeName, Map<String, String> dataSourceProperties) {
        this.name = name;
        this.dbTableName = dbTableName;
        this.loadPropertyList = loadPropertyList;
        this.jobProperties = jobProperties == null ? Maps.newHashMap() : jobProperties;
        this.typeName = typeName.toUpperCase();
        this.dataSourceProperties = dataSourceProperties;
    }

    public String getName() {
        return name;
    }

    public TableName getDBTableName() {
        return dbTableName;
    }

    public String getTypeName() {
        return typeName;
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }

    public int getDesiredConcurrentNum() {
        return desiredConcurrentNum;
    }

    public int getMaxErrorNum() {
        return maxErrorNum;
    }

    public int getMaxBatchIntervalS() {
        return maxBatchIntervalS;
    }

    public int getMaxBatchRows() {
        return maxBatchRows;
    }

    public int getMaxBatchSize() {
        return maxBatchSizeBytes;
    }

    public String getKafkaBrokerList() {
        return kafkaBrokerList;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public List<Pair<Integer, Long>> getKafkaPartitionOffsets() {
        return kafkaPartitionOffsets;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check name
        FeNameFormat.checkCommonName(NAME_TYPE, name);
        // check dbName and tableName
        dbTableName.analyze(analyzer);
        // check load properties include column separator etc.
        checkLoadProperties(analyzer);
        // check routine load job properties include desired concurrent number etc.
        checkJobProperties();
        // check data source properties
        checkDataSourceProperties();
    }

    public void checkLoadProperties(Analyzer analyzer) throws UserException {
        if (loadPropertyList == null) {
            return;
        }
        ColumnSeparator columnSeparator = null;
        ImportColumnsStmt importColumnsStmt = null;
        ImportWhereStmt importWhereStmt = null;
        PartitionNames partitionNames = null;
        for (ParseNode parseNode : loadPropertyList) {
            if (parseNode instanceof ColumnSeparator) {
                // check column separator
                if (columnSeparator != null) {
                    throw new AnalysisException("repeat setting of column separator");
                }
                columnSeparator = (ColumnSeparator) parseNode;
                columnSeparator.analyze(null);
            } else if (parseNode instanceof ImportColumnsStmt) {
                // check columns info
                if (importColumnsStmt != null) {
                    throw new AnalysisException("repeat setting of columns info");
                }
                importColumnsStmt = (ImportColumnsStmt) parseNode;
            } else if (parseNode instanceof ImportWhereStmt) {
                // check where expr
                if (importWhereStmt != null) {
                    throw new AnalysisException("repeat setting of where predicate");
                }
                importWhereStmt = (ImportWhereStmt) parseNode;
            } else if (parseNode instanceof PartitionNames) {
                // check partition names
                if (partitionNames != null) {
                    throw new AnalysisException("repeat setting of partition names");
                }
                partitionNames = (PartitionNames) parseNode;
                partitionNames.analyze(null);
            }
        }
        routineLoadDesc = new RoutineLoadDesc(columnSeparator, importColumnsStmt, importWhereStmt,
                                              partitionNames.getPartitionNames());
    }

    private void checkJobProperties() throws AnalysisException {
        Optional<String> optional = jobProperties.keySet().parallelStream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        desiredConcurrentNum = getIntegetPropertyOrDefault(DESIRED_CONCURRENT_NUMBER_PROPERTY,
                "must be greater then 0", desiredConcurrentNum);
        maxErrorNum = getIntegetPropertyOrDefault(MAX_ERROR_NUMBER_PROPERTY,
                "must be greater then or equal to 0", maxErrorNum);
        maxBatchIntervalS = getIntegetPropertyOrDefault(MAX_BATCH_INTERVAL_SECOND,
                "must be greater then 0", maxBatchIntervalS);
        maxBatchRows = getIntegetPropertyOrDefault(MAX_BATCH_ROWS, "must be greater then 0", maxBatchRows);
        maxBatchSizeBytes = getIntegetPropertyOrDefault(MAX_BATCH_SIZE, "must be greater then 0", maxBatchSizeBytes);
    }

    private int getIntegetPropertyOrDefault(String propName, String hintMsg, int defaultVal) throws AnalysisException {
        final String propVal = jobProperties.get(propName);
        if (propVal != null) {
            int intVal = getIntegerValueFromString(propVal, propName);
            if (intVal <= 0) {
                throw new AnalysisException(propName + " " + hintMsg);
            }
            return intVal;
        }
        return defaultVal;
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

    private void checkKafkaProperties() throws AnalysisException {
        Optional<String> optional = dataSourceProperties.keySet().parallelStream()
                .filter(entity -> !KAFKA_PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid kafka custom property");
        }

        // check broker list
        kafkaBrokerList = Strings.nullToEmpty(dataSourceProperties.get(KAFKA_BROKER_LIST_PROPERTY)).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(kafkaBrokerList)) {
            throw new AnalysisException(KAFKA_BROKER_LIST_PROPERTY + " is a required property");
        }
        String[] kafkaBrokerList = this.kafkaBrokerList.split(",");
        for (String broker : kafkaBrokerList) {
            if (!Pattern.matches(ENDPOINT_REGEX, broker)) {
                throw new AnalysisException(KAFKA_BROKER_LIST_PROPERTY + ":" + broker
                                                    + " not match pattern " + ENDPOINT_REGEX);
            }
        }

        // check topic
        kafkaTopic = Strings.nullToEmpty(dataSourceProperties.get(KAFKA_TOPIC_PROPERTY)).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(kafkaTopic)) {
            throw new AnalysisException(KAFKA_TOPIC_PROPERTY + " is a required property");
        }

        // check partitions
        final String kafkaPartitionsString = dataSourceProperties.get(KAFKA_PARTITIONS_PROPERTY);
        if (kafkaPartitionsString != null) {
            kafkaPartitionsString.replaceAll(" ", "");
            if (kafkaPartitionsString.isEmpty()) {
                throw new AnalysisException(KAFKA_PARTITIONS_PROPERTY + " could not be a empty string");
            }
            String[] kafkaPartionsStringList = kafkaPartitionsString.split(",");
            for (String s : kafkaPartionsStringList) {
                try {
                    kafkaPartitionOffsets.add(Pair.create(getIntegerValueFromString(s, KAFKA_PARTITIONS_PROPERTY), 0L));
                } catch (AnalysisException e) {
                    throw new AnalysisException(KAFKA_PARTITIONS_PROPERTY
                                                        + " must be a number string with comma-separated");
                }
            }
        }

        // check offset
        final String kafkaOffsetsString = dataSourceProperties.get(KAFKA_OFFSETS_PROPERTY);
        if (kafkaOffsetsString != null) {
            kafkaOffsetsString.replaceAll(" ", "");
            if (kafkaOffsetsString.isEmpty()) {
                throw new AnalysisException(KAFKA_OFFSETS_PROPERTY + " could not be a empty string");
            }
            String[] kafkaOffsetsStringList = kafkaOffsetsString.split(",");
            if (kafkaOffsetsStringList.length != kafkaPartitionOffsets.size()) {
                throw new AnalysisException("Partitions number should be equals to offsets number");
            }

            for (int i = 0; i < kafkaOffsetsStringList.length; i++) {
                kafkaPartitionOffsets.get(i).second = getLongValueFromString(kafkaOffsetsStringList[i],
                        KAFKA_OFFSETS_PROPERTY);
            }
        }
    }

    private int getIntegerValueFromString(String valueString, String propertyName) throws AnalysisException {
        if (valueString.isEmpty()) {
            throw new AnalysisException(propertyName + " could not be a empty string");
        }
        int value;
        try {
            value = Integer.valueOf(valueString);
        } catch (NumberFormatException e) {
            throw new AnalysisException(propertyName + " must be a integer");
        }
        return value;
    }

    private long getLongValueFromString(String valueString, String propertyName) throws AnalysisException {
        if (valueString.isEmpty()) {
            throw new AnalysisException(propertyName + " could not be a empty string");
        }
        long value;
        try {
            value = Long.valueOf(valueString);
        } catch (NumberFormatException e) {
            throw new AnalysisException(propertyName + " must be a integer");
        }
        return value;
    }
}

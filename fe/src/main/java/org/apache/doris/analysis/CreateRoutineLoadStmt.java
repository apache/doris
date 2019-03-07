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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.load.routineload.LoadDataSourceType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
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
          column separator | columns | partitions | where

      column separator:
          COLUMNS TERMINATED BY xxx
      columns:
          COLUMNS (c1, c2, c3) set (c1, c2, c3=c1+c2)
      partitions:
          PARTITIONS (p1, p2, p3)
      where:
          WHERE xxx

      type of routine load:
          KAFKA
*/
public class CreateRoutineLoadStmt extends DdlStmt {
    // routine load properties
    public static final String DESIRED_CONCURRENT_NUMBER_PROPERTY = "desired_concurrent_number";
    // max error number in ten thousand records
    public static final String MAX_ERROR_NUMBER_PROPERTY = "max_error_number";

    // kafka type properties
    public static final String KAFKA_BROKER_LIST_PROPERTY = "kafka_broker_list";
    public static final String KAFKA_TOPIC_PROPERTY = "kafka_topic";
    // optional
    public static final String KAFKA_PARTITIONS_PROPERTY = "kafka_partitions";
    public static final String KAFKA_OFFSETS_PROPERTY = "kafka_offsets";

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";
    private static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";
    private static final String EMPTY_STRING = "";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(MAX_ERROR_NUMBER_PROPERTY)
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
    private final Map<String, String> properties;
    private final String typeName;
    private final Map<String, String> customProperties;


    // those load properties will be initialized after analyze
    private RoutineLoadDesc routineLoadDesc;
    private int desiredConcurrentNum;
    private int maxErrorNum;
    private String kafkaBrokerList;
    private String kafkaTopic;
    private List<Integer> kafkaPartitions;
    private List<Long> kafkaOffsets;

    public CreateRoutineLoadStmt(String name, TableName dbTableName, List<ParseNode> loadPropertyList,
                                 Map<String, String> properties,
                                 String typeName, Map<String, String> customProperties) {
        this.name = name;
        this.dbTableName = dbTableName;
        this.loadPropertyList = loadPropertyList;
        this.properties = properties;
        this.typeName = typeName.toUpperCase();
        this.customProperties = customProperties;
    }

    public String getName() {
        return name;
    }

    public TableName getDBTableName() {
        return dbTableName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getTypeName() {
        return typeName;
    }

    public Map<String, String> getCustomProperties() {
        return customProperties;
    }

    // nullable
    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }

    public int getDesiredConcurrentNum() {
        return desiredConcurrentNum;
    }

    public int getMaxErrorNum() {
        return maxErrorNum;
    }

    public String getKafkaBrokerList() {
        return kafkaBrokerList;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public List<Integer> getKafkaPartitions() {
        return kafkaPartitions;
    }

    public List<Long> getKafkaOffsets(){
        return kafkaOffsets;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        // check name
        FeNameFormat.checkCommonName(NAME_TYPE, name);
        // check dbName and tableName
        checkDBTableName();
        dbTableName.analyze(analyzer);
        // check load properties include column separator etc.
        checkLoadProperties(analyzer);
        // check routine load properties include desired concurrent number etc.
        checkRoutineLoadProperties();
        // check custom properties
        checkCustomProperties();
    }

    private void checkDBTableName() throws AnalysisException {
        if (Strings.isNullOrEmpty(dbTableName.getDb())) {
            String dbName = ConnectContext.get().getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                throw new AnalysisException("please choose a database first");
            }
            dbTableName.setDb(dbName);
        }
        if (Strings.isNullOrEmpty(dbTableName.getTbl())) {
            throw new AnalysisException("empty table name in create routine load statement");
        }
    }

    private void checkLoadProperties(Analyzer analyzer) throws AnalysisException {
        if (loadPropertyList == null) {
            return;
        }
        ColumnSeparator columnSeparator = null;
        LoadColumnsInfo columnsInfo = null;
        Expr wherePredicate = null;
        PartitionNames partitionNames = null;
        for (ParseNode parseNode : loadPropertyList) {
            if (parseNode instanceof ColumnSeparator) {
                // check column separator
                if (columnSeparator != null) {
                    throw new AnalysisException("repeat setting of column separator");
                }
                columnSeparator = (ColumnSeparator) parseNode;
                columnSeparator.analyze(analyzer);
            } else if (parseNode instanceof LoadColumnsInfo) {
                // check columns info
                if (columnsInfo != null) {
                    throw new AnalysisException("repeat setting of columns info");
                }
                columnsInfo = (LoadColumnsInfo) parseNode;
                columnsInfo.analyze(analyzer);
            } else if (parseNode instanceof Expr) {
                // check where expr
                if (wherePredicate != null) {
                    throw new AnalysisException("repeat setting of where predicate");
                }
                wherePredicate = (Expr) parseNode;
                wherePredicate.analyze(analyzer);
            } else if (parseNode instanceof PartitionNames) {
                // check partition names
                if (partitionNames != null) {
                    throw new AnalysisException("repeat setting of partition names");
                }
                partitionNames = (PartitionNames) parseNode;
                partitionNames.analyze(analyzer);
            }
        }
        routineLoadDesc = new RoutineLoadDesc(columnSeparator, columnsInfo, wherePredicate,
                                              partitionNames.getPartitionNames());
    }

    private void checkRoutineLoadProperties() throws AnalysisException {
        if (properties != null) {
            Optional<String> optional = properties.keySet().parallelStream()
                    .filter(entity -> !PROPERTIES_SET.contains(entity)).findFirst();
            if (optional.isPresent()) {
                throw new AnalysisException(optional.get() + " is invalid property");
            }

            // check desired concurrent number
            final String desiredConcurrentNumberString = properties.get(DESIRED_CONCURRENT_NUMBER_PROPERTY);
            if (desiredConcurrentNumberString != null) {
                desiredConcurrentNum = getIntegerValueFromString(desiredConcurrentNumberString,
                                                                 DESIRED_CONCURRENT_NUMBER_PROPERTY);
                if (desiredConcurrentNum <= 0) {
                    throw new AnalysisException(DESIRED_CONCURRENT_NUMBER_PROPERTY + " must be greater then 0");
                }
            }

            // check max error number
            final String maxErrorNumberString = properties.get(MAX_ERROR_NUMBER_PROPERTY);
            if (maxErrorNumberString != null) {
                maxErrorNum = getIntegerValueFromString(maxErrorNumberString, MAX_ERROR_NUMBER_PROPERTY);
                if (maxErrorNum < 0) {
                    throw new AnalysisException(MAX_ERROR_NUMBER_PROPERTY + " must be greater then or equal to 0");
                }

            }
        }
    }

    private void checkCustomProperties() throws AnalysisException {
        LoadDataSourceType type;
        try {
            type = LoadDataSourceType.valueOf(typeName);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("routine load job does not support this type " + typeName);
        }
        switch (type) {
            case KAFKA:
                checkKafkaCustomProperties();
                break;
            default:
                break;
        }
    }

    private void checkKafkaCustomProperties() throws AnalysisException {
        Optional<String> optional = customProperties.keySet().parallelStream()
                .filter(entity -> !KAFKA_PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid kafka custom property");
        }
        // check endpoint
        kafkaBrokerList = customProperties.get(KAFKA_BROKER_LIST_PROPERTY);
        if (Strings.isNullOrEmpty(kafkaBrokerList)) {
            throw new AnalysisException(KAFKA_BROKER_LIST_PROPERTY + " is required property");
        }
        String[] kafkaBrokerList = this.kafkaBrokerList.split(",");
        for (String broker : kafkaBrokerList) {
            if (!Pattern.matches(ENDPOINT_REGEX, broker)) {
                throw new AnalysisException(KAFKA_BROKER_LIST_PROPERTY + ":" + broker
                                                    + " not match pattern " + ENDPOINT_REGEX);
            }
        }
        // check topic
        kafkaTopic = customProperties.get(KAFKA_TOPIC_PROPERTY);
        if (Strings.isNullOrEmpty(kafkaTopic)) {
            throw new AnalysisException(KAFKA_TOPIC_PROPERTY + " is required property");
        }
        // check partitions
        final String kafkaPartitionsString = customProperties.get(KAFKA_PARTITIONS_PROPERTY);
        if (kafkaPartitionsString != null) {
            kafkaPartitions = new ArrayList<>();
            if (kafkaPartitionsString.equals(EMPTY_STRING)) {
                throw new AnalysisException(KAFKA_PARTITIONS_PROPERTY + " could not be a empty string");
            }
            String[] kafkaPartionsStringList = kafkaPartitionsString.split(",");
            for (String s : kafkaPartionsStringList) {
                try {
                    kafkaPartitions.add(getIntegerValueFromString(s, KAFKA_PARTITIONS_PROPERTY));
                } catch (AnalysisException e) {
                    throw new AnalysisException(KAFKA_PARTITIONS_PROPERTY
                                                        + " must be a number string with comma-separated");
                }
            }
        }
        // check offsets
        // Todo(ml)
        final String kafkaOffsetsString = customProperties.get(KAFKA_OFFSETS_PROPERTY);
        if (kafkaOffsetsString != null) {
            kafkaOffsets = new ArrayList<>();
            String[] kafkaOffsetsStringList = customProperties.get(KAFKA_OFFSETS_PROPERTY).split(",");
            for (String s : kafkaOffsetsStringList) {
                kafkaOffsets.add(Long.valueOf(s));
            }
        }
    }

    private int getIntegerValueFromString(String valueString, String propertyName) throws AnalysisException {
        if (valueString.equals(EMPTY_STRING)) {
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
}

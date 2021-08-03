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
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.routineload.KafkaProgress;
import org.apache.doris.load.routineload.LoadDataSourceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.regex.Pattern;

public class RoutineLoadDataSourceProperties {

    private static final ImmutableSet<String> DATA_SOURCE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS)
            .build();

    private static final ImmutableSet<String> CONFIGURABLE_DATA_SOURCE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS)
            .build();

    // origin properties, no need to persist
    private Map<String, String> properties = Maps.newHashMap();
    private boolean isAlter = false;

    @SerializedName(value = "type")
    private String type = "KAFKA";
    @SerializedName(value = "kafkaPartitionOffsets")
    private List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();
    @SerializedName(value = "customKafkaProperties")
    private Map<String, String> customKafkaProperties = Maps.newHashMap();
    @SerializedName(value = "isOffsetsForTimes")
    private boolean isOffsetsForTimes = false;
    @SerializedName(value = "kafkaBrokerList")
    private String kafkaBrokerList;
    @SerializedName(value = "KafkaTopic")
    private String kafkaTopic;
    @SerializedName(value = "timezone")
    private String timezone;

    public RoutineLoadDataSourceProperties() {
        // for unit test, and empty data source properties when altering routine load
        this.isAlter = true;
    }

    public RoutineLoadDataSourceProperties(String type, Map<String, String> properties, boolean isAlter) {
        this.type = type.toUpperCase();
        this.properties = properties;
        this.isAlter = isAlter;
    }

    public void analyze() throws UserException {
        if (properties.isEmpty()) {
            if (!isAlter) {
                throw new AnalysisException("No data source properties");
            } else {
                // for alter routine load stmt, the datasource property can by null
                return;
            }
        }
        Preconditions.checkState(!Strings.isNullOrEmpty(timezone), "timezone must be set before analyzing");
        checkDataSourceProperties();
    }

    public boolean hasAnalyzedProperties() {
        return !kafkaPartitionOffsets.isEmpty() || !customKafkaProperties.isEmpty();
    }

    public String getType() {
        return type;
    }

    public List<Pair<Integer, Long>> getKafkaPartitionOffsets() {
        return kafkaPartitionOffsets;
    }

    public void setKafkaPartitionOffsets(List<Pair<Integer, Long>> kafkaPartitionOffsets) {
        this.kafkaPartitionOffsets = kafkaPartitionOffsets;
    }

    public Map<String, String> getCustomKafkaProperties() {
        return customKafkaProperties;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getKafkaBrokerList() {
        return kafkaBrokerList;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public boolean isOffsetsForTimes() {
        return isOffsetsForTimes;
    }

    private void checkDataSourceProperties() throws UserException {
        LoadDataSourceType sourceType;
        try {
            sourceType = LoadDataSourceType.valueOf(type);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("routine load job does not support this type " + type);
        }
        switch (sourceType) {
            case KAFKA:
                checkKafkaProperties();
                break;
            default:
                break;
        }
    }

    /*
     * Kafka properties includes follows:
     * 1. broker list
     * 2. topic
     * 3. partition offset info
     * 4. other properties start with "property."
     */
    private void checkKafkaProperties() throws UserException {
        ImmutableSet<String> propertySet = isAlter ? CONFIGURABLE_DATA_SOURCE_PROPERTIES_SET : DATA_SOURCE_PROPERTIES_SET;
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !propertySet.contains(entity)).filter(
                entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid kafka property or can not be set");
        }

        // check broker list
        kafkaBrokerList = Strings.nullToEmpty(properties.get(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY)).replaceAll(" ", "");
        if (!isAlter && Strings.isNullOrEmpty(kafkaBrokerList)) {
            throw new AnalysisException(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY + " is a required property");
        }
        if (!Strings.isNullOrEmpty(kafkaBrokerList)) {
            String[] kafkaBrokerList = this.kafkaBrokerList.split(",");
            for (String broker : kafkaBrokerList) {
                if (!Pattern.matches(CreateRoutineLoadStmt.ENDPOINT_REGEX, broker)) {
                    throw new AnalysisException(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY + ":" + broker
                            + " not match pattern " + CreateRoutineLoadStmt.ENDPOINT_REGEX);
                }
            }
        }

        // check topic
        kafkaTopic = Strings.nullToEmpty(properties.get(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY)).replaceAll(" ", "");
        if (!isAlter && Strings.isNullOrEmpty(kafkaTopic)) {
            throw new AnalysisException(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY + " is a required property");
        }

        // check custom kafka property
        // This should be done before check partition and offsets, because we need KAFKA_DEFAULT_OFFSETS,
        // which is in custom properties.
        analyzeCustomProperties(this.properties, this.customKafkaProperties);

        // The partition offset properties are all optional,
        // and there are 5 valid cases for specifying partition offsets：
        // A. partition, offset and default offset are not set
        //      Doris will set default offset to OFFSET_END
        // B. partition and offset are set, default offset is not set
        //      fill the "kafkaPartitionOffsets" with partition and offset
        // C. partition and default offset are set, offset is not set
        //      fill the "kafkaPartitionOffsets" with partition and default offset
        // D. partition is set, offset and default offset are not set
        //      this is only valid when doing create routine load operation,
        //      fill the "kafkaPartitionOffsets" with partition and OFFSET_END
        // E. only default offset is set.
        //      this is only valid when doing alter routine load operation.
        // Other cases are illegal.

        // check partitions
        String kafkaPartitionsString = properties.get(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY);
        if (kafkaPartitionsString != null) {
            analyzeKafkaPartitionProperty(kafkaPartitionsString, this.kafkaPartitionOffsets);
        }

        // check offset
        String kafkaOffsetsString = properties.get(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY);
        String kafkaDefaultOffsetString = customKafkaProperties.get(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS);
        if (kafkaOffsetsString != null && kafkaDefaultOffsetString != null) {
            throw new AnalysisException("Only one of " + CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY +
                    " and " + CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS + " can be set.");
        }
        if (isAlter && kafkaPartitionsString != null && kafkaOffsetsString == null && kafkaDefaultOffsetString == null) {
            // if this is an alter operation, the partition and (default)offset must be set together.
            throw new AnalysisException("Must set offset or default offset with partition property");
        }

        if (kafkaOffsetsString != null) {
            this.isOffsetsForTimes = analyzeKafkaOffsetProperty(kafkaOffsetsString, this.kafkaPartitionOffsets, this.timezone);
        } else {
            // offset is not set, check default offset.
            this.isOffsetsForTimes = analyzeKafkaDefaultOffsetProperty(this.customKafkaProperties, this.timezone);
            if (!this.kafkaPartitionOffsets.isEmpty()) {
                // Case C
                kafkaDefaultOffsetString = customKafkaProperties.get(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS);
                setDefaultOffsetForPartition(this.kafkaPartitionOffsets, kafkaDefaultOffsetString, this.isOffsetsForTimes);
            }
        }
    }

    private static void setDefaultOffsetForPartition(List<Pair<Integer, Long>> kafkaPartitionOffsets,
                                                     String kafkaDefaultOffsetString, boolean isOffsetsForTimes) {
        if (isOffsetsForTimes) {
            for (Pair<Integer, Long> pair : kafkaPartitionOffsets) {
                pair.second = Long.valueOf(kafkaDefaultOffsetString);
            }
        } else {
            for (Pair<Integer, Long> pair : kafkaPartitionOffsets) {
                if (kafkaDefaultOffsetString.equalsIgnoreCase(KafkaProgress.OFFSET_BEGINNING)) {
                    pair.second = KafkaProgress.OFFSET_BEGINNING_VAL;
                } else {
                    pair.second = KafkaProgress.OFFSET_END_VAL;
                }
            }
        }
    }

    // If the default offset is not set, set the default offset to OFFSET_END.
    // If the offset is in datetime format, convert it to a timestamp, and also save the origin datatime formatted offset
    // in "customKafkaProperties"
    // return true if the offset is in datetime format.
    private static boolean analyzeKafkaDefaultOffsetProperty(Map<String, String> customKafkaProperties, String timeZoneStr)
            throws AnalysisException {
        customKafkaProperties.putIfAbsent(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, KafkaProgress.OFFSET_END);
        String defaultOffsetStr = customKafkaProperties.get(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS);
        TimeZone timeZone = TimeUtils.getOrSystemTimeZone(timeZoneStr);
        long defaultOffset = TimeUtils.timeStringToLong(defaultOffsetStr, timeZone);
        if (defaultOffset != -1) {
            // this is a datetime format offset
            customKafkaProperties.put(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS, String.valueOf(defaultOffset));
            // we convert datetime to timestamp, and save the origin datetime formatted offset for further use.
            customKafkaProperties.put(CreateRoutineLoadStmt.KAFKA_ORIGIN_DEFAULT_OFFSETS, defaultOffsetStr);
            return true;
        } else {
            if (!defaultOffsetStr.equalsIgnoreCase(KafkaProgress.OFFSET_BEGINNING) && !defaultOffsetStr.equalsIgnoreCase(KafkaProgress.OFFSET_END)) {
                throw new AnalysisException(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS + " can only be set to OFFSET_BEGINNING, OFFSET_END or date time");
            }
            return false;
        }
    }

    // init "kafkaPartitionOffsets" with partition property.
    // The offset will be set to OFFSET_END for now, and will be changed in later analysis process.
    private static void analyzeKafkaPartitionProperty(String kafkaPartitionsString,
                                                      List<Pair<Integer, Long>> kafkaPartitionOffsets) throws AnalysisException {
        kafkaPartitionsString = kafkaPartitionsString.replaceAll(" ", "");
        if (kafkaPartitionsString.isEmpty()) {
            throw new AnalysisException(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY + " could not be a empty string");
        }
        String[] kafkaPartitionsStringList = kafkaPartitionsString.split(",");
        for (String s : kafkaPartitionsStringList) {
            try {
                kafkaPartitionOffsets.add(Pair.create(getIntegerValueFromString(s, CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY),
                        KafkaProgress.OFFSET_END_VAL));
            } catch (AnalysisException e) {
                throw new AnalysisException(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY
                        + " must be a number string with comma-separated");
            }
        }
    }

    // Fill the partition's offset with given kafkaOffsetsString,
    // Return true if offset is specified by timestamp.
    private static boolean analyzeKafkaOffsetProperty(String kafkaOffsetsString, List<Pair<Integer, Long>> kafkaPartitionOffsets,
                                                      String timeZoneStr)
            throws UserException {
        if (Strings.isNullOrEmpty(kafkaOffsetsString)) {
            throw new AnalysisException(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY + " could not be a empty string");
        }
        List<String> kafkaOffsetsStringList = Splitter.on(",").trimResults().splitToList(kafkaOffsetsString);
        if (kafkaOffsetsStringList.size() != kafkaPartitionOffsets.size()) {
            throw new AnalysisException("Partitions number should be equals to offsets number");
        }

        // We support two ways to specify the offset,
        // one is to specify the offset directly, the other is to specify a timestamp.
        // Doris will get the offset of the corresponding partition through the timestamp.
        // The user can only choose one of these methods.
        boolean foundTime = false;
        boolean foundOffset = false;
        for (String kafkaOffsetsStr : kafkaOffsetsStringList) {
            if (TimeUtils.timeStringToLong(kafkaOffsetsStr) != -1) {
                foundTime = true;
            } else {
                foundOffset = true;
            }
        }
        if (foundTime && foundOffset) {
            throw new AnalysisException("The offset of the partition cannot be specified by the timestamp " +
                    "and the offset at the same time");
        }

        if (foundTime) {
            // convert all datetime strs to timestamps
            // and set them as the partition's offset.
            // These timestamps will be converted to real offset when job is running.
            TimeZone timeZone = TimeUtils.getOrSystemTimeZone(timeZoneStr);
            for (int i = 0; i < kafkaOffsetsStringList.size(); i++) {
                String kafkaOffsetsStr = kafkaOffsetsStringList.get(i);
                long timestamp = TimeUtils.timeStringToLong(kafkaOffsetsStr, timeZone);
                Preconditions.checkState(timestamp != -1);
                kafkaPartitionOffsets.get(i).second = timestamp;
            }
        } else {
            for (int i = 0; i < kafkaOffsetsStringList.size(); i++) {
                String kafkaOffsetsStr = kafkaOffsetsStringList.get(i);
                if (kafkaOffsetsStr.equalsIgnoreCase(KafkaProgress.OFFSET_BEGINNING)) {
                    kafkaPartitionOffsets.get(i).second = KafkaProgress.OFFSET_BEGINNING_VAL;
                } else if (kafkaOffsetsStr.equalsIgnoreCase(KafkaProgress.OFFSET_END)) {
                    kafkaPartitionOffsets.get(i).second = KafkaProgress.OFFSET_END_VAL;
                } else if (NumberUtils.isDigits(kafkaOffsetsStr)) {
                    kafkaPartitionOffsets.get(i).second = Long.valueOf(NumberUtils.toLong(kafkaOffsetsStr));
                } else {
                    throw new AnalysisException(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY + " must be an integer or a date time");
                }
            }
        }

        return foundTime;
    }

    private static void analyzeCustomProperties(Map<String, String> dataSourceProperties,
                                                Map<String, String> customKafkaProperties) throws AnalysisException {
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

    private static int getIntegerValueFromString(String valueString, String propertyName) throws AnalysisException {
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

    @Override
    public String toString() {
        if (!hasAnalyzedProperties()) {
            return "empty";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("type: ").append(type);
        sb.append(", kafka partition offsets: ").append(kafkaPartitionOffsets);
        sb.append(", custome properties: ").append(customKafkaProperties);
        return sb.toString();
    }
}

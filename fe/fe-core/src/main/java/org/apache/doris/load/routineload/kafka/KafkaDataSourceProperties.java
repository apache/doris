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

package org.apache.doris.load.routineload.kafka;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.KafkaProgress;
import org.apache.doris.load.routineload.LoadDataSourceType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Kafka data source properties
 */
public class KafkaDataSourceProperties extends AbstractDataSourceProperties {

    private static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";

    private static final String CUSTOM_KAFKA_PROPERTY_PREFIX = "property.";

    @Getter
    @Setter
    @SerializedName(value = "kafkaPartitionOffsets")
    private List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();

    @Getter
    @SerializedName(value = "customKafkaProperties")
    private Map<String, String> customKafkaProperties;

    @Getter
    @SerializedName(value = "isOffsetsForTimes")
    private boolean isOffsetsForTimes = false;

    @Getter
    @SerializedName(value = "brokerList")
    private String brokerList;

    @Getter
    @SerializedName(value = "topic")
    private String topic;

    /**
     * The table name properties of kafka data source
     * <p>
     * table_name_location: table name location
     * 1. table name location is in the key of kafka message
     * 2. table name location is in the value of kafka message
     * <p>
     * table_name_format: table name format
     * 1.json format
     * 2.txt format
     * <p>
     * table_name_regex: table name regex
     */
    @Getter
    @SerializedName(value = "tableNameProperties")
    private Map<String, String> tableNameProperties;

    private static final ImmutableSet<String> CONFIGURABLE_DATA_SOURCE_PROPERTIES_SET =
            new ImmutableSet.Builder<String>().add(KafkaConfiguration.KAFKA_BROKER_LIST.getName())
                    .add(KafkaConfiguration.KAFKA_TOPIC.getName())
                    .add(KafkaConfiguration.KAFKA_PARTITIONS.getName())
                    .add(KafkaConfiguration.KAFKA_OFFSETS.getName())
                    .add(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName())
                    .add(KafkaConfiguration.KAFKA_TABLE_NAME_LOCATION.getName())
                    .add(KafkaConfiguration.KAFKA_TABLE_NAME_FORMAT.getName())
                    .add(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_DELIMITER.getName())
                    .add(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_INDEX.getName())
                    .build();

    public KafkaDataSourceProperties(Map<String, String> dataSourceProperties, boolean multiLoad) {
        super(dataSourceProperties, multiLoad);
    }

    public KafkaDataSourceProperties(Map<String, String> originalDataSourceProperties) {
        super(originalDataSourceProperties);
    }

    @Override
    protected String getDataSourceType() {
        return LoadDataSourceType.KAFKA.name();
    }

    @Override
    protected List<String> getRequiredProperties() {
        return Arrays.asList(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), KafkaConfiguration.KAFKA_TOPIC.getName());
    }

    @Override
    public void convertAndCheckDataSourceProperties() throws UserException {
        Optional<String> optional = originalDataSourceProperties.keySet()
                .stream().filter(entity -> !CONFIGURABLE_DATA_SOURCE_PROPERTIES_SET.contains(entity))
                .filter(entity -> !entity.startsWith(CUSTOM_KAFKA_PROPERTY_PREFIX)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid kafka property or can not be set");
        }

        this.brokerList = KafkaConfiguration.KAFKA_BROKER_LIST.getParameterValue(originalDataSourceProperties
                .get(KafkaConfiguration.KAFKA_BROKER_LIST.getName()));
        if (!isAlter() && StringUtils.isBlank(brokerList)) {
            throw new AnalysisException(KafkaConfiguration.KAFKA_BROKER_LIST.getName() + " is a required property");
        }
        //check broker list
        if (StringUtils.isNotBlank(brokerList)) {
            for (String broker : brokerList.split(",")) {
                if (!Pattern.matches(ENDPOINT_REGEX, broker)) {
                    throw new AnalysisException(KafkaConfiguration.KAFKA_BROKER_LIST
                            + ":" + broker + " not match pattern " + ENDPOINT_REGEX);
                }
            }
        }
        //check topic
        this.topic = KafkaConfiguration.KAFKA_TOPIC.getParameterValue(originalDataSourceProperties
                .get(KafkaConfiguration.KAFKA_TOPIC.getName()));
        if (!isAlter() && StringUtils.isBlank(topic)) {
            throw new AnalysisException(KafkaConfiguration.KAFKA_TOPIC.getName() + " is a required property");
        }
        // check custom kafka property
        // This should be done before check partition and offsets, because we need KAFKA_DEFAULT_OFFSETS,
        // which is in custom properties.
        analyzeCustomProperties();

        List<Integer> partitions = KafkaConfiguration.KAFKA_PARTITIONS.getParameterValue(originalDataSourceProperties
                .get(KafkaConfiguration.KAFKA_PARTITIONS.getName()));
        if (CollectionUtils.isNotEmpty(partitions)) {
            analyzeKafkaPartitionProperty(partitions);
        }
        //check offset
        List<String> offsets = KafkaConfiguration.KAFKA_OFFSETS.getParameterValue(originalDataSourceProperties
                .get(KafkaConfiguration.KAFKA_OFFSETS.getName()));
        String defaultOffsetString = originalDataSourceProperties
                .get(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName());
        if (CollectionUtils.isNotEmpty(offsets) && StringUtils.isNotBlank(defaultOffsetString)) {
            throw new AnalysisException("Only one of " + KafkaConfiguration.KAFKA_OFFSETS.getName() + " and "
                    + KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName() + " can be set.");
        }
        if (multiTable) {
            checkAndSetMultiLoadProperties();
        }
        if (isAlter() && CollectionUtils.isNotEmpty(partitions) && CollectionUtils.isEmpty(offsets)
                && StringUtils.isBlank(defaultOffsetString)) {
            // if this is an alter operation, the partition and (default)offset must be set together.
            throw new AnalysisException("Must set offset or default offset with partition property");
        }
        if (CollectionUtils.isNotEmpty(offsets)) {
            this.isOffsetsForTimes = analyzeKafkaOffsetProperty(offsets);
            return;
        }
        this.isOffsetsForTimes = analyzeKafkaDefaultOffsetProperty();
        if (CollectionUtils.isNotEmpty(kafkaPartitionOffsets)) {
            defaultOffsetString = customKafkaProperties.get(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName());
            setDefaultOffsetForPartition(this.kafkaPartitionOffsets, defaultOffsetString, this.isOffsetsForTimes);
        }

    }

    private void checkAndSetMultiLoadProperties() throws AnalysisException {
        String tableNameFormat = KafkaConfiguration.KAFKA_TABLE_NAME_FORMAT.getParameterValue(
                originalDataSourceProperties.get(KafkaConfiguration.KAFKA_TABLE_NAME_FORMAT.getName()));
        if (!KafkaConfigType.TableNameFormat.TEXT.name().equalsIgnoreCase(tableNameFormat)) {
            throw new AnalysisException("Multi load olay supported for table name format TEXT");
        }
        String tableNameDelimiter = KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_DELIMITER.getParameterValue(
                originalDataSourceProperties.get(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_DELIMITER.getName()));

        Integer tableNameIndex = KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_INDEX.getParameterValue(
                originalDataSourceProperties.get(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_INDEX.getName()));
        tableNameProperties = new HashMap<>();
        tableNameProperties.put(KafkaConfiguration.KAFKA_TABLE_NAME_FORMAT.getName(), tableNameFormat);
        tableNameProperties.put(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_DELIMITER.getName(), tableNameDelimiter);
        tableNameProperties.put(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_INDEX.getName(),
                String.valueOf(tableNameIndex));
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

    // init "kafkaPartitionOffsets" with partition property.
    // The offset will be set to OFFSET_END for now, and will be changed in later analysis process.
    private void analyzeKafkaPartitionProperty(List<Integer> partitions) {
        partitions.forEach(partition -> this.kafkaPartitionOffsets
                .add(Pair.of(partition, KafkaProgress.OFFSET_END_VAL)));
    }

    private void analyzeCustomProperties() throws AnalysisException {
        this.customKafkaProperties = new HashMap<>();
        for (Map.Entry<String, String> dataSourceProperty : originalDataSourceProperties.entrySet()) {
            if (dataSourceProperty.getKey().startsWith(CUSTOM_KAFKA_PROPERTY_PREFIX)) {
                String propertyKey = dataSourceProperty.getKey();
                String propertyValue = dataSourceProperty.getValue();
                String[] propertyValueArr = propertyKey.split("\\.");
                if (propertyValueArr.length < 2) {
                    throw new AnalysisException("kafka property value could not be a empty string");
                }
                this.customKafkaProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
            }
            // can be extended in the future which other prefix
        }
    }

    // Fill the partition's offset with given kafkaOffsetsString,
    // Return true if offset is specified by timestamp.
    private boolean analyzeKafkaOffsetProperty(List<String> kafkaOffsetsStringList) throws UserException {
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
            throw new AnalysisException("The offset of the partition cannot be specified by the timestamp "
                    + "and the offset at the same time");
        }

        if (foundTime) {
            // convert all datetime strs to timestamps
            // and set them as the partition's offset.
            // These timestamps will be converted to real offset when job is running.
            TimeZone timeZone = TimeUtils.getOrSystemTimeZone(getTimezone());
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
                    kafkaPartitionOffsets.get(i).second = NumberUtils.toLong(kafkaOffsetsStr);
                } else {
                    throw new AnalysisException(KafkaConfiguration.KAFKA_OFFSETS.getName()
                            + " must be an integer or a date time");
                }
            }
        }
        return foundTime;
    }

    // If the default offset is not set, set the default offset to OFFSET_END.
    // If the offset is in datetime format, convert it to a timestamp,
    // and also save the origin datatime formatted offset
    // in "customKafkaProperties"
    // return true if the offset is in datetime format.
    private boolean analyzeKafkaDefaultOffsetProperty() throws AnalysisException {
        customKafkaProperties.putIfAbsent(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName(), KafkaProgress.OFFSET_END);
        String defaultOffsetStr = customKafkaProperties.get(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName());
        TimeZone timeZone = TimeUtils.getOrSystemTimeZone(this.getTimezone());
        long defaultOffset = TimeUtils.timeStringToLong(defaultOffsetStr, timeZone);
        if (defaultOffset != -1) {
            // this is a datetime format offset
            customKafkaProperties.put(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName(),
                    String.valueOf(defaultOffset));
            // we convert datetime to timestamp, and save the origin datetime formatted offset for further use.
            customKafkaProperties.put(KafkaConfiguration.KAFKA_ORIGIN_DEFAULT_OFFSETS.getName(), defaultOffsetStr);
            return true;
        } else {
            if (!defaultOffsetStr.equalsIgnoreCase(KafkaProgress.OFFSET_BEGINNING)
                    && !defaultOffsetStr.equalsIgnoreCase(KafkaProgress.OFFSET_END)) {
                throw new AnalysisException(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName()
                        + " can only be set to OFFSET_BEGINNING, OFFSET_END or date time");
            }
            return false;
        }
    }
}

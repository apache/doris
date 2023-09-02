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

import com.google.common.base.Splitter;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum KafkaConfiguration {

    KAFKA_BROKER_LIST("kafka_broker_list", null, value -> value.replace(" ", "")),

    KAFKA_TOPIC("kafka_topic", null, value -> value.replace(" ", "")),

    KAFKA_PARTITIONS("kafka_partitions", null, partitionsString ->
            Arrays.stream(partitionsString.replace(" ", "").split(","))
                    .map(Integer::parseInt)
                    .collect(Collectors.toList())),

    KAFKA_OFFSETS("kafka_offsets", null, offsetsString -> Splitter.on(",").trimResults().splitToList(offsetsString)),

    KAFKA_DEFAULT_OFFSETS("kafka_default_offsets", "OFFSET_END", offset -> offset),
    KAFKA_ORIGIN_DEFAULT_OFFSETS("kafka_origin_default_offsets", null, offset -> offset),
    KAFKA_TABLE_NAME_LOCATION("kafka_table_name_location", "key",
            value -> value.replace(" ", "")),
    KAFKA_TABLE_NAME_FORMAT("kafka_table_name_format", "TEXT",
            value -> value.replace(" ", "")),
    KAFKA_TEXT_TABLE_NAME_FIELD_INDEX("kafka_text_table_name_field_index", 0, Integer::parseInt),

    KAFKA_TEXT_TABLE_NAME_FIELD_DELIMITER("kafka_text_table_name_field_delimiter", ",",
            value -> value.replace(" ", ""));
    private final String name;

    public String getName() {
        return name;
    }

    private final Object defaultValue;

    private final Function<String, Object> converter;

    <T> KafkaConfiguration(String name, T defaultValue, Function<String, T> converter) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.converter = (Function<String, Object>) converter;
    }

    KafkaConfiguration getByName(String name) {
        return Arrays.stream(KafkaConfiguration.values())
                .filter(config -> config.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown configuration " + name));
    }


    public <T> T getParameterValue(String param) {
        Object value = param != null ? converter.apply(param) : defaultValue;
        return (T) value;
    }
}

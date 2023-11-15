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

package org.apache.doris.load.routineload.pulsar;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum PulsarConfiguration {

    PULSAR_SERVICE_URL_PROPERTY("pulsar_service_url", null, value -> value.replace(" ", "")),

    PULSAR_TOPIC_PROPERTY("pulsar_topic", null, value -> value.replace(" ", "")),

    PULSAR_SUBSCRIPTION_PROPERTY("pulsar_subscription", null, value -> value.replace(" ", "")),

    PULSAR_PARTITIONS_PROPERTY("pulsar_partitions", null, partitionsString ->
        Arrays.stream(partitionsString.replace(" ", "").split(","))
            .collect(Collectors.toList())),

    PULSAR_INITIAL_POSITIONS_PROPERTY("pulsar_initial_positions", null, partitionsString ->
        Arrays.stream(partitionsString.replace(" ", "").split(","))
            .collect(Collectors.toList())),
    PULSAR_DEFAULT_INITIAL_POSITION("pulsar_default_initial_position", "POSITION_EARLIEST", position -> position);
    private final String name;

    public String getName() {
        return name;
    }

    private final Object defaultValue;

    private final Function<String, Object> converter;

    <T> PulsarConfiguration(String name, T defaultValue, Function<String, T> converter) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.converter = (Function<String, Object>) converter;
    }

    PulsarConfiguration getByName(String name) {
        return Arrays.stream(PulsarConfiguration.values())
            .filter(config -> config.getName().equals(name))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown configuration " + name));
    }


    public <T> T getParameterValue(String param) {
        Object value = param != null ? converter.apply(param) : defaultValue;
        return (T) value;
    }
}

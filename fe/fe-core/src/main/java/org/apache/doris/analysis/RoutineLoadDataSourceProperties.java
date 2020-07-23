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
import org.apache.doris.load.routineload.LoadDataSourceType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RoutineLoadDataSourceProperties {
    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)
            .build();
    
    @SerializedName(value = "type")
    private String type = "KAFKA";
    // origin properties, no need to persist
    private Map<String, String> properties = Maps.newHashMap();
    @SerializedName(value = "kafkaPartitionOffsets")
    private List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();
    @SerializedName(value = "customKafkaProperties")
    private Map<String, String> customKafkaProperties = Maps.newHashMap();

    public RoutineLoadDataSourceProperties() {
        // empty
    }

    public RoutineLoadDataSourceProperties(String type, Map<String, String> properties) {
        this.type = type.toUpperCase();
        this.properties = properties;
    }

    public void analyze() throws AnalysisException {
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

    public Map<String, String> getCustomKafkaProperties() {
        return customKafkaProperties;
    }

    private void checkDataSourceProperties() throws AnalysisException {
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

    private void checkKafkaProperties() throws AnalysisException {
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !CONFIGURABLE_PROPERTIES_SET.contains(entity)).filter(
                        entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid kafka custom property");
        }

        // check partitions
        final String kafkaPartitionsString = properties.get(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY);
        if (kafkaPartitionsString != null) {

            if (!properties.containsKey(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)) {
                throw new AnalysisException("Partition and offset must be specified at the same time");
            }

            CreateRoutineLoadStmt.analyzeKafkaPartitionProperty(kafkaPartitionsString, kafkaPartitionOffsets);
        } else {
            if (properties.containsKey(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)) {
                throw new AnalysisException("Missing kafka partition info");
            }
        }

        // check offset
        String kafkaOffsetsString = properties.get(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY);
        if (kafkaOffsetsString != null) {
            CreateRoutineLoadStmt.analyzeKafkaOffsetProperty(kafkaOffsetsString, kafkaPartitionOffsets);
        }

        // check custom properties
        CreateRoutineLoadStmt.analyzeCustomProperties(properties, customKafkaProperties);
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

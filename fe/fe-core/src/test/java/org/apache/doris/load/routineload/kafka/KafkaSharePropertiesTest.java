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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class KafkaSharePropertiesTest {
    private Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("kafka_broker_list", "localhost:9092");
        properties.put("kafka_topic", "events");
        return properties;
    }

    @Test
    void standardRemainsDefault() throws Exception {
        KafkaDataSourceProperties properties = new KafkaDataSourceProperties(properties());
        properties.convertAndCheckDataSourceProperties();
        Assertions.assertFalse(properties.isUseShareConsumer());
    }

    @Test
    void explicitStandardUsesPartitionOffsets() throws Exception {
        Map<String, String> values = properties();
        values.put("kafka_consumer_type", "standard");
        values.put("kafka_partitions", "0");
        values.put("kafka_offsets", "42");
        KafkaDataSourceProperties properties = new KafkaDataSourceProperties(values);
        properties.convertAndCheckDataSourceProperties();
        Assertions.assertFalse(properties.isUseShareConsumer());
        Assertions.assertEquals(1, properties.getKafkaPartitionOffsets().size());
        Assertions.assertEquals(42L, properties.getKafkaPartitionOffsets().get(0).second);
    }

    @Test
    void rejectPreviousExperimentalNameAndValue() {
        Map<String, String> oldName = properties();
        oldName.put("kafka_consumer_mode", "share");
        KafkaDataSourceProperties invalidName = new KafkaDataSourceProperties(oldName);
        Assertions.assertThrows(AnalysisException.class, invalidName::convertAndCheckDataSourceProperties);
        Map<String, String> oldValue = properties();
        oldValue.put("kafka_consumer_type", "classic");
        KafkaDataSourceProperties invalidValue = new KafkaDataSourceProperties(oldValue);
        Assertions.assertThrows(AnalysisException.class, invalidValue::convertAndCheckDataSourceProperties);
    }

    @Test
    void shareDoesNotCreateOffsetProgress() throws Exception {
        Map<String, String> values = properties();
        values.put("kafka_consumer_type", "share");
        values.put("property.group.id", "demo-share");
        KafkaDataSourceProperties properties = new KafkaDataSourceProperties(values);
        properties.convertAndCheckDataSourceProperties();
        Assertions.assertTrue(properties.isUseShareConsumer());
        Assertions.assertTrue(properties.getKafkaPartitionOffsets().isEmpty());
        Assertions.assertEquals("demo-share", properties.getCustomKafkaProperties().get("group.id"));
        Assertions.assertFalse(properties.getCustomKafkaProperties().containsKey("kafka_default_offsets"));
    }

    @Test
    void rejectOffsetsAndImplicitAcknowledgements() {
        for (String key : new String[] {"kafka_partitions", "kafka_offsets", "kafka_default_offsets",
                "property.kafka_default_offsets", "property.auto.offset.reset",
                "property.enable.auto.commit", "property.share.acknowledgement.mode"}) {
            Map<String, String> values = properties();
            values.put("kafka_consumer_type", "share");
            values.put(key, "1");
            KafkaDataSourceProperties properties = new KafkaDataSourceProperties(values);
            Assertions.assertThrows(AnalysisException.class, properties::convertAndCheckDataSourceProperties, key);
        }
    }

    @Test
    void rejectUnsupportedTypeAndMultiTable() {
        Map<String, String> values = properties();
        values.put("kafka_consumer_type", "typo");
        KafkaDataSourceProperties invalid = new KafkaDataSourceProperties(values);
        Assertions.assertThrows(AnalysisException.class, invalid::convertAndCheckDataSourceProperties);
        values.put("kafka_consumer_type", "share");
        KafkaDataSourceProperties multi = new KafkaDataSourceProperties(values, true);
        Assertions.assertThrows(AnalysisException.class, multi::convertAndCheckDataSourceProperties);
    }

    @Test
    void rejectTypeChange() {
        Map<String, String> values = new HashMap<>();
        values.put("kafka_consumer_type", "share");
        KafkaDataSourceProperties properties = new KafkaDataSourceProperties(values);
        properties.setAlter(true);
        Assertions.assertThrows(AnalysisException.class, properties::convertAndCheckDataSourceProperties);
    }
}

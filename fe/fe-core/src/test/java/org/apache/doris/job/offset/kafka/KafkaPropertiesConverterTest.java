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

package org.apache.doris.job.offset.kafka;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaPropertiesConverterTest {

    @Test
    public void testExtractBrokerList() {
        Map<String, String> props = new HashMap<>();
        props.put("trino.kafka.nodes", "broker1:9092,broker2:9092");
        
        String brokerList = KafkaPropertiesConverter.extractBrokerList(props);
        Assert.assertEquals("broker1:9092,broker2:9092", brokerList);
    }
    
    @Test
    public void testExtractBrokerListWithoutPrefix() {
        Map<String, String> props = new HashMap<>();
        props.put("kafka.nodes", "broker1:9092");
        
        String brokerList = KafkaPropertiesConverter.extractBrokerList(props);
        Assert.assertEquals("broker1:9092", brokerList);
    }
    
    @Test
    public void testExtractBrokerListPriorityTrinoPrefix() {
        // trino.kafka.nodes should take priority over kafka.nodes
        Map<String, String> props = new HashMap<>();
        props.put("trino.kafka.nodes", "broker1:9092");
        props.put("kafka.nodes", "broker2:9092");
        
        String brokerList = KafkaPropertiesConverter.extractBrokerList(props);
        Assert.assertEquals("broker1:9092", brokerList);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testExtractBrokerListMissing() {
        Map<String, String> props = new HashMap<>();
        KafkaPropertiesConverter.extractBrokerList(props);
    }
    
    @Test
    public void testConvertToKafkaClientProperties() {
        Map<String, String> props = new HashMap<>();
        // Trino-specific properties (should be filtered out)
        props.put("trino.kafka.nodes", "broker1:9092");
        props.put("trino.kafka.default-schema", "default");
        props.put("trino.kafka.table-names", "topic1,topic2");
        props.put("trino.kafka.hide-internal-columns", "false");
        
        // Kafka client properties (should be retained)
        props.put("trino.kafka.security.protocol", "SASL_PLAINTEXT");
        props.put("trino.kafka.sasl.mechanism", "PLAIN");
        props.put("trino.kafka.request.timeout.ms", "30000");
        
        Map<String, String> kafkaProps = KafkaPropertiesConverter.convertToKafkaClientProperties(props);
        
        // Verify Trino-specific properties are filtered out
        Assert.assertFalse(kafkaProps.containsKey("nodes"));
        Assert.assertFalse(kafkaProps.containsKey("default-schema"));
        Assert.assertFalse(kafkaProps.containsKey("table-names"));
        Assert.assertFalse(kafkaProps.containsKey("hide-internal-columns"));
        
        // Verify Kafka client properties are retained
        Assert.assertEquals("SASL_PLAINTEXT", kafkaProps.get("security.protocol"));
        Assert.assertEquals("PLAIN", kafkaProps.get("sasl.mechanism"));
        Assert.assertEquals("30000", kafkaProps.get("request.timeout.ms"));
    }
    
    @Test
    public void testConvertEmptyProperties() {
        Map<String, String> props = new HashMap<>();
        Map<String, String> kafkaProps = KafkaPropertiesConverter.convertToKafkaClientProperties(props);
        Assert.assertTrue(kafkaProps.isEmpty());
    }
    
    @Test
    public void testConvertPropertiesWithoutKafkaPrefix() {
        Map<String, String> props = new HashMap<>();
        props.put("other.property", "value");
        props.put("trino.other.property", "value2");
        
        Map<String, String> kafkaProps = KafkaPropertiesConverter.convertToKafkaClientProperties(props);
        Assert.assertTrue(kafkaProps.isEmpty());
    }
    
    @Test
    public void testNormalizeSecurityProperties() {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put("sasl.mechanism", "PLAIN");
        kafkaProps.put("sasl.jaas.config", "...");
        
        KafkaPropertiesConverter.normalizeSecurityProperties(kafkaProps);
        
        // When SASL config is present, security.protocol should be defaulted
        Assert.assertEquals("SASL_PLAINTEXT", kafkaProps.get("security.protocol"));
    }
    
    @Test
    public void testNormalizeSecurityPropertiesExistingProtocol() {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put("sasl.mechanism", "PLAIN");
        kafkaProps.put("security.protocol", "SASL_SSL");
        
        KafkaPropertiesConverter.normalizeSecurityProperties(kafkaProps);
        
        // Should not override existing protocol
        Assert.assertEquals("SASL_SSL", kafkaProps.get("security.protocol"));
    }
    
    @Test
    public void testValidateKafkaPropertiesSuccess() {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.put("sasl.mechanism", "PLAIN");
        
        // Should not throw
        KafkaPropertiesConverter.validateKafkaProperties("broker1:9092", "topic", kafkaProps);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testValidateKafkaPropertiesNullBroker() {
        Map<String, String> kafkaProps = new HashMap<>();
        KafkaPropertiesConverter.validateKafkaProperties(null, "topic", kafkaProps);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testValidateKafkaPropertiesEmptyTopic() {
        Map<String, String> kafkaProps = new HashMap<>();
        KafkaPropertiesConverter.validateKafkaProperties("broker:9092", "", kafkaProps);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testValidateKafkaPropertiesMissingSaslMechanism() {
        Map<String, String> kafkaProps = new HashMap<>();
        kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
        // Missing sasl.mechanism
        
        KafkaPropertiesConverter.validateKafkaProperties("broker:9092", "topic", kafkaProps);
    }
}

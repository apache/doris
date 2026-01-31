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

import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to convert Trino Kafka connector properties to KafkaUtil parameters.
 * 
 * KafkaUtil was originally designed for KafkaRoutineLoadJob and expects parameters
 * in a specific format. This converter bridges the gap between TrinoConnectorExternalCatalog
 * properties and KafkaUtil expectations.
 * 
 * Property format conversion:
 * - Trino Catalog: trino.kafka.nodes=broker1:9092
 * - KafkaUtil: brokerList=broker1:9092
 */
public class KafkaPropertiesConverter {
    
    private static final String TRINO_PREFIX = "trino.";
    private static final String KAFKA_PREFIX = "kafka.";
    
    /**
     * Trino-specific properties that should not be passed to the Kafka client.
     * These are configuration options used only by Trino's Kafka connector,
     * not by the underlying Kafka client.
     */
    private static final Set<String> TRINO_ONLY_PROPERTIES = ImmutableSet.of(
            "nodes",                    // Broker list, handled separately
            "default-schema",           // Trino schema configuration
            "table-names",              // Trino table names list
            "table-description-dir",    // Trino table description directory
            "hide-internal-columns",    // Trino column visibility
            "timestamp-upper-bound-force-push-down-enabled",
            "internal-column-prefix",   // Trino internal column prefix
            "messages-per-split"        // Trino split configuration
    );
    
    private KafkaPropertiesConverter() {
        // Utility class, no instantiation
    }
    
    /**
     * Extract the Kafka broker list from catalog properties.
     * 
     * Looks for the broker list in the following order:
     * 1. trino.kafka.nodes
     * 2. kafka.nodes
     * 
     * @param catalogProps the catalog properties
     * @return the broker list string (e.g., "broker1:9092,broker2:9092")
     * @throws IllegalArgumentException if no broker list is found
     */
    public static String extractBrokerList(Map<String, String> catalogProps) {
        // Try trino.kafka.nodes first
        String brokers = catalogProps.get("trino.kafka.nodes");
        if (brokers != null && !brokers.isEmpty()) {
            return brokers;
        }
        
        // Fallback to kafka.nodes (without trino prefix)
        brokers = catalogProps.get("kafka.nodes");
        if (brokers != null && !brokers.isEmpty()) {
            return brokers;
        }
        
        throw new IllegalArgumentException(
                "Missing required property: kafka.nodes or trino.kafka.nodes. "
                + "Please configure the Kafka broker list in the Trino Kafka catalog.");
    }
    
    /**
     * Extract and convert Kafka client properties from catalog properties.
     * 
     * This method performs the following conversions:
     * - Strips the "trino.kafka." prefix from property keys
     * - Filters out Trino-specific properties
     * - Retains Kafka client properties (security, SASL, etc.)
     * 
     * @param catalogProps the catalog properties
     * @return a map of Kafka client properties ready for KafkaUtil
     */
    public static Map<String, String> convertToKafkaClientProperties(Map<String, String> catalogProps) {
        Map<String, String> kafkaProps = new HashMap<>();
        
        String fullPrefix = TRINO_PREFIX + KAFKA_PREFIX;  // "trino.kafka."
        
        for (Map.Entry<String, String> entry : catalogProps.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            
            if (key.startsWith(fullPrefix)) {
                // Remove "trino.kafka." prefix to get the Kafka property name
                String kafkaKey = key.substring(fullPrefix.length());
                
                // Only include properties that are valid Kafka client properties
                if (isKafkaClientProperty(kafkaKey)) {
                    kafkaProps.put(kafkaKey, value);
                }
            }
        }
        
        return kafkaProps;
    }
    
    /**
     * Check if a property key is a valid Kafka client property.
     * 
     * Returns true for:
     * - Security properties (security.protocol, ssl.*, sasl.*)
     * - Connection properties (request.timeout.ms, etc.)
     * - Any property not in the TRINO_ONLY_PROPERTIES set
     * 
     * @param key the property key (without "trino.kafka." prefix)
     * @return true if this is a valid Kafka client property
     */
    private static boolean isKafkaClientProperty(String key) {
        return !TRINO_ONLY_PROPERTIES.contains(key);
    }
    
    /**
     * Convert Trino security protocol format to Kafka client format if needed.
     * 
     * Trino may use different property names for security configuration.
     * This method normalizes them to the Kafka client expected format.
     * 
     * @param kafkaProps the Kafka properties map (will be modified in place)
     */
    public static void normalizeSecurityProperties(Map<String, String> kafkaProps) {
        // Normalize security protocol property names if needed
        // Trino uses "security.protocol", Kafka client also uses "security.protocol"
        // This method is reserved for future compatibility needs
        
        // If any SASL properties are present, ensure security protocol is set
        boolean hasSaslConfig = kafkaProps.keySet().stream()
                .anyMatch(k -> k.startsWith("sasl."));
        if (hasSaslConfig && !kafkaProps.containsKey("security.protocol")) {
            // Default to SASL_PLAINTEXT if SASL config present but no protocol specified
            kafkaProps.putIfAbsent("security.protocol", "SASL_PLAINTEXT");
        }
    }
    
    /**
     * Validate that all required properties are present for Kafka connection.
     * 
     * @param brokerList the broker list
     * @param topic the topic name
     * @param kafkaProps additional Kafka properties
     * @throws IllegalArgumentException if required properties are missing
     */
    public static void validateKafkaProperties(String brokerList, String topic,
            Map<String, String> kafkaProps) {
        if (brokerList == null || brokerList.isEmpty()) {
            throw new IllegalArgumentException("Kafka broker list is required");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Kafka topic is required");
        }
        
        // Check for valid security configuration
        String securityProtocol = kafkaProps.get("security.protocol");
        if (securityProtocol != null) {
            String upperProtocol = securityProtocol.toUpperCase();
            if (upperProtocol.contains("SASL")) {
                // SASL authentication requires mechanism configuration
                if (!kafkaProps.containsKey("sasl.mechanism")) {
                    throw new IllegalArgumentException(
                            "SASL authentication requires 'sasl.mechanism' property");
                }
            }
        }
    }
}

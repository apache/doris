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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

public class KafkaPropertiesUtilTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "sasl.password", "property.sasl.password", "PrOpErTy.SaSl.PaSsWoRd",
            "sasl.jaas.config", "property.sasl.oauthbearer.config", "sasl.oauthbearer.client.secret",
            "sasl.oauthbearer.token", "aws.access_key", "property.aws.secret_key", "aws.session_token",
            "ssl.keystore.password", "ssl.keystore.key", "ssl.key.pem", "ssl.key.password",
            "sasl.oauthbearer.assertion.private.key.pem", "sasl.oauthbearer.assertion.private.key.passphrase",
            "password", "secret_key", "session_token", "property.sasl.oauthbearer.credentials",
            "token", "property.token", "property.credentials",
            "sasl.password\0ignored", "property.sasl.password\0ignored", "PrOpErTy.SaSl.PaSsWoRd\0ignored",
            "property.sasl.jaas.config\0ignored", "property.ssl.key.pem\0ignored"
    })
    public void testMaskSensitivePropertiesWithoutChangingInput(String key) {
        Map<String, String> properties = Map.of(key, "synthetic-secret");
        Map<String, String> masked = KafkaPropertiesUtil.maskProperties(properties);

        Assertions.assertEquals(Map.of(key, "******"), masked);
        Assertions.assertEquals("synthetic-secret", properties.get(key));
    }

    @Test
    public void testPreserveOperationalProperties() {
        Map<String, String> properties = Map.of(
                "kafka_topic", "orders",
                "kafka_broker_list", "localhost:9092",
                "property.client.id", "routine-load-client",
                "property.security.protocol", "SASL_SSL",
                "property.sasl.mechanism", "OAUTHBEARER",
                "property.sasl.oauthbearer.token.endpoint.url", "https://auth.example/token",
                "property.ssl.ca.location", "FILE:ca.pem");
        Map<String, String> masked = KafkaPropertiesUtil.maskProperties(properties);

        Assertions.assertEquals(properties, masked);
        Assertions.assertNotSame(properties, masked);
    }

    @Test
    public void testClassifyOnlyEffectiveNativePropertyName() {
        Assertions.assertFalse(KafkaPropertiesUtil.isSensitiveKey("property.client.id\0.password"));
        Assertions.assertEquals(Map.of("property.client.id\0ignored", "client-value"),
                KafkaPropertiesUtil.maskProperties(Map.of("property.client.id\0ignored", "client-value")));
    }

    @Test
    public void testAbsentProperties() {
        Assertions.assertTrue(KafkaPropertiesUtil.maskProperties(null).isEmpty());
        Assertions.assertTrue(KafkaPropertiesUtil.maskProperties(new HashMap<>()).isEmpty());
    }
}

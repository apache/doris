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

package org.apache.doris.datasource.property.storage;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class BrokerPropertiesTest {

    @Test
    void testOfMethod_initializesBrokerNameAndParams() {
        Map<String, String> inputProps = new HashMap<>();
        inputProps.put("fs.s3a.access.key", "abc");
        inputProps.put("broker.name", "broker-01");
        BrokerProperties props = BrokerProperties.of("broker-01", inputProps);
        Assertions.assertEquals("broker-01", props.getBrokerName());
        Assertions.assertEquals("abc", props.getBackendConfigProperties().get("fs.s3a.access.key"));
    }

    @Test
    void testGuessIsMe_returnsTrueWhenBrokerNamePresent() {
        Map<String, String> props1 = ImmutableMap.of("broker.name", "test");
        Map<String, String> props2 = ImmutableMap.of("BROKER.NAME", "test");
        Map<String, String> props3 = ImmutableMap.of("some.other.key", "value");
        Assertions.assertTrue(BrokerProperties.guessIsMe(props1));
        Assertions.assertTrue(BrokerProperties.guessIsMe(props2));
        Assertions.assertFalse(BrokerProperties.guessIsMe(props3));
    }

    @Test
    void testValidateAndNormalizeUri_returnsInput() throws Exception {
        BrokerProperties props = BrokerProperties.of("broker", new HashMap<>());
        String uri = "hdfs://localhost:9000/path";
        Assertions.assertEquals(uri, props.validateAndGetUri(ImmutableMap.of("uri", uri)));
    }

    @Test
    void testValidateAndGetUri_returnsUriFromProps() throws Exception {
        Map<String, String> loadProps = ImmutableMap.of("uri", "s3://bucket/file");
        BrokerProperties props = BrokerProperties.of("broker", loadProps);
        String result = props.validateAndGetUri(loadProps);
        Assertions.assertEquals("s3://bucket/file", result);
    }

    @Test
    void testGetStorageNameReturnsBroker() {
        BrokerProperties props = BrokerProperties.of("broker", new HashMap<>());
        Assertions.assertEquals("BROKER", props.getStorageName());
    }
}

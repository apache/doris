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

package org.apache.doris.filesystem.broker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link BrokerFileSystemProvider}.
 * Tests validation logic and SPI contract — no real Broker service required.
 */
class BrokerFileSystemProviderTest {

    private BrokerFileSystemProvider provider;

    @BeforeEach
    void setUp() {
        provider = new BrokerFileSystemProvider();
    }

    // ------------------------------------------------------------------
    // name()
    // ------------------------------------------------------------------

    @Test
    void name_returnsBroker() {
        Assertions.assertEquals("Broker", provider.name());
    }

    // ------------------------------------------------------------------
    // supports()
    // ------------------------------------------------------------------

    @Test
    void supports_trueWhenTypeIsBrokerAndHostPresent() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "BROKER");
        props.put("BROKER_HOST", "broker-host");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_falseWhenTypeMissing() {
        Map<String, String> props = new HashMap<>();
        props.put("BROKER_HOST", "broker-host");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_falseWhenTypeNotBroker() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "S3");
        props.put("BROKER_HOST", "broker-host");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_falseWhenHostMissing() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "BROKER");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_falseForEmptyMap() {
        Assertions.assertFalse(provider.supports(new HashMap<>()));
    }

    // ------------------------------------------------------------------
    // create() - validation
    // ------------------------------------------------------------------

    @Test
    void create_throwsWhenHostIsNull() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "BROKER");
        props.put("BROKER_PORT", "9999");

        IOException ex = Assertions.assertThrows(IOException.class, () -> provider.create(props));
        Assertions.assertTrue(ex.getMessage().contains("BROKER_HOST"));
    }

    @Test
    void create_throwsWhenHostIsEmpty() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "BROKER");
        props.put("BROKER_HOST", "");
        props.put("BROKER_PORT", "9999");

        IOException ex = Assertions.assertThrows(IOException.class, () -> provider.create(props));
        Assertions.assertTrue(ex.getMessage().contains("BROKER_HOST"));
    }

    @Test
    void create_throwsWhenPortIsNull() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "BROKER");
        props.put("BROKER_HOST", "broker-host");

        IOException ex = Assertions.assertThrows(IOException.class, () -> provider.create(props));
        Assertions.assertTrue(ex.getMessage().contains("BROKER_PORT"));
    }

    @Test
    void create_throwsWhenPortIsEmpty() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "BROKER");
        props.put("BROKER_HOST", "broker-host");
        props.put("BROKER_PORT", "");

        IOException ex = Assertions.assertThrows(IOException.class, () -> provider.create(props));
        Assertions.assertTrue(ex.getMessage().contains("BROKER_PORT"));
    }

    @Test
    void create_throwsWhenPortIsNotANumber() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "BROKER");
        props.put("BROKER_HOST", "broker-host");
        props.put("BROKER_PORT", "not-a-number");

        IOException ex = Assertions.assertThrows(IOException.class, () -> provider.create(props));
        Assertions.assertTrue(ex.getMessage().contains("Invalid BROKER_PORT"));
    }
}

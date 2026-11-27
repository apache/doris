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

package org.apache.doris.connector.metastore.hms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

class HmsDlfMetaStorePropertiesTest {
    private Map<String, String> properties() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("hive.metastore.type", "dlf");
        properties.put("dlf.catalog.accessKeyId", "test-key");
        properties.put("dlf.catalog.accessKeySecret", "test-secret");
        properties.put("dlf.catalog.securityToken", "test-token");
        properties.put("dlf.catalog.region", "test-region");
        properties.put("dlf.catalog.uid", "test-account");
        return properties;
    }

    @Test
    void canonicalRegionDerivesEndpointAndUidDefaultsCatalogId() {
        for (boolean publicAccess : new boolean[] {false, true}) {
            Map<String, String> properties = properties();
            properties.put("dlf.access.public", Boolean.toString(publicAccess));
            DefaultHmsMetaStoreProperties parsed = DefaultHmsMetaStoreProperties.of(properties, Map.of());
            Assertions.assertDoesNotThrow(parsed::validate);
            Map<String, String> conf = parsed.toHiveConfOverrides("20");
            Assertions.assertEquals((publicAccess ? "dlf." : "dlf-vpc.") + "test-region.aliyuncs.com",
                    conf.get("dlf.catalog.endpoint"));
            Assertions.assertEquals("test-region", conf.get("dlf.catalog.region"));
            Assertions.assertEquals("test-account", conf.get("dlf.catalog.id"));
            Assertions.assertEquals("test-token", conf.get("dlf.catalog.securityToken"));
        }
    }

    @Test
    void explicitRegionAndEndpointWinOverDerivedValues() {
        Map<String, String> properties = properties();
        properties.put("dlf.region", "explicit-region");
        properties.put("dlf.catalog.endpoint", "dlf.example.test");
        properties.put("dlf.catalog.id", "explicit-catalog");
        Map<String, String> conf = DefaultHmsMetaStoreProperties.of(properties, Map.of()).toHiveConfOverrides("20");
        Assertions.assertEquals("explicit-region", conf.get("dlf.catalog.region"));
        Assertions.assertEquals("dlf.example.test", conf.get("dlf.catalog.endpoint"));
        Assertions.assertEquals("explicit-catalog", conf.get("dlf.catalog.id"));
        Assertions.assertEquals("test-region", properties.get("dlf.catalog.region"));
    }

    @Test
    void plainHmsStillRequiresThriftUriAndDoesNotEmitDlfConfiguration() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DefaultHmsMetaStoreProperties.of(Map.of(), Map.of()).validate());
        DefaultHmsMetaStoreProperties properties = DefaultHmsMetaStoreProperties.of(
                Map.of("hive.metastore.uris", "thrift://hms:9083"), Map.of());
        Assertions.assertDoesNotThrow(properties::validate);
        Assertions.assertFalse(properties.toHiveConfOverrides("20").containsKey("dlf.catalog.endpoint"));
        Assertions.assertFalse(new HmsMetaStoreProvider().supportsType("dlf"));
    }
}

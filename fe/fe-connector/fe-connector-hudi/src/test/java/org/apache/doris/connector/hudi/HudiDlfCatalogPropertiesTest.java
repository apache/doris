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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

class HudiDlfCatalogPropertiesTest {
    @Test
    void embeddedDlfCatalogDoesNotRequireThriftUri() {
        for (String type : Set.of("dlf", "DLF", "Dlf")) {
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("hive.metastore.type", type);
            properties.put("dlf.endpoint", "dlf.example.test");
            properties.put("dlf.region", "region-a");
            properties.put("dlf.access_key", "test-access-key");
            properties.put("dlf.secret_key", "test-secret-key");
            HudiCatalogProperties parsed = Assertions.assertDoesNotThrow(() -> HudiCatalogProperties.of(properties));
            Assertions.assertTrue(parsed.getMetastoreUri() == null || parsed.getMetastoreUri().isEmpty());
            Assertions.assertEquals(8, parsed.getHmsClientPoolSize());
        }
    }

    @Test
    void dlfMetadataAndStorageKeepSeparateAuthenticators() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("hive.metastore.type", "dlf");
        properties.put("hadoop.security.authentication", "kerberos");
        properties.put("hadoop.kerberos.principal", "storage@EXAMPLE.COM");
        properties.put("hadoop.kerberos.keytab", "/test/storage.keytab");
        Assertions.assertNull(HudiConnector.buildHmsAuthenticator(properties));
        Assertions.assertNotNull(HudiConnector.buildPluginAuthenticator(properties));
    }

    @Test
    void liveHudiConnectorReceivesCanonicalDlfConfiguration() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("hive.metastore.type", "DLF");
        properties.put("dlf.catalog.endpoint", "dlf.example.test");
        properties.put("dlf.catalog.region", "region-a");
        properties.put("dlf.catalog.accessKeyId", "test-access-key");
        properties.put("dlf.catalog.accessKeySecret", "test-secret-key");
        HudiConnector connector = new HudiConnector(properties, new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test";
            }

            @Override
            public long getCatalogId() {
                return 1;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return Map.of("hive_metastore_client_timeout_second", "47");
            }
        });
        Map<String, String> conf = connector.buildHmsClientConfig().getProperties();
        Assertions.assertEquals("dlf.example.test", conf.get("dlf.catalog.endpoint"));
        Assertions.assertEquals("region-a", conf.get("dlf.catalog.region"));
        Assertions.assertEquals("test-access-key", conf.get("dlf.catalog.accessKeyId"));
        Assertions.assertEquals("47", conf.get("hive.metastore.client.socket.timeout"));
    }

    @Test
    void dlfStillRequiresCredentials() {
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> HudiCatalogProperties.of(Map.of("hive.metastore.type", "dlf")));
        Assertions.assertTrue(error.getMessage().contains("dlf.access_key is required"));
    }
}

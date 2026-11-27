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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.hms.HmsMetaStoreProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

class HiveDlfCatalogPropertiesTest {
    static Map<String, String> dlfProperties() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("hive.metastore.type", "dlf");
        properties.put("dlf.endpoint", "dlf.example.test");
        properties.put("dlf.region", "region-a");
        properties.put("dlf.uid", "test-account");
        properties.put("dlf.catalog_id", "test-catalog");
        properties.put("dlf.access_key", "test-access-key");
        properties.put("dlf.secret_key", "test-secret-key");
        properties.put("dlf.session_token", "test-session-token");
        return properties;
    }

    @Test
    void acceptsDlfWithoutThriftUri() {
        for (String type : Set.of("dlf", "DLF", "Dlf")) {
            Map<String, String> properties = dlfProperties();
            properties.put("hive.metastore.type", type);
            HiveCatalogProperties parsed = Assertions.assertDoesNotThrow(() -> HiveCatalogProperties.of(properties));
            Assertions.assertTrue(parsed.getMetastoreUri() == null || parsed.getMetastoreUri().isEmpty());
        }
    }

    @Test
    void mapsDlfMetadataCredentialsAndStorageSeparately() {
        HmsMetaStoreProperties parsed = new HmsMetaStoreProvider().bind(dlfProperties(),
                Map.of("fs.s3a.access.key", "test-storage-key"));
        Assertions.assertDoesNotThrow(parsed::validate);
        Map<String, String> conf = parsed.toHiveConfOverrides("37");
        Assertions.assertEquals("dlf.example.test", conf.get("dlf.catalog.endpoint"));
        Assertions.assertEquals("region-a", conf.get("dlf.catalog.region"));
        Assertions.assertEquals("test-account", conf.get("dlf.catalog.uid"));
        Assertions.assertEquals("test-catalog", conf.get("dlf.catalog.id"));
        Assertions.assertEquals("test-access-key", conf.get("dlf.catalog.accessKeyId"));
        Assertions.assertEquals("test-secret-key", conf.get("dlf.catalog.accessKeySecret"));
        Assertions.assertEquals("test-session-token", conf.get("dlf.catalog.securityToken"));
        Assertions.assertEquals("DLF_ONLY", conf.get("dlf.catalog.proxyMode"));
        Assertions.assertEquals("test-storage-key", conf.get("fs.s3a.access.key"));
        Assertions.assertEquals("37", conf.get("hive.metastore.client.socket.timeout"));
    }

    @Test
    void declaresDlfCredentialAliasesAsSensitive() {
        Set<String> sensitive = new HmsMetaStoreProvider().sensitivePropertyKeys();
        for (String key : Set.of("dlf.access_key", "dlf.secret_key", "dlf.session_token",
                "dlf.catalog.accessKeyId", "dlf.catalog.accessKeySecret", "dlf.catalog.securityToken")) {
            Assertions.assertTrue(sensitive.contains(key), key);
        }
    }

    @Test
    void keepsIcebergSiblingOnTheDlfMetastore() {
        Map<String, String> properties = dlfProperties();
        properties.put("iceberg.catalog.type", "hms");
        Map<String, String> sibling = IcebergSiblingProperties.synthesize(properties);
        Assertions.assertEquals("dlf", sibling.get("iceberg.catalog.type"));
        Assertions.assertEquals("test-catalog", sibling.get("dlf.catalog_id"));
        Assertions.assertEquals("hms", properties.get("iceberg.catalog.type"));
    }

    @Test
    void liveConnectorReceivesCanonicalDlfConfigAndTimeout() {
        Map<String, String> properties = dlfProperties();
        properties.put("hive.conf.resources", "hive-site.xml");
        properties.put("hive.metastore.client.pool.size", "3");
        HiveConnector connector = new HiveConnector(properties,
                new FakeConnectorContext(Map.of("hive_metastore_client_timeout_second", "47")));
        HmsClientConfig config = connector.buildHmsClientConfig();
        Assertions.assertEquals("test-access-key", config.getProperties().get("dlf.catalog.accessKeyId"));
        Assertions.assertEquals("47", config.getProperties().get("hive.metastore.client.socket.timeout"));
        Assertions.assertEquals(3, config.getPoolSize());
        Assertions.assertEquals("hive-site.xml", config.getConfResources());
    }

    @Test
    void rejectsMissingDlfConnectionFacts() {
        for (String key : Set.of("dlf.access_key", "dlf.secret_key", "dlf.endpoint")) {
            Map<String, String> properties = dlfProperties();
            properties.remove(key);
            if (key.equals("dlf.endpoint")) {
                properties.remove("dlf.region");
            }
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> HiveCatalogProperties.of(properties));
            Assertions.assertTrue(error.getMessage().contains(key + " is required"), error.getMessage());
        }
    }

    @Test
    void dlfMetadataDoesNotUseStorageKerberosIdentity() {
        Map<String, String> properties = dlfProperties();
        properties.put("hadoop.security.authentication", "kerberos");
        properties.put("hadoop.kerberos.principal", "storage@EXAMPLE.COM");
        properties.put("hadoop.kerberos.keytab", "/test/storage.keytab");
        Assertions.assertNull(HiveConnector.buildPluginAuthenticator(properties));
    }

    @Test
    void hudiSiblingRetainsDlfMetadataAndStorageProperties() {
        Map<String, String> properties = dlfProperties();
        properties.put("fs.s3a.access.key", "test-storage-key");
        Map<String, String> sibling = HudiSiblingProperties.synthesize(properties);
        Assertions.assertEquals(properties, sibling);
        sibling.put("dlf.catalog_id", "other-catalog");
        Assertions.assertEquals("test-catalog", properties.get("dlf.catalog_id"));
    }
}

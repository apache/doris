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

package org.apache.doris.datasource.storage;

import org.apache.doris.connector.iceberg.IcebergCatalogFactory;
import org.apache.doris.connector.iceberg.IcebergConnector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.fs.FileSystemPluginManager;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageAdapterBindingOriginTest {

    private FileSystemPluginManager manager;

    @BeforeEach
    void installBuiltins() {
        manager = new FileSystemPluginManager();
        manager.loadBuiltins();
        StorageAdapter.initPluginManager(manager);
    }

    @AfterEach
    void resetPluginManager() {
        StorageAdapter.initPluginManager(null);
    }

    @Test
    void bindingOriginDoesNotChangeWhenRawPropertiesChange() {
        Map<String, String> raw = new HashMap<>(Map.of(
                "azure.account_name", "account", "azure.account_key", "key"));
        StorageAdapter hdfs = StorageAdapter.ofAll(raw).stream()
                .filter(adapter -> adapter.getType() == StorageTypeId.HDFS).findFirst().get();

        Assertions.assertFalse(hdfs.isExplicitlyConfigured());
        raw.put("hadoop.username", "test-user");
        Assertions.assertFalse(hdfs.isExplicitlyConfigured());
    }

    @Test
    void directProviderBindingIsNotAnInferredFallback() {
        StorageAdapter hdfs = StorageAdapter.ofProvider("HDFS", Map.of());

        Assertions.assertTrue(hdfs.isExplicitlyConfigured());
        Assertions.assertFalse(hdfs.getSpiProperties().isSyntheticDefault());
    }

    @Test
    void explicitAndUriMatchedHdfsRemainConfigured() {
        for (Map<String, String> raw : List.of(
                Map.of("fs.hdfs.support", "true"),
                Map.of("uri", "hdfs://namenode/warehouse"),
                Map.of("URI", "viewfs://mount/warehouse"))) {
            List<StorageAdapter> result = StorageAdapter.ofAll(raw);

            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(StorageTypeId.HDFS, result.get(0).getType());
            Assertions.assertTrue(result.get(0).isExplicitlyConfigured());
        }
    }

    @Test
    void mixedAzureAndHdfsBothRemainConfigured() {
        List<StorageAdapter> result = StorageAdapter.ofAll(Map.of(
                "azure.account_name", "account", "azure.account_key", "key",
                "uri", "hdfs://namenode/warehouse"));

        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.stream().allMatch(StorageAdapter::isExplicitlyConfigured));
    }

    @Test
    void hadoopResourcesRemainInIcebergS3Configuration(@TempDir Path tmp) throws Exception {
        Path xml = tmp.resolve("storage-site.xml");
        Files.writeString(xml, "<configuration>"
                + "<property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>"
                + "<property><name>fs.s3a.proxy.host</name><value>xml-proxy</value></property>"
                + "</configuration>");
        Map<String, String> raw = Map.of(
                "iceberg.catalog.type", "hadoop",
                "warehouse", "s3a://bucket/warehouse",
                "s3.endpoint", "s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.access_key", "access-key",
                "s3.secret_key", "secret-key",
                "hadoop.config.resources", xml.toString(),
                "fs.s3a.proxy.host", "inline-proxy");
        List<StorageProperties> bindings = new ArrayList<>(manager.bindAll(raw));
        StorageProperties hdfs = bindings.stream()
                .filter(binding -> "HDFS".equals(binding.providerName())).findFirst().get();

        Assertions.assertFalse(hdfs.isSyntheticDefault());
        Assertions.assertEquals("false",
                hdfs.toHadoopProperties().get().toHadoopConfigurationMap().get("fs.s3a.connection.ssl.enabled"));
        Assertions.assertEquals(bindings, IcebergCatalogFactory.selectEffectiveStorages(bindings));

        ConnectorContext context = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "storage-origin-test";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public ConnectorStorageContext getStorageContext() {
                return new ConnectorStorageContext() {
                    @Override
                    public List<StorageProperties> getStorageProperties() {
                        return bindings;
                    }
                };
            }
        };
        try (IcebergConnector connector = new IcebergConnector(raw, context)) {
            // Exercise the actual metadata configuration consumer without opening a remote catalog.
            Method buildConfig = IcebergConnector.class.getDeclaredMethod("buildStorageHadoopConfig");
            buildConfig.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, String> storageConfig = (Map<String, String>) buildConfig.invoke(connector);
            Assertions.assertEquals("false", storageConfig.get("fs.s3a.connection.ssl.enabled"));
            Assertions.assertEquals("inline-proxy", storageConfig.get("fs.s3a.proxy.host"));
            Assertions.assertEquals("access-key", storageConfig.get("fs.s3a.access.key"));

            Configuration configuration = IcebergCatalogFactory.buildHadoopConfiguration(raw, storageConfig);
            Assertions.assertEquals("false", configuration.get("fs.s3a.connection.ssl.enabled"));
            Assertions.assertEquals("inline-proxy", configuration.get("fs.s3a.proxy.host"));
            Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", configuration.get("fs.s3a.impl"));
        }
    }
}

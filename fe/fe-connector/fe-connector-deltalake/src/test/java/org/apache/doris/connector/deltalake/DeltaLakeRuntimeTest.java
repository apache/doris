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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.metastore.spi.AbstractHmsMetaStoreProperties;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class DeltaLakeRuntimeTest {
    @Test
    void storageConfigurationUsesTypedPropertiesAndExplicitOverrides() {
        RecordingConnectorContext context = new RecordingConnectorContext() {
            @Override
            public ConnectorStorageContext getStorageContext() {
                return new ConnectorStorageContext() {
                    @Override
                    public List<StorageProperties> getStorageProperties() {
                        return Collections.singletonList(new TestStorageProperties());
                    }
                };
            }
        };
        Configuration conf = DeltaLakeRuntime.storageConfiguration(
                Map.of("fs.s3a.endpoint", "http://explicit:9000", "dfs.client.socket-timeout", "1234"), context);
        Assertions.assertEquals("http://explicit:9000", conf.get("fs.s3a.endpoint"));
        Assertions.assertEquals("true", conf.get("fs.s3a.path.style.access"));
        Assertions.assertEquals("1234", conf.get("dfs.client.socket-timeout"));
        Assertions.assertSame(DeltaLakeRuntime.class.getClassLoader(), conf.getClassLoader());
    }

    @Test
    void hmsSimpleIdentityDoesNotUseStorageUsername() throws Exception {
        DeltaLakeCatalogProperties properties = DeltaLakeCatalogProperties.of(Map.of(
                "deltalake.catalog.type", "hms", "hive.metastore.uris", "thrift://localhost:9083",
                "hive.metastore.authentication.type", "simple", "hive.metastore.username", "delta-metastore",
                "hadoop.username", "delta-storage"));
        Assertions.assertEquals("delta-metastore", DeltaLakeRuntime.hmsAuthenticator(
                (AbstractHmsMetaStoreProperties) properties.getMetastore()).getUGI().getUserName());
    }

    @Test
    void rejectsInvalidPoolSizeBeforeConnecting() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> DeltaLakeCatalogProperties.of(
                Map.of("warehouse", "/warehouse", "hive.metastore.client.pool.size", "-1")));
    }

    @Test
    void explicitStorageUserReachesPluginUgi() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("hadoop.username", "delta-storage");
        RecordingConnectorContext delegate = new RecordingConnectorContext();
        TcclPinningConnectorContext context = new TcclPinningConnectorContext(delegate,
                () -> DeltaLakeRuntime.storageAuthenticator(configuration));
        Assertions.assertEquals("delta-storage", context.executeAuthenticated(
                () -> UserGroupInformation.getCurrentUser().getUserName()));
        Assertions.assertEquals(0, delegate.authenticationCalls.get());
    }

    @Test
    void metadataInterruptionRestoresInterruptFlag() throws Exception {
        DeltaLakeCatalogProperties properties = DeltaLakeCatalogProperties.of(Map.of("warehouse", "/warehouse"));
        try (DeltaLakeRuntime runtime = new DeltaLakeRuntime(properties, new RecordingConnectorContext())) {
            Assertions.assertThrows(DorisConnectorException.class, () -> runtime.execute(() -> {
                throw new InterruptedException("query cancelled");
            }));
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    private static final class TestStorageProperties implements StorageProperties, HadoopStorageProperties {
        @Override
        public Optional<HadoopStorageProperties> toHadoopProperties() {
            return Optional.of(this);
        }

        @Override
        public Map<String, String> toHadoopConfigurationMap() {
            return Map.of("fs.s3a.endpoint", "http://normalized:9000", "fs.s3a.path.style.access", "true");
        }

        @Override
        public String providerName() {
            return "TEST";
        }

        @Override
        public StorageKind kind() {
            return StorageKind.HDFS_COMPATIBLE;
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.HDFS;
        }

        @Override
        public Map<String, String> rawProperties() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, String> matchedProperties() {
            return Collections.emptyMap();
        }
    }
}

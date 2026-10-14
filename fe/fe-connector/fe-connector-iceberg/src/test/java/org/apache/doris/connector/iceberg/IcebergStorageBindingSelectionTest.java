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

package org.apache.doris.connector.iceberg;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class IcebergStorageBindingSelectionTest {

    private final StorageProperties azure = binding("AZURE", StorageKind.OBJECT_STORAGE,
            FileSystemType.AZURE, false, Map.of());

    @Test
    void dropsSyntheticDefaultEvenWhenRawMapContainsHadoopKeys() {
        // The registry did not match this arbitrary Hadoop option as an HDFS configuration.
        // Reconstructing the binding origin from a hadoop.* whitelist would retain the fallback.
        StorageProperties fallback = hdfs(true, Map.of("hadoop.catalog.option", "value"));

        Assertions.assertEquals(List.of(azure),
                IcebergCatalogFactory.selectEffectiveStorages(List.of(fallback, azure)));
    }

    @Test
    void keepsHdfsOnlyFallback() {
        StorageProperties fallback = hdfs(true, Map.of());

        Assertions.assertEquals(List.of(fallback),
                IcebergCatalogFactory.selectEffectiveStorages(List.of(fallback)));
    }

    @Test
    void keepsHdfsSelectedByUriWithoutAnyHadoopKeyPrefix() {
        StorageProperties hdfs = hdfs(false, Map.of("uri", "hdfs://namenode/warehouse"));
        List<StorageProperties> mixed = List.of(hdfs, azure);

        Assertions.assertEquals(mixed, IcebergCatalogFactory.selectEffectiveStorages(mixed));
    }

    @Test
    void keepsExplicitHdfsInMixedCatalog() {
        StorageProperties hdfs = hdfs(false, Map.of("fs.hdfs.support", "true"));
        List<StorageProperties> mixed = List.of(hdfs, azure);

        Assertions.assertEquals(mixed, IcebergCatalogFactory.selectEffectiveStorages(mixed));
    }

    @Test
    void keepsNormalBindingEvenWithoutRawMatchingHints() {
        // A provider selected directly is not the registry's fallback, even with an empty raw map.
        StorageProperties hdfs = hdfs(false, Map.of());
        List<StorageProperties> mixed = List.of(hdfs, azure);

        Assertions.assertEquals(mixed, IcebergCatalogFactory.selectEffectiveStorages(mixed));
    }

    @Test
    void objectStorageSelectionDoesNotDependOnAzureProviderName() {
        StorageProperties objectStorage = binding("CUSTOM", StorageKind.OBJECT_STORAGE,
                FileSystemType.S3, false, Map.of());

        Assertions.assertEquals(List.of(objectStorage), IcebergCatalogFactory.selectEffectiveStorages(
                List.of(hdfs(true, Map.of()), objectStorage)));
    }

    @Test
    void keepsFallbackAndRealBindingsForMixedFileSystems() {
        StorageProperties ossHdfs = binding("OSS_HDFS", StorageKind.HDFS_COMPATIBLE,
                FileSystemType.HDFS, false, Map.of());
        List<StorageProperties> mixed = List.of(hdfs(true, Map.of()), ossHdfs, azure);

        Assertions.assertEquals(mixed, IcebergCatalogFactory.selectEffectiveStorages(mixed));
    }

    @Test
    void keepsFallbackWithoutAnObjectStorageBinding() {
        StorageProperties local = binding("LOCAL", StorageKind.LOCAL, FileSystemType.FILE, false, Map.of());
        List<StorageProperties> storages = List.of(hdfs(true, Map.of()), local);

        Assertions.assertEquals(storages, IcebergCatalogFactory.selectEffectiveStorages(storages));
    }

    @Test
    void keepsS3CompatibleProviderPrecedenceWhenDroppingDefault() {
        FakeS3CompatibleStorageProperties genericS3 = new FakeS3CompatibleStorageProperties("S3");
        FakeS3CompatibleStorageProperties oss = new FakeS3CompatibleStorageProperties("OSS");

        Assertions.assertEquals(List.of(oss), IcebergCatalogFactory.selectEffectiveStorages(
                List.of(hdfs(true, Map.of()), genericS3, oss)));
    }

    private static StorageProperties hdfs(boolean syntheticDefault, Map<String, String> raw) {
        return binding("HDFS", StorageKind.HDFS_COMPATIBLE, FileSystemType.HDFS, syntheticDefault, raw);
    }

    private static StorageProperties binding(String provider, StorageKind kind, FileSystemType type,
            boolean syntheticDefault, Map<String, String> raw) {
        return new StorageProperties() {
            @Override
            public String providerName() {
                return provider;
            }

            @Override
            public StorageKind kind() {
                return kind;
            }

            @Override
            public FileSystemType type() {
                return type;
            }

            @Override
            public boolean isSyntheticDefault() {
                return syntheticDefault;
            }

            @Override
            public Map<String, String> rawProperties() {
                return raw;
            }

            @Override
            public Map<String, String> matchedProperties() {
                return Map.of();
            }
        };
    }
}

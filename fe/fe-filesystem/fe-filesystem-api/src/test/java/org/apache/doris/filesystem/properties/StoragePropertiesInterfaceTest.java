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

package org.apache.doris.filesystem.properties;

import org.apache.doris.filesystem.FileSystemType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

class StoragePropertiesInterfaceTest {

    @Test
    void fileSystemPropertiesIsStorageProperties() {
        FileSystemProperties properties = new TestProperties();

        Assertions.assertTrue(properties instanceof StorageProperties);
    }

    @Test
    void normalBindingsAreNotSyntheticByDefault() {
        StorageProperties properties = new TestProperties();

        Assertions.assertFalse(properties.isSyntheticDefault());
    }

    @Test
    void syntheticOriginIsVisibleThroughStorageProperties() {
        StorageProperties properties = new TestProperties() {
            @Override
            public boolean isSyntheticDefault() {
                return true;
            }
        };

        Assertions.assertTrue(properties.isSyntheticDefault());
    }

    @Test
    void accessValidationDefaultsToNoOp() {
        StorageProperties properties = new TestProperties();

        Assertions.assertDoesNotThrow(properties::validateForAccess);
    }

    @Test
    void fileIOPropertiesDefaultDoesNotChangeOtherProvidersSelectionOrCredentials() {
        StorageProperties properties = new TestProperties();

        Map<String, String> output = properties.toIcebergFileIOProperties();

        Assertions.assertEquals(Collections.emptyMap(), output);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> output.put("io-impl", "other"));
    }

    @Test
    void fileIOConnectionPropertiesDefaultIsEmptyAndImmutable() {
        StorageProperties properties = new TestProperties();

        Map<String, String> output = properties.toIcebergFileIOConnectionProperties();

        Assertions.assertEquals(Collections.emptyMap(), output);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> output.put("adls.connection-string.account", "https://example.org"));
    }

    @Test
    void defaultIcebergHadoopViewPreservesOtherProvidersConfigurationAndIdentity() {
        HadoopStorageProperties hadoop = () -> Map.of("fs.defaultFS", "hdfs://namenode:8020");
        StorageProperties properties = new TestProperties() {
            @Override
            public Optional<HadoopStorageProperties> toHadoopProperties() {
                return Optional.of(hadoop);
            }
        };

        Assertions.assertSame(hadoop, properties.toIcebergHadoopProperties().orElseThrow());
        Assertions.assertEquals(Map.of("fs.defaultFS", "hdfs://namenode:8020"),
                properties.toIcebergHadoopProperties().orElseThrow().toHadoopConfigurationMap());
        Assertions.assertTrue(new TestProperties().toIcebergHadoopProperties().isEmpty());
    }

    @Test
    void bindingValidationDoesNotPerformAccessValidation() {
        StorageProperties properties = new TestProperties() {
            @Override
            public void validateForAccess() {
                throw new IllegalArgumentException("Test credential is expired");
            }
        };

        Assertions.assertDoesNotThrow(properties::validate);
        Assertions.assertThrows(IllegalArgumentException.class, properties::validateForAccess);
    }

    private static class TestProperties implements FileSystemProperties {
        @Override
        public String providerName() {
            return "test";
        }

        @Override
        public StorageKind kind() {
            return StorageKind.LOCAL;
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.FILE;
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

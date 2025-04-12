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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * Enum representing supported storage provider types and their associated configuration logic.
 *
 * <p>This enum acts as a central registry for all built-in storage providers (e.g., S3, Azure, HDFS, etc.)
 * and is responsible for creating corresponding {@link StorageProperties} instances based on a
 * given configuration map.
 *
 * <p>Each enum constant overrides the {@link #create(Map)} method to instantiate its specific
 * implementation of {@code StorageProperties}.
 *
 * <p>While this enum currently serves as a simple type-safe factory for internal use, it is also
 * designed to facilitate a future transition to a Service Provider Interface (SPI) model. At the
 * current stage, we deliberately avoid using {@code ServiceLoader} or external factory classes
 * to reduce complexity and maintain tighter control over supported providers.
 *
 * <p>To map a string key (e.g., from user config) to a provider type, use the {@link #fromKey(String)}
 * method which performs a case-insensitive match.
 *
 * @see StorageProperties
 * @see AzureProperties
 * @see HDFSProperties
 * @see OSSProperties
 * @see OBSProperties
 * @see COSProperties
 * @see S3Properties
 */
public enum StorageProviderType {
    AZURE("azure") {
        @Override
        public StorageProperties create(Map<String, String> props) {
            return new AzureProperties(props);
        }
    },
    HDFS("hdfs") {
        @Override
        public StorageProperties create(Map<String, String> props) {
            return new HDFSProperties(props);
        }
    },
    OSS("oss") {
        @Override
        public StorageProperties create(Map<String, String> props) {
            return new OSSProperties(props);
        }
    },
    OBS("obs") {
        @Override
        public StorageProperties create(Map<String, String> props) {
            return new OBSProperties(props);
        }
    },
    COS("cos") {
        @Override
        public StorageProperties create(Map<String, String> props) {
            return new COSProperties(props);
        }
    },
    S3("s3") {
        @Override
        public StorageProperties create(Map<String, String> props) {
            return new S3Properties(props);
        }
    },

    ;

    private final String key;

    StorageProviderType(String key) {
        this.key = key;
    }

    public abstract StorageProperties create(Map<String, String> props);

    public static Optional<StorageProviderType> fromKey(String key) {
        return Arrays.stream(values())
                .filter(e -> e.key.equalsIgnoreCase(key))
                .findFirst();
    }
}

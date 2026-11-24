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

package org.apache.doris.connector.lance;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.Collections;
import java.util.Map;

/**
 * A storage binding that is not S3-compatible, such as the HDFS one the engine lists by default or the one
 * it binds an OSS-HDFS configuration to; only its provider name is ever read.
 */
final class FakeStorageBinding implements StorageProperties {
    private final String providerName;

    FakeStorageBinding(String providerName) {
        this.providerName = providerName;
    }

    @Override
    public String providerName() {
        return providerName;
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

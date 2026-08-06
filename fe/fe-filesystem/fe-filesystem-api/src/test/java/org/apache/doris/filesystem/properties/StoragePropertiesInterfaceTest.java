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

class StoragePropertiesInterfaceTest {

    @Test
    void fileSystemPropertiesIsStorageProperties() {
        FileSystemProperties properties = new TestProperties();

        Assertions.assertTrue(properties instanceof StorageProperties);
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

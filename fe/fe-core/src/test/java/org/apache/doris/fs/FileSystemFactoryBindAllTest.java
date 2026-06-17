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

package org.apache.doris.fs;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileSystemFactoryBindAllTest {

    @AfterEach
    public void resetFactoryState() {
        // bindAllStorageProperties / initPluginManager mutate static state; restore the default.
        FileSystemFactory.clearProviderCache();
    }

    @Test
    public void bindAllStorageProperties_delegatesToLivePluginManager() {
        // Production path: a plugin-loaded manager is set at FE startup; bindAllStorageProperties must
        // delegate to its bindAll (the only place the runtime object-store directory plugins live).
        FileSystemProperties bound = new FakeFsProps();
        FileSystemPluginManager mgr = new FileSystemPluginManager();
        mgr.registerProvider(supportingProvider(bound));
        FileSystemFactory.initPluginManager(mgr);

        List<StorageProperties> result = FileSystemFactory.bindAllStorageProperties(new HashMap<>());

        Assertions.assertEquals(1, result.size());
        Assertions.assertSame(bound, result.get(0));
    }

    @Test
    public void bindAllStorageProperties_fallsBackToServiceLoaderWhenNoManager() {
        // Migration / unit-test path: no live manager -> ServiceLoader fallback (mirrors getFileSystem).
        // No object-store binding provider is on fe-core's unit-test classpath, so the result is empty,
        // but it must never be null or throw.
        FileSystemFactory.clearProviderCache();
        List<StorageProperties> result = FileSystemFactory.bindAllStorageProperties(new HashMap<>());
        Assertions.assertNotNull(result);
    }

    private static FileSystemProvider<FileSystemProperties> supportingProvider(FileSystemProperties bound) {
        return new FileSystemProvider<FileSystemProperties>() {
            @Override
            public boolean supports(Map<String, String> properties) {
                return true;
            }

            @Override
            public FileSystemProperties bind(Map<String, String> properties) {
                return bound;
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                return null;
            }

            @Override
            public String name() {
                return "fake";
            }
        };
    }

    private static final class FakeFsProps implements FileSystemProperties {
        @Override
        public String providerName() {
            return "FAKE";
        }

        @Override
        public StorageKind kind() {
            return null;
        }

        @Override
        public FileSystemType type() {
            return null;
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

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

import org.apache.doris.common.Config;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FileSystemFactoryBindAllTest {

    @AfterEach
    public void resetFactoryState() {
        // bindAllStorageProperties / initPluginManager mutate static state; restore the default.
        FileSystemFactory.initPluginManager(null);
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

    @ParameterizedTest
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    public void typedCreationPreservesProviderBoundAliasesAndRuntimeSnapshot(boolean cached, boolean skipList)
            throws IOException {
        S3CompatibleFileSystemProperties original = Mockito.mock(S3CompatibleFileSystemProperties.class);
        S3CompatibleFileSystemProperties rebound = Mockito.mock(S3CompatibleFileSystemProperties.class);
        Mockito.when(original.providerName()).thenReturn("CUSTOM_S3");
        Map<String, String> raw = Map.of("s3.endpoint", "https://objects.example.com",
                "S3.REGION", "us-east-1");
        // The initial case-insensitive binding consumes the declared alias, while rawProperties
        // retains the user's spelling in a plain map, as the real S3 property model does.
        Mockito.when(original.rawProperties()).thenReturn(raw);
        Mockito.when(original.matchedProperties()).thenReturn(Map.of(
                "s3.region", "us-east-1", "s3_skip_list_for_deterministic_path", "true",
                "s3_head_request_max_paths", "100"));
        FileSystem expected = Mockito.mock(FileSystem.class);
        Map<String, String> snapshot = FileSystemFactory.withRuntimeFileSystemProperties(
                raw, skipList, 7);
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(new FileSystemProvider<S3CompatibleFileSystemProperties>() {
            @Override
            public String name() {
                return "CUSTOM_S3";
            }

            @Override
            public boolean supports(Map<String, String> properties) {
                Assertions.fail("An already-bound provider must not be reselected");
                return false;
            }

            @Override
            public S3CompatibleFileSystemProperties bind(Map<String, String> properties) {
                Assertions.assertEquals("us-east-1", properties.get("s3.region"));
                Assertions.assertEquals("us-east-1", properties.get("S3.REGION"));
                Assertions.assertEquals(String.valueOf(skipList), properties.get("s3_skip_list_for_deterministic_path"));
                Assertions.assertEquals("7", properties.get("s3_head_request_max_paths"));
                return rebound;
            }

            @Override
            public FileSystem create(S3CompatibleFileSystemProperties properties) {
                Assertions.assertSame(rebound, properties);
                return expected;
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                throw new AssertionError("Typed creation must preserve the original provider");
            }
        });
        FileSystemFactory.initPluginManager(manager);
        boolean oldSkipList = Config.s3_skip_list_for_deterministic_path;
        int oldMaxPaths = Config.s3_head_request_max_paths;
        try {
            Config.s3_skip_list_for_deterministic_path = cached ? !skipList : skipList;
            Config.s3_head_request_max_paths = cached ? 100 : 7;
            Assertions.assertSame(expected,
                    cached ? FileSystemFactory.getFileSystemWithEffectiveProperties(original, snapshot)
                            : FileSystemFactory.getFileSystem(original));
            Assertions.assertFalse(snapshot.containsKey("s3.region"), "Rebinding must not mutate the cache snapshot");
        } finally {
            Config.s3_skip_list_for_deterministic_path = oldSkipList;
            Config.s3_head_request_max_paths = oldMaxPaths;
        }
    }

    @Test
    public void runtimeOverlayPreservesCaseInsensitiveLookup() {
        Map<String, String> raw = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        raw.put("S3.REGION", "us-east-1");
        raw.put("S3_SKIP_LIST_FOR_DETERMINISTIC_PATH", "true");
        raw.put("S3_HEAD_REQUEST_MAX_PATHS", "100");

        Map<String, String> effective = FileSystemFactory.withRuntimeFileSystemProperties(raw, false, 7);

        Assertions.assertEquals("us-east-1", effective.get("s3.region"));
        Assertions.assertEquals("false", effective.get("S3_SKIP_LIST_FOR_DETERMINISTIC_PATH"));
        Assertions.assertEquals("7", effective.get("S3_HEAD_REQUEST_MAX_PATHS"));
        Assertions.assertEquals("true", raw.get("s3_skip_list_for_deterministic_path"));
        Assertions.assertEquals("100", raw.get("s3_head_request_max_paths"));
    }

    private static FileSystemProvider<FileSystemProperties> supportingProvider(FileSystemProperties bound) {
        return new FileSystemProvider<FileSystemProperties>() {
            @Override
            public boolean supports(Map<String, String> properties) {
                return true;
            }

            @Override
            public boolean supportsGuess(Map<String, String> properties) {
                // Out-of-tree providers are selected by supportsExplicit/supportsGuess since upstream
                // #66004 tightened FileSystemPluginManager.bindAll; supports() alone only feeds
                // createFileSystem(Map) and is warned about, so it would never reach bind() here.
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

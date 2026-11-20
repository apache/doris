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
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.S3CompatibleFileSystem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void creationKeepsBindingAndReadsRuntimePolicyWithoutRecreatingStorage(boolean typed, boolean liveManager)
            throws Exception {
        S3CompatibleFileSystemProperties bound = Mockito.mock(S3CompatibleFileSystemProperties.class);
        Mockito.when(bound.providerName()).thenReturn("CUSTOM_S3");
        ObjStorage<?> storage = Mockito.mock(ObjStorage.class);
        RemoteObject object = new RemoteObject("file.csv", "file.csv", null, 7L, 123L);
        Mockito.when(storage.headObject("s3://bucket/file.csv")).thenReturn(object);
        Mockito.when(storage.listObjectsWithOptions(Mockito.anyString(), Mockito.any()))
                .thenReturn(new RemoteObjects(List.of(object), false, null));
        S3CompatibleFileSystem expected = new S3CompatibleFileSystem(storage, false, Set.of("s3")) {};
        AtomicInteger creates = new AtomicInteger();
        FileSystemProvider<S3CompatibleFileSystemProperties> provider =
                new FileSystemProvider<S3CompatibleFileSystemProperties>() {
                    @Override
                    public String name() {
                        return "CUSTOM_S3";
                    }

                    @Override
                    public boolean supports(Map<String, String> properties) {
                        Assertions.assertFalse(typed, "Typed bindings must not trigger provider reselection");
                        return true;
                    }

                    @Override
                    public S3CompatibleFileSystemProperties bind(Map<String, String> properties) {
                        throw new AssertionError("The original typed binding must not be reparsed");
                    }

                    @Override
                    public FileSystem create(S3CompatibleFileSystemProperties properties) {
                        Assertions.assertSame(bound, properties);
                        creates.incrementAndGet();
                        return expected;
                    }

                    @Override
                    public FileSystem create(Map<String, String> properties) {
                        Assertions.assertFalse(properties.containsKey("s3_head_request_max_paths"));
                        creates.incrementAndGet();
                        return expected;
                    }
                };
        if (liveManager) {
            FileSystemPluginManager manager = new FileSystemPluginManager();
            manager.registerProvider(provider);
            FileSystemFactory.initPluginManager(manager);
        } else {
            Field providers = FileSystemFactory.class.getDeclaredField("cachedProviders");
            providers.setAccessible(true);
            providers.set(null, List.of(provider));
        }
        boolean oldSkipList = Config.s3_skip_list_for_deterministic_path;
        int oldMaxPaths = Config.s3_head_request_max_paths;
        try (FileSystem fs = typed ? FileSystemFactory.getFileSystem(bound)
                : FileSystemFactory.getFileSystem(Map.of())) {
            Assertions.assertSame(expected, fs);
            Config.s3_skip_list_for_deterministic_path = true;
            for (int boundValue : new int[] {100, 0, -1, 200}) {
                Config.s3_head_request_max_paths = boundValue;
                Assertions.assertEquals(123L, fs.globListWithLimit(
                        Location.of("s3://bucket/file.csv"), null, 0, 0).getFiles().get(0).modificationTime());
            }
            Config.s3_skip_list_for_deterministic_path = false;
            fs.globListWithLimit(Location.of("s3://bucket/file.csv"), null, 0, 0);
            Assertions.assertEquals(1, creates.get());
            Mockito.verify(storage, Mockito.times(2)).headObject("s3://bucket/file.csv");
            Mockito.verify(storage, Mockito.times(3)).listObjectsWithOptions(Mockito.anyString(), Mockito.any());
            Mockito.verify(storage, Mockito.never()).close();
        } finally {
            Config.s3_skip_list_for_deterministic_path = oldSkipList;
            Config.s3_head_request_max_paths = oldMaxPaths;
        }
        Mockito.verify(storage).close();
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

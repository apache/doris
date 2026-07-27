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
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemContext;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class FileSystemPluginManagerTest {

    @Test
    public void registerProvider_registersProviderSensitiveKeysForMasking() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(new FileSystemProvider<FileSystemProperties>() {
            @Override
            public boolean supports(Map<String, String> properties) {
                return false;
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                return null;
            }

            @Override
            public Set<String> sensitivePropertyKeys() {
                return Collections.singleton("PLUGIN_MANAGER_TEST_SECRET_ALIAS");
            }
        });

        Assertions.assertTrue(
                DatasourcePrintableMap.SENSITIVE_KEY.contains("PLUGIN_MANAGER_TEST_SECRET_ALIAS"));
    }

    @Test
    public void createFileSystem_passesConfiguredHttpSchemeInClientContext() throws Exception {
        String originalScheme = Config.s3_client_http_scheme;
        AtomicReference<FileSystemContext> createdContext = new AtomicReference<>();
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(new FileSystemProvider<FileSystemProperties>() {
            @Override
            public boolean supports(Map<String, String> properties) {
                return true;
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                return null;
            }

            @Override
            public FileSystem create(Map<String, String> properties, FileSystemContext context) {
                createdContext.set(context);
                return null;
            }
        });

        Map<String, String> properties = Collections.emptyMap();
        try {
            Config.s3_client_http_scheme = "http";
            manager.createFileSystem(properties);
            Assertions.assertEquals("http", createdContext.get().getS3ClientHttpScheme());
            Assertions.assertFalse(properties.containsKey("s3_client_http_scheme"));
        } finally {
            Config.s3_client_http_scheme = originalScheme;
        }
    }
}

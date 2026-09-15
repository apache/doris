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

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Builds the registry of an FE that started with some of its shipped filesystem plugins absent - the
 * state a plugin directory that failed to load leaves behind - from the providers on the test classpath.
 * Install with {@link StorageAdapter#initPluginManager} and restore with {@code initPluginManager(null)}
 * in a {@code finally}: the facade's registry is process-wide.
 */
public final class TestFileSystemPluginManagers {

    private TestFileSystemPluginManagers() {
    }

    /** Every provider on the test classpath except the named ones (case-insensitive). */
    public static FileSystemPluginManager withoutProviders(String... absentNames) {
        List<String> absent = Arrays.stream(absentNames)
                .map(name -> name.toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
        FileSystemPluginManager manager = new FileSystemPluginManager();
        for (FileSystemProvider<?> provider : ServiceLoader.load(FileSystemProvider.class)) {
            if (!absent.contains(provider.name().toUpperCase(Locale.ROOT))) {
                manager.registerProvider(provider);
            }
        }
        return manager;
    }
}

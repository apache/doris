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

package org.apache.doris.extension.loader;

import org.apache.doris.extension.spi.PluginFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Runtime handle for one loaded plugin factory.
 */
public final class PluginHandle<F extends PluginFactory> {

    private final String pluginName;
    private final Path pluginDir;
    private final List<Path> resolvedJars;
    private final ClassLoader classLoader;
    private final F factory;
    private final Instant loadedAt;

    public PluginHandle(String pluginName, Path pluginDir, List<Path> resolvedJars,
            ClassLoader classLoader, F factory, Instant loadedAt) {
        this.pluginName = requireNonBlank(pluginName, "pluginName");
        this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir");
        this.resolvedJars = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(resolvedJars, "resolvedJars")));
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.loadedAt = Objects.requireNonNull(loadedAt, "loadedAt");
    }

    public String getPluginName() {
        return pluginName;
    }

    public Path getPluginDir() {
        return pluginDir;
    }

    public List<Path> getResolvedJars() {
        return resolvedJars;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public F getFactory() {
        return factory;
    }

    public Instant getLoadedAt() {
        return loadedAt;
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " is blank");
        }
        return trimmed;
    }
}

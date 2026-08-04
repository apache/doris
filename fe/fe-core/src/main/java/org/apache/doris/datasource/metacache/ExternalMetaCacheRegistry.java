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

package org.apache.doris.datasource.metacache;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Registry for engine cache instances and alias resolution.
 */
public class ExternalMetaCacheRegistry {
    private static final Logger LOG = LogManager.getLogger(ExternalMetaCacheRegistry.class);
    private static final String ENGINE_DEFAULT = "default";

    private final Map<String, ExternalMetaCache> engineCaches = Maps.newConcurrentMap();
    private final Map<String, String> engineAliasIndex = Maps.newConcurrentMap();

    public ExternalMetaCache resolve(String engine) {
        Objects.requireNonNull(engine, "engine is null");
        String normalizedEngine = normalizeEngineName(engine);
        String primaryEngine = engineAliasIndex.getOrDefault(normalizedEngine, normalizedEngine);
        ExternalMetaCache found = engineCaches.get(primaryEngine);
        if (found != null) {
            return found;
        }
        throw new IllegalArgumentException(
                String.format("unsupported external meta cache engine '%s'", normalizedEngine));
    }

    public Collection<ExternalMetaCache> allCaches() {
        return engineCaches.values();
    }

    public void register(ExternalMetaCache cache) {
        String engineName = normalizeEngineName(cache.engine());
        ExternalMetaCache existing = engineCaches.putIfAbsent(engineName, cache);
        if (existing != null) {
            LOG.warn("skip duplicated external meta cache engine '{}', existing class: {}, new class: {}",
                    engineName, existing.getClass().getName(), cache.getClass().getName());
            return;
        }
        registerAliases(cache, engineName);
        LOG.debug("registered external meta cache engine '{}'", engineName);
    }

    public void resetForTest(Collection<? extends ExternalMetaCache> caches) {
        engineCaches.clear();
        engineAliasIndex.clear();
        caches.forEach(this::register);
    }

    static String normalizeEngineName(String engine) {
        if (engine == null) {
            return ENGINE_DEFAULT;
        }
        String normalized = engine.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return ENGINE_DEFAULT;
        }
        return normalized;
    }

    private void registerAliases(ExternalMetaCache cache, String primaryEngineName) {
        registerAlias(primaryEngineName, primaryEngineName);
        for (String alias : cache.aliases()) {
            registerAlias(alias, primaryEngineName);
        }
    }

    private void registerAlias(String alias, String primaryEngineName) {
        String normalizedAlias = normalizeEngineName(alias);
        String existing = engineAliasIndex.putIfAbsent(normalizedAlias, primaryEngineName);
        if (existing != null && !existing.equals(primaryEngineName)) {
            LOG.warn("skip duplicated external meta cache alias '{}', existing engine: {}, new engine: {}",
                    normalizedAlias, existing, primaryEngineName);
        }
    }
}

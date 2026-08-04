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

package org.apache.doris.connector.metacache;

import org.apache.doris.connector.metacache.spi.MetaCacheLifecycle;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data-source-neutral registry for metadata cache engines and their aliases.
 */
public class MetaCacheRegistry<T extends MetaCacheLifecycle> {
    private static final String ENGINE_DEFAULT = "default";

    private final Map<String, T> engineCaches = new ConcurrentHashMap<>();
    private final Map<String, String> engineAliasIndex = new ConcurrentHashMap<>();

    public final T resolve(String engine) {
        Objects.requireNonNull(engine, "engine is null");
        String normalizedEngine = normalizeEngineName(engine);
        String primaryEngine = engineAliasIndex.getOrDefault(normalizedEngine, normalizedEngine);
        T found = engineCaches.get(primaryEngine);
        if (found != null) {
            return found;
        }
        throw new IllegalArgumentException(
                String.format("unsupported external meta cache engine '%s'", normalizedEngine));
    }

    public final Collection<T> allCaches() {
        return engineCaches.values();
    }

    public final void register(T cache) {
        Objects.requireNonNull(cache, "cache is null");
        String engineName = normalizeEngineName(cache.engine());
        T existing = engineCaches.putIfAbsent(engineName, cache);
        if (existing != null) {
            onDuplicatedEngine(engineName, existing, cache);
            return;
        }
        registerAlias(engineName, engineName);
        for (String alias : cache.aliases()) {
            registerAlias(alias, engineName);
        }
        onRegistered(engineName, cache);
    }

    public final void resetForTest(Collection<? extends T> caches) {
        engineCaches.clear();
        engineAliasIndex.clear();
        caches.forEach(this::register);
    }

    protected void onRegistered(String engineName, T cache) {
    }

    protected void onDuplicatedEngine(String engineName, T existing, T duplicate) {
    }

    protected void onDuplicatedAlias(String alias, String existingEngine, String duplicateEngine) {
    }

    static String normalizeEngineName(String engine) {
        if (engine == null) {
            return ENGINE_DEFAULT;
        }
        String normalized = engine.trim().toLowerCase(Locale.ROOT);
        return normalized.isEmpty() ? ENGINE_DEFAULT : normalized;
    }

    private void registerAlias(String alias, String primaryEngineName) {
        String normalizedAlias = normalizeEngineName(alias);
        String existing = engineAliasIndex.putIfAbsent(normalizedAlias, primaryEngineName);
        if (existing != null && !existing.equals(primaryEngineName)) {
            onDuplicatedAlias(normalizedAlias, existing, primaryEngineName);
        }
    }
}

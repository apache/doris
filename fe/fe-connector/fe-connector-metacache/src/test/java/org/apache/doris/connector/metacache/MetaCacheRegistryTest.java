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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MetaCacheRegistryTest {
    @Test
    public void resolvesNormalizedEngineAndAliases() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheRegistry<TestMetaCache> registry = new MetaCacheRegistry<>();
            TestMetaCache cache = new TestMetaCache("Iceberg", refreshExecutor, "iceberg-rest", "iceberg-hms");
            registry.register(cache);

            Assertions.assertSame(cache, registry.resolve(" ICEBERG "));
            Assertions.assertSame(cache, registry.resolve("iceberg-rest"));
            Assertions.assertSame(cache, registry.resolve("ICEBERG-HMS"));
            Assertions.assertEquals(1, registry.allCaches().size());
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    @Test
    public void firstEngineAndAliasRegistrationWins() {
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        try {
            MetaCacheRegistry<TestMetaCache> registry = new MetaCacheRegistry<>();
            TestMetaCache first = new TestMetaCache("first", refreshExecutor, "shared");
            TestMetaCache duplicatedEngine = new TestMetaCache("FIRST", refreshExecutor, "other");
            TestMetaCache conflictingAlias = new TestMetaCache("second", refreshExecutor, "shared");

            registry.register(first);
            registry.register(duplicatedEngine);
            registry.register(conflictingAlias);

            Assertions.assertSame(first, registry.resolve("first"));
            Assertions.assertSame(first, registry.resolve("shared"));
            Assertions.assertSame(conflictingAlias, registry.resolve("second"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> registry.resolve("other"));
        } finally {
            refreshExecutor.shutdownNow();
        }
    }

    private static final class TestMetaCache extends AbstractMetaCache {
        private final Collection<String> aliases;

        private TestMetaCache(String engine, ExecutorService refreshExecutor, String... aliases) {
            super(engine, refreshExecutor, 60L, 16);
            this.aliases = Arrays.asList(aliases);
        }

        @Override
        public Collection<String> aliases() {
            return aliases;
        }
    }
}

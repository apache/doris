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

import org.apache.doris.connector.metacache.MetaCacheEntry;
import org.apache.doris.connector.metacache.spi.MetaCacheLifecycle;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;

import java.util.Optional;

/**
 * Engine-level abstraction for external metadata cache.
 * It defines a unified access path (engine -> catalog -> entry), scoped
 * invalidation APIs, and a common stats output shape.
 */
public interface ExternalMetaCache extends MetaCacheLifecycle {
    /**
     * Get one cache entry under an engine and catalog.
     *
     * <p>This is a low-level extension API. Prefer typed engine operations when
     * available.
     */
    <K, V> MetaCacheEntry<K, V> entry(long catalogId, String entryName, Class<K> keyType, Class<V> valueType);

    /**
     * Typed schema cache access that hides entry-name and class plumbing from callers.
     */
    @SuppressWarnings("unchecked")
    default <K extends SchemaCacheKey> Optional<SchemaCacheValue> getSchemaValue(long catalogId, K key) {
        return Optional.ofNullable(entry(catalogId, "schema", (Class<K>) key.getClass(), SchemaCacheValue.class)
                .get(key));
    }

}

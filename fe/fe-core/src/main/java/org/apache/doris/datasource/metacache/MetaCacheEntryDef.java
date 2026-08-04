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

import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Immutable definition of a logical {@link MetaCacheEntry}.
 *
 * <p>This class only describes "what an entry is", not "entry runtime data".
 * Runtime instances are still created by {@link AbstractExternalMetaCache}
 * per catalog during {@code initCatalog(long)}.
 *
 * <p>A definition contains:
 * <ul>
 *   <li>a stable logical name ({@link #name})</li>
 *   <li>declared key/value Java types ({@link #keyType}/{@link #valueType})</li>
 *   <li>a required miss loader ({@link #loader})</li>
 *   <li>default cache spec ({@link #defaultCacheSpec}) used when catalog params are absent</li>
 *   <li>whether refresh-after-write is enabled ({@link #autoRefresh})</li>
 * </ul>
 *
 * <p>Use case 1: load-on-miss entry (recommended for table/object metadata).
 * <pre>{@code
 * private final MetaCacheEntryDef<NameMapping, IcebergTableCacheValue> tableEntryDef =
 *         MetaCacheEntryDef.of(
 *                 "table",
 *                 NameMapping.class,
 *                 IcebergTableCacheValue.class,
 *                 this::loadTableCacheValue,
 *                 defaultEntryCacheSpec());
 *
 * registerMetaCacheEntryDef(tableEntryDef);
 * }</pre>
 */
public final class MetaCacheEntryDef<K, V> {
    /**
     * Logical entry name inside one engine.
     *
     * <p>It is used as:
     * <ul>
     *   <li>the lookup key in catalog entry groups</li>
     *   <li>the identity in stats output and error messages</li>
     * </ul>
     *
     * <p>Constraint: must be unique inside one concrete external meta cache
     * implementation (for example inside one IcebergExternalMetaCache).
     */
    private final String name;

    /**
     * Declared key type of this entry.
     *
     * <p>Used by {@link AbstractExternalMetaCache} to validate that callers use
     * the expected key class when obtaining the entry via
     * {@code entry(catalogId, def)}.
     */
    private final Class<K> keyType;

    /**
     * Declared value type of this entry.
     *
     * <p>Used by {@link AbstractExternalMetaCache} to validate value type
     * compatibility for the requested entry and to prevent cross-entry misuse.
     */
    private final Class<V> valueType;

    /**
     * Loader function used by {@link MetaCacheEntry#get(Object)}.
     *
     * <p>Cache miss triggers loader invocation. Loader is bound once at
     * definition creation time and reused by all per-catalog runtime entries.
     */
    @Nullable
    private final Function<K, V> loader;

    /**
     * Default cache spec of this entry definition.
     *
     * <p>This value is used as fallback when no catalog-level override is provided.
     * Keeping it on the definition makes each entry's default policy explicit.
     */
    private final CacheSpec defaultCacheSpec;
    private final boolean autoRefresh;
    private final boolean contextualOnly;
    private final MetaCacheEntryInvalidation<K> invalidation;

    private MetaCacheEntryDef(String name, Class<K> keyType, Class<V> valueType,
            @Nullable Function<K, V> loader, CacheSpec defaultCacheSpec, boolean autoRefresh, boolean contextualOnly,
            MetaCacheEntryInvalidation<K> invalidation) {
        this.name = Objects.requireNonNull(name, "entry name is required");
        this.keyType = Objects.requireNonNull(keyType, "entry key type is required");
        this.valueType = Objects.requireNonNull(valueType, "entry value type is required");
        if (contextualOnly) {
            if (loader != null) {
                throw new IllegalArgumentException("contextual-only entry loader must be null");
            }
            if (autoRefresh) {
                throw new IllegalArgumentException("contextual-only entry can not enable auto refresh");
            }
        } else {
            Objects.requireNonNull(loader, "entry loader is required");
        }
        this.loader = loader;
        this.defaultCacheSpec = Objects.requireNonNull(defaultCacheSpec, "entry default cache spec is required");
        this.autoRefresh = autoRefresh;
        this.contextualOnly = contextualOnly;
        this.invalidation = Objects.requireNonNull(invalidation, "entry invalidation is required");
    }

    /**
     * Create an entry definition with miss loader and an explicit default cache spec.
     *
     * @param name logical entry name, unique in one cache implementation
     * @param keyType declared key class
     * @param valueType declared value class
     * @param loader miss loader invoked by {@link MetaCacheEntry#get(Object)}
     * @param defaultCacheSpec default cache policy used by this entry definition
     */
    public static <K, V> MetaCacheEntryDef<K, V> of(String name, Class<K> keyType, Class<V> valueType,
            Function<K, V> loader, CacheSpec defaultCacheSpec) {
        return of(name, keyType, valueType, loader, defaultCacheSpec, MetaCacheEntryInvalidation.none());
    }

    public static <K, V> MetaCacheEntryDef<K, V> of(String name, Class<K> keyType, Class<V> valueType,
            Function<K, V> loader, CacheSpec defaultCacheSpec, MetaCacheEntryInvalidation<K> invalidation) {
        return new MetaCacheEntryDef<>(name, keyType, valueType, loader, defaultCacheSpec, true, false,
                invalidation);
    }

    /**
     * Create an entry definition with miss loader, explicit default cache spec and refresh policy.
     *
     * @param name logical entry name, unique in one cache implementation
     * @param keyType declared key class
     * @param valueType declared value class
     * @param loader miss loader invoked by {@link MetaCacheEntry#get(Object)}
     * @param defaultCacheSpec default cache policy used by this entry definition
     * @param autoRefresh whether to enable refresh-after-write
     */
    public static <K, V> MetaCacheEntryDef<K, V> of(String name, Class<K> keyType, Class<V> valueType,
            Function<K, V> loader, CacheSpec defaultCacheSpec, boolean autoRefresh) {
        return of(name, keyType, valueType, loader, defaultCacheSpec, autoRefresh, MetaCacheEntryInvalidation.none());
    }

    public static <K, V> MetaCacheEntryDef<K, V> of(String name, Class<K> keyType, Class<V> valueType,
            Function<K, V> loader, CacheSpec defaultCacheSpec, boolean autoRefresh,
            MetaCacheEntryInvalidation<K> invalidation) {
        return new MetaCacheEntryDef<>(name, keyType, valueType, loader, defaultCacheSpec, autoRefresh, false,
                invalidation);
    }

    /**
     * Create an entry definition that requires callers to provide a contextual miss loader.
     */
    public static <K, V> MetaCacheEntryDef<K, V> contextualOnly(
            String name, Class<K> keyType, Class<V> valueType, CacheSpec defaultCacheSpec) {
        return contextualOnly(name, keyType, valueType, defaultCacheSpec, MetaCacheEntryInvalidation.none());
    }

    public static <K, V> MetaCacheEntryDef<K, V> contextualOnly(
            String name, Class<K> keyType, Class<V> valueType, CacheSpec defaultCacheSpec,
            MetaCacheEntryInvalidation<K> invalidation) {
        return new MetaCacheEntryDef<>(name, keyType, valueType, null, defaultCacheSpec, false, true,
                invalidation);
    }

    /**
     * @return logical entry name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return declared key class.
     */
    public Class<K> getKeyType() {
        return keyType;
    }

    /**
     * @return declared value class.
     */
    public Class<V> getValueType() {
        return valueType;
    }

    /**
     * @return loader function.
     */
    @Nullable
    public Function<K, V> getLoader() {
        return loader;
    }

    /**
     * @return default cache spec of this entry definition.
     */
    public CacheSpec getDefaultCacheSpec() {
        return defaultCacheSpec;
    }

    /**
     * @return true when refresh-after-write is enabled.
     */
    public boolean isAutoRefresh() {
        return autoRefresh;
    }

    public boolean isContextualOnly() {
        return contextualOnly;
    }

    public MetaCacheEntryInvalidation<K> getInvalidation() {
        return invalidation;
    }
}

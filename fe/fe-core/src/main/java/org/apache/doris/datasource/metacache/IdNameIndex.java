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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Runtime-only identity index. Identities survive object TTL/eviction, but share their semantic resource scope.
 * Physical cleanup removes the exact identity record, so delayed cleanup cannot erase a same-name recreation.
 */
public final class IdNameIndex {
    private final String scope;
    private final ConcurrentMap<Long, Identity> byId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Identity> byName = new ConcurrentHashMap<>();
    private volatile Binding binding;

    public IdNameIndex(String scope) {
        this.scope = Objects.requireNonNull(scope, "scope");
    }

    /** Bind once per owning catalog/database instance; standalone indexes retain the same validation contract. */
    public synchronized void bind(CatalogMetaCache owner, String entryName, Function<String, ScopePath> resolver) {
        if (binding != null && binding.owner == owner) {
            return;
        }
        if (binding != null) {
            close(binding.owner);
        }
        MetaCache<String, Identity> cache = owner.create(MetaCacheDefinition.<String, Identity>builder(entryName,
                CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, Long.MAX_VALUE),
                name -> resolver.apply(name).in(ScopePath.Coordinates.LOCAL))
                .budgetPolicy(MetaCacheDefinition.BudgetPolicy.COUNT_ONLY)
                .removalListener((name, identity, reason) -> removeExact(identity))
                .build());
        binding = new Binding(owner, cache);
    }

    @Nullable
    public String getName(long id) {
        Identity identity = byId.get(id);
        return isCurrent(identity) ? identity.name : null;
    }

    private boolean isCurrent(Identity identity) {
        if (identity == null || byId.get(identity.id) != identity || byName.get(identity.name) != identity) {
            return false;
        }
        Binding current = binding;
        if (current == null) {
            return true;
        }
        try {
            return !current.owner.isClosed() && current.cache.getIfPresent(identity.name) == identity;
        } catch (IllegalStateException failure) {
            // The catalog root may close the physical registration concurrently with this index read.
            if (binding != current || current.owner.isClosed()) {
                return false;
            }
            throw failure;
        }
    }

    public void checkCanPut(long id, String name) {
        Identity named = byName.get(name);
        Identity identified = byId.get(id);
        if (named == identified && (named == null || (named.id == id && named.name.equals(name)))) {
            return;
        }
        synchronized (this) {
            checkCanPutLocked(id, name);
        }
    }

    private void checkCanPutLocked(long id, String name) {
        Objects.requireNonNull(name, "name");
        Identity named = byName.get(name);
        Identity identified = byId.get(id);
        if (!isCurrent(named)) {
            removeExact(named);
            named = null;
        }
        if (!isCurrent(identified)) {
            removeExact(identified);
            identified = null;
        }
        Preconditions.checkState(named == null || named.id == id,
                "%s name '%s' is already mapped to id %s, can not map to id %s",
                scope, name, named == null ? null : named.id, id);
        Preconditions.checkState(identified == null || identified.name.equals(name),
                "%s id %s is already mapped to name '%s', can not map to name '%s'",
                scope, id, identified == null ? null : identified.name, name);
        Preconditions.checkState(named == identified, "%s index is inconsistent for %s:%s", scope, id, name);
    }

    /** Called inside the object cache's guarded publication action. */
    public void put(long id, String name) {
        checkCanPut(id, name);
        Binding captured = binding;
        MetaCache<String, Identity> cache = captured == null ? null : captured.cache;
        Identity identity = new Identity(id, name);
        // Enter cache publication before taking the index monitor. Other guarded object publications may
        // hold the catalog read phase while waiting for this monitor; the reverse order would deadlock.
        if (cache != null) {
            while (true) {
                Identity current = cache.getIfPresent(name);
                if (current != null) {
                    Preconditions.checkState(current.id == id, "%s identity conflict for %s", scope, name);
                    identity = current;
                    break;
                }
                if (cache.compareAndSet(name, null, identity)) {
                    break;
                }
            }
        }
        synchronized (this) {
            if (binding != captured || (cache != null && cache.getIfPresent(name) != identity)) {
                return;
            }
            checkCanPutLocked(id, name);
            byId.put(id, identity);
            byName.put(name, identity);
        }
    }

    private synchronized void removeExact(Identity identity) {
        if (identity != null) {
            byId.remove(identity.id, identity);
            byName.remove(identity.name, identity);
        }
    }

    public synchronized void removeName(String name) {
        Objects.requireNonNull(name, "name");
        Identity identity = byName.get(name);
        Binding current = binding;
        if (current != null && !current.owner.isClosed()) {
            current.cache.invalidateKey(name);
        }
        removeExact(identity);
    }

    @Nullable
    public String findNameIgnoreCase(String requestedName) {
        return byId.values().stream().filter(this::isCurrent).map(identity -> identity.name)
                .filter(name -> name.equalsIgnoreCase(requestedName)).findFirst().orElse(null);
    }

    public synchronized void clear() {
        Binding current = binding;
        if (current != null && !current.owner.isClosed()) {
            byName.keySet().forEach(current.cache::invalidateKey);
        }
        byId.clear();
        byName.clear();
    }

    /** Retires this instance's registration without invalidating a replacement resource. */
    public synchronized void close(CatalogMetaCache owner) {
        Binding current = binding;
        binding = null;
        if (current != null) {
            owner.remove(current.cache);
        }
        byId.clear();
        byName.clear();
    }

    @VisibleForTesting
    public boolean containsMappingForTest(long id, String name) {
        return name.equals(getName(id));
    }

    @VisibleForTesting
    public synchronized int sizeForTest() {
        return (int) byId.values().stream().filter(this::isCurrent).count();
    }

    private static final class Binding {
        private final CatalogMetaCache owner;
        private final MetaCache<String, Identity> cache;

        private Binding(CatalogMetaCache owner, MetaCache<String, Identity> cache) {
            this.owner = owner;
            this.cache = cache;
        }
    }

    private static final class Identity {
        private final long id;
        private final String name;

        private Identity(long id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}

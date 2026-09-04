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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * Runtime-only one-to-one index between metadata IDs and canonical local names.
 *
 * <p>Reads by ID are lock-free. All mutations are serialized so the two maps have a stable one-to-one relationship
 * after each mutation completes. The reverse map is intentionally kept internal: production callers use it only
 * indirectly for uniqueness validation and constant-time removal by name.
 */
public final class IdNameIndex {
    private final String scope;
    private final ConcurrentMap<Long, String> idToName = Maps.newConcurrentMap();
    private final ConcurrentMap<String, Long> nameToId = Maps.newConcurrentMap();

    public IdNameIndex(String scope) {
        this.scope = Objects.requireNonNull(scope, "scope can not be null");
    }

    @Nullable
    public String getName(long id) {
        return idToName.get(id);
    }

    /**
     * Validate one identity without publishing it.
     *
     * <p>Callers use this before mutating names/object caches so a pre-existing identity conflict fails without
     * leaving those caches partially updated. The final {@link #put(long, String)} must still run inside the
     * FeMetaCacheEntry publication action to keep the ID side effect ordered with object invalidation.
     */
    public void checkCanPut(long id, String name) {
        Objects.requireNonNull(name, "name can not be null");
        Long existingId = nameToId.get(name);
        String existingName = idToName.get(id);

        // Keep the common exact-match and cold-index paths lock-free. A later put remains the authoritative
        // validation inside the caller's FeMetaCacheEntry publication window.
        if ((existingId == null && existingName == null)
                || (Long.valueOf(id).equals(existingId) && name.equals(existingName))) {
            return;
        }

        // Re-read suspicious snapshots under the mutation lock. This both confirms real conflicts and avoids
        // rejecting a reader that observed the two maps between the writes of one synchronized mutation.
        synchronized (this) {
            checkCanPutLocked(id, name);
        }
    }

    private void checkCanPutLocked(long id, String name) {
        Long existingId = nameToId.get(name);
        String existingName = idToName.get(id);

        Preconditions.checkState(existingId == null || existingId == id,
                "%s name '%s' is already mapped to id %s, can not map to id %s",
                scope, name, existingId, id);
        Preconditions.checkState(existingName == null || existingName.equals(name),
                "%s id %s is already mapped to name '%s', can not map to name '%s'",
                scope, id, existingName, name);
        Preconditions.checkState((existingId == null) == (existingName == null),
                "%s index is inconsistent for id %s and name '%s': existing id %s, existing name '%s'",
                scope, id, name, existingId, existingName);
    }

    /**
     * Publish one identity. Re-publishing the same pair is idempotent; every other conflict is a correctness error.
     */
    public synchronized void put(long id, String name) {
        Objects.requireNonNull(name, "name can not be null");
        checkCanPutLocked(id, name);
        idToName.put(id, name);
        nameToId.put(name, id);
    }

    /**
     * Remove the identity associated with a canonical local name in constant time.
     */
    public synchronized void removeName(String name) {
        Objects.requireNonNull(name, "name can not be null");
        Long id = nameToId.get(name);
        if (id == null) {
            return;
        }
        String indexedName = idToName.get(id);
        Preconditions.checkState(name.equals(indexedName),
                "%s index is inconsistent for name '%s' and id %s: indexed name '%s'",
                scope, name, id, indexedName);

        nameToId.remove(name);
        boolean removed = idToName.remove(id, name);
        Preconditions.checkState(removed,
                "%s index failed to remove id %s and name '%s'", scope, id, name);
    }

    /**
     * Find a retained canonical name for the mode-2 cache-only invalidation fallback.
     *
     * <p>This deliberately preserves the existing O(N) fallback behavior. Normal lookup and exact-name cleanup do
     * not use this method.
     */
    @Nullable
    public String findNameIgnoreCase(String requestedName) {
        Objects.requireNonNull(requestedName, "requestedName can not be null");
        return idToName.values().stream()
                .filter(name -> name.equalsIgnoreCase(requestedName))
                .findFirst()
                .orElse(null);
    }

    public synchronized void clear() {
        idToName.clear();
        nameToId.clear();
    }

    @VisibleForTesting
    public synchronized boolean containsMappingForTest(long id, String name) {
        return name.equals(idToName.get(id)) && Long.valueOf(id).equals(nameToId.get(name));
    }

    @VisibleForTesting
    public synchronized int sizeForTest() {
        Preconditions.checkState(idToName.size() == nameToId.size(),
                "%s index size mismatch: idToName=%s, nameToId=%s",
                scope, idToName.size(), nameToId.size());
        return idToName.size();
    }
}

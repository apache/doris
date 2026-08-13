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

import org.apache.doris.common.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FE-wide admission accounting for managed external metadata caches.
 *
 * <p>All changes are serialized by one short critical section. Cache loads and
 * estimators run outside it, so the lock only protects a few arithmetic and map
 * operations while making global/catalog/entry reservation atomic.
 */
public final class ExternalMetaCacheBudgetManager {
    public static final String CATALOG_MAX_WEIGHT_PROPERTY = "meta.cache.max-weight";

    private final Object lock = new Object();
    private final OptionalLong globalMaxWeight;
    private final Map<Long, Bucket> catalogBuckets = new HashMap<>();
    private final Map<EntryScope, Bucket> entryBuckets = new HashMap<>();
    private long globalUsedWeight;
    private final AtomicLong globalRejectedCount = new AtomicLong();

    public ExternalMetaCacheBudgetManager(OptionalLong globalMaxWeight) {
        this.globalMaxWeight = Objects.requireNonNull(globalMaxWeight, "globalMaxWeight");
        if (globalMaxWeight.isPresent() && globalMaxWeight.getAsLong() <= 0) {
            throw new IllegalArgumentException("global max weight must be positive when enabled");
        }
    }

    public static ExternalMetaCacheBudgetManager fromConfig() {
        String configured = Config.external_meta_cache_max_weight;
        long parsed = CacheSpec.parseWeight(
                configured,
                "external_meta_cache_max_weight",
                true,
                Runtime.getRuntime().maxMemory());
        if (configured.trim().endsWith("%") && parsed == 0L) {
            throw new IllegalArgumentException(
                    "external_meta_cache_max_weight percentage must be greater than 0%");
        }
        return new ExternalMetaCacheBudgetManager(parsed == 0L ? OptionalLong.empty() : OptionalLong.of(parsed));
    }

    public OptionalLong parseCatalogMaxWeight(Map<String, String> catalogProperties) {
        String configured = catalogProperties.get(CATALOG_MAX_WEIGHT_PROPERTY);
        if (configured == null) {
            return OptionalLong.empty();
        }
        long parsed = CacheSpec.parseWeight(configured, CATALOG_MAX_WEIGHT_PROPERTY, false, 0L);
        if (parsed <= 0) {
            throw new IllegalArgumentException(CATALOG_MAX_WEIGHT_PROPERTY + " must be positive");
        }
        return OptionalLong.of(parsed);
    }

    /** Validate a catalog limit at DDL time against this FE's configured global bound. */
    public OptionalLong validateCatalogMaxWeight(Map<String, String> catalogProperties) {
        OptionalLong catalogMaxWeight = parseCatalogMaxWeight(catalogProperties);
        validateHierarchy(catalogMaxWeight, OptionalLong.empty());
        return catalogMaxWeight;
    }

    /**
     * Create the budget handle used by one physical per-catalog cache entry.
     */
    public EntryBudget createEntryBudget(long catalogId, String engine, String entryName,
            OptionalLong catalogMaxWeight, OptionalLong entryMaxWeight) {
        Objects.requireNonNull(engine, "engine");
        Objects.requireNonNull(entryName, "entryName");
        Objects.requireNonNull(catalogMaxWeight, "catalogMaxWeight");
        Objects.requireNonNull(entryMaxWeight, "entryMaxWeight");
        validateCatalogEntryHierarchy(catalogMaxWeight, entryMaxWeight);

        OptionalLong effectiveMax = minimumPresent(globalMaxWeight, catalogMaxWeight, entryMaxWeight);
        if (!effectiveMax.isPresent()) {
            throw new IllegalArgumentException("entry budget requires at least one configured weight bound");
        }

        EntryScope scope = new EntryScope(catalogId, engine, entryName);
        synchronized (lock) {
            Bucket catalogBucket = catalogBuckets.get(catalogId);
            long catalogLimit = minimumLimit(globalMaxWeight, catalogMaxWeight);
            if (catalogBucket == null) {
                catalogBucket = new Bucket(catalogLimit);
                catalogBuckets.put(catalogId, catalogBucket);
            } else if (catalogBucket.maxWeight != catalogLimit) {
                throw new IllegalStateException("Conflicting catalog cache max weight for catalog " + catalogId);
            }

            if (entryBuckets.containsKey(scope)) {
                throw new IllegalStateException("Duplicated external meta cache budget: " + scope);
            }
            Bucket entryBucket = new Bucket(effectiveMax.getAsLong());
            entryBuckets.put(scope, entryBucket);
            return new EntryBudget(this, scope, catalogBucket, entryBucket, effectiveMax.getAsLong());
        }
    }

    public OptionalLong getGlobalMaxWeight() {
        return globalMaxWeight;
    }

    public long getGlobalUsedWeight() {
        synchronized (lock) {
            return globalUsedWeight;
        }
    }

    public long getGlobalRejectedCount() {
        return globalRejectedCount.get();
    }

    public void validateHierarchy(OptionalLong catalogMaxWeight, OptionalLong entryMaxWeight) {
        if (globalMaxWeight.isPresent() && catalogMaxWeight.isPresent()
                && catalogMaxWeight.getAsLong() > globalMaxWeight.getAsLong()) {
            throw new IllegalArgumentException(CATALOG_MAX_WEIGHT_PROPERTY + " can not exceed FE global max weight");
        }
        OptionalLong parent = catalogMaxWeight.isPresent() ? catalogMaxWeight : globalMaxWeight;
        if (parent.isPresent() && entryMaxWeight.isPresent()
                && entryMaxWeight.getAsLong() > parent.getAsLong()) {
            throw new IllegalArgumentException("entry max weight can not exceed its parent max weight");
        }
    }

    /**
     * Validate persisted catalog-to-entry hierarchy without comparing it with this FE's local
     * global bound. Catalog properties are validated on the master, while the global percentage
     * is resolved independently from each FE's heap. Runtime admission therefore clamps to the
     * local global limit instead of rejecting a catalog accepted on a larger master.
     */
    public void validateCatalogEntryHierarchy(OptionalLong catalogMaxWeight, OptionalLong entryMaxWeight) {
        OptionalLong parent = catalogMaxWeight;
        if (parent.isPresent() && entryMaxWeight.isPresent()
                && entryMaxWeight.getAsLong() > parent.getAsLong()) {
            throw new IllegalArgumentException("entry max weight can not exceed its parent max weight");
        }
    }

    private Optional<AdmissionReservation> tryReserve(EntryBudget entryBudget, long bytes) {
        checkWeight(bytes);
        synchronized (lock) {
            if (entryBudget.closed) {
                return Optional.empty();
            }
            if (!fits(limitOf(globalMaxWeight), globalUsedWeight, bytes)
                    || !fits(entryBudget.catalogBucket.maxWeight, entryBudget.catalogBucket.usedWeight, bytes)
                    || !fits(entryBudget.entryBucket.maxWeight, entryBudget.entryBucket.usedWeight, bytes)) {
                entryBudget.rejectedCount.incrementAndGet();
                globalRejectedCount.incrementAndGet();
                return Optional.empty();
            }
            addUsed(entryBudget, bytes);
            return Optional.of(new AdmissionReservation(this, entryBudget, bytes));
        }
    }

    private boolean resize(AdmissionReservation reservation, long newBytes) {
        checkWeight(newBytes);
        synchronized (lock) {
            if (!reservation.active || reservation.entryBudget.closed) {
                return false;
            }
            long delta = newBytes - reservation.bytes;
            if (delta > 0 && (!fits(limitOf(globalMaxWeight), globalUsedWeight, delta)
                    || !fits(reservation.entryBudget.catalogBucket.maxWeight,
                            reservation.entryBudget.catalogBucket.usedWeight, delta)
                    || !fits(reservation.entryBudget.entryBucket.maxWeight,
                            reservation.entryBudget.entryBucket.usedWeight, delta))) {
                reservation.entryBudget.rejectedCount.incrementAndGet();
                globalRejectedCount.incrementAndGet();
                return false;
            }
            if (delta >= 0) {
                addUsed(reservation.entryBudget, delta);
            } else {
                subtractUsed(reservation.entryBudget, -delta);
            }
            reservation.bytes = newBytes;
            return true;
        }
    }

    private void release(AdmissionReservation reservation) {
        synchronized (lock) {
            if (!reservation.active) {
                return;
            }
            subtractUsed(reservation.entryBudget, reservation.bytes);
            reservation.bytes = 0L;
            reservation.active = false;
        }
    }

    private void close(EntryBudget entryBudget) {
        synchronized (lock) {
            if (entryBudget.closed) {
                return;
            }
            if (entryBudget.entryBucket.usedWeight != 0L) {
                throw new IllegalStateException("entry budget closed with active reservations: " + entryBudget.scope);
            }
            entryBudget.closed = true;
            entryBuckets.remove(entryBudget.scope, entryBudget.entryBucket);
            Bucket catalogBucket = entryBudget.catalogBucket;
            boolean catalogStillReferenced = entryBuckets.keySet().stream()
                    .anyMatch(scope -> scope.catalogId == entryBudget.scope.catalogId);
            if (!catalogStillReferenced && catalogBucket.usedWeight == 0L) {
                catalogBuckets.remove(entryBudget.scope.catalogId, catalogBucket);
            }
        }
    }

    private void addUsed(EntryBudget entryBudget, long bytes) {
        globalUsedWeight += bytes;
        entryBudget.catalogBucket.usedWeight += bytes;
        entryBudget.entryBucket.usedWeight += bytes;
    }

    private void subtractUsed(EntryBudget entryBudget, long bytes) {
        if (bytes > globalUsedWeight
                || bytes > entryBudget.catalogBucket.usedWeight
                || bytes > entryBudget.entryBucket.usedWeight) {
            throw new IllegalStateException("external meta cache budget accounting underflow");
        }
        globalUsedWeight -= bytes;
        entryBudget.catalogBucket.usedWeight -= bytes;
        entryBudget.entryBucket.usedWeight -= bytes;
    }

    private static boolean fits(long maxWeight, long usedWeight, long delta) {
        return delta >= 0 && usedWeight <= maxWeight && delta <= maxWeight - usedWeight;
    }

    private static long limitOf(OptionalLong configured) {
        return configured.isPresent() ? configured.getAsLong() : Long.MAX_VALUE;
    }

    private static long minimumLimit(OptionalLong first, OptionalLong second) {
        return Math.min(limitOf(first), limitOf(second));
    }

    private static OptionalLong minimumPresent(OptionalLong first, OptionalLong second, OptionalLong third) {
        if (!first.isPresent() && !second.isPresent() && !third.isPresent()) {
            return OptionalLong.empty();
        }
        long minimum = Math.min(limitOf(first), Math.min(limitOf(second), limitOf(third)));
        return OptionalLong.of(minimum);
    }

    private static void checkWeight(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("cache reservation can not be negative: " + bytes);
        }
    }

    private static final class Bucket {
        private final long maxWeight;
        private long usedWeight;

        private Bucket(long maxWeight) {
            this.maxWeight = maxWeight;
        }
    }

    private static final class EntryScope {
        private final long catalogId;
        private final String engine;
        private final String entryName;

        private EntryScope(long catalogId, String engine, String entryName) {
            this.catalogId = catalogId;
            this.engine = engine;
            this.entryName = entryName;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof EntryScope)) {
                return false;
            }
            EntryScope that = (EntryScope) other;
            return catalogId == that.catalogId && engine.equals(that.engine) && entryName.equals(that.entryName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogId, engine, entryName);
        }

        @Override
        public String toString() {
            return catalogId + "/" + engine + "/" + entryName;
        }
    }

    public static final class EntryBudget {
        private final ExternalMetaCacheBudgetManager manager;
        private final EntryScope scope;
        private final Bucket catalogBucket;
        private final Bucket entryBucket;
        private final long effectiveMaxWeight;
        private final AtomicLong rejectedCount = new AtomicLong();
        // Guarded by manager.lock. A closed handle must never re-enter accounting.
        private boolean closed;

        private EntryBudget(ExternalMetaCacheBudgetManager manager, EntryScope scope,
                Bucket catalogBucket, Bucket entryBucket, long effectiveMaxWeight) {
            this.manager = manager;
            this.scope = scope;
            this.catalogBucket = catalogBucket;
            this.entryBucket = entryBucket;
            this.effectiveMaxWeight = effectiveMaxWeight;
        }

        public Optional<AdmissionReservation> tryReserve(long bytes) {
            return manager.tryReserve(this, bytes);
        }

        public long getEffectiveMaxWeight() {
            return effectiveMaxWeight;
        }

        public long getUsedWeight() {
            synchronized (manager.lock) {
                return entryBucket.usedWeight;
            }
        }

        public long getCatalogUsedWeight() {
            synchronized (manager.lock) {
                return catalogBucket.usedWeight;
            }
        }

        public long getCatalogMaxWeight() {
            return catalogBucket.maxWeight == Long.MAX_VALUE ? -1L : catalogBucket.maxWeight;
        }

        public long getRejectedCount() {
            return rejectedCount.get();
        }

        public long getGlobalUsedWeight() {
            return manager.getGlobalUsedWeight();
        }

        public long getGlobalMaxWeight() {
            return manager.globalMaxWeight.isPresent() ? manager.globalMaxWeight.getAsLong() : -1L;
        }

        public void close() {
            manager.close(this);
        }
    }

    public static final class AdmissionReservation {
        private final ExternalMetaCacheBudgetManager manager;
        private final EntryBudget entryBudget;
        private long bytes;
        private boolean active = true;

        private AdmissionReservation(ExternalMetaCacheBudgetManager manager, EntryBudget entryBudget, long bytes) {
            this.manager = manager;
            this.entryBudget = entryBudget;
            this.bytes = bytes;
        }

        public boolean tryResize(long newBytes) {
            return manager.resize(this, newBytes);
        }

        public void release() {
            manager.release(this);
        }

        public long getBytes() {
            synchronized (manager.lock) {
                return bytes;
            }
        }

        public boolean isActive() {
            synchronized (manager.lock) {
                return active;
            }
        }
    }
}

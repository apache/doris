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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

/**
 * FE-wide admission accounting for managed external metadata caches.
 *
 * <p>All changes are serialized by one short critical section. Cache loads and
 * estimators run outside it, so the lock only protects a few arithmetic and map
 * operations while making global/catalog/entry reservation atomic.
 */
public final class ExternalMetaCacheBudgetManager {
    private static final Logger LOG = LogManager.getLogger(ExternalMetaCacheBudgetManager.class);
    private static final ExecutorService PEER_RECLAIM_EXECUTOR = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "external-meta-cache-peer-reclaim");
        thread.setDaemon(true);
        return thread;
    });

    public static final String CATALOG_MAX_WEIGHT_PROPERTY = "meta.cache.max-weight";

    private final Object lock = new Object();
    private final OptionalLong globalMaxWeight;
    private final Map<Long, Bucket> catalogBuckets = new HashMap<>();
    private final Map<EntryScope, Bucket> entryBuckets = new HashMap<>();
    private final Map<EntryScope, EntryBudget> entryBudgets = new HashMap<>();
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
            EntryBudget entryBudget = new EntryBudget(
                    this, scope, catalogBucket, entryBucket, effectiveMax.getAsLong());
            entryBuckets.put(scope, entryBucket);
            entryBudgets.put(scope, entryBudget);
            return entryBudget;
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
            if (reservation.entryBudget.closed) {
                reservation.bytes = 0L;
                reservation.active = false;
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
                long leakedWeight = entryBudget.entryBucket.usedWeight;
                LOG.error("Force-closing external metadata cache budget {} with {} bytes still reserved",
                        entryBudget.scope, leakedWeight);
                if (leakedWeight <= globalUsedWeight
                        && leakedWeight <= entryBudget.catalogBucket.usedWeight) {
                    globalUsedWeight -= leakedWeight;
                    entryBudget.catalogBucket.usedWeight -= leakedWeight;
                    entryBudget.entryBucket.usedWeight = 0L;
                } else {
                    LOG.error("External metadata cache accounting is inconsistent while closing {}; "
                                    + "globalUsed={}, catalogUsed={}, entryUsed={}",
                            entryBudget.scope, globalUsedWeight,
                            entryBudget.catalogBucket.usedWeight, leakedWeight);
                    globalUsedWeight = Math.max(0L, globalUsedWeight - leakedWeight);
                    entryBudget.catalogBucket.usedWeight = Math.max(
                            0L, entryBudget.catalogBucket.usedWeight - leakedWeight);
                    entryBudget.entryBucket.usedWeight = 0L;
                }
            }
            entryBudget.closed = true;
            entryBudget.reclaimer = null;
            entryBuckets.remove(entryBudget.scope, entryBudget.entryBucket);
            entryBudgets.remove(entryBudget.scope, entryBudget);
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

    private void requestPeerReclaim(EntryBudget requester, long additionalBytes) {
        if (additionalBytes <= 0L || requester.closed) {
            return;
        }
        long reclaimBytes;
        synchronized (lock) {
            if (requester.closed) {
                return;
            }
            long globalDeficit = deficit(limitOf(globalMaxWeight), globalUsedWeight, additionalBytes);
            long catalogDeficit = deficit(
                    requester.catalogBucket.maxWeight, requester.catalogBucket.usedWeight, additionalBytes);
            reclaimBytes = Math.max(globalDeficit, catalogDeficit);
        }
        if (reclaimBytes <= 0L) {
            return;
        }
        // Rejected values are returned uncached; there is no queue of pending admissions to fund.
        // Coalesce concurrent misses to the largest single admission instead of summing identical
        // deficits and evicting an entire peer cache during a miss burst.
        requester.requestedAdmissionBytes.accumulateAndGet(additionalBytes, Math::max);
        schedulePeerReclaim(requester);
    }

    private void schedulePeerReclaim(EntryBudget requester) {
        if (!requester.reclaimScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            PEER_RECLAIM_EXECUTOR.execute(() -> drainPeerReclaim(requester));
        } catch (RejectedExecutionException e) {
            requester.reclaimScheduled.set(false);
            LOG.warn("Failed to schedule peer reclamation for external metadata cache budget {}",
                    requester.scope, e);
        }
    }

    private void drainPeerReclaim(EntryBudget requester) {
        try {
            long requestedAdmissionBytes = requester.requestedAdmissionBytes.getAndSet(0L);
            if (requestedAdmissionBytes <= 0L || requester.closed) {
                return;
            }
            List<EntryBudget> candidates;
            synchronized (lock) {
                candidates = entryBudgets.values().stream()
                        .filter(candidate -> candidate != requester && !candidate.closed)
                        .filter(candidate -> candidate.reclaimer != null)
                        .filter(candidate -> candidate.entryBucket.usedWeight > 0L)
                        .sorted((left, right) -> {
                            boolean leftSibling = left.scope.catalogId == requester.scope.catalogId;
                            boolean rightSibling = right.scope.catalogId == requester.scope.catalogId;
                            if (leftSibling != rightSibling) {
                                return leftSibling ? -1 : 1;
                            }
                            return Long.compare(
                                    right.entryBucket.usedWeight, left.entryBucket.usedWeight);
                        })
                        .collect(Collectors.toList());
            }
            long remaining = currentReclaimDeficit(requester, requestedAdmissionBytes);
            for (EntryBudget candidate : candidates) {
                boolean sibling = candidate.scope.catalogId == requester.scope.catalogId;
                if (!sibling && currentCatalogDeficit(requester, requestedAdmissionBytes) > 0L) {
                    // Another catalog cannot create headroom under the requester's catalog limit.
                    continue;
                }
                LongUnaryOperator reclaimer = candidate.reclaimer;
                if (reclaimer == null || candidate.closed) {
                    continue;
                }
                try {
                    reclaimer.applyAsLong(remaining);
                    remaining = currentReclaimDeficit(requester, requestedAdmissionBytes);
                } catch (RuntimeException e) {
                    LOG.warn("Failed to reclaim external metadata cache budget from peer {}",
                            candidate.scope, e);
                }
                if (remaining == 0L) {
                    break;
                }
            }
        } finally {
            requester.reclaimScheduled.set(false);
            if (!requester.closed && requester.requestedAdmissionBytes.get() > 0L) {
                schedulePeerReclaim(requester);
            }
        }
    }

    private long currentReclaimDeficit(EntryBudget requester, long additionalBytes) {
        synchronized (lock) {
            if (requester.closed) {
                return 0L;
            }
            long globalDeficit = deficit(limitOf(globalMaxWeight), globalUsedWeight, additionalBytes);
            long catalogDeficit = deficit(
                    requester.catalogBucket.maxWeight, requester.catalogBucket.usedWeight, additionalBytes);
            return Math.max(globalDeficit, catalogDeficit);
        }
    }

    private long currentCatalogDeficit(EntryBudget requester, long additionalBytes) {
        synchronized (lock) {
            return requester.closed ? 0L : deficit(
                    requester.catalogBucket.maxWeight,
                    requester.catalogBucket.usedWeight, additionalBytes);
        }
    }

    private static long deficit(long maxWeight, long usedWeight, long additionalBytes) {
        if (maxWeight == Long.MAX_VALUE || additionalBytes <= maxWeight - Math.min(usedWeight, maxWeight)) {
            return 0L;
        }
        return MetaCacheWeightUtils.saturatedAdd(usedWeight, additionalBytes) - maxWeight;
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
        private final AtomicLong requestedAdmissionBytes = new AtomicLong();
        private final AtomicBoolean reclaimScheduled = new AtomicBoolean();
        private volatile LongUnaryOperator reclaimer;
        // Mutated under manager.lock and read by asynchronous reclamation workers.
        private volatile boolean closed;

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

        void setReclaimer(LongUnaryOperator reclaimer) {
            this.reclaimer = Objects.requireNonNull(reclaimer, "reclaimer");
        }

        void requestPeerReclaim(long additionalBytes) {
            manager.requestPeerReclaim(this, additionalBytes);
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

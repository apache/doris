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

package org.apache.doris.connector.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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

/**
 * Process-wide admission accounting shared by FE and connector metadata caches.
 * Estimation happens before this class is entered; its lock covers only arithmetic and small maps.
 */
public final class MetaCacheBudgetManager {
    private static final Logger LOG = LogManager.getLogger(MetaCacheBudgetManager.class);
    private static final ExecutorService PEER_RECLAIM_EXECUTOR = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "external-meta-cache-peer-reclaim");
        thread.setDaemon(true);
        return thread;
    });

    public static final String CATALOG_MAX_WEIGHT_PROPERTY = "meta.cache.max-weight";

    private final Object lock = new Object();
    private final OptionalLong globalMaxWeight;
    private final Map<Long, Bucket> catalogBuckets = new HashMap<>();
    private final Map<EntryGroupScope, Bucket> entryGroupBuckets = new HashMap<>();
    private final Map<EntryScope, EntryBudget> entryBudgets = new HashMap<>();
    private final AtomicLong nextEntryBudgetId = new AtomicLong();
    private long globalUsedWeight;

    public MetaCacheBudgetManager(OptionalLong globalMaxWeight) {
        this.globalMaxWeight = Objects.requireNonNull(globalMaxWeight, "globalMaxWeight");
        if (globalMaxWeight.isPresent() && globalMaxWeight.getAsLong() <= 0L) {
            throw new IllegalArgumentException("global max weight must be positive when enabled");
        }
    }

    public OptionalLong parseCatalogMaxWeight(Map<String, String> catalogProperties) {
        String configured = catalogProperties == null ? null
                : catalogProperties.get(CATALOG_MAX_WEIGHT_PROPERTY);
        if (configured == null) {
            return OptionalLong.empty();
        }
        try {
            long parsed = CacheSpec.parseWeight(configured, CATALOG_MAX_WEIGHT_PROPERTY, false, 0L);
            if (parsed <= 0L) {
                throw new IllegalArgumentException(CATALOG_MAX_WEIGHT_PROPERTY + " must be positive");
            }
            return OptionalLong.of(parsed);
        } catch (IllegalArgumentException e) {
            LOG.warn("Ignoring invalid persisted metadata cache property {}={}",
                    CATALOG_MAX_WEIGHT_PROPERTY, configured);
            return OptionalLong.empty();
        }
    }

    public boolean hasLimit(OptionalLong catalogMaxWeight, OptionalLong entryMaxWeight) {
        return globalMaxWeight.isPresent() || catalogMaxWeight.isPresent() || entryMaxWeight.isPresent();
    }

    public EntryBudget createEntryBudget(long catalogId, String engine, String entryName, String budgetGroup,
            OptionalLong catalogMaxWeight, OptionalLong entryMaxWeight) {
        Objects.requireNonNull(engine, "engine");
        Objects.requireNonNull(entryName, "entryName");
        Objects.requireNonNull(budgetGroup, "budgetGroup");
        OptionalLong effectiveMax = minimumPresent(globalMaxWeight, catalogMaxWeight, entryMaxWeight);
        if (!effectiveMax.isPresent()) {
            throw new IllegalArgumentException("entry budget requires at least one configured weight bound");
        }

        EntryGroupScope groupScope = new EntryGroupScope(catalogId, engine, budgetGroup);
        EntryScope scope = new EntryScope(nextEntryBudgetId.incrementAndGet(), groupScope, entryName);
        synchronized (lock) {
            Bucket catalogBucket = catalogBuckets.get(catalogId);
            long catalogLimit = minimumLimit(globalMaxWeight, catalogMaxWeight);
            if (catalogBucket == null) {
                catalogBucket = new Bucket(catalogLimit);
                catalogBuckets.put(catalogId, catalogBucket);
            } else if (catalogBucket.maxWeight != catalogLimit) {
                throw new IllegalStateException("Conflicting catalog cache max weight for catalog " + catalogId);
            }
            Bucket groupBucket = entryGroupBuckets.get(groupScope);
            if (groupBucket == null) {
                groupBucket = new Bucket(effectiveMax.getAsLong());
                entryGroupBuckets.put(groupScope, groupBucket);
            } else if (groupBucket.maxWeight != effectiveMax.getAsLong()) {
                throw new IllegalStateException("Conflicting metadata cache entry max weight for " + groupScope);
            }
            Bucket entryBucket = new Bucket(effectiveMax.getAsLong());
            EntryBudget budget = new EntryBudget(this, scope, catalogBucket, groupBucket, entryBucket,
                    effectiveMax.getAsLong());
            entryBudgets.put(scope, budget);
            catalogBucket.liveEntries++;
            groupBucket.liveEntries++;
            return budget;
        }
    }

    public long getGlobalUsedWeight() {
        synchronized (lock) {
            return globalUsedWeight;
        }
    }

    private Optional<AdmissionReservation> tryReserve(EntryBudget budget, long bytes) {
        checkWeight(bytes);
        synchronized (lock) {
            if (budget.closed) {
                return Optional.empty();
            }
            if (!fits(limitOf(globalMaxWeight), globalUsedWeight, bytes)
                    || !fits(budget.catalogBucket.maxWeight, budget.catalogBucket.usedWeight, bytes)
                    || !fits(budget.groupBucket.maxWeight, budget.groupBucket.usedWeight, bytes)) {
                return Optional.empty();
            }
            addUsed(budget, bytes);
            return Optional.of(new AdmissionReservation(this, budget, bytes));
        }
    }

    private Optional<ReservationReplacement> tryReplace(
            AdmissionReservation previous, long newBytes) {
        checkWeight(newBytes);
        synchronized (lock) {
            if (!previous.active || previous.entryBudget.closed) {
                return Optional.empty();
            }
            EntryBudget budget = previous.entryBudget;
            long heldBytes = Math.max(previous.accountedBytes, newBytes);
            long delta = heldBytes - previous.accountedBytes;
            if (delta > 0L && (!fits(limitOf(globalMaxWeight), globalUsedWeight, delta)
                    || !fits(budget.catalogBucket.maxWeight, budget.catalogBucket.usedWeight, delta)
                    || !fits(budget.groupBucket.maxWeight, budget.groupBucket.usedWeight, delta))) {
                return Optional.empty();
            }
            addUsed(budget, delta);
            previous.active = false;
            AdmissionReservation replacement = new AdmissionReservation(
                    this, budget, newBytes, heldBytes);
            return Optional.of(new ReservationReplacement(this, previous, replacement));
        }
    }

    private void commitReplacement(ReservationReplacement replacement) {
        synchronized (lock) {
            replacement.checkPending();
            AdmissionReservation current = replacement.current;
            subtractUsed(current.entryBudget, current.accountedBytes - current.bytes);
            current.accountedBytes = current.bytes;
            replacement.finished = true;
        }
    }

    private void rollbackReplacement(ReservationReplacement replacement) {
        synchronized (lock) {
            replacement.checkPending();
            AdmissionReservation previous = replacement.previous;
            AdmissionReservation current = replacement.current;
            subtractUsed(current.entryBudget, current.accountedBytes - previous.accountedBytes);
            current.bytes = 0L;
            current.accountedBytes = 0L;
            current.active = false;
            previous.active = true;
            replacement.finished = true;
        }
    }

    private void release(AdmissionReservation reservation) {
        synchronized (lock) {
            if (!reservation.active) {
                return;
            }
            if (!reservation.entryBudget.closed) {
                subtractUsed(reservation.entryBudget, reservation.accountedBytes);
            }
            reservation.bytes = 0L;
            reservation.accountedBytes = 0L;
            reservation.active = false;
        }
    }

    private void close(EntryBudget budget) {
        synchronized (lock) {
            if (budget.closed) {
                return;
            }
            long leaked = budget.entryBucket.usedWeight;
            if (leaked != 0L) {
                LOG.error("Force-closing metadata cache budget {} with {} bytes still reserved",
                        budget.scope, leaked);
                globalUsedWeight = Math.max(0L, globalUsedWeight - leaked);
                budget.catalogBucket.usedWeight = Math.max(0L, budget.catalogBucket.usedWeight - leaked);
                budget.groupBucket.usedWeight = Math.max(0L, budget.groupBucket.usedWeight - leaked);
                budget.entryBucket.usedWeight = 0L;
            }
            budget.closed = true;
            budget.reclaimer = null;
            entryBudgets.remove(budget.scope, budget);
            budget.catalogBucket.liveEntries--;
            if (budget.catalogBucket.liveEntries == 0) {
                catalogBuckets.remove(budget.scope.groupScope.catalogId, budget.catalogBucket);
            }
            budget.groupBucket.liveEntries--;
            if (budget.groupBucket.liveEntries == 0) {
                entryGroupBuckets.remove(budget.scope.groupScope, budget.groupBucket);
            }
        }
    }

    private void addUsed(EntryBudget budget, long bytes) {
        globalUsedWeight += bytes;
        budget.catalogBucket.usedWeight += bytes;
        budget.groupBucket.usedWeight += bytes;
        budget.entryBucket.usedWeight += bytes;
    }

    private void subtractUsed(EntryBudget budget, long bytes) {
        if (bytes > globalUsedWeight || bytes > budget.catalogBucket.usedWeight
                || bytes > budget.groupBucket.usedWeight
                || bytes > budget.entryBucket.usedWeight) {
            throw new IllegalStateException("metadata cache budget accounting underflow");
        }
        globalUsedWeight -= bytes;
        budget.catalogBucket.usedWeight -= bytes;
        budget.groupBucket.usedWeight -= bytes;
        budget.entryBucket.usedWeight -= bytes;
    }

    private void requestPeerReclaim(EntryBudget requester, long additionalBytes) {
        if (additionalBytes <= 0L || requester.closed) {
            return;
        }
        synchronized (lock) {
            long globalDeficit = deficit(limitOf(globalMaxWeight), globalUsedWeight, additionalBytes);
            long catalogDeficit = deficit(
                    requester.catalogBucket.maxWeight, requester.catalogBucket.usedWeight, additionalBytes);
            long groupDeficit = deficit(
                    requester.groupBucket.maxWeight, requester.groupBucket.usedWeight, additionalBytes);
            if (Math.max(groupDeficit, Math.max(globalDeficit, catalogDeficit)) == 0L) {
                return;
            }
        }
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
            LOG.warn("Failed to schedule metadata cache peer reclamation for {}", requester.scope, e);
        }
    }

    private void drainPeerReclaim(EntryBudget requester) {
        try {
            long requested = requester.requestedAdmissionBytes.getAndSet(0L);
            if (requested <= 0L || requester.closed) {
                return;
            }
            List<EntryBudget> candidates;
            synchronized (lock) {
                candidates = new ArrayList<>();
                for (EntryBudget candidate : entryBudgets.values()) {
                    if (!candidate.closed && candidate.reclaimer != null
                            && candidate.entryBucket.usedWeight > 0L) {
                        candidates.add(candidate);
                    }
                }
                candidates.sort((left, right) -> {
                    boolean leftSibling = left.scope.groupScope.catalogId
                            == requester.scope.groupScope.catalogId;
                    boolean rightSibling = right.scope.groupScope.catalogId
                            == requester.scope.groupScope.catalogId;
                    if (leftSibling != rightSibling) {
                        return leftSibling ? -1 : 1;
                    }
                    return Long.compare(right.entryBucket.usedWeight, left.entryBucket.usedWeight);
                });
            }
            long remaining = currentDeficit(requester, requested);
            for (EntryBudget candidate : candidates) {
                if (currentGroupDeficit(requester, requested) > 0L
                        && candidate.groupBucket != requester.groupBucket) {
                    continue;
                }
                boolean sibling = candidate.scope.groupScope.catalogId
                        == requester.scope.groupScope.catalogId;
                if (!sibling && currentCatalogDeficit(requester, requested) > 0L) {
                    continue;
                }
                try {
                    candidate.reclaimer.applyAsLong(remaining);
                } catch (RuntimeException e) {
                    LOG.warn("Failed to reclaim metadata cache budget from peer {}", candidate.scope, e);
                }
                remaining = currentDeficit(requester, requested);
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

    private long currentDeficit(EntryBudget requester, long additionalBytes) {
        synchronized (lock) {
            if (requester.closed) {
                return 0L;
            }
            return Math.max(
                    deficit(requester.groupBucket.maxWeight,
                            requester.groupBucket.usedWeight, additionalBytes),
                    Math.max(deficit(limitOf(globalMaxWeight), globalUsedWeight, additionalBytes),
                            deficit(requester.catalogBucket.maxWeight,
                                    requester.catalogBucket.usedWeight, additionalBytes)));
        }
    }

    private long currentGroupDeficit(EntryBudget requester, long additionalBytes) {
        synchronized (lock) {
            return requester.closed ? 0L : deficit(requester.groupBucket.maxWeight,
                    requester.groupBucket.usedWeight, additionalBytes);
        }
    }

    private long currentCatalogDeficit(EntryBudget requester, long additionalBytes) {
        synchronized (lock) {
            return requester.closed ? 0L : deficit(requester.catalogBucket.maxWeight,
                    requester.catalogBucket.usedWeight, additionalBytes);
        }
    }

    private static long deficit(long maxWeight, long usedWeight, long additionalBytes) {
        if (maxWeight == Long.MAX_VALUE || additionalBytes <= maxWeight - Math.min(usedWeight, maxWeight)) {
            return 0L;
        }
        return JvmSizeUtils.saturatedAdd(usedWeight, additionalBytes) - maxWeight;
    }

    private static boolean fits(long maxWeight, long usedWeight, long delta) {
        return delta >= 0L && usedWeight <= maxWeight && delta <= maxWeight - usedWeight;
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
        return OptionalLong.of(Math.min(limitOf(first), Math.min(limitOf(second), limitOf(third))));
    }

    private static void checkWeight(long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("cache reservation can not be negative: " + bytes);
        }
    }

    private static final class Bucket {
        private final long maxWeight;
        private long usedWeight;
        private int liveEntries;

        private Bucket(long maxWeight) {
            this.maxWeight = maxWeight;
        }
    }

    private static final class EntryGroupScope {
        private final long catalogId;
        private final String engine;
        private final String groupName;

        private EntryGroupScope(long catalogId, String engine, String groupName) {
            this.catalogId = catalogId;
            this.engine = engine;
            this.groupName = groupName;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof EntryGroupScope)) {
                return false;
            }
            EntryGroupScope that = (EntryGroupScope) other;
            return catalogId == that.catalogId && engine.equals(that.engine) && groupName.equals(that.groupName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogId, engine, groupName);
        }

        @Override
        public String toString() {
            return catalogId + "/" + engine + "/" + groupName;
        }
    }

    private static final class EntryScope {
        private final long budgetId;
        private final EntryGroupScope groupScope;
        private final String entryName;

        private EntryScope(long budgetId, EntryGroupScope groupScope, String entryName) {
            this.budgetId = budgetId;
            this.groupScope = groupScope;
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
            return budgetId == that.budgetId;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(budgetId);
        }

        @Override
        public String toString() {
            return groupScope + "/" + entryName + "#" + budgetId;
        }
    }

    public static final class EntryBudget implements AutoCloseable {
        private final MetaCacheBudgetManager manager;
        private final EntryScope scope;
        private final Bucket catalogBucket;
        private final Bucket groupBucket;
        private final Bucket entryBucket;
        private final long effectiveMaxWeight;
        private final AtomicLong requestedAdmissionBytes = new AtomicLong();
        private final AtomicBoolean reclaimScheduled = new AtomicBoolean();
        private volatile LongUnaryOperator reclaimer;
        private volatile boolean closed;

        private EntryBudget(MetaCacheBudgetManager manager, EntryScope scope, Bucket catalogBucket,
                Bucket groupBucket, Bucket entryBucket, long effectiveMaxWeight) {
            this.manager = manager;
            this.scope = scope;
            this.catalogBucket = catalogBucket;
            this.groupBucket = groupBucket;
            this.entryBucket = entryBucket;
            this.effectiveMaxWeight = effectiveMaxWeight;
        }

        public Optional<AdmissionReservation> tryReserve(long bytes) {
            return manager.tryReserve(this, bytes);
        }

        public Optional<ReservationReplacement> tryReplace(
                AdmissionReservation previous, long newBytes) {
            Objects.requireNonNull(previous, "previous reservation");
            if (previous.entryBudget != this) {
                throw new IllegalArgumentException("replacement reservation belongs to another entry");
            }
            return manager.tryReplace(previous, newBytes);
        }

        public void setReclaimer(LongUnaryOperator reclaimer) {
            this.reclaimer = Objects.requireNonNull(reclaimer, "reclaimer");
        }

        public void requestPeerReclaim(long additionalBytes) {
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

        @Override
        public void close() {
            manager.close(this);
        }
    }

    public static final class AdmissionReservation {
        private final MetaCacheBudgetManager manager;
        private final EntryBudget entryBudget;
        private long bytes;
        private long accountedBytes;
        private boolean active = true;

        private AdmissionReservation(MetaCacheBudgetManager manager, EntryBudget entryBudget, long bytes) {
            this(manager, entryBudget, bytes, bytes);
        }

        private AdmissionReservation(MetaCacheBudgetManager manager, EntryBudget entryBudget,
                long bytes, long accountedBytes) {
            this.manager = manager;
            this.entryBudget = entryBudget;
            this.bytes = bytes;
            this.accountedBytes = accountedBytes;
        }

        public void release() {
            manager.release(this);
        }

        public long getBytes() {
            synchronized (manager.lock) {
                return bytes;
            }
        }
    }

    /**
     * Atomic accounting hand-off between two generations of the same cache key.
     * The manager temporarily holds max(old, new), never old + new, and keeps enough
     * accounting to restore the old generation until publication commits.
     */
    public static final class ReservationReplacement {
        private final MetaCacheBudgetManager manager;
        private final AdmissionReservation previous;
        private final AdmissionReservation current;
        private boolean finished;

        private ReservationReplacement(MetaCacheBudgetManager manager,
                AdmissionReservation previous, AdmissionReservation current) {
            this.manager = manager;
            this.previous = previous;
            this.current = current;
        }

        public AdmissionReservation current() {
            return current;
        }

        public void commit() {
            manager.commitReplacement(this);
        }

        public void rollback() {
            manager.rollbackReplacement(this);
        }

        private void checkPending() {
            if (finished) {
                throw new IllegalStateException("reservation replacement is already finished");
            }
        }
    }
}

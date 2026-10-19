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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.statistics.repository.ResultRow;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * White-box access to {@link BaselineManager} internals for unit tests.
 *
 * The manager ships NO test hooks: every state read / test driver lives here, on the test
 * side, and reaches the production members through reflection. The helpers mirror exactly
 * the behavior of the historic {@code *ForTest} accessors so the tests keep asserting the
 * same state; when a production member is renamed the failure is loud and local.
 */
public final class BaselineManagerTestAccess {

    private static final Class<?> MANAGER = BaselineManager.class;

    private BaselineManagerTestAccess() {
    }

    // ==================== generic reflection ====================

    private static Field field(String name) {
        try {
            Field f = MANAGER.getDeclaredField(name);
            f.setAccessible(true);
            return f;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object get(BaselineManager manager, String name) {
        try {
            return field(name).get(manager);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void set(BaselineManager manager, String name, Object value) {
        try {
            field(name).set(manager, value);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object staticValue(String name) {
        try {
            return field(name).get(null);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Method method(Class<?> owner, String name, Class<?>... params) {
        try {
            Method m = owner.getDeclaredMethod(name, params);
            m.setAccessible(true);
            return m;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object invoke(BaselineManager manager, String name, Class<?>[] params, Object... args) {
        try {
            return method(MANAGER, name, params).invoke(manager, args);
        } catch (java.lang.reflect.InvocationTargetException e) {
            throw rethrow(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object invokeStatic(String name, Class<?>[] params, Object... args) {
        try {
            return method(MANAGER, name, params).invoke(null, args);
        } catch (java.lang.reflect.InvocationTargetException e) {
            throw rethrow(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static RuntimeException rethrow(Throwable cause) {
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IllegalStateException(cause);
    }

    // ==================== store state ====================

    /**
     * Resets the in-memory store to the "test-managed" state (mirror of the historic
     * clearForTest): the published maps / pending registries start empty and the durable
     * table is never touched from a unit test.
     */
    public static void clear(BaselineManager manager) {
        ReentrantReadWriteLock lock = (ReentrantReadWriteLock) get(manager, "stateLock");
        lock.writeLock().lock();
        try {
            set(manager, "loaded", true);
            set(manager, "persistToTable", false);
            ((List<?>) get(manager, "pendingCreates")).clear();
            ((Map<?, ?>) get(manager, "pendingMutationFences")).clear();
            ((Map<?, ?>) staticValue("pendingDroppedMarkers")).clear();
            ((Map<?, ?>) staticValue("pendingDropTombstoneConfirmations")).clear();
            ((Map<?, ?>) staticValue("unresolvedMutationWindows")).clear();
            set(manager, "knownHashes", new ConcurrentHashMap<Long, Long>());
            clearRowCache(manager);
            set(manager, "maxPersistedIdSeen", 0L);
            set(manager, "stateVersion", ((Number) get(manager, "stateVersion")).longValue() + 1);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Evicts a single baseline row through the production drop path (mirror of the
     * cache-eviction call site): the row leaves the cache / masks and the key epoch
     * advances so an in-flight load of the same key cannot republish it.
     */
    public static void removeCachedBaseline(BaselineManager manager, BaselinePlan plan) {
        invoke(manager, "removeCachedBaseline", new Class<?>[] {BaselinePlan.class}, plan);
    }

    /** Unloads the published maps without touching the table (mirror of prepareLoadForTest). */
    public static void prepareLoad(BaselineManager manager) {
        ReentrantReadWriteLock lock = (ReentrantReadWriteLock) get(manager, "stateLock");
        lock.writeLock().lock();
        try {
            set(manager, "loaded", false);
            set(manager, "knownHashes", new ConcurrentHashMap<Long, Long>());
            clearRowCache(manager);
            set(manager, "stateVersion", ((Number) get(manager, "stateVersion")).longValue() + 1);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static void clearRowCache(BaselineManager manager) {
        invokeOnCache(manager, "clear");
    }

    private static Object invokeOnCache(BaselineManager manager, String name) {
        return invokeOnCache(manager, name, new Class<?>[0]);
    }

    private static Object invokeOnCache(BaselineManager manager, String name,
            Class<?>[] params, Object... args) {
        Object cache = get(manager, "baselineCache");
        try {
            java.lang.reflect.Method m;
            try {
                m = cache.getClass().getDeclaredMethod(name, params);
            } catch (NoSuchMethodException inherited) {
                m = cache.getClass().getMethod(name, params);
            }
            m.setAccessible(true);
            return m.invoke(cache, args);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static int cachedHashCount(BaselineManager manager) {
        return ((Number) invokeOnCache(manager, "entryCount", new Class<?>[0])).intValue();
    }

    /**
     * The number of rows the cache serves for one hash WITHOUT a read: 0 proves the hash
     * was evicted (or its publication was discarded), a positive count proves the
     * reconciled set was cached (round-63: the miss-path regression tests).
     */
    public static int cachedHashCountFor(BaselineManager manager, long hash) {
        Object rows = invokeOnCache(manager, "getComplete",
                new Class<?>[] {long.class}, hash);
        return rows == null ? 0 : ((List<?>) rows).size();
    }

    /** The cached row of one id (null proves the row-cache entry is absent). */
    public static BaselinePlan rawCacheGet(BaselineManager manager, long id) {
        return (BaselinePlan) invokeOnCache(manager, "getById",
                new Class<?>[] {long.class}, id);
    }

    /**
     * Removes ONLY the row-cache entry of one id (no epoch change, no durable effect):
     * forces the DROP path under test to run its CACHE-MISS branch deterministically.
     */
    public static void evictRowFromCacheOnly(BaselineManager manager, long id) {
        invokeOnCache(manager, "removeRow", new Class<?>[] {long.class}, id);
    }

    /**
     * The hashes with a matching-path cache-miss load in flight (round-64 miss
     * coalescing): 0 proves the marker of a finished read was released.
     */
    public static int inflightMissLoadCount(BaselineManager manager) {
        return ((Set<?>) get(manager, "inflightHashLoads")).size();
    }

    public static int pendingCreateCount(BaselineManager manager) {
        return ((List<?>) get(manager, "pendingCreates")).size();
    }

    public static boolean hasPendingMutationFence(BaselineManager manager, long id) {
        return ((Map<?, ?>) get(manager, "pendingMutationFences")).containsKey(id);
    }

    public static boolean isPendingMutationFenceConfirmed(BaselineManager manager, long id) {
        Object fence = ((Map<?, ?>) get(manager, "pendingMutationFences")).get(id);
        if (fence == null) {
            return false;
        }
        try {
            Field confirmed = fence.getClass().getDeclaredField("confirmed");
            confirmed.setAccessible(true);
            return (Boolean) confirmed.get(fence);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean owesDropTombstoneConfirmation(long id) {
        return ((Map<?, ?>) staticValue("pendingDropTombstoneConfirmations")).containsKey(id);
    }

    public static boolean holdsUnresolvedMutationWindow() {
        return !((Map<?, ?>) staticValue("unresolvedMutationWindows")).isEmpty();
    }

    /**
     * The count statements the prune visibility proof runs for one schema fingerprint
     * shape (see the round-72 fix): an EMPTY fingerprint stores two shapes, so the proof
     * must cover both.
     */
    @SuppressWarnings("unchecked")
    public static java.util.List<String> countVersionsBeforeSqlTexts(String schemaFingerprint) {
        return (java.util.List<String>) invokeStatic("countVersionsBeforeSqlTexts",
                new Class<?>[] {String.class}, schemaFingerprint);
    }

    /**
     * The stored second (epoch millis) the single HELD mutation window expects: a snapshot
     * row only proves the outcome when it carries this second or a LATER one (a same-status
     * row the intent supersedes is NOT the outcome, see round-71 #10a).
     */
    public static long heldMutationWindowAttemptedUpdateTime() {
        Map<?, ?> windows = (Map<?, ?>) staticValue("unresolvedMutationWindows");
        if (windows.isEmpty()) {
            throw new IllegalStateException("no held mutation window");
        }
        Object window = windows.values().iterator().next();
        try {
            Field attempted = window.getClass().getDeclaredField("attemptedUpdateTimeMillis");
            attempted.setAccessible(true);
            return (Long) attempted.get(window);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Records one mutation fence (round-63: the by-hash miss reconciliation). */
    public static void recordPendingMutationFence(BaselineManager manager, long id,
            org.apache.doris.nereids.spm.BaselineStatus status, long updateTimeMs) {
        invoke(manager, "recordPendingMutationFence",
                new Class<?>[] {long.class, org.apache.doris.nereids.spm.BaselineStatus.class,
                        long.class},
                id, status, updateTimeMs);
    }

    public static void removePendingMutationFence(BaselineManager manager, long id) {
        ((Map<?, ?>) get(manager, "pendingMutationFences")).remove(id);
    }

    /** Publishes one CONFIRMED status flip (round-63: the epoch bump lives here). */
    public static void publishStatus(BaselineManager manager, BaselinePlan plan,
            org.apache.doris.nereids.spm.BaselineStatus status, long updateTime) {
        invoke(manager, "publishStatus",
                new Class<?>[] {BaselinePlan.class,
                        org.apache.doris.nereids.spm.BaselineStatus.class, long.class},
                plan, status, updateTime);
    }

    /** Holds one unresolved mutation window open (round-63: the pre-snapshot settlement). */
    public static boolean holdUnresolvedMutationWindow(BaselineManager manager, long tick,
            BaselinePlan row) {
        return (Boolean) invoke(manager, "holdUnresolvedMutationWindow",
                new Class<?>[] {long.class, BaselinePlan.class}, tick, row);
    }

    /** Drives the pre-snapshot settlement of held mutation windows (round-63). */
    public static void settleHeldMutationWindowsBeforeSnapshot(BaselineManager manager) {
        invoke(manager, "settleHeldMutationWindowsBeforeSnapshot", new Class<?>[0]);
    }

    /** readStableRawSnapshot with the pre-attempt settlement callback (round-63). */
    public static Map<Long, List<BaselinePlan>> readStableRawSnapshotWithSettle(
            BaselineManager.SnapshotPageReader reader,
            BaselineManager.SnapshotFenceReader fenceReader, Runnable settleBeforeAttempt) {
        return (Map<Long, List<BaselinePlan>>) invokeStatic("readStableRawSnapshot",
                new Class<?>[] {BaselineManager.SnapshotPageReader.class,
                        BaselineManager.SnapshotFenceReader.class, Runnable.class},
                reader, fenceReader, settleBeforeAttempt);
    }

    public static void setMutationTickSalt(long salt) {
        staticFieldSet("mutationTickSalt", salt);
    }

    private static void staticFieldSet(String name, Object value) {
        try {
            field(name).set(null, value);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    // ==================== test drivers ====================

    public static void scheduleAsyncLoad(BaselineManager manager) {
        invoke(manager, "scheduleAsyncLoad", new Class<?>[0]);
    }

    public static void invalidatePublishedStore(BaselineManager manager) {
        invoke(manager, "invalidatePublishedStore", new Class<?>[0]);
    }

    public static void writeHwmRecord(BaselineManager manager, long lastId, long tick, long pending) {
        invoke(manager, "writeHwmRecord", new Class<?>[] {long.class, long.class, long.class, boolean.class},
                lastId, tick, pending, true);
    }

    // ==================== reads ====================

    public static long compactIdWatermark(BaselineManager manager) {
        return (Long) invoke(manager, "readCompactIdWatermark", new Class<?>[0]);
    }

    public static BaselineManager.SnapshotFence snapshotFence(BaselineManager manager) {
        return (BaselineManager.SnapshotFence) invoke(manager, "readSnapshotFence", new Class<?>[0]);
    }

    public static BaselineManager.SnapshotFenceReader fenceReader(BaselineManager manager) {
        return () -> snapshotFence(manager);
    }

    public static Set<String> droppedIdentities(BaselineManager manager, Collection<Long> ids, boolean durableOnly) {
        String name = durableOnly ? "readDroppedIdentitiesDurable" : "readDroppedIdentities";
        return (Set<String>) invoke(manager, name, new Class<?>[] {Collection.class}, ids);
    }

    public static Map<Long, BaselinePlan> collectFilteredSnapshot(BaselineManager manager,
            BaselineManager.SnapshotPageReader reader) {
        return (Map<Long, BaselinePlan>) invoke(manager, "collectFilteredSnapshot",
                new Class<?>[] {BaselineManager.SnapshotPageReader.class}, reader);
    }

    public static Map<Long, BaselinePlan> readStableSnapshot(BaselineManager.SnapshotPageReader reader,
            BaselineManager.SnapshotFenceReader fenceReader) {
        return (Map<Long, BaselinePlan>) invokeStatic("readStableSnapshot",
                new Class<?>[] {BaselineManager.SnapshotPageReader.class,
                        BaselineManager.SnapshotFenceReader.class},
                reader, fenceReader);
    }

    public static Map<Long, BaselinePlan> collectSnapshotPages(BaselineManager.SnapshotPageReader reader,
            int pageSize) {
        return (Map<Long, BaselinePlan>) invokeStatic("collectSnapshotPages",
                new Class<?>[] {BaselineManager.SnapshotPageReader.class, int.class}, reader, pageSize);
    }

    public static Map<Long, BaselinePlan> collectSnapshotPages(BaselineManager.SnapshotPageReader reader,
            int pageSize, AtomicLong rowsReadSink) {
        return (Map<Long, BaselinePlan>) invokeStatic("collectSnapshotPages",
                new Class<?>[] {BaselineManager.SnapshotPageReader.class, int.class, AtomicLong.class},
                reader, pageSize, rowsReadSink);
    }

    public static Map<Long, List<BaselinePlan>> collectSnapshotRows(BaselineManager.SnapshotPageReader reader,
            int pageSize, AtomicLong rowsReadSink) {
        return (Map<Long, List<BaselinePlan>>) invokeStatic("collectSnapshotRows",
                new Class<?>[] {BaselineManager.SnapshotPageReader.class, int.class, AtomicLong.class},
                reader, pageSize, rowsReadSink);
    }

    public static String snapshotPageSql(Long pageStart, long offset) {
        return (String) invokeStatic("snapshotPageSql", new Class<?>[] {Long.class, long.class}, pageStart, offset);
    }

    public static boolean mustScanDurableForKey(BaselineManager manager, long tableWatermark) {
        return (Boolean) invoke(manager, "mustScanDurableForKey", new Class<?>[] {long.class}, tableWatermark);
    }

    // ==================== static helpers ====================

    public static long internalIoMode() {
        try {
            Class<?> ioType = Class.forName("org.apache.doris.nereids.spm.manager.BaselineManager$InternalIo");
            InvocationHandler handler = (proxy, method, args) -> SqlModeHelper.currentMode();
            Object body = Proxy.newProxyInstance(ioType.getClassLoader(), new Class<?>[] {ioType}, handler);
            return (Long) method(MANAGER, "inInternalIoMode", ioType).invoke(null, body);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static BaselinePlan parsePersistedRow(ResultRow row) {
        return (BaselinePlan) invokeStatic("parsePersistedRow", new Class<?>[] {ResultRow.class}, row);
    }

    public static BaselinePlan pickDurableWinner(BaselinePlan first, BaselinePlan second) {
        return (BaselinePlan) invokeStatic("pickDurableWinner",
                new Class<?>[] {BaselinePlan.class, BaselinePlan.class}, first, second);
    }

    public static boolean observedInsertRowIsOurs(BaselineManager manager, BaselinePlan plan) {
        return (Boolean) invoke(manager, "observedInsertRowIsOurs", new Class<?>[] {BaselinePlan.class}, plan);
    }

    public static long tombstoneSeqId(long baselineId) {
        return (Long) invokeStatic("tombstoneSeqId", new Class<?>[] {long.class}, baselineId);
    }

    public static boolean insertWroteRows(long affectedRows) {
        return (Boolean) invokeStatic("insertWroteRows", new Class<?>[] {long.class}, affectedRows);
    }

    public static String toTs(long epochMillis) {
        return (String) invokeStatic("toTs", new Class<?>[] {long.class}, epochMillis);
    }

    public static String droppedMarkerInsertSql() {
        return (String) staticValue("INSERT_SEQ_DROPPED_SQL");
    }

    public static String droppedMarkerReadSql() {
        return (String) staticValue("SELECT_SEQ_DROPPED_SQL");
    }

    public static String pendingSeqLookupSql() {
        return (String) staticValue("SELECT_COMPACT_SEQ_SQL");
    }

    public static String[] pruneHwmSql() {
        return new String[] {
                (String) staticValue("PRUNE_HWM_SQL"),
                (String) staticValue("PRUNE_HWM_SAME_WATERMARK_SQL"),
                (String) staticValue("PRUNE_HWM_OPEN_SQL")};
    }
}

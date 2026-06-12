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

package org.apache.doris.foundation.util;

import it.unimi.dsi.fastutil.longs.AbstractLong2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectFunction;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * A concurrent map with primitive long keys and object values, backed by segmented
 * {@link Long2ObjectOpenHashMap} instances with {@link ReentrantReadWriteLock} per segment.
 *
 * <p>This class provides similar concurrency guarantees to {@link java.util.concurrent.ConcurrentHashMap}
 * while avoiding the memory overhead of boxing long keys. For a cluster with millions of tablet entries,
 * this saves ~32 bytes per entry compared to {@code ConcurrentHashMap<Long, V>}.
 *
 * <p>Like {@link java.util.concurrent.ConcurrentHashMap}, null values are not permitted.
 *
 * <p>Iteration methods ({@link #long2ObjectEntrySet()}, {@link #keySet()}, {@link #values()})
 * return snapshot copies and are weakly consistent.
 *
 * <p><b>Important:</b> All compound operations from both {@link Long2ObjectMap} and {@link Map}
 * interfaces (computeIfAbsent, computeIfPresent, compute, merge, putIfAbsent, replace, remove)
 * are overridden to ensure atomicity within a segment.
 *
 * @param <V> the type of mapped values
 */
public class ConcurrentLong2ObjectHashMap<V> extends AbstractLong2ObjectMap<V> {

    private static final int DEFAULT_SEGMENT_COUNT = 16;
    private static final int DEFAULT_INITIAL_CAPACITY_PER_SEGMENT = 16;

    private final Segment<V>[] segments;
    private final int segmentMask;
    private final int segmentBits;

    public ConcurrentLong2ObjectHashMap() {
        this(DEFAULT_SEGMENT_COUNT);
    }

    @SuppressWarnings("unchecked")
    public ConcurrentLong2ObjectHashMap(int segmentCount) {
        if (segmentCount <= 0 || (segmentCount & (segmentCount - 1)) != 0) {
            throw new IllegalArgumentException("segmentCount must be a positive power of 2: " + segmentCount);
        }
        this.segmentBits = Integer.numberOfTrailingZeros(segmentCount);
        this.segmentMask = segmentCount - 1;
        this.segments = new Segment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment<>(DEFAULT_INITIAL_CAPACITY_PER_SEGMENT);
        }
    }

    /** Murmur3 64-bit finalizer for segment selection. */
    private static long mix(long x) {
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        x ^= x >>> 33;
        return x;
    }

    private Segment<V> segmentFor(long key) {
        return segments[(int) (mix(key) >>> (64 - segmentBits)) & segmentMask];
    }

    // ---- Read operations (read-lock) ----

    @Override
    public V get(long key) {
        Segment<V> seg = segmentFor(key);
        seg.lock.readLock().lock();
        try {
            return seg.map.get(key);
        } finally {
            seg.lock.readLock().unlock();
        }
    }

    public V getOrDefault(long key, V defaultValue) {
        Segment<V> seg = segmentFor(key);
        seg.lock.readLock().lock();
        try {
            V val = seg.map.get(key);
            return (val != null || seg.map.containsKey(key)) ? val : defaultValue;
        } finally {
            seg.lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(long key) {
        Segment<V> seg = segmentFor(key);
        seg.lock.readLock().lock();
        try {
            return seg.map.containsKey(key);
        } finally {
            seg.lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        for (Segment<V> seg : segments) {
            seg.lock.readLock().lock();
            try {
                if (seg.map.containsValue(value)) {
                    return true;
                }
            } finally {
                seg.lock.readLock().unlock();
            }
        }
        return false;
    }

    @Override
    public int size() {
        long total = 0;
        for (Segment<V> seg : segments) {
            seg.lock.readLock().lock();
            try {
                total += seg.map.size();
            } finally {
                seg.lock.readLock().unlock();
            }
        }
        return (int) Math.min(total, Integer.MAX_VALUE);
    }

    @Override
    public boolean isEmpty() {
        for (Segment<V> seg : segments) {
            seg.lock.readLock().lock();
            try {
                if (!seg.map.isEmpty()) {
                    return false;
                }
            } finally {
                seg.lock.readLock().unlock();
            }
        }
        return true;
    }

    // ---- Write operations (write-lock) ----

    @Override
    public V put(long key, V value) {
        Objects.requireNonNull(value, "Null values are not permitted");
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            return seg.map.put(key, value);
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public V remove(long key) {
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            return seg.map.remove(key);
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    public V putIfAbsent(long key, V value) {
        Objects.requireNonNull(value, "Null values are not permitted");
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            V existing = seg.map.get(key);
            if (existing != null || seg.map.containsKey(key)) {
                return existing;
            }
            seg.map.put(key, value);
            return null;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    public boolean replace(long key, V oldValue, V newValue) {
        Objects.requireNonNull(newValue, "Null values are not permitted");
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            V curValue = seg.map.get(key);
            if (!Objects.equals(curValue, oldValue) || (curValue == null && !seg.map.containsKey(key))) {
                return false;
            }
            seg.map.put(key, newValue);
            return true;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    public V replace(long key, V value) {
        Objects.requireNonNull(value, "Null values are not permitted");
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            if (seg.map.containsKey(key)) {
                return seg.map.put(key, value);
            }
            return null;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        if (!(key instanceof Long)) {
            return false;
        }
        long k = (Long) key;
        Segment<V> seg = segmentFor(k);
        seg.lock.writeLock().lock();
        try {
            V curValue = seg.map.get(k);
            if (!Objects.equals(curValue, value) || (curValue == null && !seg.map.containsKey(k))) {
                return false;
            }
            seg.map.remove(k);
            return true;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        for (Segment<V> seg : segments) {
            seg.lock.writeLock().lock();
            try {
                seg.map.clear();
            } finally {
                seg.lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void putAll(Map<? extends Long, ? extends V> m) {
        for (Map.Entry<? extends Long, ? extends V> entry : m.entrySet()) {
            put(entry.getKey().longValue(), entry.getValue());
        }
    }

    // ---- Atomic compound operations ----
    // Override ALL compound methods from both Long2ObjectMap and Map interfaces
    // to ensure the check-then-act is atomic within a segment's write lock.

    public V computeIfAbsent(long key, LongFunction<? extends V> mappingFunction) {
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            V val = seg.map.get(key);
            if (val != null || seg.map.containsKey(key)) {
                return val;
            }
            V newValue = mappingFunction.apply(key);
            if (newValue != null) {
                seg.map.put(key, newValue);
            }
            return newValue;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    public V computeIfAbsent(long key, Long2ObjectFunction<? extends V> mappingFunction) {
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            V val = seg.map.get(key);
            if (val != null || seg.map.containsKey(key)) {
                return val;
            }
            V newValue = mappingFunction.get(key);
            if (newValue != null) {
                seg.map.put(key, newValue);
            }
            return newValue;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public V computeIfAbsent(Long key, Function<? super Long, ? extends V> mappingFunction) {
        return computeIfAbsent(key.longValue(), (long k) -> mappingFunction.apply(k));
    }

    public V computeIfPresent(long key, BiFunction<? super Long, ? super V, ? extends V> remappingFunction) {
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            V oldValue = seg.map.get(key);
            if (oldValue != null || seg.map.containsKey(key)) {
                V newValue = remappingFunction.apply(key, oldValue);
                if (newValue != null) {
                    seg.map.put(key, newValue);
                } else {
                    seg.map.remove(key);
                }
                return newValue;
            }
            return null;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    public V compute(long key, BiFunction<? super Long, ? super V, ? extends V> remappingFunction) {
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            V oldValue = seg.map.containsKey(key) ? seg.map.get(key) : null;
            V newValue = remappingFunction.apply(key, oldValue);
            if (newValue != null) {
                seg.map.put(key, newValue);
            } else if (seg.map.containsKey(key)) {
                seg.map.remove(key);
            }
            return newValue;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    public V merge(long key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        Segment<V> seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            V oldValue = seg.map.get(key);
            V newValue;
            if (oldValue != null || seg.map.containsKey(key)) {
                newValue = remappingFunction.apply(oldValue, value);
            } else {
                newValue = value;
            }
            if (newValue != null) {
                seg.map.put(key, newValue);
            } else {
                seg.map.remove(key);
            }
            return newValue;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    // ---- Iteration (weakly consistent snapshots) ----

    @Override
    public ObjectSet<Long2ObjectMap.Entry<V>> long2ObjectEntrySet() {
        ObjectOpenHashSet<Long2ObjectMap.Entry<V>> snapshot = new ObjectOpenHashSet<>();
        for (Segment<V> seg : segments) {
            seg.lock.readLock().lock();
            try {
                for (Long2ObjectMap.Entry<V> entry : seg.map.long2ObjectEntrySet()) {
                    snapshot.add(new AbstractLong2ObjectMap.BasicEntry<>(entry.getLongKey(), entry.getValue()));
                }
            } finally {
                seg.lock.readLock().unlock();
            }
        }
        return snapshot;
    }

    @Override
    public LongSet keySet() {
        LongOpenHashSet snapshot = new LongOpenHashSet();
        for (Segment<V> seg : segments) {
            seg.lock.readLock().lock();
            try {
                snapshot.addAll(seg.map.keySet());
            } finally {
                seg.lock.readLock().unlock();
            }
        }
        return snapshot;
    }

    /**
     * Returns the keys as a {@link LongArrayList}. Useful when callers need indexed access
     * or will iterate the keys once. Snapshot-based and weakly consistent.
     */
    public LongArrayList keyList() {
        LongArrayList list = new LongArrayList(size());
        for (Segment<V> seg : segments) {
            seg.lock.readLock().lock();
            try {
                list.addAll(seg.map.keySet());
            } finally {
                seg.lock.readLock().unlock();
            }
        }
        return list;
    }

    @Override
    public ObjectCollection<V> values() {
        ObjectArrayList<V> snapshot = new ObjectArrayList<>();
        for (Segment<V> seg : segments) {
            seg.lock.readLock().lock();
            try {
                snapshot.addAll(seg.map.values());
            } finally {
                seg.lock.readLock().unlock();
            }
        }
        return snapshot;
    }

    /**
     * Applies the given action to each entry under read-lock per segment.
     * This is more efficient than iterating {@link #long2ObjectEntrySet()} as it avoids
     * creating a snapshot.
     */
    public void forEach(LongObjConsumer<? super V> action) {
        for (Segment<V> seg : segments) {
            seg.lock.readLock().lock();
            try {
                for (Long2ObjectMap.Entry<V> entry : seg.map.long2ObjectEntrySet()) {
                    action.accept(entry.getLongKey(), entry.getValue());
                }
            } finally {
                seg.lock.readLock().unlock();
            }
        }
    }

    @FunctionalInterface
    public interface LongObjConsumer<V> {
        void accept(long key, V value);
    }

    // ---- Segment inner class ----

    private static final class Segment<V> {
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        final Long2ObjectOpenHashMap<V> map;

        Segment(int initialCapacity) {
            this.map = new Long2ObjectOpenHashMap<>(initialCapacity);
        }
    }
}

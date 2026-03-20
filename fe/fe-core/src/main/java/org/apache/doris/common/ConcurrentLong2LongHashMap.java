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

package org.apache.doris.common;

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.AbstractLong2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongFunction;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

/**
 * A concurrent map with primitive long keys and primitive long values, backed by segmented
 * {@link Long2LongOpenHashMap} instances with {@link ReentrantReadWriteLock} per segment.
 *
 * <p>This class saves ~48 bytes per entry compared to {@code ConcurrentHashMap<Long, Long>}
 * by avoiding boxing of both keys and values. For fields like partition update row counts
 * with millions of entries, this translates to hundreds of MB of heap savings.
 *
 * <p>The {@link #addTo(long, long)} method provides atomic increment semantics, useful for
 * counter patterns.
 *
 * <p><b>Note:</b> The {@code defaultReturnValue} is fixed at 0. Calling
 * {@code defaultReturnValue(long)} on this wrapper will NOT propagate to the underlying
 * segment maps, and reads/removes will still return 0 for missing keys.
 *
 * <p><b>Important:</b> All primitive-key compound operations (computeIfAbsent, computeIfPresent,
 * compute, merge, mergeLong, putIfAbsent, replace, remove, addTo) are overridden to ensure
 * atomicity within a segment. Boxed {@link Map}-level compound methods delegate to these
 * primitive overrides via fastutil's bridge methods, so they are also atomic.
 */
public class ConcurrentLong2LongHashMap extends AbstractLong2LongMap {

    private static final int DEFAULT_SEGMENT_COUNT = 16;
    private static final int DEFAULT_INITIAL_CAPACITY_PER_SEGMENT = 16;

    private final Segment[] segments;
    private final int segmentMask;
    private final int segmentBits;

    public ConcurrentLong2LongHashMap() {
        this(DEFAULT_SEGMENT_COUNT);
    }

    public ConcurrentLong2LongHashMap(int segmentCount) {
        if (segmentCount <= 0 || (segmentCount & (segmentCount - 1)) != 0) {
            throw new IllegalArgumentException("segmentCount must be a positive power of 2: " + segmentCount);
        }
        this.segmentBits = Integer.numberOfTrailingZeros(segmentCount);
        this.segmentMask = segmentCount - 1;
        this.segments = new Segment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment(DEFAULT_INITIAL_CAPACITY_PER_SEGMENT);
        }
    }

    private Segment segmentFor(long key) {
        return segments[(int) (HashCommon.mix(key) >>> (64 - segmentBits)) & segmentMask];
    }

    // ---- Read operations (read-lock) ----

    @Override
    public long get(long key) {
        Segment seg = segmentFor(key);
        seg.lock.readLock().lock();
        try {
            return seg.map.get(key);
        } finally {
            seg.lock.readLock().unlock();
        }
    }

    @Override
    public long getOrDefault(long key, long defaultValue) {
        Segment seg = segmentFor(key);
        seg.lock.readLock().lock();
        try {
            return seg.map.getOrDefault(key, defaultValue);
        } finally {
            seg.lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(long key) {
        Segment seg = segmentFor(key);
        seg.lock.readLock().lock();
        try {
            return seg.map.containsKey(key);
        } finally {
            seg.lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsValue(long value) {
        for (Segment seg : segments) {
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
        for (Segment seg : segments) {
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
        for (Segment seg : segments) {
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
    public long put(long key, long value) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            return seg.map.put(key, value);
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public long remove(long key) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            return seg.map.remove(key);
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public long putIfAbsent(long key, long value) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            return seg.map.putIfAbsent(key, value);
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public boolean replace(long key, long oldValue, long newValue) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            return seg.map.replace(key, oldValue, newValue);
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public long replace(long key, long value) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            return seg.map.replace(key, value);
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        if (!(key instanceof Long) || !(value instanceof Long)) {
            return false;
        }
        long k = (Long) key;
        long v = (Long) value;
        Segment seg = segmentFor(k);
        seg.lock.writeLock().lock();
        try {
            if (!seg.map.containsKey(k) || seg.map.get(k) != v) {
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
        for (Segment seg : segments) {
            seg.lock.writeLock().lock();
            try {
                seg.map.clear();
            } finally {
                seg.lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void putAll(Map<? extends Long, ? extends Long> m) {
        for (Map.Entry<? extends Long, ? extends Long> entry : m.entrySet()) {
            put(entry.getKey().longValue(), entry.getValue().longValue());
        }
    }

    // ---- Atomic compound operations ----
    // Override ALL compound methods from both Long2LongMap and Map interfaces.

    /**
     * Atomically adds the given increment to the value associated with the key.
     * If the key is not present, the entry is created with the increment as value
     * (starting from defaultReturnValue, which is 0L by default).
     *
     * @return the new value after the increment
     */
    public long addTo(long key, long increment) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            long newValue = seg.map.addTo(key, increment) + increment;
            return newValue;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public long computeIfAbsent(long key, LongUnaryOperator mappingFunction) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            if (seg.map.containsKey(key)) {
                return seg.map.get(key);
            }
            long newValue = mappingFunction.applyAsLong(key);
            seg.map.put(key, newValue);
            return newValue;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public long computeIfAbsent(long key, Long2LongFunction mappingFunction) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            if (seg.map.containsKey(key)) {
                return seg.map.get(key);
            }
            long newValue = mappingFunction.get(key);
            seg.map.put(key, newValue);
            return newValue;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public Long computeIfAbsent(Long key, Function<? super Long, ? extends Long> mappingFunction) {
        long k = key.longValue();
        Segment seg = segmentFor(k);
        seg.lock.writeLock().lock();
        try {
            if (seg.map.containsKey(k)) {
                return seg.map.get(k);
            }
            Long newValue = mappingFunction.apply(key);
            if (newValue != null) {
                seg.map.put(k, newValue.longValue());
            }
            return newValue;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public long computeIfPresent(long key,
            BiFunction<? super Long, ? super Long, ? extends Long> remappingFunction) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            if (!seg.map.containsKey(key)) {
                return defaultReturnValue();
            }
            long oldValue = seg.map.get(key);
            Long newValue = remappingFunction.apply(key, oldValue);
            if (newValue != null) {
                seg.map.put(key, newValue.longValue());
                return newValue;
            } else {
                seg.map.remove(key);
                return defaultReturnValue();
            }
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public long compute(long key, BiFunction<? super Long, ? super Long, ? extends Long> remappingFunction) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            Long oldValue = seg.map.containsKey(key) ? seg.map.get(key) : null;
            Long newValue = remappingFunction.apply(key, oldValue);
            if (newValue != null) {
                seg.map.put(key, newValue.longValue());
                return newValue;
            } else if (oldValue != null) {
                seg.map.remove(key);
            }
            return defaultReturnValue();
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public long merge(long key, long value,
            BiFunction<? super Long, ? super Long, ? extends Long> remappingFunction) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            if (!seg.map.containsKey(key)) {
                seg.map.put(key, value);
                return value;
            }
            long oldValue = seg.map.get(key);
            Long newValue = remappingFunction.apply(oldValue, value);
            if (newValue != null) {
                seg.map.put(key, newValue.longValue());
                return newValue;
            } else {
                seg.map.remove(key);
                return defaultReturnValue();
            }
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    @Override
    public long mergeLong(long key, long value, java.util.function.LongBinaryOperator remappingFunction) {
        Segment seg = segmentFor(key);
        seg.lock.writeLock().lock();
        try {
            if (!seg.map.containsKey(key)) {
                seg.map.put(key, value);
                return value;
            }
            long oldValue = seg.map.get(key);
            long newValue = remappingFunction.applyAsLong(oldValue, value);
            seg.map.put(key, newValue);
            return newValue;
        } finally {
            seg.lock.writeLock().unlock();
        }
    }

    // ---- Iteration (weakly consistent snapshots) ----

    @Override
    public ObjectSet<Long2LongMap.Entry> long2LongEntrySet() {
        ObjectOpenHashSet<Long2LongMap.Entry> snapshot = new ObjectOpenHashSet<>();
        for (Segment seg : segments) {
            seg.lock.readLock().lock();
            try {
                for (Long2LongMap.Entry entry : seg.map.long2LongEntrySet()) {
                    snapshot.add(new AbstractLong2LongMap.BasicEntry(entry.getLongKey(), entry.getLongValue()));
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
        for (Segment seg : segments) {
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
     * Returns the keys as a {@link LongArrayList}.
     */
    public LongArrayList keyList() {
        LongArrayList list = new LongArrayList(size());
        for (Segment seg : segments) {
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
    public it.unimi.dsi.fastutil.longs.LongCollection values() {
        LongArrayList snapshot = new LongArrayList();
        for (Segment seg : segments) {
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
     */
    public void forEach(LongLongConsumer action) {
        for (Segment seg : segments) {
            seg.lock.readLock().lock();
            try {
                for (Long2LongMap.Entry entry : seg.map.long2LongEntrySet()) {
                    action.accept(entry.getLongKey(), entry.getLongValue());
                }
            } finally {
                seg.lock.readLock().unlock();
            }
        }
    }

    @FunctionalInterface
    public interface LongLongConsumer {
        void accept(long key, long value);
    }

    // ---- Segment inner class ----

    private static final class Segment {
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        final Long2LongOpenHashMap map;

        Segment(int initialCapacity) {
            this.map = new Long2LongOpenHashMap(initialCapacity);
        }
    }
}

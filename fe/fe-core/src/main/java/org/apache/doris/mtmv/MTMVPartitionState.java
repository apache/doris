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

package org.apache.doris.mtmv;

import com.google.gson.annotations.SerializedName;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The per-partition refresh state of one MV partition.
 *
 * <p>{@code refreshEpoch} says which generation of data the partition currently holds, {@code
 * latestEpoch} says which generation it must hold. A partition whose {@code latestEpoch} is ahead of
 * its {@code refreshEpoch} is dirty: it holds rows read before a metadata-only change of a base table
 * (a dropped / truncated / replaced / recovered partition emits no row binlog), so those rows can no
 * longer be removed incrementally and the partition has to be rebuilt.
 *
 * <p>Keyed by MV partition name in {@code MTMV.partitionStates}. The name is deliberately the only
 * identity: an MV partition is rewritten by {@code INSERT OVERWRITE} on every refresh and gets a new
 * partition id each time, so an id would stop matching as soon as the partition is refreshed.
 *
 * <p>The two values are plain {@code long}s rather than atomics because this is a persisted DTO: it is
 * serialized into the alter journal, so it has to stay a plain bean.
 */
public class MTMVPartitionState {
    /** The generation of the data this MV partition currently holds; 0 means it was never refreshed. */
    @SerializedName("re")
    private long refreshEpoch;

    /** The generation the data must reach; starts at 1 and grows on every invalidation. */
    @SerializedName("le")
    private long latestEpoch;


    public MTMVPartitionState() {
    }

    public MTMVPartitionState(long refreshEpoch, long latestEpoch) {
        this.refreshEpoch = refreshEpoch;
        this.latestEpoch = latestEpoch;
    }

    public MTMVPartitionState(MTMVPartitionState other) {
        this.refreshEpoch = other.refreshEpoch;
        this.latestEpoch = other.latestEpoch;
    }

    /**
     * The state a partition gets when it is first aligned: never refreshed, one generation required.
     *
     * <p>Alignment only ever creates this pair, so "no entry yet" and "this pair" say the same thing
     * about the past -- the partition was never marked and holds no rows.
     */
    public static MTMVPartitionState initial() {
        return new MTMVPartitionState(0, 1);
    }

    /**
     * Whether this partition holds rows that a metadata-only change of a base table has made unusable,
     * so it has to be rebuilt rather than caught up incrementally.
     *
     * <p>{@code refreshEpoch == 0} carries no exemption. It reads as "the partition was never refreshed,
     * so it holds no rows", and that is not durable: a refresh commits the MV data transaction before its
     * task result publishes the epochs, so a crash in between leaves rows in a partition whose state says
     * never refreshed ({@code {0, 1}}). Reading that pair as clean would let a later invalidation raising
     * {@code latestEpoch} -- {@code {0, 2}} -- go unnoticed, and a strict INCREMENTAL refresh would keep
     * rows nothing can remove. Without the exemption the cut closes on its own: {@code 2 > 0} is dirty.
     *
     * <p>The cost is that a fresh MV's first refresh rebuilds every partition instead of skipping the
     * partitions whose base partitions have no rows. That is the safe direction, and it is the same
     * reading the whole-MV escalation already used ("every partition is dirty or never refreshed").
     */
    public boolean isDirty() {
        return latestEpoch > refreshEpoch;
    }


    /** Whether the partition was never refreshed, which means it holds no rows. */
    public boolean isNeverRefreshed() {
        return refreshEpoch == 0;
    }

    /**
     * Deep-copies a state map, or returns null for null.
     *
     * <p>The journal needs this on both sides. A payload is serialized by the journal thread, which
     * runs after the submitting thread released the MV lock, so a payload that shared state with the
     * live map could be written out half-mutated. The replay path goes through the same helper so that
     * both sides of the journal follow one rule instead of two.
     */
    public static Map<String, MTMVPartitionState> copyOf(Map<String, MTMVPartitionState> states) {
        if (states == null) {
            return null;
        }
        Map<String, MTMVPartitionState> copy = new LinkedHashMap<>();
        for (Entry<String, MTMVPartitionState> entry : states.entrySet()) {
            copy.put(entry.getKey(), new MTMVPartitionState(entry.getValue()));
        }
        return copy;
    }

    public long getRefreshEpoch() {
        return refreshEpoch;
    }

    public void setRefreshEpoch(long refreshEpoch) {
        this.refreshEpoch = refreshEpoch;
    }

    public long getLatestEpoch() {
        return latestEpoch;
    }

    public void setLatestEpoch(long latestEpoch) {
        this.latestEpoch = latestEpoch;
    }

}

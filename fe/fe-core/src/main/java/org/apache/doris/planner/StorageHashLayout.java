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

package org.apache.doris.planner;

import org.apache.doris.catalog.HashDistributionInfo.HashType;

import com.google.common.base.Preconditions;

/**
 * Storage bucketing algorithm of a plan subtree, not its execution-shuffle algorithm.
 * UNKNOWN retains the one algorithm observed in partially bucketed inputs. This lets a later
 * merge detect a conflict without a second traversal of the subtree.
 */
public final class StorageHashLayout {
    public enum State {
        NONE,
        KNOWN,
        UNKNOWN,
        CONFLICT
    }

    public static final StorageHashLayout NONE = new StorageHashLayout(State.NONE, null);
    public static final StorageHashLayout CONFLICT = new StorageHashLayout(State.CONFLICT, null);

    private final State state;
    // Present for KNOWN and UNKNOWN only. UNKNOWN must never be used as a bucket-shuffle target.
    private final HashType hashType;

    private StorageHashLayout(State state, HashType hashType) {
        this.state = state;
        this.hashType = hashType;
    }

    public static StorageHashLayout known(HashType hashType) {
        return new StorageHashLayout(State.KNOWN, Preconditions.checkNotNull(hashType));
    }

    public State getState() {
        return state;
    }

    public boolean isKnown() {
        return state == State.KNOWN;
    }

    public HashType getHashType() {
        Preconditions.checkState(isKnown(), "Expected a known storage hash layout, got %s", this);
        return hashType;
    }

    /** Merge all inputs, including layout-less ones; NONE is not an identity element. */
    public StorageHashLayout merge(StorageHashLayout other) {
        if (state == State.CONFLICT || other.state == State.CONFLICT) {
            return CONFLICT;
        }
        if (hashType != null && other.hashType != null && hashType != other.hashType) {
            return CONFLICT;
        }
        HashType mergedHashType = hashType != null ? hashType : other.hashType;
        if (mergedHashType == null) {
            return NONE;
        }
        return new StorageHashLayout(isKnown() && other.isKnown() ? State.KNOWN : State.UNKNOWN,
                mergedHashType);
    }

    public StorageHashLayout checkNoConflict(String context) {
        Preconditions.checkState(state != State.CONFLICT,
                "%s mixes distribution hash types; bucket local exchanges need one "
                        + "unambiguous storage layout, re-align the inputs explicitly", context);
        return this;
    }

    @Override
    public String toString() {
        return hashType == null ? state.name() : state.name() + "(" + hashType + ")";
    }
}

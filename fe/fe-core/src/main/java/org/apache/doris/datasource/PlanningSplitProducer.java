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

package org.apache.doris.datasource;

import org.apache.doris.common.UserException;

/**
 * Producer-side abstraction for split planning.
 *
 * <p>The producer hides how a datasource plans splits from the generic consumers in
 * {@link FileQueryScanNode} and {@link SplitAssignment}. Implementations may plan splits
 * synchronously in the caller thread or asynchronously in background workers, but they must
 * publish all planned splits to the provided {@link SplitSink}.
 */
public interface PlanningSplitProducer {
    /**
     * Whether this producer should be consumed through the lazy batch protocol in
     * {@link SplitAssignment}.
     *
     * <p>When {@code false}, callers normally collect all planned splits eagerly and assign them
     * in one shot. When {@code true}, callers expose a split source to backends and let them fetch
     * planned batches on demand.
     */
    default boolean isBatchMode() {
        return false;
    }

    /**
     * Returns an estimated split count for explain output and scan concurrency estimation.
     *
     * <p>The value is only used when {@link #isBatchMode()} is {@code true}. Return {@code -1}
     * when the producer cannot provide a stable estimate.
     */
    default int numApproximateSplits() {
        return -1;
    }

    /**
     * Exposes read-only planning metadata collected by this producer.
     *
     * <p>This is intended for side-channel information such as explain details or partition
     * statistics. Callers must tolerate the metadata being empty or partially populated before
     * planning finishes.
     */
    default PlanningSplitMetadata getPlanningMetadata() {
        return PlanningSplitMetadata.EMPTY;
    }

    /**
     * Starts split planning and pushes planned splits into {@code splitSink}.
     *
     * <p>The producer must eventually call {@link SplitSink#finish()} on success or
     * {@link SplitSink#fail(UserException)} on failure. {@code numBackends} is provided so
     * implementations can shape split planning using backend parallelism when needed.
     */
    void start(int numBackends, SplitSink splitSink) throws UserException;

    /**
     * Stops background planning work and releases producer-side resources.
     */
    default void stop() {
    }
}

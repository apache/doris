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

package org.apache.doris.resource;

import org.apache.doris.catalog.Replica;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import java.util.List;
import java.util.function.Function;

/**
 * SPI for optional backend selection hints.
 * <p>
 * Implementations are discovered through {@link java.util.ServiceLoader}. Default methods preserve
 * the existing backend selection behavior. Candidate-ordering methods must not mutate their input list:
 * implementations that change the order must return a new list, while implementations that keep the order
 * may return the input list unchanged.
 */
public interface BackendSelectionPolicy {

    /** Observability classification of how a repair clone source was selected. */
    enum RepairSourceSelectionResult {
        DISABLED,
        PREFERRED_HIT,
        FALLBACK_NO_PREFERRED,
        FALLBACK_PREFERRED_UNAVAILABLE,
        FALLBACK_SLOT_FULL
    }

    /** Whether repair-clone source selection is active. */
    default boolean isRepairSourceSelectionEnabled() {
        return false;
    }

    /**
     * Reorder healthy clone-source candidates while preserving the caller's existing ordering inside
     * each implementation-defined tier. Implementations must not drop candidates.
     */
    default List<Replica> orderRepairSourceCandidates(List<Replica> healthyCandidates, long destBackendId) {
        return healthyCandidates;
    }

    /**
     * Classify which implementation-defined tier the finally chosen source fell into, for observability.
     */
    default RepairSourceSelectionResult classifyRepairSource(long chosenSrcBackendId, long destBackendId,
            List<Replica> allReplicas, List<Replica> healthyCandidates) {
        return RepairSourceSelectionResult.DISABLED;
    }

    default BackendSelection.SelectionHint getQuerySelectionHint(ConnectContext context) {
        return BackendSelection.SelectionHint.noSelection();
    }

    default boolean hasQuerySelectionPreference(BackendSelection.SelectionHint hint) {
        return false;
    }

    /**
     * Classify query selection after the kernel has applied availability and access filters.
     * Implementations must not mutate the candidates and must return a non-null result.
     */
    default <T> BackendSelection.QuerySelectionResult classifyQuerySelection(
            BackendSelection.SelectionHint hint, List<T> candidates, Function<T, Tag> beTagOf) {
        return BackendSelection.QuerySelectionResult.DISABLED;
    }

    /** Whether this provider implements required query and load candidate partitioning. */
    default boolean supportsRequiredSelection() {
        return false;
    }

    /**
     * Partition query candidates for {@link BackendSelection.Mode#REQUIRE}. The two lists must contain every
     * input candidate exactly once using the original instances. The kernel schedules only preferred candidates.
     */
    default <T> BackendSelection.CandidateSelection<T> partitionRequiredQueryCandidates(
            BackendSelection.SelectionHint hint, List<T> candidates, Function<T, Tag> beTagOf) throws UserException {
        throw new UserException("BackendSelectionPolicy does not support required backend selection");
    }

    /**
     * Optionally reorder query scan candidates before the existing scheduler chooses a backend. This
     * is only a placement hint: callers may still apply their normal load-balancing policy after this
     * method. Implementations must not drop candidates.
     */
    default <T> List<T> orderQueryCandidates(BackendSelection.SelectionHint hint, List<T> candidates,
            Function<T, Tag> beTagOf) throws UserException {
        return candidates;
    }

    default boolean isLoadSelectionEnabled(ConnectContext context) {
        return false;
    }

    /**
     * Reorder load candidates without changing the candidate set. Implementations must return every input
     * candidate exactly once and must not add candidates.
     */
    default List<Backend> orderLoadCandidates(BackendSelection.SelectionHint hint,
            List<Backend> candidates) throws UserException {
        return candidates;
    }

    /**
     * Partition load candidates for {@link BackendSelection.Mode#REQUIRE}. The two lists must contain every
     * input candidate exactly once using the original instances. The kernel schedules only preferred candidates.
     */
    default BackendSelection.CandidateSelection<Backend> partitionRequiredLoadCandidates(
            BackendSelection.SelectionHint hint, List<Backend> candidates) throws UserException {
        throw new UserException("BackendSelectionPolicy does not support required backend selection");
    }

    default boolean hasLoadSelectionPreference(BackendSelection.SelectionHint hint) {
        return false;
    }

    default BackendSelection.SelectionHint getLoadSelectionHint(ConnectContext context) {
        return null;
    }

    default BackendSelection.SelectionHint getForwardedLoadSelectionHint(String preferredKey, String mode) {
        return null;
    }
}

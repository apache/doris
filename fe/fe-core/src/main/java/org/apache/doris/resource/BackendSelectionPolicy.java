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
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * SPI for optional backend selection hints.
 * <p>
 * The public default is a pass-through no-op. Extension builds may register an implementation
 * via {@link java.util.ServiceLoader}; {@link BackendSelectionPolicyFactory} then selects it.
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

    /** Whether repair-clone source selection is active. Open-source no-op: always {@code false}. */
    default boolean isRepairSourceSelectionEnabled() {
        return false;
    }

    /**
     * Reorder healthy clone-source candidates while preserving the caller's existing ordering inside
     * each implementation-defined tier. Implementations must not drop candidates.
     * <p>
     * Open-source no-op: returns {@code healthyCandidates} unchanged.
     */
    default List<Replica> orderRepairSourceCandidates(List<Replica> healthyCandidates, long destBackendId) {
        return healthyCandidates;
    }

    /**
     * Classify which implementation-defined tier the finally chosen source fell into, for observability.
     * Open-source no-op: returns {@link RepairSourceSelectionResult#DISABLED}.
     */
    default RepairSourceSelectionResult classifyRepairSource(long chosenSrcBackendId, long destBackendId,
            List<Replica> allReplicas, List<Replica> healthyCandidates) {
        return RepairSourceSelectionResult.DISABLED;
    }

    default BackendSelection.SelectionHint getQuerySelectionHint(ConnectContext context) {
        return BackendSelection.SelectionHint.noSelection();
    }

    default BackendSelection.SelectionHint getQuerySelectionHint(
            ConnectContext context, Set<Tag> allowedTags, boolean needCheckTags) {
        return BackendSelection.SelectionHint.noSelection();
    }

    default boolean hasQuerySelectionPreference(BackendSelection.SelectionHint hint) {
        return false;
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

    /**
     * Optionally reorder only candidates that are otherwise tied by the caller's load-balancing
     * key. Implementations must preserve candidates outside the tied groups.
     */
    default <T> List<T> orderTiedQueryCandidates(BackendSelection.SelectionHint hint,
            List<T> sortedCandidates, Comparator<T> tieKey, Function<T, Tag> beTagOf) throws UserException {
        return sortedCandidates;
    }

    default boolean isLoadSelectionEnabled(ConnectContext context) {
        return false;
    }

    default List<Backend> orderLoadCandidates(ConnectContext context, List<Backend> candidates) throws UserException {
        return candidates;
    }

    default List<Backend> orderLoadCandidates(BackendSelection.SelectionHint hint,
            List<Backend> candidates) throws UserException {
        return candidates;
    }

    default Backend chooseFirstAvailableLoadBackend(ConnectContext context, List<Backend> candidates,
            Predicate<Backend> available) throws UserException {
        for (Backend backend : candidates) {
            if (available.test(backend)) {
                return backend;
            }
        }
        return null;
    }

    default boolean hasLoadSelectionPreference(BackendSelection.SelectionHint hint) {
        return false;
    }

    default Backend chooseLoadBackend(ConnectContext context, List<Backend> candidates)
            throws LoadException {
        for (Backend backend : candidates) {
            if (backend.isLoadAvailable()) {
                return backend;
            }
        }
        return null;
    }

    default BackendSelection.SelectionHint getLoadSelectionHint(ConnectContext context) {
        return null;
    }

    default BackendSelection.SelectionHint getForwardedLoadSelectionHint(String preferredKey, String mode) {
        return null;
    }
}

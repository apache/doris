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
 * SPI for resource-group (tag.location) affinity decisions.
 * <p>
 * The public default is a pass-through no-op. Downstream builds may register a real implementation
 * via {@link java.util.ServiceLoader}; {@link ResourceGroupAffinityPolicyFactory} then selects it.
 */
public interface ResourceGroupAffinityPolicy {

    /** Observability classification of how a repair clone source was selected. */
    enum SrcAffinityResult {
        DISABLED,
        LOCAL_HIT,
        FALLBACK_NO_LOCAL,
        FALLBACK_LOCAL_UNHEALTHY,
        FALLBACK_SLOT_FULL
    }

    /** Whether repair-clone source affinity is active. Open-source no-op: always {@code false}. */
    default boolean isRepairSrcAffinityEnabled() {
        return false;
    }

    /**
     * Reorder healthy clone-source candidates to prefer the same {@code tag.location} as the repair
     * destination backend. Must be a <strong>stable</strong> reorder so the caller's existing
     * version-based ordering is preserved within each tier, and must not drop candidates so that a
     * same-AZ slot-full case still falls through to a cross-AZ source.
     * <p>
     * Open-source no-op: returns {@code healthyCandidates} unchanged.
     */
    default List<Replica> orderRepairSrcCandidates(List<Replica> healthyCandidates, long destBackendId) {
        return healthyCandidates;
    }

    /**
     * Classify which affinity tier the finally chosen source fell into, for observability.
     * Open-source no-op: returns {@link SrcAffinityResult#DISABLED}.
     */
    default SrcAffinityResult classifyRepairSrc(long chosenSrcBackendId, long destBackendId,
            List<Replica> allReplicas, List<Replica> healthyCandidates) {
        return SrcAffinityResult.DISABLED;
    }

    default ResourceGroupAffinity.AffinityDecision decideForQuery(ConnectContext context) {
        return ResourceGroupAffinity.AffinityDecision.noAffinity();
    }

    default ResourceGroupAffinity.AffinityDecision decideForQuery(
            ConnectContext context, Set<Tag> allowedTags, boolean needCheckTags) {
        return ResourceGroupAffinity.AffinityDecision.noAffinity();
    }

    default boolean hasEffectiveQueryAffinity(ResourceGroupAffinity.AffinityDecision decision) {
        return false;
    }

    /**
     * Optionally reorder query scan candidates before the existing scheduler chooses a backend.
     * This is a placement hint: callers may still apply their normal load-balancing policy after
     * this method. Implementations must not drop candidates.
     */
    default <T> List<T> applyQueryAffinity(ResourceGroupAffinity.AffinityDecision decision, List<T> candidates,
            Function<T, Tag> beTagOf) throws UserException {
        return candidates;
    }

    /**
     * Optionally reorder only candidates that are otherwise tied by the caller's load-balancing
     * key. Implementations must preserve candidates outside the tied groups.
     */
    default <T> List<T> applyQueryAffinityWithinTies(ResourceGroupAffinity.AffinityDecision decision,
            List<T> sortedCandidates, Comparator<T> tieKey, Function<T, Tag> beTagOf) throws UserException {
        return sortedCandidates;
    }

    default boolean isLoadAffinityEnabled(ConnectContext context) {
        return false;
    }

    default List<Backend> orderLoadBackends(ConnectContext context, List<Backend> candidates) throws UserException {
        return candidates;
    }

    default List<Backend> orderLoadBackends(ResourceGroupAffinity.AffinityDecision decision,
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

    default boolean hasEffectiveLoadAffinity(ResourceGroupAffinity.AffinityDecision decision) {
        return false;
    }

    default Backend chooseLoadBackendWithAffinity(ConnectContext context, List<Backend> candidates)
            throws LoadException {
        for (Backend backend : candidates) {
            if (backend.isLoadAvailable()) {
                return backend;
            }
        }
        return null;
    }

    default ResourceGroupAffinity.AffinityDecision decideForLoad(ConnectContext context) {
        return null;
    }

    default ResourceGroupAffinity.AffinityDecision forwardedLoadDecision(String effectivePreferredGroup,
            String policy) {
        return null;
    }
}

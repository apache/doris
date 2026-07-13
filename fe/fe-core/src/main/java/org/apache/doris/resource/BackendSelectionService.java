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
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/** Lifecycle facade for optional load backend selection. */
public final class BackendSelectionService {

    public static boolean isLoadSelectionEnabled(ConnectContext context) {
        if (Config.isCloudMode() || context == null) {
            return false;
        }
        return context.getLoadBackendSelectionDecision() != null
                || BackendSelectionPolicyFactory.get().isLoadSelectionEnabled(context);
    }

    public static BackendSelection.SelectionHint resolveLoadSelectionHint(ConnectContext context) {
        if (Config.isCloudMode() || context == null) {
            return null;
        }
        BackendSelection.SelectionHint recorded = context.getLoadBackendSelectionDecision();
        if (recorded != null) {
            return recorded;
        }
        BackendSelectionPolicy policy = BackendSelectionPolicyFactory.get();
        if (!policy.isLoadSelectionEnabled(context)) {
            return null;
        }
        BackendSelection.SelectionHint hint = policy.getLoadSelectionHint(context);
        context.recordLoadBackendSelectionDecision(hint);
        return hint;
    }

    /** Capture the resolved load selection intent before an asynchronous load loses its session context. */
    public static BackendSelection.SelectionHint captureLoadSelection(ConnectContext context) {
        return resolveLoadSelectionHint(context);
    }

    /** Restore a previously captured load selection intent into the execution context. */
    public static void restoreLoadSelection(ConnectContext context, BackendSelection.SelectionHint hint) {
        context.recordLoadBackendSelectionDecision(hint);
    }

    public static boolean hasLoadSelectionPreference(ConnectContext context) {
        if (Config.isCloudMode()) {
            return false;
        }
        return hasLoadSelectionPreference(resolveLoadSelectionHint(context));
    }

    public static boolean hasLoadSelectionPreference(BackendSelection.SelectionHint hint) {
        return !Config.isCloudMode() && hint != null
                && (isRequiredSelection(hint)
                        || BackendSelectionPolicyFactory.get().hasLoadSelectionPreference(hint));
    }

    /** Apply an optional load policy while preserving the caller's candidate and availability semantics. */
    public static List<Backend> orderLoadCandidates(ConnectContext context, List<Backend> candidates)
            throws UserException {
        if (Config.isCloudMode()) {
            return candidates;
        }
        BackendSelection.SelectionHint hint = resolveLoadSelectionHint(context);
        BackendSelectionPolicy policy = BackendSelectionPolicyFactory.get();
        if (isRequiredSelection(hint)) {
            return requiredLoadCandidates(policy, hint, candidates);
        }
        if (hint == null || !policy.hasLoadSelectionPreference(hint)) {
            return candidates;
        }
        List<Backend> orderedCandidates = policy.orderLoadCandidates(hint, candidates);
        validateOrderedCandidates("orderLoadCandidates", candidates, orderedCandidates);
        return orderedCandidates;
    }

    public static List<Backend> orderLoadCandidates(BackendSelection.SelectionHint hint, List<Backend> candidates)
            throws UserException {
        BackendSelectionPolicy policy = BackendSelectionPolicyFactory.get();
        if (isRequiredSelection(hint)) {
            return requiredLoadCandidates(policy, hint, candidates);
        }
        if (hint == null || !policy.hasLoadSelectionPreference(hint)) {
            return candidates;
        }
        List<Backend> orderedCandidates = policy.orderLoadCandidates(hint, candidates);
        validateOrderedCandidates("orderLoadCandidates", candidates, orderedCandidates);
        return orderedCandidates;
    }

    /** Apply query ordering while requiring the provider to preserve the exact candidate instances. */
    public static <T> List<T> orderQueryCandidates(BackendSelection.SelectionHint hint, List<T> candidates,
            Function<T, Tag> locationKey) throws UserException {
        BackendSelectionPolicy policy = BackendSelectionPolicyFactory.get();
        if (isRequiredSelection(hint)) {
            BackendSelection.CandidateSelection<T> selection =
                    policy.partitionRequiredQueryCandidates(hint, candidates, locationKey);
            return requiredCandidates("partitionRequiredQueryCandidates", hint, candidates, selection);
        }
        if (!policy.hasQuerySelectionPreference(hint)) {
            return candidates;
        }
        List<T> orderedCandidates = policy.orderQueryCandidates(hint, candidates, locationKey);
        validateOrderedCandidates("orderQueryCandidates", candidates, orderedCandidates);
        return orderedCandidates;
    }

    /**
     * Apply query selection independently within each contiguous tie group while preserving group order.
     * The input candidates must already be sorted by {@code tieComparator}, and candidates with the same
     * priority must be contiguous. The provider must preserve every candidate instance in each group.
     * Required selection is applied globally because it is a hard filter.
     */
    public static <T> List<T> orderQueryCandidatesWithinTies(BackendSelection.SelectionHint hint,
            List<T> candidates, Comparator<T> tieComparator, Function<T, Tag> tagOf) throws UserException {
        BackendSelectionPolicy policy = BackendSelectionPolicyFactory.get();
        if (isRequiredSelection(hint)) {
            BackendSelection.CandidateSelection<T> selection =
                    policy.partitionRequiredQueryCandidates(hint, candidates, tagOf);
            return requiredCandidates("partitionRequiredQueryCandidates", hint, candidates, selection);
        }
        if (!policy.hasQuerySelectionPreference(hint) || candidates.size() < 2) {
            return candidates;
        }

        List<T> result = new ArrayList<>(candidates.size());
        boolean changed = false;
        int start = 0;
        while (start < candidates.size()) {
            int end = start + 1;
            while (end < candidates.size()
                    && tieComparator.compare(candidates.get(start), candidates.get(end)) == 0) {
                end++;
            }

            List<T> originalGroup = new ArrayList<>(candidates.subList(start, end));
            List<T> providerInput = new ArrayList<>(originalGroup);
            List<T> ordered = policy.orderQueryCandidates(hint, providerInput, tagOf);
            validateOrderedCandidates("orderQueryCandidatesWithinTies", originalGroup, ordered);
            for (int i = 0; i < originalGroup.size(); i++) {
                if (ordered.get(i) != originalGroup.get(i)) {
                    changed = true;
                    break;
                }
            }
            result.addAll(ordered);
            start = end;
        }
        return changed ? result : candidates;
    }

    /** Classify the query selection outcome after the kernel has applied its candidate filters. */
    public static <T> BackendSelection.QuerySelectionResult classifyQuerySelection(
            BackendSelection.SelectionHint hint, List<T> candidates, Function<T, Tag> locationKey) {
        if (isRequiredSelection(hint)) {
            return BackendSelection.QuerySelectionResult.PREFERRED_HIT;
        }
        BackendSelectionPolicy policy = BackendSelectionPolicyFactory.get();
        if (hint == null || !policy.hasQuerySelectionPreference(hint)) {
            return BackendSelection.QuerySelectionResult.DISABLED;
        }
        return policy.classifyQuerySelection(hint, candidates, locationKey);
    }

    /** Apply repair-source ordering while requiring the provider to preserve the exact replicas. */
    public static List<Replica> orderRepairSourceCandidates(List<Replica> candidates, long destBackendId)
            throws UserException {
        List<Replica> orderedCandidates = BackendSelectionPolicyFactory.get()
                .orderRepairSourceCandidates(candidates, destBackendId);
        validateOrderedCandidates("orderRepairSourceCandidates", candidates, orderedCandidates);
        return orderedCandidates;
    }

    public static Backend chooseLoadBackend(ConnectContext context, List<Backend> candidates)
            throws LoadException {
        if (context == null) {
            return chooseFirstAvailable(candidates, Backend::isLoadAvailable);
        }
        List<Backend> orderedCandidates;
        try {
            orderedCandidates = orderLoadCandidates(context, candidates);
        } catch (UserException e) {
            throw new LoadException(e.getMessage(), e);
        }
        return chooseFirstAvailable(orderedCandidates, Backend::isLoadAvailable);
    }

    public static Backend chooseFirstPreferredLoadBackend(ConnectContext context, List<Backend> candidates,
            Predicate<Backend> available) throws UserException {
        if (!hasLoadSelectionPreference(context)) {
            return null;
        }
        BackendSelection.SelectionHint hint = resolveLoadSelectionHint(context);
        Backend selected = chooseFirstAvailable(orderLoadCandidates(hint, candidates), available);
        ensureRequiredSelectionSatisfied(hint, selected != null);
        return selected;
    }

    public static boolean isRequiredSelection(BackendSelection.SelectionHint hint) {
        return hint != null && hint.getMode() == BackendSelection.Mode.REQUIRE;
    }

    public static void ensureRequiredSelectionSatisfied(BackendSelection.SelectionHint hint, boolean satisfied)
            throws UserException {
        if (isRequiredSelection(hint) && !satisfied) {
            throw new UserException("No available candidate satisfies required backend selection key '"
                    + hint.getPreferredKey() + "'");
        }
    }

    private static Backend chooseFirstAvailable(List<Backend> candidates, Predicate<Backend> available) {
        for (Backend backend : candidates) {
            if (available.test(backend)) {
                return backend;
            }
        }
        return null;
    }

    private static List<Backend> requiredLoadCandidates(BackendSelectionPolicy policy,
            BackendSelection.SelectionHint hint, List<Backend> candidates) throws UserException {
        BackendSelection.CandidateSelection<Backend> selection =
                policy.partitionRequiredLoadCandidates(hint, candidates);
        return requiredCandidates("partitionRequiredLoadCandidates", hint, candidates, selection);
    }

    private static <T> List<T> requiredCandidates(String method, BackendSelection.SelectionHint hint,
            List<T> candidates, BackendSelection.CandidateSelection<T> selection) throws UserException {
        if (selection == null || selection.getPreferredCandidates() == null
                || selection.getFallbackCandidates() == null) {
            throw invalidCandidatePartition(method);
        }
        List<T> partitionedCandidates = new ArrayList<>(selection.getPreferredCandidates().size()
                + selection.getFallbackCandidates().size());
        partitionedCandidates.addAll(selection.getPreferredCandidates());
        partitionedCandidates.addAll(selection.getFallbackCandidates());
        validateOrderedCandidates(method, candidates, partitionedCandidates);
        if (selection.getPreferredCandidates().isEmpty()) {
            throw new UserException("No candidate satisfies required backend selection key '"
                    + hint.getPreferredKey() + "'");
        }
        return new ArrayList<>(selection.getPreferredCandidates());
    }

    private static <T> void validateOrderedCandidates(String method, List<T> candidates, List<T> orderedCandidates)
            throws UserException {
        if (orderedCandidates == null || orderedCandidates.size() != candidates.size()) {
            throw invalidCandidateOrder(method);
        }
        Map<T, Integer> remainingCandidates = new IdentityHashMap<>();
        for (T candidate : candidates) {
            remainingCandidates.merge(candidate, 1, Integer::sum);
        }
        for (T candidate : orderedCandidates) {
            Integer remaining = remainingCandidates.get(candidate);
            if (remaining == null || remaining == 0) {
                throw invalidCandidateOrder(method);
            }
            remainingCandidates.put(candidate, remaining - 1);
        }
    }

    private static UserException invalidCandidateOrder(String method) {
        if (method.startsWith("partitionRequired")) {
            return invalidCandidatePartition(method);
        }
        return new UserException("BackendSelectionPolicy." + method
                + " must preserve all candidates using the original instances");
    }

    private static UserException invalidCandidatePartition(String method) {
        return new UserException("BackendSelectionPolicy." + method
                + " must partition all candidates exactly once using the original instances");
    }

    private BackendSelectionService() {
    }
}

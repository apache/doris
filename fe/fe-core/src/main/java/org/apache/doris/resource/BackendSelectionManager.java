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
import org.apache.doris.resource.spi.BackendSelectionProvider;
import org.apache.doris.system.Backend;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;

/** Kernel facade and provider lifecycle manager for optional backend selection. */
public final class BackendSelectionManager {
    private static final Logger LOG = LogManager.getLogger(BackendSelectionManager.class);

    private static final BackendSelectionProvider DEFAULT_PROVIDER = new BackendSelectionProvider() {
    };

    private static volatile BackendSelectionProvider provider;

    private static BackendSelectionProvider provider() {
        BackendSelectionProvider current = provider;
        if (current == null) {
            synchronized (BackendSelectionManager.class) {
                current = provider;
                if (current == null) {
                    current = loadProvider(BackendSelectionManager.class.getClassLoader());
                    provider = current;
                }
            }
        }
        return current;
    }

    @VisibleForTesting
    static BackendSelectionProvider loadProvider(ClassLoader classLoader) {
        ServiceLoader<BackendSelectionProvider> loader =
                ServiceLoader.load(BackendSelectionProvider.class, classLoader);
        Iterator<BackendSelectionProvider> it = loader.iterator();
        if (it.hasNext()) {
            BackendSelectionProvider loadedProvider = it.next();
            if (it.hasNext()) {
                BackendSelectionProvider duplicateProvider = it.next();
                throw new IllegalStateException("Multiple BackendSelectionProvider implementations found: "
                        + loadedProvider.getClass().getName() + ", "
                        + duplicateProvider.getClass().getName());
            }
            LOG.info("Loaded BackendSelectionProvider implementation: {}",
                    loadedProvider.getClass().getName());
            return loadedProvider;
        }
        LOG.info("No BackendSelectionProvider implementation found, using no-op backend selection provider");
        return DEFAULT_PROVIDER;
    }

    @VisibleForTesting
    public static synchronized void setProviderForTest(BackendSelectionProvider testProvider) {
        provider = Objects.requireNonNull(testProvider, "testProvider must not be null");
    }

    @VisibleForTesting
    public static synchronized void resetProviderForTest() {
        provider = null;
    }

    public static boolean supportsRequiredSelection() {
        return provider().supportsRequiredSelection();
    }

    public static BackendSelection.SelectionHint getQuerySelectionHint(ConnectContext context) {
        return provider().getQuerySelectionHint(context);
    }

    public static BackendSelection.SelectionHint getForwardedLoadSelectionHint(String preferredKey, String mode) {
        return provider().getForwardedLoadSelectionHint(preferredKey, mode);
    }

    public static boolean isRepairSourceSelectionEnabled() {
        return provider().isRepairSourceSelectionEnabled();
    }

    public static BackendSelectionProvider.RepairSourceSelectionResult classifyRepairSource(
            long chosenSrcBackendId, long destBackendId, List<Replica> allReplicas, List<Replica> healthyCandidates) {
        return provider().classifyRepairSource(
                chosenSrcBackendId, destBackendId, allReplicas, healthyCandidates);
    }

    public static boolean isLoadSelectionEnabled(ConnectContext context) {
        if (Config.isCloudMode() || context == null) {
            return false;
        }
        return context.getLoadBackendSelectionDecision() != null
                || context.getLoadBackendSelectionHint() != null
                || provider().isLoadSelectionEnabled(context);
    }

    public static BackendSelection.SelectionHint resolveLoadSelectionHint(ConnectContext context) {
        if (Config.isCloudMode() || context == null) {
            return null;
        }
        BackendSelection.SelectionHint recorded = context.getLoadBackendSelectionDecision();
        if (recorded != null) {
            return recorded;
        }
        BackendSelection.SelectionHint persisted = context.getLoadBackendSelectionHint();
        if (persisted != null) {
            context.recordLoadBackendSelectionDecision(persisted);
            return persisted;
        }
        BackendSelectionProvider policy = provider();
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
        context.recordLoadBackendSelectionHint(hint);
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
                        || provider().hasLoadSelectionPreference(hint));
    }

    /** Apply an optional load policy while preserving the caller's candidate and availability semantics. */
    public static List<Backend> orderLoadCandidates(ConnectContext context, List<Backend> candidates)
            throws UserException {
        if (Config.isCloudMode()) {
            return candidates;
        }
        BackendSelection.SelectionHint hint = resolveLoadSelectionHint(context);
        BackendSelectionProvider policy = provider();
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
        BackendSelectionProvider policy = provider();
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

    /** Apply preferred load selection as availability-aware candidate tiers. */
    public static BackendSelection.CandidateSelection<Backend> partitionPreferredLoadCandidates(
            BackendSelection.SelectionHint hint, List<Backend> candidates) throws UserException {
        BackendSelectionProvider policy = provider();
        if (hint == null || hint.getMode() != BackendSelection.Mode.PREFER
                || !policy.hasLoadSelectionPreference(hint)) {
            return null;
        }
        BackendSelection.CandidateSelection<Backend> selection =
                policy.partitionPreferredLoadCandidates(hint, candidates);
        if (selection == null || selection.getPreferredCandidates() == null
                || selection.getFallbackCandidates() == null) {
            return null;
        }
        validateCandidatePartition("partitionPreferredLoadCandidates", candidates, selection);
        return selection;
    }

    /** Apply query ordering while requiring the provider to preserve the exact candidate instances. */
    public static <T> List<T> orderQueryCandidates(BackendSelection.SelectionHint hint, List<T> candidates,
            Function<T, Tag> locationKey) throws UserException {
        BackendSelectionProvider policy = provider();
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

    /** Apply strict preferred query selection when the provider supports candidate partitioning. */
    public static <T> BackendSelection.CandidateSelection<T> partitionPreferredQueryCandidates(
            BackendSelection.SelectionHint hint, List<T> candidates, Function<T, Tag> locationKey)
            throws UserException {
        BackendSelectionProvider policy = provider();
        if (hint == null || hint.getMode() != BackendSelection.Mode.PREFER
                || !policy.hasQuerySelectionPreference(hint)) {
            return null;
        }
        BackendSelection.CandidateSelection<T> selection =
                policy.partitionPreferredQueryCandidates(hint, candidates, locationKey);
        if (selection == null || selection.getPreferredCandidates() == null
                || selection.getFallbackCandidates() == null) {
            return null;
        }
        validateCandidatePartition("partitionPreferredQueryCandidates", candidates, selection);
        return selection;
    }

    /**
     * Apply query selection independently within each contiguous tie group while preserving group order.
     * The input candidates must already be sorted by {@code tieComparator}, and candidates with the same
     * priority must be contiguous. The provider must preserve every candidate instance in each group.
     * Required selection is applied globally because it is a hard filter.
     */
    public static <T> List<T> orderQueryCandidatesWithinTies(BackendSelection.SelectionHint hint,
            List<T> candidates, Comparator<T> tieComparator, Function<T, Tag> tagOf) throws UserException {
        BackendSelectionProvider policy = provider();
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
        BackendSelectionProvider policy = provider();
        if (hint == null || !policy.hasQuerySelectionPreference(hint)) {
            return BackendSelection.QuerySelectionResult.DISABLED;
        }
        return policy.classifyQuerySelection(hint, candidates, locationKey);
    }

    /** Apply repair-source ordering while requiring the provider to preserve the exact replicas. */
    public static List<Replica> orderRepairSourceCandidates(List<Replica> candidates, long destBackendId)
            throws UserException {
        List<Replica> orderedCandidates = provider()
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

    private static List<Backend> requiredLoadCandidates(BackendSelectionProvider policy,
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

    private static <T> void validateCandidatePartition(String method, List<T> candidates,
            BackendSelection.CandidateSelection<T> selection) throws UserException {
        List<T> partitionedCandidates = new ArrayList<>(selection.getPreferredCandidates().size()
                + selection.getFallbackCandidates().size());
        partitionedCandidates.addAll(selection.getPreferredCandidates());
        partitionedCandidates.addAll(selection.getFallbackCandidates());
        validateOrderedCandidates(method, candidates, partitionedCandidates);
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
        return new UserException("BackendSelectionProvider." + method
                + " must preserve all candidates using the original instances");
    }

    private static UserException invalidCandidatePartition(String method) {
        return new UserException("BackendSelectionProvider." + method
                + " must partition all candidates exactly once using the original instances");
    }

    private BackendSelectionManager() {
    }
}

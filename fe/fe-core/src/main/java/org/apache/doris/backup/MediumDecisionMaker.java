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

package org.apache.doris.backup;

import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TStorageMedium;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unified medium decision maker for restore operations.
 *
 * This class centralizes all storage medium selection logic, replacing scattered
 * decision-making across multiple classes (OlapTable, RestoreJob, etc.).
 *
 * Key responsibilities:
 * 1. Determine target storage medium based on restore configuration
 * 2. Handle adaptive mode downgrade when preferred medium is unavailable
 * 3. Implement "avoid migration" strategy for atomic restore
 * 4. Provide complete decision traceability through structured results
 */
public class MediumDecisionMaker {
    private static final Logger LOG = LogManager.getLogger(MediumDecisionMaker.class);

    private final String storageMedium;              // hdd/ssd/same_with_upstream
    private final String mediumAllocationMode;     // strict/adaptive

    /**
     * Decision result containing the final medium and decision metadata.
     */
    public static class MediumDecision {
        private TStorageMedium finalMedium;          // Final medium to use
        private boolean wasDowngraded;               // Whether adaptive downgrade occurred
        private TStorageMedium originalMedium;       // Original preferred medium
        private String reason;                       // Decision reason for logging

        public MediumDecision(TStorageMedium finalMedium, TStorageMedium originalMedium,
                              boolean wasDowngraded, String reason) {
            this.finalMedium = finalMedium;
            this.originalMedium = originalMedium;
            this.wasDowngraded = wasDowngraded;
            this.reason = reason;
        }

        public TStorageMedium getFinalMedium() {
            return finalMedium;
        }

        public boolean wasDowngraded() {
            return wasDowngraded;
        }

        public TStorageMedium getOriginalMedium() {
            return originalMedium;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return String.format("MediumDecision[final=%s, original=%s, downgraded=%s, reason='%s']",
                    finalMedium, originalMedium, wasDowngraded, reason);
        }
    }

    public MediumDecisionMaker(String storageMedium, String mediumAllocationMode) {
        this.storageMedium = storageMedium;
        this.mediumAllocationMode = mediumAllocationMode;
    }

    /**
     * Scenario 1: Decide medium for new partition (non-atomic restore or new table).
     *
     * Logic:
     * 1. Determine original preferred medium from config
     * 2. Try to allocate backends with preferred medium
     * 3. If adaptive mode and preferred unavailable, downgrade to available medium
     *
     * @param partitionName Partition name (for logging)
     * @param upstreamDataProperty DataProperty from upstream partition
     * @param replicaAlloc Replica allocation requirement
     * @return Decision result with final medium and metadata
     * @throws DdlException If no medium is available (strict mode)
     */
    public MediumDecision decideForNewPartition(
            String partitionName,
            DataProperty upstreamDataProperty,
            ReplicaAllocation replicaAlloc) throws DdlException {

        // Step 1: Determine original preferred medium
        TStorageMedium preferredMedium;
        String source;

        if (isSameWithUpstream()) {
            preferredMedium = upstreamDataProperty.getStorageMedium();
            source = "inherited from upstream";
        } else {
            preferredMedium = getTargetStorageMedium();
            source = "user specified: " + storageMedium;
        }

        // Step 2: Try to allocate with preferred medium
        DataProperty.MediumAllocationMode mode = getTargetAllocationMode();
        Map<Tag, Integer> nextIndexes = new HashMap<>();

        Pair<Map<Tag, List<Long>>, TStorageMedium> result =
                Env.getCurrentSystemInfo().selectBackendIdsForReplicaCreation(
                        replicaAlloc, nextIndexes, preferredMedium, mode, false);

        TStorageMedium actualMedium = result.second;
        boolean downgraded = (actualMedium != preferredMedium);

        // Step 3: Build decision result
        String reason = source;
        if (downgraded) {
            reason += String.format(", downgraded from %s to %s (preferred medium unavailable)",
                    preferredMedium, actualMedium);
        }

        MediumDecision decision = new MediumDecision(actualMedium, preferredMedium, downgraded, reason);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Decided medium for new partition {}: {} (mode: {})",
                    partitionName, decision, mode);
        }

        return decision;
    }

    /**
     * Scenario 2: Decide medium for atomic restore.
     *
     * Core principle: Prefer local medium to avoid data migration, unless:
     * 1. Local medium is truly unavailable (adaptive mode)
     * 2. User explicitly specified different medium (non same_with_upstream)
     *
     * Logic:
     * 1. If same_with_upstream + adaptive:
     *    - Check if local medium is available
     *    - Available → use local (avoid migration)
     *    - Unavailable → use remote/configured (allow migration for availability)
     * 2. If same_with_upstream + strict:
     *    - Force use local medium (avoid migration)
     * 3. If hdd/ssd (explicit):
     *    - Use configured medium (allow migration, respect user config)
     *
     * @param partitionName Partition name (for logging)
     * @param upstreamDataProperty DataProperty from upstream partition
     * @param localDataProperty DataProperty from local partition
     * @param replicaAlloc Replica allocation requirement
     * @return Decision result with final medium and metadata
     * @throws DdlException If no medium is available (strict mode)
     */
    public MediumDecision decideForAtomicRestore(
            String partitionName,
            DataProperty upstreamDataProperty,
            DataProperty localDataProperty,
            ReplicaAllocation replicaAlloc) throws DdlException {

        TStorageMedium localMedium = localDataProperty.getStorageMedium();
        TStorageMedium upstreamMedium = upstreamDataProperty.getStorageMedium();

        // Determine original preferred medium from config
        TStorageMedium configuredMedium;
        if (isSameWithUpstream()) {
            configuredMedium = upstreamMedium;
        } else {
            configuredMedium = getTargetStorageMedium();
        }

        DataProperty.MediumAllocationMode mode = getTargetAllocationMode();

        // Strategy decision based on config
        if (isSameWithUpstream()) {
            // same_with_upstream: prefer local medium to avoid migration
            if (mode.isAdaptive()) {
                // Adaptive: prefer local, allow downgrade if unavailable
                return decidePreferLocalMedium(partitionName, localMedium,
                        configuredMedium, replicaAlloc, mode);
            } else {
                // Strict: must use local medium (to avoid migration), check availability
                return decideWithLocalMediumStrict(partitionName, localMedium,
                        configuredMedium, replicaAlloc, mode);
            }
        } else {
            // Explicit hdd/ssd: respect user config (allow migration)
            return decideWithConfiguredMedium(partitionName, configuredMedium,
                    localMedium, replicaAlloc, mode);
        }
    }

    /**
     * Scenario 3: Decide table-level medium.
     *
     * Table-level medium is used as the default for future partitions.
     *
     * @param upstreamTable Table from backup meta
     * @return Final table-level medium
     */
    public TStorageMedium decideForTableLevel(OlapTable upstreamTable) {
        if (isSameWithUpstream()) {
            // Inherit from upstream
            TStorageMedium medium = upstreamTable.getStorageMedium();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Table {} preserving upstream table-level medium {} (same_with_upstream)",
                        upstreamTable.getName(), medium);
            }
            return medium;
        } else {
            // Use configured medium
            TStorageMedium medium = getTargetStorageMedium();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Table {} using configured table-level medium {} (storage_medium={})",
                        upstreamTable.getName(), medium, storageMedium);
            }
            return medium;
        }
    }

    /**
     * Strategy: Use local medium in strict mode (for atomic restore with same_with_upstream).
     * Must check availability and throw exception if local medium is unavailable.
     */
    private MediumDecision decideWithLocalMediumStrict(
            String partitionName,
            TStorageMedium localMedium,
            TStorageMedium configuredMedium,
            ReplicaAllocation replicaAlloc,
            DataProperty.MediumAllocationMode mode) throws DdlException {

        // In strict mode, we must verify that local medium is available
        // If unavailable, throw exception (no fallback allowed in strict mode)
        Map<Tag, Integer> nextIndexes = new HashMap<>();
        Pair<Map<Tag, List<Long>>, TStorageMedium> result =
                Env.getCurrentSystemInfo().selectBackendIdsForReplicaCreation(
                        replicaAlloc, nextIndexes, localMedium, mode, false);

        TStorageMedium actualMedium = result.second;
        if (actualMedium != localMedium) {
            // This should not happen in strict mode, but if it does, it's an error
            throw new DdlException(String.format(
                    "Failed to allocate local medium %s for partition %s in strict mode. "
                    + "System attempted to use %s instead, but strict mode does not allow fallback.",
                    localMedium, partitionName, actualMedium));
        }

        String reason = String.format("atomic restore with strict mode, using local medium "
                        + "(avoiding migration, configured=%s, verified available)", configuredMedium);
        MediumDecision decision = new MediumDecision(localMedium, configuredMedium,
                localMedium != configuredMedium, reason);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Decided medium for atomic restore partition {}: {} (strategy: local strict, verified)",
                    partitionName, decision);
        }
        return decision;
    }

    /**
     * Strategy: Prefer local medium to avoid migration (adaptive mode).
     */
    private MediumDecision decidePreferLocalMedium(
            String partitionName,
            TStorageMedium localMedium,
            TStorageMedium configuredMedium,
            ReplicaAllocation replicaAlloc,
            DataProperty.MediumAllocationMode mode) throws DdlException {

        try {
            // Check if local medium is available
            Map<Tag, Integer> nextIndexes = new HashMap<>();
            Pair<Map<Tag, List<Long>>, TStorageMedium> testResult =
                    Env.getCurrentSystemInfo().selectBackendIdsForReplicaCreation(
                            replicaAlloc, nextIndexes, localMedium, mode, true /* check only */);

            if (testResult.second == localMedium) {
                // Local medium available → use it (avoid migration)
                String reason = String.format("atomic restore with adaptive mode, prefer local medium "
                                + "(avoiding migration, configured=%s)", configuredMedium);
                MediumDecision decision = new MediumDecision(localMedium, configuredMedium,
                        localMedium != configuredMedium, reason);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Decided medium for atomic restore partition {}: {} "
                                    + "(strategy: prefer local, available)",
                            partitionName, decision);
                }
                return decision;
            } else {
                // Local medium unavailable → use downgraded medium (allow migration)
                TStorageMedium downgradedMedium = testResult.second;
                String reason = String.format("atomic restore with adaptive mode, local medium %s unavailable, "
                                + "using %s (migration needed, configured=%s)",
                        localMedium, downgradedMedium, configuredMedium);
                MediumDecision decision = new MediumDecision(downgradedMedium, configuredMedium,
                        true, reason);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Decided medium for atomic restore partition {}: {} "
                                    + "(strategy: prefer local, but unavailable)",
                            partitionName, decision);
                }
                return decision;
            }
        } catch (DdlException e) {
            // Check failed, use conservative strategy: local medium
            String reason = String.format("atomic restore, check failed, using local medium %s "
                            + "(conservative strategy, configured=%s): %s",
                    localMedium, configuredMedium, e.getMessage());
            MediumDecision decision = new MediumDecision(localMedium, configuredMedium,
                    localMedium != configuredMedium, reason);
            LOG.warn("Decided medium for atomic restore partition {}: {} (strategy: conservative due to error)",
                    partitionName, decision);
            return decision;
        }
    }

    /**
     * Strategy: Use configured medium (explicit hdd/ssd, allow migration).
     */
    private MediumDecision decideWithConfiguredMedium(
            String partitionName,
            TStorageMedium configuredMedium,
            TStorageMedium localMedium,
            ReplicaAllocation replicaAlloc,
            DataProperty.MediumAllocationMode mode) throws DdlException {

        Map<Tag, Integer> nextIndexes = new HashMap<>();
        Pair<Map<Tag, List<Long>>, TStorageMedium> result =
                Env.getCurrentSystemInfo().selectBackendIdsForReplicaCreation(
                        replicaAlloc, nextIndexes, configuredMedium, mode, false);

        TStorageMedium actualMedium = result.second;
        boolean downgraded = (actualMedium != configuredMedium);

        String reason = String.format("atomic restore with explicit medium, using configured medium "
                        + "(migration allowed if needed, local=%s, configured=%s)",
                localMedium, configuredMedium);
        if (downgraded) {
            reason += String.format(", downgraded to %s (configured medium unavailable)", actualMedium);
        }

        MediumDecision decision = new MediumDecision(actualMedium, configuredMedium, downgraded, reason);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Decided medium for atomic restore partition {}: {} (strategy: use configured, mode={})",
                    partitionName, decision, mode);
        }
        return decision;
    }

    // Helper methods

    private boolean isSameWithUpstream() {
        return RestoreCommand.STORAGE_MEDIUM_SAME_WITH_UPSTREAM.equals(storageMedium);
    }

    private TStorageMedium getTargetStorageMedium() {
        if (RestoreCommand.STORAGE_MEDIUM_HDD.equals(storageMedium)) {
            return TStorageMedium.HDD;
        } else if (RestoreCommand.STORAGE_MEDIUM_SSD.equals(storageMedium)) {
            return TStorageMedium.SSD;
        }
        throw new IllegalStateException("getTargetStorageMedium() should not be called "
                + "when storage_medium is 'same_with_upstream'");
    }

    private DataProperty.MediumAllocationMode getTargetAllocationMode() {
        if (RestoreCommand.MEDIUM_ALLOCATION_MODE_STRICT.equals(mediumAllocationMode)) {
            return DataProperty.MediumAllocationMode.STRICT;
        } else if (RestoreCommand.MEDIUM_ALLOCATION_MODE_ADAPTIVE.equals(mediumAllocationMode)) {
            return DataProperty.MediumAllocationMode.ADAPTIVE;
        }
        // Default to strict
        return DataProperty.MediumAllocationMode.STRICT;
    }
}

